from datetime import datetime, timezone

import great_expectations as gx
from pyspark.sql.functions import col, concat_ws, current_timestamp, lit, when

from data_generator.logger import get_logger
from src.quality.contracts.settlements_contract import SETTLEMENTS_CONTRACT
from src.quality.quarantine.quarantine_writer import write_quarantine_dataframe
from src.quality.results.result_writer import write_result_summary
from src.utils.spark_session import create_spark_session

logger = get_logger(__name__)


def load_silver_dataframe(spark):
    return (
        spark.read
        .format("delta")
        .load(SETTLEMENTS_CONTRACT["silver_path"])
    )


def validate_required_columns(dataframe) -> None:
    actual_columns = set(dataframe.columns)
    required_columns = set(SETTLEMENTS_CONTRACT["required_columns"])
    missing_columns = sorted(required_columns - actual_columns)

    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")


def get_gx_context():
    return gx.get_context(mode="file", project_root_dir=".")


def get_batch_definition(context):
    datasource_name = "sentinelpay_spark"
    asset_name = "silver_settlements"
    batch_definition_name = "whole_dataframe"

    try:
        datasource = context.data_sources.get(datasource_name)
    except Exception:
        datasource = context.data_sources.add_spark(name=datasource_name)

    try:
        asset = datasource.get_asset(asset_name)
    except Exception:
        asset = datasource.add_dataframe_asset(name=asset_name)

    existing_batch_definitions = [
        batch_definition.name
        for batch_definition in asset.batch_definitions
    ]

    if batch_definition_name in existing_batch_definitions:
        return asset.get_batch_definition(batch_definition_name)

    return asset.add_batch_definition_whole_dataframe(batch_definition_name)


def build_expectation_suite():
    return gx.ExpectationSuite(
        name="settlements_suite",
        expectations=[
            gx.expectations.ExpectColumnValuesToNotBeNull(column="settlement_id"),
            gx.expectations.ExpectColumnValuesToNotBeNull(column="merchant_id"),
            gx.expectations.ExpectColumnValuesToNotBeNull(column="settlement_amount"),
            gx.expectations.ExpectColumnValuesToNotBeNull(column="settlement_date"),
            gx.expectations.ExpectColumnValuesToNotBeNull(column="settlement_status"),
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="settlement_amount",
                min_value=SETTLEMENTS_CONTRACT["amount_range"]["min_value"],
                max_value=SETTLEMENTS_CONTRACT["amount_range"]["max_value"],
            ),
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="settlement_status",
                value_set=SETTLEMENTS_CONTRACT["allowed_settlement_statuses"],
            ),
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="merchant_status",
                value_set=SETTLEMENTS_CONTRACT["allowed_merchant_statuses"],
            ),
            gx.expectations.ExpectColumnValuesToBeUnique(
                column=SETTLEMENTS_CONTRACT["business_key"],
            ),
        ],
    )


def run_expectation_check(dataframe):
    context = get_gx_context()
    batch_definition = get_batch_definition(context)

    suite = context.suites.add_or_update(build_expectation_suite())

    validation_definition = gx.ValidationDefinition(
        name="settlements_validation_definition",
        data=batch_definition,
        suite=suite,
    )
    validation_definition = context.validation_definitions.add_or_update(
        validation_definition
    )

    return validation_definition.run(
        batch_parameters={"dataframe": dataframe},
        result_format="COMPLETE",
    )


def build_result_summary(validation_result):
    total_expectations = len(validation_result.results)
    passed_expectations = sum(
        1 for result in validation_result.results if result.success
    )
    failed_expectations = total_expectations - passed_expectations

    return {
        "dataset_name": SETTLEMENTS_CONTRACT["dataset_name"],
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "validation_status": "PASSED" if validation_result.success else "FAILED",
        "total_expectations": total_expectations,
        "passed_expectations": passed_expectations,
        "failed_expectations": failed_expectations,
    }


def build_failed_rows_dataframe(dataframe):
    amount_min = SETTLEMENTS_CONTRACT["amount_range"]["min_value"]
    amount_max = SETTLEMENTS_CONTRACT["amount_range"]["max_value"]

    failed_reason = concat_ws(
        ", ",
        when(col("settlement_id").isNull(), lit("settlement_id_null")),
        when(col("merchant_id").isNull(), lit("merchant_id_null")),
        when(col("settlement_amount").isNull(), lit("settlement_amount_null")),
        when(col("settlement_date").isNull(), lit("settlement_date_null")),
        when(col("settlement_status").isNull(), lit("settlement_status_null")),
        when(
            (col("settlement_amount") < lit(amount_min))
            | (col("settlement_amount") > lit(amount_max)),
            lit("invalid_settlement_amount_range"),
        ),
        when(
            ~col("settlement_status").isin(
                SETTLEMENTS_CONTRACT["allowed_settlement_statuses"]
            ),
            lit("invalid_settlement_status"),
        ),
        when(
            ~col("merchant_status").isin(
                SETTLEMENTS_CONTRACT["allowed_merchant_statuses"]
            ),
            lit("invalid_merchant_status"),
        ),
    )

    return (
        dataframe
        .withColumn("failed_reason", failed_reason)
        .withColumn("failed_at", current_timestamp())
        .withColumn("dataset_name", lit(SETTLEMENTS_CONTRACT["dataset_name"]))
        .filter(col("failed_reason") != "")
    )


def build_duplicate_rows_dataframe(dataframe):
    business_key = SETTLEMENTS_CONTRACT["business_key"]

    duplicate_keys_df = (
        dataframe
        .groupBy(business_key)
        .count()
        .filter(col("count") > 1)
        .select(business_key)
    )

    return (
        dataframe
        .join(duplicate_keys_df, on=business_key, how="inner")
        .withColumn("failed_reason", lit("duplicate_settlement_id"))
        .withColumn("failed_at", current_timestamp())
        .withColumn("dataset_name", lit(SETTLEMENTS_CONTRACT["dataset_name"]))
    )


def main() -> None:
    logger.info("=" * 60)
    logger.info("Starting settlements quality validation...")
    logger.info("=" * 60)

    spark = create_spark_session("SentinelPay Quality Settlements")

    try:
        silver_df = load_silver_dataframe(spark)
        validate_required_columns(silver_df)

        validation_result = run_expectation_check(silver_df)
        summary = build_result_summary(validation_result)

        failed_df = build_failed_rows_dataframe(silver_df)
        duplicate_failed_df = build_duplicate_rows_dataframe(silver_df)

        failed_df = (
            failed_df
            .unionByName(duplicate_failed_df, allowMissingColumns=True)
            .dropDuplicates(["settlement_id", "failed_reason"])
        )

        write_quarantine_dataframe(
            dataframe=failed_df,
            output_path=SETTLEMENTS_CONTRACT["quarantine_path"],
        )

        write_result_summary(
            spark=spark,
            summary=summary,
            output_path=SETTLEMENTS_CONTRACT["result_path"],
        )

        logger.info(f"Validation status: {summary['validation_status']}")
        logger.info(f"Failed rows written: {failed_df.count()}")

    except Exception:
        logger.exception("settlements quality validation failed.")
        raise

    finally:
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()

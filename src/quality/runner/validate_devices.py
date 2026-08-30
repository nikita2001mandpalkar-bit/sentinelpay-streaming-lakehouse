from datetime import datetime, timezone

import great_expectations as gx
from pyspark.sql.functions import col, concat_ws, current_timestamp, lit, when

from data_generator.logger import get_logger
from src.quality.contracts.devices_contract import DEVICES_CONTRACT
from src.quality.quarantine.quarantine_writer import write_quarantine_dataframe
from src.quality.results.result_writer import write_result_summary
from src.utils.spark_session import create_spark_session

logger = get_logger(__name__)


def load_silver_dataframe(spark):
    return (
        spark.read
        .format("delta")
        .load(DEVICES_CONTRACT["silver_path"])
    )


def validate_required_columns(dataframe) -> None:
    actual_columns = set(dataframe.columns)
    required_columns = set(DEVICES_CONTRACT["required_columns"])
    missing_columns = sorted(required_columns - actual_columns)

    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")


def get_gx_context():
    return gx.get_context(mode="file", project_root_dir=".")


def get_batch_definition(context):
    datasource_name = "sentinelpay_spark"
    asset_name = "silver_devices"
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
        name="devices_suite",
        expectations=[
            gx.expectations.ExpectColumnValuesToNotBeNull(column="device_id"),
            gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id"),
            gx.expectations.ExpectColumnValuesToNotBeNull(column="device_type"),
            gx.expectations.ExpectColumnValuesToNotBeNull(column="device_os"),
            gx.expectations.ExpectColumnValuesToNotBeNull(column="registered_at"),
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="device_type",
                value_set=DEVICES_CONTRACT["allowed_device_types"],
            ),
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="device_os",
                value_set=DEVICES_CONTRACT["allowed_device_os"],
            ),
            gx.expectations.ExpectColumnValuesToBeUnique(
                column=DEVICES_CONTRACT["business_key"],
            ),
        ],
    )


def run_expectation_check(dataframe):
    context = get_gx_context()
    batch_definition = get_batch_definition(context)

    suite = context.suites.add_or_update(build_expectation_suite())

    validation_definition = gx.ValidationDefinition(
        name="devices_validation_definition",
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
        "dataset_name": DEVICES_CONTRACT["dataset_name"],
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "validation_status": "PASSED" if validation_result.success else "FAILED",
        "total_expectations": total_expectations,
        "passed_expectations": passed_expectations,
        "failed_expectations": failed_expectations,
    }


def build_failed_rows_dataframe(dataframe):
    failed_reason = concat_ws(
        ", ",
        when(col("device_id").isNull(), lit("device_id_null")),
        when(col("customer_id").isNull(), lit("customer_id_null")),
        when(col("device_type").isNull(), lit("device_type_null")),
        when(col("device_os").isNull(), lit("device_os_null")),
        when(col("registered_at").isNull(), lit("registered_at_null")),
        when(
            ~col("device_type").isin(DEVICES_CONTRACT["allowed_device_types"]),
            lit("invalid_device_type"),
        ),
        when(
            ~col("device_os").isin(DEVICES_CONTRACT["allowed_device_os"]),
            lit("invalid_device_os"),
        ),
    )

    return (
        dataframe
        .withColumn("failed_reason", failed_reason)
        .withColumn("failed_at", current_timestamp())
        .withColumn("dataset_name", lit(DEVICES_CONTRACT["dataset_name"]))
        .filter(col("failed_reason") != "")
    )


def build_duplicate_rows_dataframe(dataframe):
    business_key = DEVICES_CONTRACT["business_key"]

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
        .withColumn("failed_reason", lit("duplicate_device_id"))
        .withColumn("failed_at", current_timestamp())
        .withColumn("dataset_name", lit(DEVICES_CONTRACT["dataset_name"]))
    )


def main() -> None:
    logger.info("=" * 60)
    logger.info("Starting devices quality validation...")
    logger.info("=" * 60)

    spark = create_spark_session("SentinelPay Quality Devices")

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
            .dropDuplicates(["device_id", "failed_reason"])
        )

        write_quarantine_dataframe(
            dataframe=failed_df,
            output_path=DEVICES_CONTRACT["quarantine_path"],
        )

        write_result_summary(
            spark=spark,
            summary=summary,
            output_path=DEVICES_CONTRACT["result_path"],
        )

        logger.info(f"Validation status: {summary['validation_status']}")
        logger.info(f"Failed rows written: {failed_df.count()}")

    except Exception:
        logger.exception("devices quality validation failed.")
        raise

    finally:
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()

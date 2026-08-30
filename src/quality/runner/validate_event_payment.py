"""
Great Expectations runner for Silver event_payment dataset.
"""

from datetime import datetime, timezone
from pyspark.sql.functions import col,concat_ws,current_timestamp,lit,when
import great_expectations as gx

from data_generator.logger import get_logger
from src.quality.contracts.event_payment_contract import (
    EVENT_PAYMENT_CONTRACT,
)
from src.quality.results.result_writer import write_result_summary
from src.utils.spark_session import create_spark_session
from src.quality.quarantine.quarantine_writer import write_quarantine_dataframe

logger = get_logger(__name__)


def load_silver_dataframe(spark):
    """
    Read the Silver Delta dataset for event_payment.
    """

    return (
        spark.read
        .format("delta")
        .load(EVENT_PAYMENT_CONTRACT["silver_path"])
    )


def validate_required_columns(dataframe) -> None:
    """
    Ensure all expected columns exist before validation starts.
    """

    actual_columns = set(dataframe.columns)
    required_columns = set(
        EVENT_PAYMENT_CONTRACT["required_columns"]
    )

    missing_columns = sorted(
        required_columns - actual_columns
    )

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {missing_columns}"
        )


def get_gx_context():
    """
    Create or load a persistent Great Expectations
    file context in the project root.
    """

    return gx.get_context(
        mode="file",
        project_root_dir=".",
    )


def get_batch_definition(context):
    """
    Create or retrieve Spark datasource, dataframe asset,
    and whole-dataframe batch definition.
    """

    datasource_name = "sentinelpay_spark"
    asset_name = "silver_event_payment"
    batch_definition_name = "whole_dataframe"

    try:
        datasource = context.data_sources.get(
            datasource_name
        )
    except Exception:
        datasource = context.data_sources.add_spark(
            name=datasource_name
        )

    try:
        asset = datasource.get_asset(asset_name)
    except Exception:
        asset = datasource.add_dataframe_asset(
            name=asset_name
        )

    try:
        batch_definition = asset.get_batch_definition(
            batch_definition_name
        )
    except Exception:
        batch_definition = (
            asset.add_batch_definition_whole_dataframe(
                batch_definition_name
            )
        )

    return batch_definition


def build_expectation_suite():
    """
    Build the expectation suite from the dataset contract.
    """

    suite = gx.ExpectationSuite(
        name="event_payment_suite",
        expectations=[
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="transaction_id"
            ),
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="wallet_id"
            ),
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="merchant_id"
            ),
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="event_timestamp"
            ),
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="amount",
                min_value=EVENT_PAYMENT_CONTRACT[
                    "amount_range"
                ]["min_value"],
                max_value=EVENT_PAYMENT_CONTRACT[
                    "amount_range"
                ]["max_value"],
            ),
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="currency",
                value_set=EVENT_PAYMENT_CONTRACT[
                    "allowed_currencies"
                ],
            ),
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="transaction_status",
                value_set=EVENT_PAYMENT_CONTRACT[
                    "allowed_statuses"
                ],
            ),
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="payment_method",
                value_set=EVENT_PAYMENT_CONTRACT[
                    "allowed_payment_methods"
                ],
            ),
            gx.expectations.ExpectColumnValuesToBeUnique(
                column=EVENT_PAYMENT_CONTRACT[
                    "business_key"
                ]
            ),
        ],
    )

    return suite


def run_expectation_check(dataframe):
    """
    Run Great Expectations validation on the Spark dataframe.
    """

    context = get_gx_context()

    batch_definition = get_batch_definition(context)

    suite = build_expectation_suite()
    suite = context.suites.add_or_update(suite)

    validation_definition = gx.ValidationDefinition(
        name="event_payment_validation_definition",
        data=batch_definition,
        suite=suite,
    )
    validation_definition = (
        context.validation_definitions.add_or_update(
            validation_definition
        )
    )

    validation_result = validation_definition.run(
        batch_parameters={"dataframe": dataframe},
        result_format="COMPLETE",
    )

    return validation_result


def build_result_summary(validation_result):
    """
    Build a validation summary dictionary.
    """

    total_expectations = len(validation_result.results)
    passed_expectations = sum(
        1
        for result in validation_result.results
        if result.success
    )
    failed_expectations = (
        total_expectations - passed_expectations
    )

    return {
        "dataset_name":
            EVENT_PAYMENT_CONTRACT["dataset_name"],

        "run_timestamp":
            datetime.now(
                timezone.utc
            ).isoformat(),

        "validation_status":
            "PASSED"
            if validation_result.success
            else "FAILED",

        "total_expectations":
            total_expectations,

        "passed_expectations":
            passed_expectations,

        "failed_expectations":
            failed_expectations,
    }

def build_failed_rows_dataframe(dataframe):
    amount_min=EVENT_PAYMENT_CONTRACT["amount_range"]["min_value"]
    amount_max=EVENT_PAYMENT_CONTRACT["amount_range"]["max_value"]

    failed_reason=concat_ws(
        ", ",
        when(
            col("transaction_id").isNull(),
            lit("transaction_id_null"),
        ),
        when(
            col("wallet_id").isNull(),
            lit("wallet_id_null"),
        ),
        when(
            col("merchant_id").isNull(),
            lit("merchant_id_null"),
        ),
        when(
            col("event_timestamp").isNull(),
            lit("event_timestamp_null"),
        ),
        when(
            ~col("currency").isin(
                EVENT_PAYMENT_CONTRACT["allowed_currencies"]
            ),
            lit("invalid_currency"),
        ),
        when(
            ~col("transaction_status").isin(
                EVENT_PAYMENT_CONTRACT["allowed_statuses"]
            ),
            lit("invalid_transaction_status"),
        ),
        when(
            ~col("payment_method").isin(
                EVENT_PAYMENT_CONTRACT["allowed_payment_methods"]
            ),
            lit("invalid_payment_method"),
        ),
        when(
            (col("amount")<lit(amount_min))|(col("amount")>lit(amount_max)),
            lit("invalid_amount_range"),
        ),
    )

    failed_df=(
        dataframe.withColumn("failed_reason",failed_reason,)
                 .withColumn("failed_at",current_timestamp(),)
                 .withColumn("dataset_name",lit(EVENT_PAYMENT_CONTRACT["dataset_name"]),)
                 .filter(col("failed_reason")!="")
    )

    return failed_df

def build_duplicate_rows_dataframe(dataframe):
    business_key=EVENT_PAYMENT_CONTRACT["business_key"]
    duplicated_keys_df=(
        dataframe
        .groupBy(business_key)
        .count()
        .filter(col("count")>1)
        .select(business_key)
    )

    duplicate_rows_df=(
        dataframe
        .join(
            duplicated_keys_df,
            on=business_key,
            how="inner",
        )
        .withColumn(
            "failed_reason",
            lit("duplicate_transaction_id"),
        )
        .withColumn(
            "failed_at",
            current_timestamp(),
        )
        .withColumn(
            "dataset_name",
            lit(EVENT_PAYMENT_CONTRACT["dataset_name"]),
        )
    )

    return duplicate_rows_df


def main() -> None:
    logger.info("=" * 60)
    logger.info(
        "Starting event_payment quality validation..."
    )
    logger.info("=" * 60)

    spark = create_spark_session(
        "SentinelPay Quality Event Payment"
    )

    try:
        silver_df = load_silver_dataframe(spark)

        validate_required_columns(silver_df)

        logger.info(
            "Silver dataset loaded and contract columns verified."
        )

        validation_result = run_expectation_check(
            silver_df
        )

        summary = build_result_summary(
            validation_result
        )

        failed_df = build_failed_rows_dataframe(
        silver_df
        )

        duplicate_failed_df = build_duplicate_rows_dataframe(
            silver_df
        )

        failed_df = failed_df.unionByName(
            duplicate_failed_df,
            allowMissingColumns=True,
        ).dropDuplicates(
            ["transaction_id", "failed_reason"]
        )
        
        write_quarantine_dataframe(
            dataframe=failed_df,
            output_path=EVENT_PAYMENT_CONTRACT["quarantine_path"],
        )

        logger.info(f"Failed rows written to quarantine:"f"{failed_df.count()}")

        logger.info(
        f"Writing result summary to: {EVENT_PAYMENT_CONTRACT['result_path']}"
        )

        write_result_summary(
            spark=spark,
            summary=summary,
            output_path=EVENT_PAYMENT_CONTRACT["result_path"],
        )

        logger.info(
            f"Validation status: "
            f"{summary['validation_status']}"
        )
        logger.info(
            f"Total expectations: "
            f"{summary['total_expectations']}"
        )
        logger.info(
            f"Passed expectations: "
            f"{summary['passed_expectations']}"
        )
        logger.info(
            f"Failed expectations: "
            f"{summary['failed_expectations']}"
        )

    except Exception:
        logger.exception(
            "event_payment quality validation failed."
        )
        raise

    finally:
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()
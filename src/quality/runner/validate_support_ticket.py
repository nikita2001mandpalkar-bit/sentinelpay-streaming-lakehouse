from datetime import datetime,timezone

import great_expectations as gx
from pyspark.sql.functions import col,concat_ws,current_timestamp,lit,when
from data_generator.logger import get_logger
from src.quality.contracts.support_ticket_contract import SUPPORT_TICKET_CONTRACT
from src.quality.quarantine.quarantine_writer import  write_quarantine_dataframe
from src.quality.results.result_writer import write_result_summary
from src.utils.spark_session import create_spark_session


logger=get_logger(__name__)

def load_silver_dataframe(spark):
    return (
        spark.read
        .format("delta")
        .load(SUPPORT_TICKET_CONTRACT["silver_path"])
    )

def validate_required_columns(dataframe)->None:
    actual_columns=set(dataframe.columns)
    required_columns=set(SUPPORT_TICKET_CONTRACT["required_columns"])
    missing_columns=sorted(required_columns-actual_columns)

    if missing_columns:
        raise ValueError(f"Missing required columns:{missing_columns}")

def get_gx_context():
    return gx.get_context(mode="file",project_root_dir=".",)

def get_batch_definition(context):
    datasource_name = "sentinelpay_spark"
    asset_name = "silver_log_support_ticket"
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

    existing_batch_definitions = [
        batch_definition.name
        for batch_definition in asset.batch_definitions
    ]

    if batch_definition_name in existing_batch_definitions:
        return asset.get_batch_definition(
            batch_definition_name
        )

    return asset.add_batch_definition_whole_dataframe(
        batch_definition_name
    )
def build_expectation_suite():
    suite=gx.ExpectationSuite(
        name="support_ticket_suite",
        expectations=[
            gx.expectations.ExpectColumnValuesToNotBeNull(column="ticket_id"),
            gx.expectations.ExpectColumnValuesToNotBeNull(column="event_id"),
            gx.expectations.ExpectColumnValuesToNotBeNull(column="ticket_type"),
            gx.expectations.ExpectColumnValuesToNotBeNull(column="priority"),
            gx.expectations.ExpectColumnValuesToNotBeNull(column="status"),
            gx.expectations.ExpectColumnValuesToBeInSet(column="ticket_type",value_set=SUPPORT_TICKET_CONTRACT["allowed_ticket_types"],),
            gx.expectations.ExpectColumnValuesToBeInSet(column="priority",value_set=SUPPORT_TICKET_CONTRACT["allowed_priorities"],),
            gx.expectations.ExpectColumnValuesToBeInSet(column="status",value_set=SUPPORT_TICKET_CONTRACT["allowed_statuses"],),
            gx.expectations.ExpectColumnValuesToBeUnique(column=SUPPORT_TICKET_CONTRACT["business_key"],),
        ],
    )

    return suite

def run_expectation_check(dataframe):
    context = get_gx_context()

    batch_definition = get_batch_definition(context)

    suite = build_expectation_suite()
    suite = context.suites.add_or_update(suite)

    validation_definition = gx.ValidationDefinition(
        name="support_ticket_validation_definition",
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
            SUPPORT_TICKET_CONTRACT["dataset_name"],

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
    failed_reason = concat_ws(
        ", ",
        when(
            col("ticket_id").isNull(),
            lit("ticket_id_null"),
        ),
        when(
            col("event_id").isNull(),
            lit("event_id_null"),
        ),
        when(
            col("ticket_type").isNull(),
            lit("ticket_type_null"),
        ),
        when(
            col("priority").isNull(),
            lit("priority_null"),
        ),
        when(
            col("status").isNull(),
            lit("status_null"),
        ),
        when(
            ~col("ticket_type").isin(
                SUPPORT_TICKET_CONTRACT[
                    "allowed_ticket_types"
                ]
            ),
            lit("invalid_ticket_type"),
        ),
        when(
            ~col("priority").isin(
                SUPPORT_TICKET_CONTRACT[
                    "allowed_priorities"
                ]
            ),
            lit("invalid_priority"),
        ),
        when(
            ~col("status").isin(
                SUPPORT_TICKET_CONTRACT[
                    "allowed_statuses"
                ]
            ),
            lit("invalid_status"),
        ),
    )

    failed_df = (
        dataframe
        .withColumn("failed_reason", failed_reason)
        .withColumn("failed_at", current_timestamp())
        .withColumn(
            "dataset_name",
            lit(SUPPORT_TICKET_CONTRACT["dataset_name"]),
        )
        .filter(col("failed_reason") != "")
    )

    return failed_df


def build_duplicate_rows_dataframe(dataframe):
    business_key = SUPPORT_TICKET_CONTRACT["business_key"]

    duplicate_keys_df = (
        dataframe
        .groupBy(business_key)
        .count()
        .filter(col("count") > 1)
        .select(business_key)
    )

    duplicate_rows_df = (
        dataframe
        .join(
            duplicate_keys_df,
            on=business_key,
            how="inner",
        )
        .withColumn(
            "failed_reason",
            lit("duplicate_ticket_id"),
        )
        .withColumn(
            "failed_at",
            current_timestamp(),
        )
        .withColumn(
            "dataset_name",
            lit(SUPPORT_TICKET_CONTRACT["dataset_name"]),
        )
    )

    return duplicate_rows_df


def main() -> None:
    logger.info("=" * 60)
    logger.info(
        "Starting support ticket quality validation..."
    )
    logger.info("=" * 60)

    spark = create_spark_session(
        "SentinelPay Quality Support Ticket"
    )

    try:
        silver_df = load_silver_dataframe(spark)

        validate_required_columns(silver_df)

        logger.info(
            "Silver support ticket dataset loaded and contract columns verified."
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

        duplicate_failed_df = (
            build_duplicate_rows_dataframe(
                silver_df
            )
        )

        failed_df = failed_df.unionByName(
            duplicate_failed_df,
            allowMissingColumns=True,
        ).dropDuplicates(
            ["ticket_id", "failed_reason"]
        )

        write_quarantine_dataframe(
            dataframe=failed_df,
            output_path=SUPPORT_TICKET_CONTRACT[
                "quarantine_path"
            ],
        )

        logger.info(
            f"Failed rows written to quarantine: "
            f"{failed_df.count()}"
        )

        logger.info(
            f"Writing result summary to: "
            f"{SUPPORT_TICKET_CONTRACT['result_path']}"
        )

        write_result_summary(
            spark=spark,
            summary=summary,
            output_path=SUPPORT_TICKET_CONTRACT[
                "result_path"
            ],
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
            "support ticket quality validation failed."
        )
        raise

    finally:
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()

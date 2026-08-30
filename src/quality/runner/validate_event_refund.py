from datetime import datetime,timezone
import great_expectations as gx
from pyspark.sql.functions import col,concat_ws,current_timestamp,lit,when
from data_generator.logger import get_logger
from src.quality.contracts.event_refund_contract import EVENT_REFUND_CONTRACT
from src.quality.quarantine.quarantine_writer import write_quarantine_dataframe
from src.quality.results.result_writer import write_result_summary
from src.utils.spark_session import create_spark_session

logger=get_logger(__name__)

def load_silver_dataframe(spark):
    return(
        spark.read
        .format("delta")
        .load(EVENT_REFUND_CONTRACT["silver_path"])
    )

def validate_required_columns(dataframe)->None:
    actual_columns=set(dataframe.columns)
    required_columns=set(
        EVENT_REFUND_CONTRACT["required_columns"]
    )

    missing_columns=sorted(
        required_columns-actual_columns
    )

    if missing_columns:
        raise ValueError(f"Missing required columns:{missing_columns}")

def get_gx_context():
    return gx.get_context(mode="file",project_root_dir=".")

def get_batch_definition(context):
    datasource_name = "sentinelpay_spark"
    asset_name = "silver_event_refund"
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
        name="event_refund_suite",
        expectations=[
            gx.expectations.ExpectColumnValuesToNotBeNull(column="refund_id"),
            gx.expectations.ExpectColumnValuesToBeNull(column="transaction_id"),
            gx.expectations.ExpectColumnValuesToBeNull(column="event_timestamp"),
            gx.expectations.ExpectColumnValuesToBeBetween(column="refund_amount",min_value=EVENT_REFUND_CONTRACT["amount_range"]["min_value"],max_value=EVENT_REFUND_CONTRACT["amount_range"]["max_value"],),
            gx.expectations.ExpectColumnValuesToBeInSet(column="refund_status",value_set=EVENT_REFUND_CONTRACT["allowed_statuses"],),
            gx.expectations.ExpectColumnValuesToBeUnique(column=EVENT_REFUND_CONTRACT["business_key"],),
        ],
    )

    return suite

def run_expectation_check(dataframe):
    context=get_gx_context()
    batch_defination=get_batch_definition(context)
    suite=build_expectation_suite()
    suite=context.suites.add_or_update(suite)

    validation_defination=gx.ValidationDefinition(name="event_refund_validation_definition",data=batch_defination,suite=suite)

    validation_defination=(context.validation_definitions.add_or_update(validation_defination))

    validation_result=validation_defination.run(batch_parameters={"dataframe":dataframe},result_format="COMPLETE",)

    return validation_result

def build_result_summary(validation_result):
    total_expectations=len(validation_result.results)
    passed_expectations=sum(1 for result in validation_result.results if result.success)

    failed_expectations=(total_expectations-passed_expectations)

    return{
        "dataset_name":EVENT_REFUND_CONTRACT["dataset_name"],
        "run_timestamp":datetime.now(
            timezone.utc
        ).isoformat(),

        "validation_status":"PASSED" if validation_result.success else "FAILED",
        "total_expectations":total_expectations,
        "passed_expectations":passed_expectations,
        "failed_expectations":failed_expectations,
    }

def build_failed_rows_dataframe(dataframe):
    amount_min = EVENT_REFUND_CONTRACT[
        "amount_range"
    ]["min_value"]
    amount_max = EVENT_REFUND_CONTRACT[
        "amount_range"
    ]["max_value"]

    failed_reason = concat_ws(
        ", ",
        when(
            col("refund_id").isNull(),
            lit("refund_id_null"),
        ),
        when(
            col("transaction_id").isNull(),
            lit("transaction_id_null"),
        ),
        when(
            col("event_timestamp").isNull(),
            lit("event_timestamp_null"),
        ),
        when(
            ~col("refund_status").isin(
                EVENT_REFUND_CONTRACT[
                    "allowed_statuses"
                ]
            ),
            lit("invalid_refund_status"),
        ),
        when(
            (col("refund_amount") < lit(amount_min))
            | (col("refund_amount") > lit(amount_max)),
            lit("invalid_refund_amount_range"),
        ),
    )

    failed_df = (
        dataframe
        .withColumn("failed_reason", failed_reason)
        .withColumn("failed_at", current_timestamp())
        .withColumn(
            "dataset_name",
            lit(EVENT_REFUND_CONTRACT["dataset_name"]),
        )
        .filter(col("failed_reason") != "")
    )

    return failed_df

def build_duplicate_rows_dataframe(dataframe):
    business_key=EVENT_REFUND_CONTRACT["business_key"]
    duplicate_keys_df=(
        dataframe
        .groupBy(business_key)
        .filter(col("count")>1)
        .select(business_key)
    )

    duplicate_rows_df=(
        dataframe
        .join(
            duplicate_keys_df,
            on=business_key,
            how="inner",
        )
        .withColumn("failed_reason",lit("duplicate_refund_id"),)
        .withColumn("failed_at",current_timestamp(),)
        .withColumn("dataset_name",lit(EVENT_REFUND_CONTRACT["dataset_name"]),)
    )

    return duplicate_rows_df

def main()->None:
    logger.info("="*60)
    logger.info("Starting event_refund quality validation...")
    logger.info("="*60)

    spark=create_spark_session("SentinelPay Quality Event Refund")

    try:
        silver_df=load_silver_dataframe(spark)
        validate_required_columns(silver_df)

        logger.info("Silver refund dataset loaded and contract columns verified.............")

        validation_result=run_expectation_check(silver_df)
        summary=build_result_summary(validation_result)
        failed_df=build_failed_rows_dataframe(silver_df)
        duplicate_failed_df=(build_failed_rows_dataframe(silver_df))
        failed_df=failed_df.unionByName(duplicate_failed_df,allowMissingColumns=True,).dropDuplicates(["refund_id","failed_reason"])

        write_quarantine_dataframe(dataframe=failed_df,output_path=EVENT_REFUND_CONTRACT["quarantine_path"],)

        logger.info(f"Failed rows written to quarantine: " f"{failed_df.count()}")

        logger.info(f"Writing result summary to: " f"{EVENT_REFUND_CONTRACT['result_path']}")

        write_result_summary(spark=spark,summary=summary,output_path=EVENT_REFUND_CONTRACT["result_path"],)

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
        logger.exception("event_refund quality validation failed.")
        raise

    finally:
        spark.stop()
        logger.info("spark session stopped......")

if __name__=="__main__":
    main()


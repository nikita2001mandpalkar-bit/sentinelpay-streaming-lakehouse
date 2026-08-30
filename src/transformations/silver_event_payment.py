from pyspark.sql.functions import col,current_timestamp,expr,lit,to_timestamp,trim,upper,when
from pyspark.sql.types import DecimalType
from data_generator.logger import get_logger
from src.utils.paths import SILVER_PATHS,SILVER_CHECKPOINT_PATHS,BRONZE_PATHS
from src.utils.spark_session import create_spark_session
from delta.tables import DeltaTable
from src.utils.paths import SILVER_PATHS, SILVER_CHECKPOINT_PATHS, BRONZE_PATHS, QUARANTINE_PATHS

logger=get_logger(__name__)

LATE_THRESHOLD_MINUTES=10

def main()->None:
    logger.info("="*60)
    logger.info("Starting Silver Payment Streaming Job.......")
    logger.info("="*60)

    spark=create_spark_session("SentinelPay Silver Event Payment")

    try:
        bronze_df=(
            spark.readStream
            .format("delta")
            .load(BRONZE_PATHS["event.payment"])
        )

        base_df = (
            bronze_df
            .select(
                trim(col("transaction_id")).alias("transaction_id"),
                trim(col("wallet_id")).alias("wallet_id"),
                trim(col("merchant_id")).alias("merchant_id"),
                col("amount").cast(DecimalType(18, 2)).alias("amount"),
                upper(trim(col("currency"))).alias("currency"),
                trim(col("payment_method")).alias("payment_method"),
                upper(trim(col("transaction_status"))).alias("transaction_status"),
                trim(col("reference_number")).alias("reference_number"),
                to_timestamp(col("transaction_timestamp")).alias("event_timestamp"),
                to_timestamp(col("created_at")).alias("created_at"),
                col("message_key"),
                col("topic"),
                col("partition"),
                col("offset"),
                to_timestamp(col("kafka_timestamp")).alias("kafka_timestamp"),
                to_timestamp(col("bronze_ingested_at")).alias("bronze_ingested_at"),
            )
            .withColumn(
                "is_late",
                when(
                    col("kafka_timestamp")
                    > col("event_timestamp") + expr(f"INTERVAL {LATE_THRESHOLD_MINUTES} MINUTES"),
                    lit(True),
                ).otherwise(lit(False)),
            )
            .withColumn("silver_processed_at", current_timestamp())
        )

        invalid_df = (
            base_df
            .filter(
                col("transaction_id").isNull()
                | col("wallet_id").isNull()
                | col("merchant_id").isNull()
                | col("event_timestamp").isNull()
            )
            .withColumn(
                "failed_reason",
                when(col("transaction_id").isNull(), lit("missing_transaction_id"))
                .when(col("wallet_id").isNull(), lit("missing_wallet_id"))
                .when(col("merchant_id").isNull(), lit("missing_merchant_id"))
                .when(col("event_timestamp").isNull(), lit("invalid_transaction_timestamp"))
                .otherwise(lit("unknown_validation_error")),
            )
            .withColumn("failed_at", current_timestamp())
            .withColumn("dataset_name", lit("event.payment"))
        )

        silver_df = (
            base_df
            .filter(
                col("transaction_id").isNotNull()
                & col("wallet_id").isNotNull()
                & col("merchant_id").isNotNull()
                & col("event_timestamp").isNotNull()
            )
            .withWatermark(
                "event_timestamp",
                f"{LATE_THRESHOLD_MINUTES} minutes",
            )
            .dropDuplicates(["transaction_id"])
        )


        def upsert_payment_batch(batch_df, batch_id):
            if batch_df.rdd.isEmpty():
                return

            target_path = SILVER_PATHS["event.payment"]

            if not DeltaTable.isDeltaTable(spark, target_path):
                (
                    batch_df.write
                    .format("delta")
                    .mode("overwrite")
                    .save(target_path)
                )
                return

            delta_table = DeltaTable.forPath(spark, target_path)

            (
                delta_table.alias("target")
                .merge(
                    batch_df.alias("source"),
                    "target.transaction_id = source.transaction_id",
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

        def write_quarantine_batch(batch_df, batch_id):
            if batch_df.rdd.isEmpty():
                return

            (
                batch_df.write
                .format("delta")
                .mode("append")
                .save(QUARANTINE_PATHS["event.payment"])
            )

        query = (
            silver_df.writeStream
            .foreachBatch(upsert_payment_batch)
            .outputMode("append")
            .option(
                "checkpointLocation",
                SILVER_CHECKPOINT_PATHS["event.payment"],
            )
            .queryName("silver_event_payment")
            .start()
        )


        quarantine_query = (
            invalid_df.writeStream
            .foreachBatch(write_quarantine_batch)
            .outputMode("append")
            .option(
                "checkpointLocation",
                SILVER_CHECKPOINT_PATHS["event.payment"] + "_quarantine",
            )
            .queryName("quarantine_event_payment")
            .start()
        )

        logger.info("Silver Payment stream started successfully....")

        query.awaitTermination()
        quarantine_query.awaitTermination()     

    except Exception:
        logger.exception("Silver payment streaming job failed")
        raise

    finally:
        spark.stop()
        logger.info("Spark Session stopped")

if __name__=="__main__":
    main()
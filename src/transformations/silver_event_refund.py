from pyspark.sql.functions import (
    col,
    current_timestamp,
    expr,
    lit,
    to_timestamp,
    trim,
    upper,
    when,
)
from pyspark.sql.types import DecimalType
from delta.tables import DeltaTable
from data_generator.logger import get_logger
from src.utils.paths import BRONZE_PATHS, SILVER_CHECKPOINT_PATHS, SILVER_PATHS
from src.utils.spark_session import create_spark_session

logger = get_logger(__name__)

late_threshold_minutes = 10


def main() -> None:
    logger.info("=" * 60)
    logger.info("Starting Silver Refund Streaming Job")
    logger.info("=" * 60)

    spark = create_spark_session(
        "SentinelPay Silver Event Refund"
    )

    try:
        bronze_df = (
            spark.readStream
            .format("delta")
            .load(BRONZE_PATHS["event.refund"])
        )

        silver_df = (
            bronze_df
            .select(
                trim(col("refund_id")).alias("refund_id"),
                trim(col("transaction_id")).alias("transaction_id"),
                col("refund_amount").cast(DecimalType(18, 2)).alias("refund_amount"),
                trim(col("refund_reason")).alias("refund_reason"),
                upper(trim(col("refund_status"))).alias("refund_status"),
                to_timestamp(col("refund_timestamp")).alias("event_timestamp"),
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
                    > col("event_timestamp") + expr(f"INTERVAL {late_threshold_minutes} MINUTES"),
                    lit(True),
                ).otherwise(lit(False)),
            )
            .withColumn(
                "silver_processed_at",
                current_timestamp(),
            )
            .filter(
                col("refund_id").isNotNull()
                & col("transaction_id").isNotNull()
                & col("event_timestamp").isNotNull()
            )
            .withWatermark(
                "event_timestamp",
                f"{late_threshold_minutes} minutes",
            )
            .dropDuplicates(["refund_id"])
        )

        def upsert_refund_batch(batch_df, batch_id):
            if batch_df.rdd.isEmpty():
                return

            target_path = SILVER_PATHS["event.refund"]

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
                    "target.refund_id = source.refund_id",
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

        query = (
            silver_df.writeStream
            .foreachBatch(upsert_refund_batch)
            .outputMode("append")
            .option(
                "checkpointLocation",
                SILVER_CHECKPOINT_PATHS["event.refund"],
            )
            .queryName("silver_event_refund")
            .start()
        )

        logger.info("Silver refund stream started successfully")
        query.awaitTermination()

    except Exception:
        logger.exception("Silver refund streaming job failed.")
        raise

    finally:
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()
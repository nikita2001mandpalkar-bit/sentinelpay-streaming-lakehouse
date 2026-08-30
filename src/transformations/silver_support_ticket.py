"""
Silver streaming job for SentinelPay support ticket events.
"""

from pyspark.sql.functions import (
    col,
    current_timestamp,
    to_timestamp,
    trim,
    upper,
)
from delta.tables import DeltaTable
from data_generator.logger import get_logger
from src.utils.paths import (
    BRONZE_PATHS,
    SILVER_CHECKPOINT_PATHS,
    SILVER_PATHS,
)
from src.utils.spark_session import create_spark_session

logger = get_logger(__name__)


def main() -> None:
    logger.info("=" * 60)
    logger.info("Starting Silver Support Ticket Streaming Job")
    logger.info("=" * 60)

    spark = create_spark_session(
        "SentinelPay Silver Support Ticket"
    )

    try:
        bronze_df = (
            spark.readStream
            .format("delta")
            .load(BRONZE_PATHS["log.support_ticket"])
        )

        silver_df = (
            bronze_df
            .select(
                trim(col("event_id")).alias("event_id"),
                trim(col("ticket_id")).alias("ticket_id"),
                upper(trim(col("ticket_type"))).alias("ticket_type"),
                trim(col("reference_id")).alias("reference_id"),
                trim(col("issue")).alias("issue"),
                upper(trim(col("priority"))).alias("priority"),
                upper(trim(col("status"))).alias("status"),
                trim(col("source_system")).alias("source_system"),
                to_timestamp(col("created_at")).alias("created_at"),
                to_timestamp(col("ingested_at")).alias("ingested_at"),
                col("message_key"),
                col("topic"),
                col("partition"),
                col("offset"),
                to_timestamp(col("kafka_timestamp")).alias("kafka_timestamp"),
                to_timestamp(col("bronze_ingested_at")).alias("bronze_ingested_at"),
            )
            .withColumn(
                "silver_processed_at",
                current_timestamp(),
            )
            .filter(
                col("event_id").isNotNull()
                & col("ticket_id").isNotNull()
                & col("ticket_type").isNotNull()
                & col("reference_id").isNotNull()
                & col("created_at").isNotNull()
            )
            .dropDuplicates(["event_id"])
        )

        def upsert_support_ticket_batch(batch_df, batch_id):
            if batch_df.rdd.isEmpty():
                return

            target_path = SILVER_PATHS["log.support_ticket"]

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
                    "target.event_id = source.event_id",
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

        query = (
            silver_df.writeStream
            .foreachBatch(upsert_support_ticket_batch)
            .outputMode("append")
            .option(
                "checkpointLocation",
                SILVER_CHECKPOINT_PATHS["log.support_ticket"],
            )
            .queryName("silver_support_ticket")
            .start()
        )

        logger.info(
            "Silver support ticket stream started successfully."
        )

        query.awaitTermination()

    except Exception:
        logger.exception(
            "Silver support ticket streaming job failed."
        )
        raise

    finally:
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()
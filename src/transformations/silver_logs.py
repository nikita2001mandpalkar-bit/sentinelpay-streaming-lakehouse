"""Silver streaming job for SentinelPay application, error, and audit logs."""

from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_timestamp, split, trim

from data_generator.logger import get_logger
from src.utils.paths import (
    BRONZE_PATHS,
    SILVER_CHECKPOINT_PATHS,
    SILVER_PATHS,
)
from src.utils.spark_session import create_spark_session

logger = get_logger(__name__)

LOG_TOPICS = [
    "log.application",
    "log.error",
    "log.audit",
]


def main() -> None:
    logger.info("=" * 60)
    logger.info("Starting Silver Log Streaming Job")
    logger.info("=" * 60)

    spark = create_spark_session("SentinelPay Silver Logs")

    try:
        queries = []

        for topic_name in LOG_TOPICS:
            bronze_df = (
                spark.readStream
                .format("delta")
                .load(BRONZE_PATHS[topic_name])
            )

            silver_df = (
                bronze_df
                .withColumn(
                    "parts",
                    split(col("log_message"), r"\s*\|\s*"),
                )
                .select(
                    trim(col("parts").getItem(0)).alias("log_timestamp"),
                    trim(col("parts").getItem(1)).alias("log_level"),
                    trim(col("parts").getItem(2)).alias("reference_id"),
                    trim(col("parts").getItem(3)).alias("log_message"),
                    col("message_key"),
                    col("topic"),
                    col("partition"),
                    col("offset"),
                    col("kafka_timestamp"),
                    col("bronze_ingested_at"),
                )
                .withColumn(
                    "silver_processed_at",
                    current_timestamp(),
                )
                .filter(
                    col("log_timestamp").isNotNull()
                    & col("log_level").isNotNull()
                    & col("reference_id").isNotNull()
                    & col("log_message").isNotNull()
                )
                .dropDuplicates(["topic", "partition", "offset"])
            )

            def upsert_log_batch(batch_df, batch_id, current_topic=topic_name):
                if batch_df.rdd.isEmpty():
                    return

                target_path = SILVER_PATHS[current_topic]

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
                        """
                        target.topic = source.topic
                        AND target.partition = source.partition
                        AND target.offset = source.offset
                        """,
                    )
                    .whenMatchedUpdateAll()
                    .whenNotMatchedInsertAll()
                    .execute()
                )

            query_name = topic_name.replace(".", "_")

            query = (
                silver_df.writeStream
                .foreachBatch(upsert_log_batch)
                .outputMode("append")
                .option(
                    "checkpointLocation",
                    SILVER_CHECKPOINT_PATHS[topic_name],
                )
                .queryName(f"silver_{query_name}")
                .start()
            )

            queries.append(query)

        logger.info(f"Started {len(queries)} Silver log streaming queries.")

        for query in queries:
            query.awaitTermination()

    except Exception:
        logger.exception("Silver log streaming job failed.")
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()
"""
Bronze streaming job for SentinelPay settlement topic.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, from_json

from data_generator.logger import get_logger
from src.utils.delta_writer import write_stream_to_delta
from src.utils.kafka_reader import read_kafka_stream
from src.utils.paths import BRONZE_PATHS, CHECKPOINT_PATHS
from src.utils.schemas import SETTLEMENT_SCHEMA
from src.utils.spark_session import create_spark_session

logger = get_logger(__name__)

TOPIC_NAME = "batch.settlement"


def parse_topic_stream(
    spark,
) -> DataFrame:
    raw_stream_df = read_kafka_stream(
        spark=spark,
        topic_name=TOPIC_NAME,
    )

    parsed_stream_df = (
        raw_stream_df
        .withColumn("raw_payload", col("message_value"))
        .withColumn(
            "parsed_value",
            from_json(col("message_value"), SETTLEMENT_SCHEMA),
        )
        .select(
            col("parsed_value.*"),
            col("raw_payload"),
            col("message_key"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("kafka_timestamp"),
            current_timestamp().alias("bronze_ingested_at"),
        )
    )

    return parsed_stream_df


def main() -> None:
    logger.info("=" * 60)
    logger.info("Starting Bronze Settlement Streaming Job")
    logger.info("=" * 60)

    spark = create_spark_session(
        "SentinelPay Bronze Settlement Data"
    )

    try:
        logger.info("Starting Bronze stream for batch.settlement...")
        parsed_df = parse_topic_stream(spark)

        query = write_stream_to_delta(
            dataframe=parsed_df,
            output_path=BRONZE_PATHS[TOPIC_NAME],
            checkpoint_path=CHECKPOINT_PATHS[TOPIC_NAME],
            query_name="bronze_batch_settlement",
        )

        logger.info("Bronze settlement streaming query started.")
        query.awaitTermination()

    except Exception:
        logger.exception(
            "Bronze settlement streaming job failed."
        )
        raise

    finally:
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()

"""
Bronze streaming job for SentinelPay raw log topics.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp

from data_generator.logger import get_logger
from src.utils.delta_writer import write_stream_to_delta
from src.utils.kafka_reader import read_kafka_stream
from src.utils.paths import BRONZE_PATHS, CHECKPOINT_PATHS
from src.utils.spark_session import create_spark_session

logger = get_logger(__name__)

LOG_TOPICS = [
    "log.application",
    "log.error",
    "log.audit",
]


def parse_log_stream(
    spark,
    topic_name: str,
) -> DataFrame:
    raw_stream_df = read_kafka_stream(
        spark=spark,
        topic_name=topic_name,
    )

    parsed_stream_df = (
        raw_stream_df
        .select(
            col("message_value").alias("log_message"),
            col("message_key"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("kafka_timestamp"),
            current_timestamp().alias("bronze_ingested_at"),
        )
    )

    return parsed_stream_df


def start_bronze_query(
    spark,
    topic_name: str,
):
    logger.info(
        f"Starting Bronze stream for {topic_name}..."
    )

    parsed_df = parse_log_stream(
        spark=spark,
        topic_name=topic_name,
    )

    query_name = topic_name.replace(".", "_")

    return write_stream_to_delta(
        dataframe=parsed_df,
        output_path=BRONZE_PATHS[topic_name],
        checkpoint_path=CHECKPOINT_PATHS[topic_name],
        query_name=f"bronze_{query_name}",
    )


def main() -> None:
    logger.info("=" * 60)
    logger.info(
        "Starting Bronze Log Data Streaming Job"
    )
    logger.info("=" * 60)

    spark = create_spark_session(
        "SentinelPay Bronze Log Data"
    )

    try:
        queries = []

        for topic_name in LOG_TOPICS:
            query = start_bronze_query(
                spark=spark,
                topic_name=topic_name,
            )
            queries.append(query)

        logger.info(
            f"Started {len(queries)} Bronze log streaming queries."
        )

        for query in queries:
            query.awaitTermination()

    except Exception:
        logger.exception(
            "Bronze log-data streaming job failed."
        )
        raise

    finally:
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()
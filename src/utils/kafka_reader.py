"""
Reusable Kafka stream reader for SentinelPay Spark jobs.
"""

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col


def read_kafka_stream(
    spark: SparkSession,
    topic_name: str,
    starting_offsets: str = "earliest",
) -> DataFrame:
    """
    Read a Kafka topic as a streaming DataFrame.
    """

    kafka_bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092",
    )

    return (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            kafka_bootstrap_servers,
        )
        .option(
            "subscribe",
            topic_name,
        )
        .option(
            "startingOffsets",
            starting_offsets,
        )
        .option(
            "failOnDataLoss",
            "false",
        )
        .load()
        .select(
            col("key").cast("string").alias("message_key"),
            col("value").cast("string").alias("message_value"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
        )
    )
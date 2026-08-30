"""
Consume master-data Kafka topics and write raw Bronze Delta tables.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col,current_timestamp,from_json
from data_generator.logger import get_logger
from src.utils.delta_writer import write_stream_to_delta
from src.utils.kafka_reader import read_kafka_stream
from src.utils.paths import BRONZE_PATHS,CHECKPOINT_PATHS
from src.utils.schemas import(
    BANK_ACCOUNT_SCHEMA,
    CUSTOMER_SCHEMA,
    MERCHANT_SCHEMA,
    DEVICE_SCHEMA,
    WALLET_SCHEMA
)
from src.utils.spark_session import create_spark_session

logger=get_logger(__name__)

TOPIC_SCHEMA_MAP={
    "master.customer":CUSTOMER_SCHEMA,
    "master.merchant":MERCHANT_SCHEMA,
    "master.bank_account":BANK_ACCOUNT_SCHEMA,
    "master.wallet":WALLET_SCHEMA,
    "master.device":DEVICE_SCHEMA
}

def parse_topic_stream(topic_name:str,schema,spark,)->DataFrame:
    "Read a kafka topic and parse JSON payload into structured columns"

    raw_stream_df=read_kafka_stream(
        spark=spark,
        topic_name=topic_name
    )

    parsed_stream_df=(
        raw_stream_df
        .withColumn(
            "parsed_value",
            from_json(
                col("message_value"),
                schema,
            ),
        )
        .select(
            col("parsed_value.*"),
            col("message_key"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("kafka_timestamp"),
            current_timestamp().alias("bronze_ingested_at"),
        )
    )

    return parsed_stream_df

def start_bronze_query(spark,topic_name:str,schema,):
    """
    Start one Bronze streaming query for one Kafka topic.
    """
    logger.info(
        f"Starting Bronze stream for {topic_name}..."
    )

    parsed_df=parse_topic_stream(
        topic_name=topic_name,
        schema=schema,
        spark=spark
    )

    query_name=(
        topic_name
        .replace(".","-")
        .replace("-","_")
    )

    return write_stream_to_delta(
        dataframe=parsed_df,
        output_path=BRONZE_PATHS[topic_name],
        checkpoint_path=CHECKPOINT_PATHS[topic_name],
        query_name=f"bronze_{query_name}",
    )

def main():
    logger.info("="*60)
    logger.info("Starting Bronze Data Ingestion...")
    logger.info("="*60)

    spark=create_spark_session(
        "SentinelPay Bronze Master Data Ingestion"
    )

    try:
        queries=[]

        for topic_name,schema in TOPIC_SCHEMA_MAP.items():
            query=start_bronze_query(
                spark=spark,
                topic_name=topic_name,
                schema=schema,
            )

            queries.append(query)

        logger.info(f"Started {len(queries)} Bronze streaming queries.")

        for query in queries:
            query.awaitTermination()

    except Exception:
        logger.exception(
            "Bronze master-data streaming job failed."
        )

        raise
    finally:
        spark.stop()
        logger.info(
            "Spark session stopped."
        )

if __name__=="__main__":
    main()
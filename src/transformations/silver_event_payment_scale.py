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

from data_generator.logger import get_logger
from src.utils.paths import (
    BRONZE_PATHS,
    SILVER_CHECKPOINT_PATHS,
    SILVER_PATHS,
)
from src.utils.spark_session import create_spark_session

logger = get_logger(__name__)

LATE_THRESHOLD_MINUTES = 10


def main() -> None:
    logger.info("=" * 60)
    logger.info("Starting Silver Payment Scale Streaming Job")
    logger.info("=" * 60)

    spark = create_spark_session("SentinelPay Silver Event Payment Scale")

    try:
        bronze_df = (
            spark.readStream
            .format("delta")
            .load(BRONZE_PATHS["event.payment.scale"])
        )

        silver_df = (
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
                current_timestamp().alias("silver_processed_at"),
                when(
                    to_timestamp(col("kafka_timestamp"))
                    > to_timestamp(col("transaction_timestamp")) + expr(f"INTERVAL {LATE_THRESHOLD_MINUTES} MINUTES"),
                    lit(True),
                ).otherwise(lit(False)).alias("is_late"),
            )
            .filter(
                col("transaction_id").isNotNull()
                & col("wallet_id").isNotNull()
                & col("merchant_id").isNotNull()
                & col("event_timestamp").isNotNull()
            )
        )

        query = (
            silver_df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", SILVER_CHECKPOINT_PATHS["event.payment.scale"])
            .option("mergeSchema", "true")
            .queryName("silver_event_payment_scale")
            .start(SILVER_PATHS["event.payment.scale"])
        )

        logger.info("Silver payment scale stream started successfully.")
        query.awaitTermination()

    except Exception:
        logger.exception("Silver payment scale streaming job failed")
        raise

    finally:
        spark.stop()
        logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
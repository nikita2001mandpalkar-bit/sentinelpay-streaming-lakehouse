from pyspark.sql.functions import count, current_timestamp

from data_generator.logger import get_logger
from src.utils.paths import GOLD_BASE_PATH, SILVER_PATHS
from src.utils.spark_session import create_spark_session

logger = get_logger(__name__)

GOLD_SUPPORT_TICKET_PATH = f"{GOLD_BASE_PATH}/support_ticket_summary"


def main() -> None:
    logger.info("=" * 60)
    logger.info("Starting Gold Support Ticket Transformation")
    logger.info("=" * 60)

    spark = create_spark_session(
        "SentinelPay Gold Support Ticket"
    )

    try:
        logger.info("Reading silver log.support_ticket table...")
        silver_df = (
            spark.read
            .format("delta")
            .load(SILVER_PATHS["log.support_ticket"])
        )

        logger.info("Building gold support ticket summary...")
        gold_df = (
            silver_df.groupBy(
                "issue",
                "priority",
                "status",
            )
            .agg(
                count("*").alias("ticket_count"),
            )
            .withColumn(
                "gold_processed_at",
                current_timestamp(),
            )
        )

        logger.info("Writing gold support ticket summary to MinIO...")
        (
            gold_df.write
            .format("delta")
            .mode("overwrite")
            .save(GOLD_SUPPORT_TICKET_PATH)
        )

        logger.info(
            f"Gold support ticket summary saved to {GOLD_SUPPORT_TICKET_PATH}"
        )

    except Exception:
        logger.exception(
            "Gold support ticket transformation failed."
        )
        raise

    finally:
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()
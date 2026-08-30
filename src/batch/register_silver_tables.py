from data_generator.logger import get_logger
from src.utils.spark_session import create_spark_session

logger = get_logger(__name__)

TABLES = {
    "silver_event_payment": "s3a://sentinelpay-lake/silver/event_payment",
    "silver_event_refund": "s3a://sentinelpay-lake/silver/event_refund",
    "silver_log_support_ticket": "s3a://sentinelpay-lake/silver/log_support_ticket",
    "silver_settlements": "s3a://sentinelpay-lake/silver/settlements",
    "silver_bank_accounts": "s3a://sentinelpay-lake/silver/bank_accounts",
    "silver_devices": "s3a://sentinelpay-lake/silver/devices",
}


def main() -> None:
    logger.info("=" * 60)
    logger.info("Registering Silver tables for dbt")
    logger.info("=" * 60)

    spark = create_spark_session("SentinelPay Register Silver Tables")

    try:
        for table_name, delta_path in TABLES.items():
            logger.info(f"Reading {table_name} from {delta_path}")

            df = (
                spark.read
                .format("delta")
                .load(delta_path)
            )

            logger.info(f"{table_name} read successful")

            (
                df.write
                .format("delta")
                .mode("overwrite")
                .saveAsTable(f"default.{table_name}")
            )

            logger.info(f"{table_name} registered in Spark catalog")

        logger.info("All Silver tables registered successfully.")

    finally:
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()
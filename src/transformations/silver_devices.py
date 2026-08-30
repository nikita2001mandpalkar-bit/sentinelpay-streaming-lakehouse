from pyspark.sql import functions as F
from delta.tables import DeltaTable
from data_generator.logger import get_logger
from src.utils.spark_session import create_spark_session

logger = get_logger(__name__)

BRONZE_PATH = "s3a://sentinelpay-lake/bronze/master_device"
SILVER_PATH = "s3a://sentinelpay-lake/silver/devices"


def main() -> None:
    logger.info("=" * 60)
    logger.info("Starting Silver devices transformation")
    logger.info("=" * 60)

    spark = create_spark_session("SentinelPay Silver Devices")

    try:
        df = spark.read.format("delta").load(BRONZE_PATH)

        silver_df = (
            df.select(
                F.trim("device_id").alias("device_id"),
                F.trim("customer_id").alias("customer_id"),
                F.upper(F.trim("device_type")).alias("device_type"),
                F.upper(F.trim("device_os")).alias("device_os"),
                F.trim("app_version").alias("app_version"),
                F.to_timestamp("registered_at").alias("registered_at"),
                F.col("kafka_timestamp"),
                F.col("bronze_ingested_at"),
            )
            .dropDuplicates(["device_id"])
            .withColumn("silver_processed_at", F.current_timestamp())
        )

        if not DeltaTable.isDeltaTable(spark, SILVER_PATH):
            (
                silver_df.write
                .format("delta")
                .mode("overwrite")
                .save(SILVER_PATH)
            )
        else:
            delta_table = DeltaTable.forPath(spark, SILVER_PATH)

            (
                delta_table.alias("target")
                .merge(
                    silver_df.alias("source"),
                    "target.device_id = source.device_id",
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

        logger.info("Silver devices transformation completed successfully.")

    finally:
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()

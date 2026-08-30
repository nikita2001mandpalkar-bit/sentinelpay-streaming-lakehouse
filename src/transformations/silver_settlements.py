from pyspark.sql import functions as F
from delta.tables import DeltaTable
from data_generator.logger import get_logger
from src.utils.spark_session import create_spark_session

logger = get_logger(__name__)

BRONZE_PATH = "s3a://sentinelpay-lake/bronze/batch_settlement"
SILVER_PATH = "s3a://sentinelpay-lake/silver/settlements"


def main() -> None:
    logger.info("=" * 60)
    logger.info("Starting Silver settlements transformation")
    logger.info("=" * 60)

    spark = create_spark_session("SentinelPay Silver Settlements")

    try:
        df = spark.read.format("delta").load(BRONZE_PATH)

        silver_df = (
            df.select(
                F.trim("settlement_id").alias("settlement_id"),
                F.trim("merchant_id").alias("merchant_id"),
                F.col("settlement_amount").cast("decimal(18,2)").alias("settlement_amount"),
                F.to_timestamp("settlement_date").alias("settlement_date"),
                F.upper(F.trim("settlement_status")).alias("settlement_status"),
                F.upper(F.trim("merchant_status")).alias("merchant_status"),
                F.to_timestamp("created_at").alias("created_at"),
                F.col("kafka_timestamp"),
                F.col("bronze_ingested_at"),
            )
            .dropDuplicates(["settlement_id"])
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
                    "target.settlement_id = source.settlement_id",
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

        logger.info("Silver settlements transformation completed successfully.")

    finally:
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()

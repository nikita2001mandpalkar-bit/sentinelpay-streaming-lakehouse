from pyspark.sql import functions as F
from delta.tables import DeltaTable
from data_generator.logger import get_logger
from src.utils.spark_session import create_spark_session

logger = get_logger(__name__)

BRONZE_PATH = "s3a://sentinelpay-lake/bronze/master_bank_account"
SILVER_PATH = "s3a://sentinelpay-lake/silver/bank_accounts"


def main() -> None:
    logger.info("=" * 60)
    logger.info("Starting Silver bank accounts transformation")
    logger.info("=" * 60)

    spark = create_spark_session("SentinelPay Silver Bank Accounts")

    try:
        df = spark.read.format("delta").load(BRONZE_PATH)

        silver_df = (
            df.select(
                F.trim("bank_account_id").alias("bank_account_id"),
                F.trim("customer_id").alias("customer_id"),
                F.trim("bank_name").alias("bank_name"),
                F.trim("account_number").alias("account_number"),
                F.trim("ifsc_code").alias("ifsc_code"),
                F.upper(F.trim("account_type")).alias("account_type"),
                F.upper(F.trim("is_primary")).alias("is_primary"),
                F.upper(F.trim("account_status")).alias("account_status"),
                F.to_timestamp("created_at").alias("created_at"),
                F.to_timestamp("updated_at").alias("updated_at"),
                F.col("kafka_timestamp"),
                F.col("bronze_ingested_at"),
            )
            .dropDuplicates(["bank_account_id"])
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
                    "target.bank_account_id = source.bank_account_id",
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

        logger.info("Silver bank accounts transformation completed successfully.")

    finally:
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()

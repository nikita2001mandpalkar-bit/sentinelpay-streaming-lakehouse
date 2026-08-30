from pyspark.sql.functions import col,count,current_timestamp,sum,when
from data_generator.logger import get_logger
from src.utils.paths import GOLD_BASE_PATH,SILVER_PATHS
from src.utils.spark_session import create_spark_session

logger=get_logger(__name__)

GOLD_PAYMENT_PATH=f"{GOLD_BASE_PATH}/payment_summary"

def main()->None:
    logger.info("="*60)
    logger.info("Starting Gold Payment Transformation")
    logger.info("="*60)

    spark=create_spark_session("Sentinel Gold Event Payment")

    try:
        silver_df=(
            spark.read
            .format("delta")
            .load(SILVER_PATHS["event.payment"])
        )

        gold_df=(
            silver_df.groupBy(
                "currency",
                "payment_method",
                "transaction_status",
            )
            .agg(
                count("*").alias("transaction_count"),
                sum("amount").alias("total_amount"),
                sum(
                    when(
                        col("transaction_status")=="SUCCESS",
                        col("amount"),
                    ).otherwise(0)
                ).alias("successful_amount"),
                sum(
                    when(
                        col("transaction_status")=="FAILED",
                        col("amount"),
                    ).otherwise(0)
                ).alias("failed_Amount"),
            )
            .withColumn("gold_processed_at",current_timestamp(),)
        )
        (
            gold_df.write
            .format("delta")
            .mode("overwrite")
            .save(GOLD_PAYMENT_PATH)
        )

        logger.info(f"Gold payment summary saved to {GOLD_PAYMENT_PATH}")

    except Exception:
        logger.exception("Gold payment transformation failed....")
        raise

    finally:
        spark.stop()
        logger.info("Spark session stopped")

if __name__=="__main__":
    main()


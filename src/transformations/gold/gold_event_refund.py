from pyspark.sql.functions import count,current_timestamp,sum
from data_generator.logger import get_logger
from src.utils.paths import GOLD_BASE_PATH,SILVER_PATHS
from src.utils.spark_session import create_spark_session


logger=get_logger(__name__)

GOLD_REFUND_PATH=f"{GOLD_BASE_PATH}/refund_summary"

def main()->None:
    logger.info("="*60)
    logger.info("Starting Gold Refund Transformation")
    logger.info("="*60)

    spark=create_spark_session("SentinelPay Gold Event Refund")

    try:
        logger.info("Reading silver event.refund table.....")
        silver_df=(
            spark.read
            .format("delta")
            .load(SILVER_PATHS["event.refund"])
        )

        logger.info("Building gold refund summary....")
        gold_df=(
            silver_df.groupBy(
                "refund_status",
                "refund_reason"
            )
            .agg(
                count("*").alias("refund_count"),
                sum("refund_amount").alias("total_refund_amount"),
            )
            .withColumn(
                "gold_processed_at",
                current_timestamp(),
            )
        )

        logger.info("Writing gold refund summary to MinIO....")
        (
            gold_df.write
            .format("delta")
            .mode("overwrite")
            .save(GOLD_REFUND_PATH)
        )

        logger.info(f"Gold refund summary to {GOLD_REFUND_PATH}")

    except Exception:
        logger.exception("Gold refund transformation failed")
        raise

    finally:
        spark.stop()
        logger.info("Spark session stopped")

if __name__=="__main__":
    main()
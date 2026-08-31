from pyspark.sql import functions as F

from data_generator.logger import get_logger
from src.utils.paths import GOLD_PATHS, SILVER_PATHS
from src.utils.spark_session import create_spark_session

logger = get_logger(__name__)

GOLD_RECONCILIATION_PATH = GOLD_PATHS["finance.reconciliation"]


def main() -> None:
    logger.info("=" * 60)
    logger.info("Starting Gold Reconciliation Transformation")
    logger.info("=" * 60)

    spark = create_spark_session("SentinelPay Gold Reconciliation")

    try:
        logger.info("Reading silver payment, refund, and settlement datasets...")
        payments_df = (
            spark.read
            .format("delta")
            .load(SILVER_PATHS["event.payment"])
        )
        refunds_df = (
            spark.read
            .format("delta")
            .load(SILVER_PATHS["event.refund"])
        )
        settlements_df = (
            spark.read
            .format("delta")
            .load(SILVER_PATHS["batch.settlement"])
        )

        logger.info("Aggregating successful payments by merchant...")
        successful_payments_df = (
            payments_df
            .filter(F.col("transaction_status") == "SUCCESS")
            .select(
                "transaction_id",
                "merchant_id",
                F.col("amount").cast("decimal(18,2)").alias("amount"),
            )
        )

        payment_summary_df = (
            successful_payments_df
            .groupBy("merchant_id")
            .agg(
                F.count("*").alias("successful_payment_count"),
                F.sum("amount").alias("successful_payment_amount"),
            )
        )

        logger.info("Attributing successful refunds back to merchants...")
        successful_refunds_df = (
            refunds_df
            .filter(F.col("refund_status") == "COMPLETED")
            .select(
                "transaction_id",
                F.col("refund_amount").cast("decimal(18,2)").alias("refund_amount"),
            )
        )

        refund_summary_df = (
            successful_refunds_df
            .join(
                successful_payments_df.select("transaction_id", "merchant_id"),
                on="transaction_id",
                how="inner",
            )
            .groupBy("merchant_id")
            .agg(
                F.count("*").alias("completed_refund_count"),
                F.sum("refund_amount").alias("completed_refund_amount"),
            )
        )

        logger.info("Aggregating completed settlements by merchant...")
        settlement_summary_df = (
            settlements_df
            .filter(F.col("settlement_status") == "COMPLETED")
            .select(
                "merchant_id",
                F.col("settlement_amount").cast("decimal(18,2)").alias(
                    "settlement_amount"
                ),
            )
            .groupBy("merchant_id")
            .agg(
                F.count("*").alias("completed_settlement_count"),
                F.sum("settlement_amount").alias("completed_settlement_amount"),
            )
        )

        logger.info("Building merchant-level reconciliation output...")
        reconciliation_df = (
            payment_summary_df.alias("payments")
            .join(
                refund_summary_df.alias("refunds"),
                on="merchant_id",
                how="full_outer",
            )
            .join(
                settlement_summary_df.alias("settlements"),
                on="merchant_id",
                how="full_outer",
            )
            .na.fill(
                {
                    "successful_payment_count": 0,
                    "successful_payment_amount": 0.0,
                    "completed_refund_count": 0,
                    "completed_refund_amount": 0.0,
                    "completed_settlement_count": 0,
                    "completed_settlement_amount": 0.0,
                }
            )
            .withColumn(
                "net_payable_amount",
                F.col("successful_payment_amount") - F.col("completed_refund_amount"),
            )
            .withColumn(
                "settlement_gap_amount",
                F.col("net_payable_amount") - F.col("completed_settlement_amount"),
            )
            .withColumn(
                "reconciliation_status",
                F.when(
                    (F.col("successful_payment_count") > 0)
                    & (F.col("completed_settlement_count") == 0),
                    F.lit("MISSING_SETTLEMENT"),
                )
                .when(
                    (F.col("successful_payment_count") == 0)
                    & (F.col("completed_settlement_count") > 0),
                    F.lit("SETTLEMENT_WITHOUT_PAYMENT"),
                )
                .when(
                    F.abs(F.col("settlement_gap_amount")) <= F.lit(0.01),
                    F.lit("MATCHED"),
                )
                .when(
                    F.col("settlement_gap_amount") > 0,
                    F.lit("UNDER_SETTLED"),
                )
                .otherwise(F.lit("OVER_SETTLED"))
            )
            .withColumn("gold_processed_at", F.current_timestamp())
        )

        logger.info("Writing reconciliation output to MinIO...")
        (
            reconciliation_df.write
            .format("delta")
            .mode("overwrite")
            .save(GOLD_RECONCILIATION_PATH)
        )

        logger.info(
            "Gold reconciliation output saved to %s",
            GOLD_RECONCILIATION_PATH,
        )

    except Exception:
        logger.exception("Gold reconciliation transformation failed.")
        raise

    finally:
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()

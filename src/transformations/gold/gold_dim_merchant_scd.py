from pyspark.sql import functions as F
from pyspark.sql.window import Window

from data_generator.logger import get_logger
from src.utils.paths import BRONZE_PATHS, GOLD_PATHS
from src.utils.spark_session import create_spark_session

logger = get_logger(__name__)

GOLD_MERCHANT_DIMENSION_PATH = GOLD_PATHS["dimension.merchant_scd"]


def main() -> None:
    logger.info("=" * 60)
    logger.info("Starting Gold Merchant SCD Type 2 Transformation")
    logger.info("=" * 60)

    spark = create_spark_session("SentinelPay Gold Merchant SCD")

    try:
        logger.info("Reading Bronze merchant master dataset...")
        merchants_df = (
            spark.read
            .format("delta")
            .load(BRONZE_PATHS["master.merchant"])
        )

        normalized_df = (
            merchants_df
            .select(
                F.trim("merchant_id").alias("merchant_id"),
                F.trim("merchant_name").alias("merchant_name"),
                F.trim("merchant_category").alias("merchant_category"),
                F.lower(F.trim("merchant_email")).alias("merchant_email"),
                F.trim("merchant_phone").alias("merchant_phone"),
                F.upper(F.trim("merchant_status")).alias("merchant_status"),
                F.trim("city").alias("city"),
                F.trim("state").alias("state"),
                F.trim("country").alias("country"),
                F.to_timestamp("created_at").alias("created_at"),
                F.coalesce(
                    F.to_timestamp("updated_at"),
                    F.to_timestamp("created_at"),
                ).alias("source_updated_at"),
                F.col("bronze_ingested_at"),
                F.col("offset"),
            )
            .filter(
                F.col("merchant_id").isNotNull()
                & F.col("source_updated_at").isNotNull()
            )
        )

        logger.info("Deduplicating repeated merchant versions...")
        dedupe_window = Window.partitionBy(
            "merchant_id",
            "source_updated_at",
        ).orderBy(
            F.col("bronze_ingested_at").desc_nulls_last(),
            F.col("offset").desc_nulls_last(),
        )

        merchant_versions_df = (
            normalized_df
            .withColumn("record_rank", F.row_number().over(dedupe_window))
            .filter(F.col("record_rank") == 1)
            .drop("record_rank")
        )

        logger.info("Building SCD Type 2 history by merchant...")
        history_window = Window.partitionBy("merchant_id").orderBy(
            F.col("source_updated_at").asc_nulls_last(),
            F.col("bronze_ingested_at").asc_nulls_last(),
        )

        merchant_dimension_df = (
            merchant_versions_df
            .withColumn("effective_from", F.col("source_updated_at"))
            .withColumn(
                "effective_to",
                F.lead("source_updated_at").over(history_window),
            )
            .withColumn(
                "is_current",
                F.col("effective_to").isNull(),
            )
            .withColumn(
                "merchant_version_sk",
                F.sha2(
                    F.concat_ws(
                        "||",
                        F.col("merchant_id"),
                        F.col("effective_from").cast("string"),
                    ),
                    256,
                ),
            )
            .withColumn("gold_processed_at", F.current_timestamp())
            .select(
                "merchant_version_sk",
                "merchant_id",
                "merchant_name",
                "merchant_category",
                "merchant_email",
                "merchant_phone",
                "merchant_status",
                "city",
                "state",
                "country",
                "created_at",
                "source_updated_at",
                "effective_from",
                "effective_to",
                "is_current",
                "gold_processed_at",
            )
        )

        logger.info("Writing merchant SCD dimension to MinIO...")
        (
            merchant_dimension_df.write
            .format("delta")
            .mode("overwrite")
            .save(GOLD_MERCHANT_DIMENSION_PATH)
        )

        logger.info(
            "Gold merchant SCD dimension saved to %s",
            GOLD_MERCHANT_DIMENSION_PATH,
        )

    except Exception:
        logger.exception("Gold merchant SCD transformation failed.")
        raise

    finally:
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()

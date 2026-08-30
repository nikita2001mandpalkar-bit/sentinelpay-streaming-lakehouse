from pyspark.sql import SparkSession


def write_result_summary(
    spark: SparkSession,
    summary: dict,
    output_path: str,
) -> None:
    summary_df = spark.createDataFrame([summary])

    (
        summary_df.write
        .format("delta")
        .mode("append")
        .save(output_path)
    )
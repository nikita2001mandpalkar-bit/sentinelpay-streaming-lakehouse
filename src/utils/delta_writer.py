"""
Reusable Delta stream writer for SentinelPay Spark jobs.
"""

from pyspark.sql import DataFrame


def write_stream_to_delta(
    dataframe: DataFrame,
    output_path: str,
    checkpoint_path: str,
    query_name: str,
    output_mode: str = "append",
):
    """
    Write a streaming DataFrame to a Delta table path.
    """

    return (
        dataframe.writeStream
        .format("delta")
        .outputMode(output_mode)
        .option(
            "checkpointLocation",
            checkpoint_path,
        )
        .option("mergeSchema","true")
        .queryName(query_name)
        .start(output_path)
    )
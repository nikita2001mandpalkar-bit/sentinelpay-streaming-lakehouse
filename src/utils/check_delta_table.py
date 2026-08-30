"""Utility script to inspect a Delta table from MinIO."""

import sys

from src.utils.spark_session import create_spark_session


def print_table(dataframe, limit: int = 20) -> None:
    rows = dataframe.limit(limit).collect()
    columns = dataframe.columns

    if not rows:
        print("No rows found.")
        return

    string_rows = []
    for row in rows:
        string_rows.append(
            [
                "" if value is None else str(value)
                for value in row
            ]
        )

    widths = []
    for index, column in enumerate(columns):
        max_width = len(column)
        for row in string_rows:
            max_width = max(max_width, len(row[index]))
        widths.append(max_width)

    header = " | ".join(
        column.ljust(widths[index])
        for index, column in enumerate(columns)
    )
    separator = "-+-".join(
        "-" * widths[index]
        for index in range(len(columns))
    )

    print(header)
    print(separator)

    for row in string_rows:
        print(
            " | ".join(
                row[index].ljust(widths[index])
                for index in range(len(columns))
            )
        )


def main() -> None:
    if len(sys.argv) != 2:
        raise ValueError(
            "Usage: python -m src.utils.check_delta_table <delta_path>"
        )

    delta_path = sys.argv[1]
    spark = create_spark_session("Check Delta Table")

    try:
        df = (
            spark.read
            .format("delta")
            .load(delta_path)
        )

        print("\n========== Schema ==========")
        df.printSchema()

        print("\n========== Row Count ==========")
        print(df.count())

        print("\n========== Sample Rows ==========")
        print_table(df, limit=20)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
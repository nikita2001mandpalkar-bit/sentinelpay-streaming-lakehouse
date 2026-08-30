"""
Scale SentinelPay datasets for performance testing.
"""

import uuid
from datetime import timedelta
from pathlib import Path

import pandas as pd

from config import OUTPUT_DIR
from logger import get_logger

logger = get_logger(__name__)


def build_scaled_chunk(
    transactions_df: pd.DataFrame,
    copy_number: int,
) -> pd.DataFrame:
    df = transactions_df.copy()

    df["transaction_id"] = [
        str(uuid.uuid4()) for _ in range(len(df))
    ]

    df["reference_number"] = [
        f"TXN{uuid.uuid4().hex[:16].upper()}"
        for _ in range(len(df))
    ]

    df["transaction_timestamp"] = (
        pd.to_datetime(df["transaction_timestamp"], errors="coerce")
        + timedelta(days=copy_number)
    )

    df["created_at"] = (
        pd.to_datetime(df["created_at"], errors="coerce")
        + timedelta(days=copy_number)
    )

    return df


def save_scaled_transactions(
    transactions_df: pd.DataFrame,
    target_rows: int,
) -> None:
    if transactions_df.empty:
        raise ValueError("Input transaction dataset is empty.")

    output_directory = Path(OUTPUT_DIR) / "csv"
    output_directory.mkdir(parents=True, exist_ok=True)

    output_file = output_directory / f"payment_transactions_{target_rows}.csv"

    rows_written = 0
    copy_number = 0
    header_written = False
    base_rows = len(transactions_df)

    logger.info(
        f"Scaling transactions from {base_rows:,} rows to {target_rows:,} rows..."
    )

    while rows_written < target_rows:
        chunk_df = build_scaled_chunk(transactions_df, copy_number)

        remaining_rows = target_rows - rows_written
        if len(chunk_df) > remaining_rows:
            chunk_df = chunk_df.head(remaining_rows)

        chunk_df.to_csv(
            output_file,
            mode="a" if header_written else "w",
            header=not header_written,
            index=False,
        )

        rows_written += len(chunk_df)
        header_written = True
        copy_number += 1

        logger.info(
            f"Written {rows_written:,} / {target_rows:,} rows..."
        )

    logger.info(f"Scaled transactions saved to {output_file}")


if __name__ == "__main__":
    transaction_file = Path(OUTPUT_DIR) / "csv" / "payment_transactions.csv"

    if not transaction_file.exists():
        logger.critical("payment_transactions.csv not found.")
        raise FileNotFoundError(transaction_file)

    transactions_df = pd.read_csv(transaction_file)

    print("\n========== Scale Data ==========")
    print("1. 500K Records")
    print("2. 1 Million Records")
    print("3. 5 Million Records")
    print("================================\n")

    choice = input("Select option (1-3): ").strip()

    target_map = {
        "1": 500_000,
        "2": 1_000_000,
        "3": 5_000_000,
    }

    if choice not in target_map:
        raise ValueError("Invalid choice.")

    save_scaled_transactions(
        transactions_df,
        target_map[choice],
    )

    logger.info("Data scaling completed successfully.")
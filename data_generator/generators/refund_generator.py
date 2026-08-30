"""
Generate refund data for SentinelPay.
"""

import random
import uuid
from datetime import timedelta
from pathlib import Path

import pandas as pd

from config import NUM_REFUNDS, OUTPUT_DIR
from logger import get_logger
from master_data import (
    FAILURE_REASONS,
    REFUND_STATUS,
)

logger = get_logger(__name__)


# --------------------------------------------------
# Helper Functions
# --------------------------------------------------

def generate_refund_timestamp(
    transaction_timestamp,
):
    """
    Generate refund timestamp.

    Refund occurs between 1 and 7 days
    after the original transaction.
    """

    days = random.randint(1, 7)

    return (
        pd.to_datetime(transaction_timestamp)
        + timedelta(days=days)
    )


# --------------------------------------------------
# Refund Generator
# --------------------------------------------------

def generate_refunds(
    transactions_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Generate refund records.

    Args:
        transactions_df:
            Transaction dataframe.

    Returns:
        Refund dataframe.
    """

    logger.info(
        f"Generating {NUM_REFUNDS:,} refunds..."
    )

    refunds = []

    try:

        successful_transactions = transactions_df[
            transactions_df["transaction_status"] == "SUCCESS"
        ]

        if successful_transactions.empty:
            raise ValueError(
                "No successful transactions available for refund generation."
            )

        sampled_transactions = successful_transactions.sample(
            n=min(
                NUM_REFUNDS,
                len(successful_transactions),
            ),
            replace=False,
        )

        for row in sampled_transactions.itertuples(
            index=False
        ):
            refund_timestamp = generate_refund_timestamp(
                row.transaction_timestamp
            )

            refund = {
                "refund_id": str(uuid.uuid4()),
                "transaction_id": row.transaction_id,
                "refund_amount": row.amount,
                "refund_reason": (
                    FAILURE_REASONS
                    .sample(1)
                    .iloc[0]["failure_reason"]
                ),
                "refund_status": (
                    REFUND_STATUS
                    .sample(1)
                    .iloc[0]["status"]
                ),
                "refund_timestamp": refund_timestamp,
                "created_at": refund_timestamp,
            }

            refunds.append(refund)

        refunds_df = pd.DataFrame(refunds)

        logger.info(
            f"Successfully generated {len(refunds_df):,} refunds."
        )

        output_directory = (
            Path(OUTPUT_DIR)
            / "csv"
        )

        output_directory.mkdir(
            parents=True,
            exist_ok=True,
        )

        output_file = (
            output_directory
            / "refunds.csv"
        )

        refunds_df.to_csv(
            output_file,
            index=False,
        )

        logger.info(
            f"Refund data saved to {output_file}"
        )

        return refunds_df

    except Exception:
        logger.exception(
            "Refund generation failed."
        )
        raise


# --------------------------------------------------
# Main
# --------------------------------------------------

if __name__ == "__main__":

    transaction_file = (
        Path(OUTPUT_DIR)
        / "csv"
        / "payment_transactions.csv"
    )

    if not transaction_file.exists():

        logger.critical(
            "payment_transactions.csv not found."
        )

        raise FileNotFoundError(
            transaction_file
        )

    transactions_df = pd.read_csv(
        transaction_file
    )

    generate_refunds(
        transactions_df
    )
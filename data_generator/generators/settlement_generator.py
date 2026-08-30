"""
Generate settlement data for SentinelPay.
"""

import random
import uuid
from datetime import timedelta
from pathlib import Path

import pandas as pd

from config import NUM_SETTLEMENTS, OUTPUT_DIR
from logger import get_logger

logger = get_logger(__name__)


# --------------------------------------------------
# Settlement Generator
# --------------------------------------------------

def generate_settlements(
    merchants_df: pd.DataFrame,
    transactions_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Generate merchant settlement records.

    Args:
        merchants_df:
            Merchant dataframe.

        transactions_df:
            Transaction dataframe.

    Returns:
        Settlement dataframe.
    """

    logger.info(
        f"Generating {NUM_SETTLEMENTS:,} settlements..."
    )

    settlements = []

    try:

        successful_transactions = transactions_df[
            transactions_df["transaction_status"] == "SUCCESS"
        ].copy()

        if successful_transactions.empty:
            raise ValueError(
                "No successful transactions available for settlement generation."
            )

        successful_transactions[
            "transaction_timestamp"
        ] = pd.to_datetime(
            successful_transactions["transaction_timestamp"]
        )

        merchant_totals = (
            successful_transactions
            .groupby("merchant_id", as_index=False)
            .agg(
                settlement_amount=("amount", "sum"),
                latest_transaction_timestamp=(
                    "transaction_timestamp",
                    "max",
                ),
            )
        )

        if merchant_totals.empty:
            raise ValueError(
                "No merchant totals available for settlement generation."
            )

        sampled_merchants = merchant_totals.sample(
            n=min(
                NUM_SETTLEMENTS,
                len(merchant_totals),
            ),
            replace=False,
        )

        active_merchants = set(
            merchants_df[
                merchants_df["merchant_status"] == "ACTIVE"
            ]["merchant_id"]
        )

        for row in sampled_merchants.itertuples(
            index=False
        ):
            settlement_timestamp = (
                row.latest_transaction_timestamp
                + timedelta(days=random.randint(1, 3))
            )

            settlement_status = random.choice(
                [
                    "COMPLETED",
                    "COMPLETED",
                    "COMPLETED",
                    "PROCESSING",
                    "FAILED",
                ]
            )

            settlement = {
                "settlement_id": str(uuid.uuid4()),
                "merchant_id": row.merchant_id,
                "settlement_amount": round(
                    row.settlement_amount,
                    2,
                ),
                "settlement_date": settlement_timestamp.date(),
                "settlement_status": settlement_status,
                "merchant_status": (
                    "ACTIVE"
                    if row.merchant_id in active_merchants
                    else "INACTIVE"
                ),
                "created_at": settlement_timestamp,
            }

            settlements.append(settlement)

        settlements_df = pd.DataFrame(
            settlements
        )

        logger.info(
            f"Successfully generated {len(settlements_df):,} settlements."
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
            / "settlements.csv"
        )

        settlements_df.to_csv(
            output_file,
            index=False,
        )

        logger.info(
            f"Settlement data saved to {output_file}"
        )

        return settlements_df

    except Exception:
        logger.exception(
            "Settlement generation failed."
        )
        raise


# --------------------------------------------------
# Main
# --------------------------------------------------

if __name__ == "__main__":

    merchant_file = (
        Path(OUTPUT_DIR)
        / "csv"
        / "merchants.csv"
    )

    transaction_file = (
        Path(OUTPUT_DIR)
        / "csv"
        / "payment_transactions.csv"
    )

    for file in [
        merchant_file,
        transaction_file,
    ]:
        if not file.exists():

            logger.critical(
                f"{file.name} not found."
            )

            raise FileNotFoundError(file)

    merchants_df = pd.read_csv(
        merchant_file
    )

    transactions_df = pd.read_csv(
        transaction_file
    )

    generate_settlements(
        merchants_df,
        transactions_df,
    )
"""
Generate payment transactions for SentinelPay.
"""

import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from config import NUM_TRANSACTIONS, OUTPUT_DIR
from logger import get_logger
from master_data import PAYMENT_METHODS

logger = get_logger(__name__)


# --------------------------------------------------
# Helper Functions
# --------------------------------------------------

def generate_reference_number() -> str:
    """
    Generate a unique transaction reference number.
    """

    return f"TXN{uuid.uuid4().hex[:16].upper()}"


def generate_transaction_timestamp() -> datetime:
    """
    Generate transaction timestamp
    within the last 90 days.
    """

    now = datetime.now()

    random_days = random.randint(0, 90)

    random_seconds = random.randint(
        0,
        86400,
    )

    return now - timedelta(
        days=random_days,
        seconds=random_seconds,
    )


def generate_amount(
    merchant_category: str,
) -> float:
    """
    Generate realistic transaction amount
    based on merchant category.
    """

    ranges = {
        "Grocery": (50, 5000),
        "Fuel": (200, 4000),
        "Electronics": (2000, 200000),
        "Healthcare": (500, 50000),
        "Travel": (500, 100000),
        "Food": (100, 3000),
        "Fashion": (500, 25000),
        "Entertainment": (100, 10000),
    }

    minimum, maximum = ranges.get(
        merchant_category,
        (100, 10000),
    )

    return round(
        random.uniform(
            minimum,
            maximum,
        ),
        2,
    )


def generate_transaction_status() -> str:
    """
    Generate weighted transaction status.
    """

    return random.choices(
        population=[
            "SUCCESS",
            "FAILED",
            "PENDING",
        ],
        weights=[
            92,
            5,
            3,
        ],
        k=1,
    )[0]


# --------------------------------------------------
# Transaction Generator
# --------------------------------------------------

def generate_transactions(
    wallets_df: pd.DataFrame,
    merchants_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Generate payment transactions.

    Args:
        wallets_df:
            Wallet dataframe.

        merchants_df:
            Merchant dataframe.

    Returns:
        Transaction dataframe.
    """

    logger.info(
        f"Generating {NUM_TRANSACTIONS:,} payment transactions..."
    )

    transactions = []

    try:

        active_wallets = wallets_df[
            wallets_df["wallet_status"] == "ACTIVE"
        ]

        active_merchants = merchants_df[
            merchants_df["merchant_status"] == "ACTIVE"
        ]

        wallet_pool = (
            active_wallets
            if not active_wallets.empty
            else wallets_df
        )

        merchant_pool = (
            active_merchants
            if not active_merchants.empty
            else merchants_df
        )

        for _ in range(NUM_TRANSACTIONS):

            wallet = wallet_pool.sample(1).iloc[0]

            merchant = merchant_pool.sample(1).iloc[0]

            transaction_timestamp = (
                generate_transaction_timestamp()
            )

            transaction = {
                "transaction_id": str(uuid.uuid4()),
                "wallet_id": wallet["wallet_id"],
                "merchant_id": merchant["merchant_id"],
                "amount": generate_amount(
                    merchant["merchant_category"]
                ),
                "currency": wallet["currency"],
                "payment_method": (
                    PAYMENT_METHODS
                    .sample(1)
                    .iloc[0]["payment_method"]
                ),
                "transaction_status": (
                    generate_transaction_status()
                ),
                "reference_number": (
                    generate_reference_number()
                ),
                "transaction_timestamp": (
                    transaction_timestamp
                ),
                "created_at": transaction_timestamp,
            }

            transactions.append(transaction)

        transactions_df = pd.DataFrame(
            transactions
        )

        logger.info(
            f"Successfully generated {len(transactions_df):,} transactions."
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
            / "payment_transactions.csv"
        )

        transactions_df.to_csv(
            output_file,
            index=False,
        )

        logger.info(
            f"Transaction data saved to {output_file}"
        )

        return transactions_df

    except Exception:
        logger.exception(
            "Failed to generate transactions."
        )
        raise


# --------------------------------------------------
# Main
# --------------------------------------------------

if __name__ == "__main__":

    wallet_file = (
        Path(OUTPUT_DIR)
        / "csv"
        / "wallets.csv"
    )

    merchant_file = (
        Path(OUTPUT_DIR)
        / "csv"
        / "merchants.csv"
    )

    for file in [
        wallet_file,
        merchant_file,
    ]:
        if not file.exists():

            logger.critical(
                f"{file.name} not found."
            )

            raise FileNotFoundError(file)

    wallets_df = pd.read_csv(
        wallet_file
    )

    merchants_df = pd.read_csv(
        merchant_file
    )

    generate_transactions(
        wallets_df,
        merchants_df,
    )
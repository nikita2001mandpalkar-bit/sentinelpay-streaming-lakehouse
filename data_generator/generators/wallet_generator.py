"""
Generate wallet data for SentinelPay.
"""

import random
import uuid
from datetime import datetime
from pathlib import Path

import pandas as pd

from config import OUTPUT_DIR
from logger import get_logger
from master_data import CURRENCIES

logger = get_logger(__name__)


def generate_wallet_balance() -> float:
    """
    Generate a realistic wallet balance.
    """

    return round(
        random.uniform(0, 100000),
        2,
    )


def generate_wallets(
    customers_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Generate wallets for customers.

    Args:
        customers_df:
            Customer dataframe.

    Returns:
        Wallet dataframe.
    """

    logger.info(
        "Generating wallets..."
    )

    wallets = []

    try:

        for row in customers_df.itertuples(
            index=False
        ):
            current_timestamp = datetime.now()

            currency = (
                CURRENCIES
                .sample(1)
                .iloc[0]["currency_code"]
            )

            wallet = {
                "wallet_id": str(uuid.uuid4()),
                "customer_id": row.customer_id,
                "wallet_balance": generate_wallet_balance(),
                "currency": currency,
                "wallet_status": random.choice(
                    [
                        "ACTIVE",
                        "ACTIVE",
                        "ACTIVE",
                        "ACTIVE",
                        "BLOCKED",
                        "CLOSED",
                    ]
                ),
                "created_at": current_timestamp,
                "updated_at": current_timestamp,
            }

            wallets.append(wallet)

        wallets_df = pd.DataFrame(
            wallets
        )

        logger.info(
            f"Successfully generated {len(wallets_df):,} wallets."
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
            / "wallets.csv"
        )

        wallets_df.to_csv(
            output_file,
            index=False,
        )

        logger.info(
            f"Wallet data saved to {output_file}"
        )

        return wallets_df

    except Exception:
        logger.exception(
            "Failed to generate wallet data."
        )
        raise


if __name__ == "__main__":

    customer_file = (
        Path(OUTPUT_DIR)
        / "csv"
        / "customers.csv"
    )

    if not customer_file.exists():

        logger.critical(
            "customers.csv not found. Generate customers first."
        )

        raise FileNotFoundError(
            customer_file
        )

    customers_df = pd.read_csv(
        customer_file
    )

    generate_wallets(
        customers_df
    )
"""
Generate bank account data for SentinelPay.
"""

import random
import uuid
from datetime import datetime
from pathlib import Path

import pandas as pd

from config import OUTPUT_DIR
from logger import get_logger
from master_data import BANKS

logger = get_logger(__name__)


def generate_account_number() -> str:
    """
    Generate a random 14-digit bank account number.
    """

    return "".join(
        random.choices(
            "0123456789",
            k=14,
        )
    )


def generate_ifsc_code(
    ifsc_prefix: str,
) -> str:
    """
    Generate IFSC code.

    Example:
        SBIN0001234
    """

    branch_code = "".join(
        random.choices(
            "0123456789",
            k=7,
        )
    )

    return f"{ifsc_prefix}{branch_code}"


def generate_bank_accounts(
    customers_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Generate bank account records.

    Args:
        customers_df:
            Customer dataframe.

    Returns:
        Bank account dataframe.
    """

    logger.info(
        "Generating bank accounts..."
    )

    bank_accounts = []
    primary_created = set()

    try:

        for row in customers_df.itertuples(
            index=False
        ):
            customer_id = row.customer_id

            number_of_accounts = random.randint(
                1,
                3,
            )

            for _ in range(number_of_accounts):
                bank = BANKS.sample(1).iloc[0]

                current_timestamp = datetime.now()

                bank_account = {
                    "bank_account_id": str(uuid.uuid4()),
                    "customer_id": customer_id,
                    "bank_name": bank["bank_name"],
                    "account_number": generate_account_number(),
                    "ifsc_code": generate_ifsc_code(
                        bank["ifsc_prefix"]
                    ),
                    "account_type": random.choice(
                        [
                            "SAVINGS",
                            "CURRENT",
                            "SALARY",
                        ]
                    ),
                    "is_primary": (
                        customer_id not in primary_created
                    ),
                    "account_status": random.choice(
                        [
                            "ACTIVE",
                            "ACTIVE",
                            "ACTIVE",
                            "ACTIVE",
                            "INACTIVE",
                        ]
                    ),
                    "created_at": current_timestamp,
                    "updated_at": current_timestamp,
                }

                bank_accounts.append(
                    bank_account
                )

                primary_created.add(
                    customer_id
                )

        bank_accounts_df = pd.DataFrame(
            bank_accounts
        )

        logger.info(
            f"Successfully generated {len(bank_accounts_df):,} bank accounts."
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
            / "bank_accounts.csv"
        )

        bank_accounts_df.to_csv(
            output_file,
            index=False,
        )

        logger.info(
            f"Bank account data saved to {output_file}"
        )

        return bank_accounts_df

    except Exception:
        logger.exception(
            "Failed to generate bank account data."
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

    generate_bank_accounts(
        customers_df
    )
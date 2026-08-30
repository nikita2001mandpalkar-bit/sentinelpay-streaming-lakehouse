"""
Generate merchant data for SentinelPay.
"""

import random
import uuid
from datetime import datetime
from pathlib import Path

import pandas as pd
from faker import Faker

from config import NUM_MERCHANTS, OUTPUT_DIR
from logger import get_logger
from master_data import (
    CITIES,
    MERCHANT_CATEGORIES,
)

logger = get_logger(__name__)

fake = Faker("en_IN")


def generate_merchants() -> pd.DataFrame:
    """
    Generate merchant records.

    Returns:
        Merchant dataset.
    """

    merchants = []

    logger.info(
        f"Generating {NUM_MERCHANTS:,} merchants..."
    )

    try:

        for _ in range(NUM_MERCHANTS):

            merchant_id = str(uuid.uuid4())

            merchant_name = fake.company()

            merchant_category = (
                MERCHANT_CATEGORIES
                .sample(1)
                .iloc[0]["category"]
            )

            city_data = CITIES.sample(1).iloc[0]

            merchant_email = (
                merchant_name.lower()
                .replace(" ", "")
                .replace(",", "")
                .replace(".", "")
                + str(random.randint(100, 999))
                + "@business.com"
            )

            merchant_phone = (
                random.choice(["6", "7", "8", "9"])
                + "".join(
                    random.choices(
                        "0123456789",
                        k=9,
                    )
                )
            )

            merchant_status = random.choice(
                [
                    "ACTIVE",
                    "ACTIVE",
                    "ACTIVE",
                    "ACTIVE",
                    "INACTIVE",
                ]
            )

            current_timestamp = datetime.now()

            merchant = {
                "merchant_id": merchant_id,
                "merchant_name": merchant_name,
                "merchant_category": merchant_category,
                "merchant_email": merchant_email,
                "merchant_phone": merchant_phone,
                "merchant_status": merchant_status,
                "city": city_data["city"],
                "state": city_data["state"],
                "country": "India",
                "created_at": current_timestamp,
                "updated_at": current_timestamp,
            }

            merchants.append(merchant)

        merchants_df = pd.DataFrame(
            merchants
        )

        logger.info(
            f"Successfully generated {len(merchants_df):,} merchants."
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
            / "merchants.csv"
        )

        merchants_df.to_csv(
            output_file,
            index=False,
        )

        logger.info(
            f"Merchant data saved to {output_file}"
        )

        return merchants_df

    except Exception:
        logger.exception(
            "Failed to generate merchant data."
        )
        raise


if __name__ == "__main__":
    generate_merchants()
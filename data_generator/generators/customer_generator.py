"""
Generate customer data for SentinelPay.
"""

import random
import uuid
from datetime import datetime
from pathlib import Path

import pandas as pd

from config import NUM_CUSTOMERS, OUTPUT_DIR
from data_factory import (
    generate_date_of_birth,
    generate_email,
    generate_full_name,
    generate_phone_number,
)
from logger import get_logger
from master_data import (
    CUSTOMER_STATUS,
    KYC_STATUS,
)

logger = get_logger(__name__)


def generate_customers() -> pd.DataFrame:
    """
    Generate customer records.

    Returns:
        Customer dataset.
    """

    customers = []

    logger.info(
        f"Generating {NUM_CUSTOMERS:,} customers..."
    )

    try:

        for _ in range(NUM_CUSTOMERS):

            customer_id = str(uuid.uuid4())

            full_name = generate_full_name()

            name_parts = full_name.split()

            first_name = name_parts[0]

            last_name = (
                " ".join(name_parts[1:])
                if len(name_parts) > 1
                else ""
            )

            email = generate_email(
                first_name,
                last_name if last_name else first_name,
            )

            phone_number = generate_phone_number()

            date_of_birth = generate_date_of_birth()

            gender = random.choice(
                [
                    "Male",
                    "Female",
                ]
            )

            kyc_status = (
                KYC_STATUS.sample(1)
                .iloc[0]["kyc_status"]
            )

            customer_status = (
                CUSTOMER_STATUS.sample(1)
                .iloc[0]["status"]
            )

            current_timestamp = datetime.now()

            customer = {
                "customer_id": customer_id,
                "first_name": first_name,
                "last_name": last_name,
                "email": email,
                "phone_number": phone_number,
                "date_of_birth": date_of_birth,
                "gender": gender,
                "kyc_status": kyc_status,
                "customer_status": customer_status,
                "created_at": current_timestamp,
                "updated_at": current_timestamp,
            }

            customers.append(customer)

        customers_df = pd.DataFrame(
            customers
        )

        logger.info(
            f"Successfully generated {len(customers_df):,} customers."
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
            / "customers.csv"
        )

        customers_df.to_csv(
            output_file,
            index=False,
        )

        logger.info(
            f"Customer data saved to {output_file}"
        )

        return customers_df

    except Exception:
        logger.exception(
            "Failed to generate customer data."
        )
        raise


if __name__ == "__main__":
    generate_customers()
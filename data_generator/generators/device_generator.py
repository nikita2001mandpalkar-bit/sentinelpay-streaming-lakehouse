"""
Generate device data for SentinelPay.
"""

import random
import uuid
from datetime import datetime
from pathlib import Path

import pandas as pd

from config import OUTPUT_DIR
from logger import get_logger
from master_data import DEVICE_MODELS

logger = get_logger(__name__)


def generate_app_version() -> str:
    """
    Generate application version.
    """

    major = random.randint(1, 5)
    minor = random.randint(0, 9)
    patch = random.randint(0, 9)

    return f"{major}.{minor}.{patch}"


def generate_devices(
    customers_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Generate customer devices.

    Args:
        customers_df:
            Customer dataframe.

    Returns:
        Device dataframe.
    """

    logger.info(
        "Generating devices..."
    )

    devices = []

    try:

        for row in customers_df.itertuples(
            index=False
        ):
            number_of_devices = random.randint(
                1,
                3,
            )

            for _ in range(number_of_devices):
                device = DEVICE_MODELS.sample(1).iloc[0]

                current_timestamp = datetime.now()

                device_record = {
                    "device_id": str(uuid.uuid4()),
                    "customer_id": row.customer_id,
                    "device_type": device["device_type"],
                    "device_os": device["os"],
                    "app_version": generate_app_version(),
                    "registered_at": current_timestamp,
                }

                devices.append(
                    device_record
                )

        devices_df = pd.DataFrame(
            devices
        )

        logger.info(
            f"Successfully generated {len(devices_df):,} devices."
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
            / "devices.csv"
        )

        devices_df.to_csv(
            output_file,
            index=False,
        )

        logger.info(
            f"Device data saved to {output_file}"
        )

        return devices_df

    except Exception:
        logger.exception(
            "Failed to generate device data."
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

    generate_devices(
        customers_df
    )
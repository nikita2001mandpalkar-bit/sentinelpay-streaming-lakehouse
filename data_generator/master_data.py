"""
Load reference datasets used across SentinelPay.
"""

from pathlib import Path

import pandas as pd

from logger import get_logger

logger = get_logger(__name__)

# --------------------------------------------------
# Dataset Directory
# --------------------------------------------------

DATASET_DIR = Path(__file__).resolve().parent.parent / "datasets"


# --------------------------------------------------
# Load Dataset
# --------------------------------------------------

def load_dataset(file_name: str) -> pd.DataFrame:
    """
    Load a reference dataset from the datasets directory.

    Args:
        file_name (str): Name of the CSV file.

    Returns:
        pd.DataFrame: Loaded dataset.

    Raises:
        FileNotFoundError: If the dataset does not exist.
        pd.errors.EmptyDataError: If the CSV file is empty.
        pd.errors.ParserError: If the CSV file is malformed.
    """

    file_path = DATASET_DIR / file_name

    if not file_path.exists():
        logger.critical(f"Dataset not found: {file_name}")
        raise FileNotFoundError(f"Dataset not found: {file_path}")

    try:
        logger.info(f"Loading dataset: {file_name}")
        return pd.read_csv(file_path)

    except pd.errors.EmptyDataError:
        logger.critical(f"Dataset is empty: {file_name}")
        raise

    except pd.errors.ParserError:
        logger.critical(f"Invalid CSV format: {file_name}")
        raise


# --------------------------------------------------
# Reference Datasets
# --------------------------------------------------

BANKS = load_dataset("banks.csv")

CITIES = load_dataset("cities.csv")

MERCHANT_CATEGORIES = load_dataset("merchant_categories.csv")

DEVICE_MODELS = load_dataset("device_models.csv")

OCCUPATIONS = load_dataset("occupations.csv")

PAYMENT_METHODS = load_dataset("payment_methods.csv")

WALLET_TYPES = load_dataset("wallet_types.csv")

CUSTOMER_STATUS = load_dataset("customer_status.csv")

KYC_STATUS = load_dataset("kyc_status.csv")

TRANSACTION_STATUS = load_dataset("transaction_status.csv")

REFUND_STATUS = load_dataset("refund_status.csv")

SETTLEMENT_STATUS = load_dataset("settlement_status.csv")

EMAIL_DOMAINS = load_dataset("email_domains.csv")

UPI_APPS = load_dataset("upi_apps.csv")

FAILURE_REASONS = load_dataset("failure_reasons.csv")

CURRENCIES = load_dataset("currencies.csv")


logger.info("All reference datasets loaded successfully.")
"""
Generate application logs for SentinelPay.
"""

import random
from pathlib import Path

import pandas as pd

from config import OUTPUT_DIR
from logger import get_logger

logger = get_logger(__name__)

# --------------------------------------------------
# Output Directory
# --------------------------------------------------

LOG_DIR = Path(OUTPUT_DIR) / "logs"

LOG_DIR.mkdir(
    parents=True,
    exist_ok=True,
)


# --------------------------------------------------
# Log Messages
# --------------------------------------------------

SUCCESS_MESSAGES = [
    "Payment request received",
    "Wallet debited successfully",
    "Merchant credited successfully",
    "Transaction completed",
]

FAILED_MESSAGES = [
    "Insufficient wallet balance",
    "Payment gateway timeout",
    "Merchant service unavailable",
    "Invalid payment method",
    "Transaction failed",
    "Database connection timeout",
    "Duplicate transaction detected",
]

PENDING_MESSAGES = [
    "Payment request received",
    "Transaction pending for confirmation",
    "Awaiting downstream settlement",
]

AUDIT_MESSAGES = [
    "Customer logged in",
    "Customer logged out",
    "Wallet created",
    "Merchant onboarded",
    "KYC verified",
    "Profile updated",
]


# --------------------------------------------------
# Helper Functions
# --------------------------------------------------

def get_log_level(
    transaction_status: str,
) -> str:
    """
    Map transaction status to log level.
    """

    if transaction_status == "SUCCESS":
        return "INFO"

    if transaction_status == "PENDING":
        return "WARN"

    if transaction_status == "FAILED":
        return "ERROR"

    return "INFO"


def get_application_message(
    transaction_status: str,
) -> str:
    """
    Generate application log message
    based on transaction status.
    """

    if transaction_status == "SUCCESS":
        return random.choice(
            SUCCESS_MESSAGES
        )

    if transaction_status == "PENDING":
        return random.choice(
            PENDING_MESSAGES
        )

    if transaction_status == "FAILED":
        return random.choice(
            FAILED_MESSAGES
        )

    return "Transaction status unknown"


# --------------------------------------------------
# Application Log
# --------------------------------------------------

def generate_application_log(
    transactions_df: pd.DataFrame,
) -> None:
    """
    Generate application log.
    """

    logger.info(
        "Generating application.log..."
    )

    try:

        with open(
            LOG_DIR / "application.log",
            "w",
            encoding="utf-8",
        ) as file:

            for row in transactions_df.itertuples(
                index=False
            ):
                level = get_log_level(
                    row.transaction_status
                )

                message = get_application_message(
                    row.transaction_status
                )

                log = (
                    f"{row.transaction_timestamp} | "
                    f"{level} | "
                    f"{row.transaction_id} | "
                    f"{message}"
                )

                file.write(log + "\n")

        logger.info(
            "application.log generated."
        )

    except Exception as error:

        logger.exception(
            "Failed to generate application.log."
        )

        raise error


# --------------------------------------------------
# Error Log
# --------------------------------------------------

def generate_error_log(
    transactions_df: pd.DataFrame,
) -> None:
    """
    Generate error log for failed transactions.
    """

    logger.info(
        "Generating error.log..."
    )

    try:

        failed_transactions = transactions_df[
            transactions_df["transaction_status"] == "FAILED"
        ]

        with open(
            LOG_DIR / "error.log",
            "w",
            encoding="utf-8",
        ) as file:

            for row in failed_transactions.itertuples(
                index=False
            ):
                message = random.choice(
                    FAILED_MESSAGES
                )

                log = (
                    f"{row.transaction_timestamp} | "
                    f"ERROR | "
                    f"{row.transaction_id} | "
                    f"{message}"
                )

                file.write(log + "\n")

        logger.info(
            "error.log generated."
        )

    except Exception as error:

        logger.exception(
            "Failed to generate error.log."
        )

        raise error


# --------------------------------------------------
# Audit Log
# --------------------------------------------------

def generate_audit_log(
    customers_df: pd.DataFrame,
) -> None:
    """
    Generate audit log.
    """

    logger.info(
        "Generating audit.log..."
    )

    try:

        with open(
            LOG_DIR / "audit.log",
            "w",
            encoding="utf-8",
        ) as file:

            for row in customers_df.itertuples(
                index=False
            ):
                message = random.choice(
                    AUDIT_MESSAGES
                )

                event_timestamp = getattr(
                    row,
                    "updated_at",
                    None,
                ) or getattr(
                    row,
                    "created_at",
                    None,
                )

                log = (
                    f"{event_timestamp} | "
                    f"AUDIT | "
                    f"{row.customer_id} | "
                    f"{message}"
                )

                file.write(log + "\n")

        logger.info(
            "audit.log generated."
        )

    except Exception as error:

        logger.exception(
            "Failed to generate audit.log."
        )

        raise error


# --------------------------------------------------
# Main
# --------------------------------------------------

if __name__ == "__main__":

    csv_dir = Path(OUTPUT_DIR) / "csv"

    transaction_file = (
        csv_dir
        / "payment_transactions.csv"
    )

    customer_file = (
        csv_dir
        / "customers.csv"
    )

    for file in [
        transaction_file,
        customer_file,
    ]:
        if not file.exists():

            logger.critical(
                f"{file.name} not found."
            )

            raise FileNotFoundError(file)

    transactions_df = pd.read_csv(
        transaction_file
    )

    customers_df = pd.read_csv(
        customer_file
    )

    generate_application_log(
        transactions_df
    )

    generate_error_log(
        transactions_df
    )

    generate_audit_log(
        customers_df
    )

    logger.info("=" * 60)

    logger.info(
        "All log files generated successfully."
    )

    logger.info("=" * 60)
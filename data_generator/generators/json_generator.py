"""
Generate JSONL events from structured CSV data.
"""

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from config import OUTPUT_DIR
from logger import get_logger

logger = get_logger(__name__)

# --------------------------------------------------
# Output Directory
# --------------------------------------------------

JSON_DIR = Path(OUTPUT_DIR) / "json"

JSON_DIR.mkdir(
    parents=True,
    exist_ok=True,
)


# --------------------------------------------------
# Helper Functions
# --------------------------------------------------

def get_ingested_at() -> str:
    """
    Generate ingestion timestamp.
    """

    return datetime.now(
        timezone.utc
    ).isoformat()


def write_jsonl_record(
    output_file,
    record: dict,
) -> None:
    """
    Write one JSON record.
    """

    output_file.write(
        json.dumps(
            record,
            default=str,
        ) + "\n"
    )


# --------------------------------------------------
# Payment Events
# --------------------------------------------------

def generate_payment_events(
    transactions_df: pd.DataFrame,
) -> None:
    """
    Generate payment events.
    """

    logger.info(
        "Generating payment_events.jsonl..."
    )

    try:

        ingested_at = get_ingested_at()

        output_file = (
            JSON_DIR
            / "payment_events.jsonl"
        )

        with open(
            output_file,
            "w",
            encoding="utf-8",
        ) as file:

            for row in transactions_df.itertuples(
                index=False
            ):

                event = {

                    "event_id": str(uuid.uuid4()),

                    "event_type": "PAYMENT",

                    "record_type": "payment_event",

                    "source_system":
                        "sentinelpay_transactions",

                    "transaction_id":
                        row.transaction_id,

                    "wallet_id":
                        row.wallet_id,

                    "merchant_id":
                        row.merchant_id,

                    "amount":
                        row.amount,

                    "currency":
                        row.currency,

                    "payment_method":
                        row.payment_method,

                    "transaction_status":
                        row.transaction_status,

                    "reference_number":
                        row.reference_number,

                    "event_timestamp":
                        row.transaction_timestamp,

                    "ingested_at":
                        ingested_at,

                }

                write_jsonl_record(
                    file,
                    event,
                )

        logger.info(
            "payment_events.jsonl generated."
        )

    except Exception as error:

        logger.exception(
            "Failed to generate payment events."
        )

        raise error


# --------------------------------------------------
# Wallet Events
# --------------------------------------------------

def generate_wallet_events(
    wallets_df: pd.DataFrame,
) -> None:
    """
    Generate wallet events.
    """

    logger.info(
        "Generating wallet_events.jsonl..."
    )

    try:

        ingested_at = get_ingested_at()

        output_file = (
            JSON_DIR
            / "wallet_events.jsonl"
        )

        with open(
            output_file,
            "w",
            encoding="utf-8",
        ) as file:

            for row in wallets_df.itertuples(
                index=False
            ):

                event = {

                    "event_id":
                        str(uuid.uuid4()),

                    "event_type":
                        "WALLET",

                    "record_type":
                        "wallet_event",

                    "source_system":
                        "sentinelpay_wallets",

                    "wallet_id":
                        row.wallet_id,

                    "customer_id":
                        row.customer_id,

                    "wallet_balance":
                        row.wallet_balance,

                    "currency":
                        row.currency,

                    "wallet_status":
                        row.wallet_status,

                    "event_timestamp":
                        row.updated_at,

                    "ingested_at":
                        ingested_at,

                }

                write_jsonl_record(
                    file,
                    event,
                )

        logger.info(
            "wallet_events.jsonl generated."
        )

    except Exception as error:

        logger.exception(
            "Failed to generate wallet events."
        )

        raise error


# --------------------------------------------------
# Refund Events
# --------------------------------------------------

def generate_refund_events(
    refunds_df: pd.DataFrame,
) -> None:
    """
    Generate refund events.
    """

    logger.info(
        "Generating refund_events.jsonl..."
    )

    try:

        ingested_at = get_ingested_at()

        output_file = (
            JSON_DIR
            / "refund_events.jsonl"
        )

        with open(
            output_file,
            "w",
            encoding="utf-8",
        ) as file:

            for row in refunds_df.itertuples(
                index=False
            ):

                event = {

                    "event_id":
                        str(uuid.uuid4()),

                    "event_type":
                        "REFUND",

                    "record_type":
                        "refund_event",

                    "source_system":
                        "sentinelpay_refunds",

                    "refund_id":
                        row.refund_id,

                    "transaction_id":
                        row.transaction_id,

                    "refund_amount":
                        row.refund_amount,

                    "refund_reason":
                        row.refund_reason,

                    "refund_status":
                        row.refund_status,

                    "event_timestamp":
                        row.refund_timestamp,

                    "ingested_at":
                        ingested_at,

                }

                write_jsonl_record(
                    file,
                    event,
                )

        logger.info(
            "refund_events.jsonl generated."
        )

    except Exception as error:

        logger.exception(
            "Failed to generate refund events."
        )

        raise error


# --------------------------------------------------
# Merchant Events
# --------------------------------------------------

def generate_merchant_events(
    merchants_df: pd.DataFrame,
) -> None:
    """
    Generate merchant events.
    """

    logger.info(
        "Generating merchant_events.jsonl..."
    )

    try:

        ingested_at = get_ingested_at()

        output_file = (
            JSON_DIR
            / "merchant_events.jsonl"
        )

        with open(
            output_file,
            "w",
            encoding="utf-8",
        ) as file:

            for row in merchants_df.itertuples(
                index=False
            ):

                event = {

                    "event_id":
                        str(uuid.uuid4()),

                    "event_type":
                        "MERCHANT",

                    "record_type":
                        "merchant_event",

                    "source_system":
                        "sentinelpay_merchants",

                    "merchant_id":
                        row.merchant_id,

                    "merchant_name":
                        row.merchant_name,

                    "merchant_category":
                        row.merchant_category,

                    "merchant_status":
                        row.merchant_status,

                    "city":
                        row.city,

                    "state":
                        row.state,

                    "country":
                        row.country,

                    "event_timestamp":
                        row.updated_at,

                    "ingested_at":
                        ingested_at,

                }

                write_jsonl_record(
                    file,
                    event,
                )

        logger.info(
            "merchant_events.jsonl generated."
        )

    except Exception as error:

        logger.exception(
            "Failed to generate merchant events."
        )

        raise error


# --------------------------------------------------
# Main
# --------------------------------------------------

if __name__ == "__main__":

    csv_dir = Path(OUTPUT_DIR) / "csv"

    transactions_df = pd.read_csv(
        csv_dir / "payment_transactions.csv"
    )

    wallets_df = pd.read_csv(
        csv_dir / "wallets.csv"
    )

    refunds_df = pd.read_csv(
        csv_dir / "refunds.csv"
    )

    merchants_df = pd.read_csv(
        csv_dir / "merchants.csv"
    )

    generate_payment_events(
        transactions_df
    )

    generate_wallet_events(
        wallets_df
    )

    generate_refund_events(
        refunds_df
    )

    generate_merchant_events(
        merchants_df
    )

    logger.info(
        "All JSONL files generated successfully."
    )
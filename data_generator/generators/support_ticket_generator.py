"""
Generate support tickets for SentinelPay.
"""

import json
import random
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

TICKET_DIR = Path(OUTPUT_DIR) / "tickets"

TICKET_DIR.mkdir(
    parents=True,
    exist_ok=True,
)

# --------------------------------------------------
# Ticket Metadata
# --------------------------------------------------

TRANSACTION_ISSUES = [
    "Payment Failed",
    "Merchant Charged Twice",
    "Transaction Pending",
]

REFUND_ISSUES = [
    "Refund Not Received",
]

WALLET_ISSUES = [
    "Wallet Balance Incorrect",
    "Wallet Blocked",
    "Unable to Add Bank Account",
]

STATUSES = [
    "OPEN",
    "IN_PROGRESS",
    "RESOLVED",
    "CLOSED",
]


# --------------------------------------------------
# Helper Functions
# --------------------------------------------------

def write_jsonl_record(
    output_file,
    record: dict,
) -> None:
    """
    Write one JSONL record.
    """

    output_file.write(
        json.dumps(
            record,
            default=str,
        ) + "\n"
    )


def build_ticket(
    ticket_type: str,
    reference_id: str,
    issue: str,
    priority: str,
    created_at,
) -> dict:
    """
    Build a support ticket record.
    """

    return {
        "event_id": str(uuid.uuid4()),
        "ticket_id": str(uuid.uuid4()),
        "source_system": "sentinelpay_support",
        "ticket_type": ticket_type,
        "reference_id": reference_id,
        "issue": issue,
        "priority": priority,
        "status": random.choice(STATUSES),
        "created_at": created_at,
        "ingested_at": datetime.now(
            timezone.utc
        ).isoformat(),
    }


# --------------------------------------------------
# Support Ticket Generator
# --------------------------------------------------

def generate_support_tickets(
    transactions_df: pd.DataFrame,
    refunds_df: pd.DataFrame,
    wallets_df: pd.DataFrame,
) -> None:
    """
    Generate support tickets in JSONL format.
    """

    logger.info(
        "Generating support_tickets.jsonl..."
    )

    output_file = (
        TICKET_DIR
        / "support_tickets.jsonl"
    )

    ticket_count = 0

    try:

        with open(
            output_file,
            "w",
            encoding="utf-8",
        ) as file:

            failed_transactions = transactions_df[
                transactions_df["transaction_status"] == "FAILED"
            ]

            for row in failed_transactions.itertuples(
                index=False
            ):
                ticket = build_ticket(
                    ticket_type="TRANSACTION",
                    reference_id=row.transaction_id,
                    issue=random.choice(
                        TRANSACTION_ISSUES
                    ),
                    priority="HIGH",
                    created_at=row.transaction_timestamp,
                )

                write_jsonl_record(
                    file,
                    ticket,
                )

                ticket_count += 1

            failed_refunds = refunds_df[
                refunds_df["refund_status"] == "FAILED"
            ]

            for row in failed_refunds.itertuples(
                index=False
            ):
                ticket = build_ticket(
                    ticket_type="REFUND",
                    reference_id=row.refund_id,
                    issue=random.choice(
                        REFUND_ISSUES
                    ),
                    priority="HIGH",
                    created_at=row.refund_timestamp,
                )

                write_jsonl_record(
                    file,
                    ticket,
                )

                ticket_count += 1

            blocked_wallets = wallets_df[
                wallets_df["wallet_status"] == "BLOCKED"
            ]

            for row in blocked_wallets.itertuples(
                index=False
            ):
                wallet_timestamp = getattr(
                    row,
                    "updated_at",
                    None,
                ) or getattr(
                    row,
                    "created_at",
                    None,
                )

                ticket = build_ticket(
                    ticket_type="WALLET",
                    reference_id=row.wallet_id,
                    issue=random.choice(
                        WALLET_ISSUES
                    ),
                    priority="CRITICAL",
                    created_at=wallet_timestamp,
                )

                write_jsonl_record(
                    file,
                    ticket,
                )

                ticket_count += 1

        logger.info(
            f"{ticket_count} support tickets generated."
        )

        logger.info(
            f"Support tickets saved to {output_file}"
        )

    except Exception as error:

        logger.exception(
            "Failed to generate support tickets."
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

    refund_file = (
        csv_dir
        / "refunds.csv"
    )

    wallet_file = (
        csv_dir
        / "wallets.csv"
    )

    for file in [
        transaction_file,
        refund_file,
        wallet_file,
    ]:
        if not file.exists():

            logger.critical(
                f"{file.name} not found."
            )

            raise FileNotFoundError(file)

    transactions_df = pd.read_csv(
        transaction_file
    )

    refunds_df = pd.read_csv(
        refund_file
    )

    wallets_df = pd.read_csv(
        wallet_file
    )

    generate_support_tickets(
        transactions_df,
        refunds_df,
        wallets_df,
    )

    logger.info("=" * 60)

    logger.info(
        "Support ticket generation completed successfully."
    )

    logger.info("=" * 60)
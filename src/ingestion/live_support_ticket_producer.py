"""
Continuously publish live support ticket events to Kafka.
"""

import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from data_generator.config import OUTPUT_DIR
from data_generator.logger import get_logger
from src.ingestion.base_producer import create_kafka_producer

logger = get_logger(__name__)


ISSUES = [
    "Payment Failed",
    "Refund Not Received",
    "Wallet Balance Incorrect",
    "Merchant Charged Twice",
    "Transaction Pending",
    "KYC Verification Issue",
    "Unable to Add Bank Account",
    "Wallet Blocked",
]

PRIORITIES = [
    "LOW",
    "MEDIUM",
    "HIGH",
    "CRITICAL",
]

STATUSES = [
    "OPEN",
    "IN_PROGRESS",
    "RESOLVED",
    "CLOSED",
]


def load_reference_data():
    csv_dir = Path(OUTPUT_DIR) / "csv"

    transactions_df = pd.read_csv(
        csv_dir / "payment_transactions.csv"
    )

    refunds_df = pd.read_csv(
        csv_dir / "refunds.csv"
    )

    wallets_df = pd.read_csv(
        csv_dir / "wallets.csv"
    )

    return transactions_df, refunds_df, wallets_df


def build_transaction_ticket(
    transactions_df: pd.DataFrame,
) -> dict:
    transaction = transactions_df.sample(1).iloc[0]
    created_at = datetime.now().isoformat()

    return {
        "event_id": str(uuid.uuid4()),
        "ticket_id": str(uuid.uuid4()),
        "ticket_type": "TRANSACTION",
        "reference_id": transaction["transaction_id"],
        "issue": random.choice(ISSUES),
        "priority": random.choice(PRIORITIES),
        "status": random.choice(STATUSES),
        "source_system": "sentinelpay_support",
        "created_at": created_at,
        "ingested_at": datetime.now(
            timezone.utc
        ).isoformat(),
    }


def build_refund_ticket(
    refunds_df: pd.DataFrame,
) -> dict:
    refund = refunds_df.sample(1).iloc[0]
    created_at = datetime.now().isoformat()

    return {
        "event_id": str(uuid.uuid4()),
        "ticket_id": str(uuid.uuid4()),
        "ticket_type": "REFUND",
        "reference_id": refund["refund_id"],
        "issue": "Refund Not Received",
        "priority": "HIGH",
        "status": random.choice(STATUSES),
        "source_system": "sentinelpay_support",
        "created_at": created_at,
        "ingested_at": datetime.now(
            timezone.utc
        ).isoformat(),
    }


def build_wallet_ticket(
    wallets_df: pd.DataFrame,
) -> dict:
    wallet = wallets_df.sample(1).iloc[0]
    created_at = datetime.now().isoformat()

    return {
        "event_id": str(uuid.uuid4()),
        "ticket_id": str(uuid.uuid4()),
        "ticket_type": "WALLET",
        "reference_id": wallet["wallet_id"],
        "issue": "Wallet Blocked",
        "priority": "CRITICAL",
        "status": random.choice(STATUSES),
        "source_system": "sentinelpay_support",
        "created_at": created_at,
        "ingested_at": datetime.now(
            timezone.utc
        ).isoformat(),
    }


def publish_support_tickets() -> None:
    producer = create_kafka_producer()
    transactions_df, refunds_df, wallets_df = load_reference_data()

    logger.info("=" * 60)
    logger.info("Starting live support ticket producer...")
    logger.info("=" * 60)

    try:
        while True:
            batch_size = random.randint(2, 5)

            for _ in range(batch_size):
                ticket_type = random.choice(
                    ["TRANSACTION", "REFUND", "WALLET"]
                )

                if ticket_type == "TRANSACTION":
                    event = build_transaction_ticket(
                        transactions_df=transactions_df,
                    )
                elif ticket_type == "REFUND":
                    event = build_refund_ticket(
                        refunds_df=refunds_df,
                    )
                else:
                    event = build_wallet_ticket(
                        wallets_df=wallets_df,
                    )

                producer.send(
                    "log.support_ticket",
                    key=event["ticket_id"],
                    value=event,
                )

            producer.flush()

            logger.info(
                f"Published {batch_size} live support ticket events."
            )

            time.sleep(8)

    except KeyboardInterrupt:
        logger.info(
            "Live support ticket producer stopped by user."
        )

    except Exception:
        logger.exception(
            "Live support ticket producer failed."
        )
        raise

    finally:
        producer.close()
        logger.info("Kafka producer closed.")


if __name__ == "__main__":
    publish_support_tickets()
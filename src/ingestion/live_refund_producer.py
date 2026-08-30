"""
Continuously publish live refund events to Kafka.
"""

import random
import time
import uuid
from datetime import datetime
from pathlib import Path

import pandas as pd

from data_generator.config import OUTPUT_DIR
from data_generator.logger import get_logger
from src.ingestion.base_producer import create_kafka_producer

logger = get_logger(__name__)


REFUND_REASONS = [
    "Customer Requested Refund",
    "Duplicate Payment",
    "Order Cancelled",
    "Product Unavailable",
    "Payment Reconciliation Error",
]

REFUND_STATUSES = [
    "REQUESTED",
    "APPROVED",
    "PROCESSED",
    "FAILED",
]


def load_reference_data() -> pd.DataFrame:
    csv_dir = Path(OUTPUT_DIR) / "csv"
    transactions_df = pd.read_csv(
        csv_dir / "payment_transactions.csv"
    )

    successful_transactions_df = transactions_df[
        transactions_df["transaction_status"] == "SUCCESS"
    ].copy()

    return successful_transactions_df


def build_refund_event(
    transactions_df: pd.DataFrame,
) -> dict:
    transaction = transactions_df.sample(1).iloc[0]

    refund_amount = round(
        random.uniform(50, float(transaction["amount"])),
        2,
    )

    refund_status = random.choices(
        REFUND_STATUSES,
        weights=[45, 25, 20, 10],
        k=1,
    )[0]

    current_timestamp = datetime.now().isoformat()

    event = {
    "refund_id": str(uuid.uuid4()),
    "transaction_id": transaction["transaction_id"],
    "refund_amount": refund_amount,
    "refund_reason": random.choice(REFUND_REASONS),
    "refund_status": refund_status,
    "refund_timestamp": current_timestamp,
    "created_at": current_timestamp,
    "updated_at": current_timestamp,
    }

    return event


def publish_refunds() -> None:
    producer = create_kafka_producer()
    transactions_df = load_reference_data()

    logger.info("=" * 60)
    logger.info("Starting live refund producer...")
    logger.info("=" * 60)

    try:
        while True:
            batch_size = random.randint(2, 6)

            for _ in range(batch_size):
                event = build_refund_event(
                    transactions_df=transactions_df,
                )

                producer.send(
                    "event.refund",
                    key=event["refund_id"],
                    value=event,
                )

            producer.flush()

            logger.info(
                f"Published {batch_size} live refund events."
            )

            time.sleep(7)

    except KeyboardInterrupt:
        logger.info(
            "Live refund producer stopped by user."
        )

    except Exception:
        logger.exception(
            "Live refund producer failed."
        )
        raise

    finally:
        producer.close()
        logger.info("Kafka producer closed.")


if __name__ == "__main__":
    publish_refunds()
"""
Continuously publish live payment events to Kafka.
"""

import json
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


def load_reference_data():
    csv_dir = Path(OUTPUT_DIR) / "csv"

    wallets_df = pd.read_csv(csv_dir / "wallets.csv")
    merchants_df = pd.read_csv(csv_dir / "merchants.csv")

    return wallets_df, merchants_df


def build_payment_event(
    wallets_df: pd.DataFrame,
    merchants_df: pd.DataFrame,
) -> dict:
    wallet = wallets_df.sample(1).iloc[0]
    merchant = merchants_df.sample(1).iloc[0]

    amount = round(random.uniform(100, 10000), 2)
    payment_method = random.choice(
        ["UPI", "Wallet", "Credit Card", "Debit Card"]
    )
    transaction_status = random.choices(
        ["SUCCESS", "FAILED", "PENDING"],
        weights=[85, 10, 5],
        k=1,
    )[0]

    event = {
        "transaction_id": str(uuid.uuid4()),
        "wallet_id": wallet["wallet_id"],
        "merchant_id": merchant["merchant_id"],
        "amount": amount,
        "currency": random.choice(["INR", "USD", "EUR"]),
        "payment_method": payment_method,
        "transaction_status": transaction_status,
        "reference_number": f"TXN{uuid.uuid4().hex[:16].upper()}",
        "transaction_timestamp": datetime.now().isoformat(),
        "created_at": datetime.now().isoformat(),
    }

    return event


def publish_payments() -> None:
    producer = create_kafka_producer()
    wallets_df, merchants_df = load_reference_data()

    logger.info("=" * 60)
    logger.info("Starting live payment producer...")
    logger.info("=" * 60)

    try:
        while True:
            batch_size = random.randint(5, 15)

            for _ in range(batch_size):
                event = build_payment_event(
                    wallets_df=wallets_df,
                    merchants_df=merchants_df,
                )

                producer.send(
                    "event.payment",
                    key=event["transaction_id"],
                    value=event,
                )

            producer.flush()

            logger.info(
                f"Published {batch_size} live payment events."
            )

            time.sleep(5)

    except KeyboardInterrupt:
        logger.info(
            "Live payment producer stopped by user."
        )

    except Exception:
        logger.exception(
            "Live payment producer failed."
        )
        raise

    finally:
        producer.close()
        logger.info("Kafka producer closed.")


if __name__ == "__main__":
    publish_payments()
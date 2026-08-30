"""
Continuously publish live application, error, and audit logs to Kafka.
"""

import random
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

from data_generator.config import OUTPUT_DIR
from data_generator.logger import get_logger
from src.ingestion.base_producer import create_kafka_producer

logger = get_logger(__name__)

APPLICATION_MESSAGES = [
    "Payment request received",
    "Wallet debited successfully",
    "Merchant credited successfully",
    "Transaction completed",
    "Refund initiated",
    "Refund completed",
    "Wallet credited",
    "Merchant settlement completed",
]

ERROR_MESSAGES = [
    "Insufficient wallet balance",
    "Payment gateway timeout",
    "Merchant service unavailable",
    "Invalid payment method",
    "Transaction failed",
    "Database connection timeout",
    "Duplicate transaction detected",
]

AUDIT_MESSAGES = [
    "Customer logged in",
    "Customer logged out",
    "Wallet created",
    "Merchant onboarded",
    "KYC verified",
    "Profile updated",
]


def load_reference_data():
    csv_dir = Path(OUTPUT_DIR) / "csv"

    transactions_df = pd.read_csv(
        csv_dir / "payment_transactions.csv"
    )

    customers_df = pd.read_csv(
        csv_dir / "customers.csv"
    )

    return transactions_df, customers_df


def build_application_log(
    transactions_df: pd.DataFrame,
) -> tuple[str, str]:
    transaction = transactions_df.sample(1).iloc[0]

    log_line = (
        f"{datetime.now().isoformat()} | "
        f"INFO | "
        f"{transaction['transaction_id']} | "
        f"{random.choice(APPLICATION_MESSAGES)}"
    )

    return "log.application", log_line


def build_error_log(
    transactions_df: pd.DataFrame,
) -> tuple[str, str]:
    transaction = transactions_df.sample(1).iloc[0]

    log_line = (
        f"{datetime.now().isoformat()} | "
        f"ERROR | "
        f"{transaction['transaction_id']} | "
        f"{random.choice(ERROR_MESSAGES)}"
    )

    return "log.error", log_line


def build_audit_log(
    customers_df: pd.DataFrame,
) -> tuple[str, str]:
    customer = customers_df.sample(1).iloc[0]

    log_line = (
        f"{datetime.now().isoformat()} | "
        f"AUDIT | "
        f"{customer['customer_id']} | "
        f"{random.choice(AUDIT_MESSAGES)}"
    )

    return "log.audit", log_line


def publish_logs() -> None:
    producer = create_kafka_producer()
    transactions_df, customers_df = load_reference_data()

    logger.info("=" * 60)
    logger.info("Starting live log producer...")
    logger.info("=" * 60)

    try:
        while True:
            batch_size = random.randint(6, 12)

            for _ in range(batch_size):
                log_type = random.choices(
                    ["application", "error", "audit"],
                    weights=[70, 15, 15],
                    k=1,
                )[0]

                if log_type == "application":
                    topic_name, log_line = build_application_log(
                        transactions_df=transactions_df,
                    )
                elif log_type == "error":
                    topic_name, log_line = build_error_log(
                        transactions_df=transactions_df,
                    )
                else:
                    topic_name, log_line = build_audit_log(
                        customers_df=customers_df,
                    )

                producer.send(
    topic_name,
    key=f"{topic_name}_{datetime.now().timestamp()}",
    value=log_line,
)

            producer.flush()

            logger.info(
                f"Published {batch_size} live log records."
            )

            time.sleep(5)

    except KeyboardInterrupt:
        logger.info(
            "Live log producer stopped by user."
        )

    except Exception:
        logger.exception(
            "Live log producer failed."
        )
        raise

    finally:
        producer.close()
        logger.info("Kafka producer closed.")


if __name__ == "__main__":
    publish_logs()
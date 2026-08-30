"""
Publish semi-structured JSONL event files to Kafka topics.
"""

import json
from pathlib import Path

from data_generator.logger import get_logger
from src.ingestion.base_producer import create_kafka_producer

logger = get_logger(__name__)

JSON_DIR = (
    Path(__file__).resolve().parents[2]
    / "data_generator"
    / "output"
    / "json"
)

JSON_TOPIC_MAP = {
    "payment_events.jsonl": (
        "event.payment_json",
        "event_id",
    ),
    "refund_events.jsonl": (
        "event.refund_json",
        "event_id",
    ),
    "wallet_events.jsonl": (
        "event.wallet",
        "event_id",
    ),
    "merchant_events.jsonl": (
        "event.merchant",
        "event_id",
    ),
}


def publish_jsonl_file(
    producer,
    file_name: str,
    topic_name: str,
    key_field: str,
) -> int:
    file_path = JSON_DIR / file_name

    if not file_path.exists():
        raise FileNotFoundError(
            f"{file_path} not found."
        )

    logger.info(
        f"Publishing {file_name} to {topic_name}..."
    )

    published_count = 0

    with open(
        file_path,
        "r",
        encoding="utf-8",
    ) as file:
        for line in file:
            record = json.loads(
                line.strip()
            )

            producer.send(
                topic_name,
                key=str(record[key_field]),
                value=record,
            )

            published_count += 1

    producer.flush()

    logger.info(
        f"Published {published_count:,} records from {file_name} to {topic_name}"
    )

    return published_count


def main() -> None:
    logger.info("=" * 60)
    logger.info(
        "Starting JSONL to Kafka producer"
    )
    logger.info("=" * 60)

    producer = None

    try:
        producer = create_kafka_producer()

        total_records = 0

        for file_name, (
            topic_name,
            key_field,
        ) in JSON_TOPIC_MAP.items():
            total_records += publish_jsonl_file(
                producer,
                file_name,
                topic_name,
                key_field,
            )

        logger.info("=" * 60)
        logger.info(
            f"JSONL to Kafka publishing completed. Total records: {total_records:,}"
        )
        logger.info("=" * 60)

    except Exception:
        logger.exception(
            "JSONL to Kafka publishing failed."
        )
        raise

    finally:
        if producer:
            producer.close()
            logger.info(
                "Kafka producer closed."
            )


if __name__ == "__main__":
    main()
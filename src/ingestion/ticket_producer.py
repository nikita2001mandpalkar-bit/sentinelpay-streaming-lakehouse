"""
Publish support ticket JSONL data to Kafka topics.
"""

import json
from pathlib import Path

from data_generator.logger import get_logger
from src.ingestion.base_producer import create_kafka_producer

logger = get_logger(__name__)

TICKET_DIR = (
    Path(__file__).resolve().parents[2]
    / "data_generator"
    / "output"
    / "tickets"
)

TICKET_FILE = "support_tickets.jsonl"
TICKET_TOPIC = "log.support_ticket"
TICKET_KEY_FIELD = "event_id"


def publish_ticket_file(
    producer,
) -> int:
    file_path = TICKET_DIR / TICKET_FILE

    if not file_path.exists():
        raise FileNotFoundError(
            f"{file_path} not found."
        )

    logger.info(
        f"Publishing {TICKET_FILE} to {TICKET_TOPIC}..."
    )

    published_count = 0

    with open(
        file_path,
        "r",
        encoding="utf-8",
    ) as file:
        for line in file:
            if not line.strip():
                continue

            record = json.loads(
                line.strip()
            )

            producer.send(
                TICKET_TOPIC,
                key=str(record[TICKET_KEY_FIELD]),
                value=record,
            )

            published_count += 1

    producer.flush()

    logger.info(
        f"Published {published_count:,} records from {TICKET_FILE} to {TICKET_TOPIC}"
    )

    return published_count


def main() -> None:
    logger.info("=" * 60)
    logger.info(
        "Starting support ticket to Kafka producer"
    )
    logger.info("=" * 60)

    producer = None

    try:
        producer = create_kafka_producer()

        total_records = publish_ticket_file(
            producer
        )

        logger.info("=" * 60)
        logger.info(
            f"Support ticket to Kafka publishing completed. Total records: {total_records:,}"
        )
        logger.info("=" * 60)

    except Exception:
        logger.exception(
            "Support ticket to Kafka publishing failed."
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
"""
Publish unstructured log files to Kafka topics.
"""

from datetime import datetime, timezone
from pathlib import Path

from data_generator.logger import get_logger
from src.ingestion.base_producer import create_kafka_producer

logger = get_logger(__name__)

LOG_DIR = (
    Path(__file__).resolve().parents[2]
    / "data_generator"
    / "output"
    / "logs"
)

LOG_TOPIC_MAP = {
    "application.log": "log.application",
    "error.log": "log.error",
    "audit.log": "log.audit",
}


def build_log_record(
    log_line: str,
    source_file: str,
) -> dict:
    return {
        "source_system": "sentinelpay_logs",
        "source_file": source_file,
        "log_line": log_line.strip(),
        "ingested_at": datetime.now(
            timezone.utc
        ).isoformat(),
    }


def publish_log_file(
    producer,
    file_name: str,
    topic_name: str,
) -> int:
    file_path = LOG_DIR / file_name

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
        for line_number, line in enumerate(
            file,
            start=1,
        ):
            if not line.strip():
                continue

            record = build_log_record(
                line,
                file_name,
            )

            producer.send(
                topic_name,
                key=f"{file_name}:{line_number}",
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
        "Starting log to Kafka producer"
    )
    logger.info("=" * 60)

    producer = None

    try:
        producer = create_kafka_producer()

        total_records = 0

        for file_name, topic_name in LOG_TOPIC_MAP.items():
            total_records += publish_log_file(
                producer,
                file_name,
                topic_name,
            )

        logger.info("=" * 60)
        logger.info(
            f"Log to Kafka publishing completed. Total records: {total_records:,}"
        )
        logger.info("=" * 60)

    except Exception:
        logger.exception(
            "Log to Kafka publishing failed."
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
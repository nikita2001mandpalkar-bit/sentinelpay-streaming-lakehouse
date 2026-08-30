"""
Publish structured PostgreSQL tables to Kafka topics.
"""

from src.ingestion.base_producer import create_kafka_producer
from data_generator.database import get_connection
from data_generator.logger import get_logger

logger = get_logger(__name__)

TABLE_TOPIC_MAP = [
    ("customers", "customer_id", "master.customer"),
    ("merchants", "merchant_id", "master.merchant"),
    ("bank_accounts", "bank_account_id", "master.bank_account"),
    ("wallets", "wallet_id", "master.wallet"),
    ("devices", "device_id", "master.device"),
    ("payment_transactions", "transaction_id", "event.payment"),
    ("refunds", "refund_id", "event.refund"),
    ("settlements", "settlement_id", "batch.settlement"),
]


def fetch_table_rows(
    connection,
    table_name: str,
) -> list[dict]:
    with connection.cursor() as cursor:
        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

    return [
        dict(zip(columns, row))
        for row in rows
    ]


def publish_table(
    producer,
    connection,
    table_name: str,
    key_column: str,
    topic_name: str,
) -> int:
    logger.info(
        f"Publishing {table_name} to {topic_name}..."
    )

    rows = fetch_table_rows(
        connection,
        table_name,
    )

    published_count = 0

    for row in rows:
        producer.send(
            topic_name,
            key=str(row[key_column]),
            value=row,
        )
        published_count += 1

    producer.flush()

    logger.info(
        f"Published {published_count:,} records from {table_name} to {topic_name}"
    )

    return published_count


def main() -> None:
    logger.info("=" * 60)
    logger.info(
        "Starting PostgreSQL to Kafka Producer"
    )
    logger.info("=" * 60)

    connection = None
    producer = None

    try:
        connection = get_connection()
        producer = create_kafka_producer()

        total_records = 0

        for table_name, key_column, topic_name in TABLE_TOPIC_MAP:
            total_records += publish_table(
                producer,
                connection,
                table_name,
                key_column,
                topic_name,
            )

        logger.info("=" * 60)
        logger.info(
            f"PostgreSQL to Kafka publishing completed. Total records: {total_records:,}"
        )
        logger.info("=" * 60)

    except Exception:
        logger.exception(
            "PostgreSQL to Kafka publishing failed."
        )
        raise

    finally:
        if producer:
            producer.close()
            logger.info(
                "Kafka producer closed."
            )

        if connection:
            connection.close()
            logger.info(
                "Database connection closed."
            )


if __name__ == "__main__":
    main()
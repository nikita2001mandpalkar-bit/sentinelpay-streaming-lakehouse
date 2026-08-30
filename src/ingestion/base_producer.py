"""
Shared Kafka producer utilities for SentinelPay ingestion.
"""

import json
import os

from kafka import KafkaProducer


def json_serializer(value: dict) -> bytes:
    return json.dumps(
        value,
        default=str,
    ).encode("utf-8")


def key_serializer(value: str) -> bytes:
    return value.encode("utf-8")


def create_kafka_producer() -> KafkaProducer:
    bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092",
    )

    return KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(","),
        value_serializer=json_serializer,
        key_serializer=key_serializer,
        acks="all",
        retries=5,
    )
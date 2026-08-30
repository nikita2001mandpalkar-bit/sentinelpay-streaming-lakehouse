"""
Spark schemas for SentinelPay Kafka payload parsing.
"""

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


CUSTOMER_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("date_of_birth", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("kyc_status", StringType(), True),
        StructField("customer_status", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
    ]
)

MERCHANT_SCHEMA = StructType(
    [
        StructField("merchant_id", StringType(), True),
        StructField("merchant_name", StringType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("merchant_email", StringType(), True),
        StructField("merchant_phone", StringType(), True),
        StructField("merchant_status", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("country", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
    ]
)

BANK_ACCOUNT_SCHEMA = StructType(
    [
        StructField("bank_account_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("bank_name", StringType(), True),
        StructField("account_number", StringType(), True),
        StructField("ifsc_code", StringType(), True),
        StructField("account_type", StringType(), True),
        StructField("is_primary", StringType(), True),
        StructField("account_status", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
    ]
)

WALLET_SCHEMA = StructType(
    [
        StructField("wallet_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("wallet_balance", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("wallet_status", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
    ]
)

DEVICE_SCHEMA = StructType(
    [
        StructField("device_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("device_os", StringType(), True),
        StructField("app_version", StringType(), True),
        StructField("registered_at", StringType(), True),
    ]
)

TRANSACTION_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("wallet_id", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("transaction_status", StringType(), True),
        StructField("reference_number", StringType(), True),
        StructField("transaction_timestamp", StringType(), True),
        StructField("created_at", StringType(), True),
    ]
)

REFUND_SCHEMA = StructType(
    [
        StructField("refund_id", StringType(), True),
        StructField("transaction_id", StringType(), True),
        StructField("refund_amount", DoubleType(), True),
        StructField("refund_reason", StringType(), True),
        StructField("refund_status", StringType(), True),
        StructField("refund_timestamp", StringType(), True),
        StructField("created_at", StringType(), True),
    ]
)

SETTLEMENT_SCHEMA = StructType(
    [
        StructField("settlement_id", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("settlement_amount", DoubleType(), True),
        StructField("settlement_date", StringType(), True),
        StructField("settlement_status", StringType(), True),
        StructField("merchant_status", StringType(), True),
        StructField("created_at", StringType(), True),
    ]
)

PAYMENT_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("record_type", StringType(), True),
        StructField("source_system", StringType(), True),
        StructField("transaction_id", StringType(), True),
        StructField("wallet_id", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("transaction_status", StringType(), True),
        StructField("reference_number", StringType(), True),
        StructField("event_timestamp", StringType(), True),
        StructField("ingested_at", StringType(), True),
    ]
)

REFUND_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("record_type", StringType(), True),
        StructField("source_system", StringType(), True),
        StructField("refund_id", StringType(), True),
        StructField("transaction_id", StringType(), True),
        StructField("refund_amount", DoubleType(), True),
        StructField("refund_reason", StringType(), True),
        StructField("refund_status", StringType(), True),
        StructField("event_timestamp", StringType(), True),
        StructField("ingested_at", StringType(), True),
    ]
)

WALLET_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("record_type", StringType(), True),
        StructField("source_system", StringType(), True),
        StructField("wallet_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("wallet_balance", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("wallet_status", StringType(), True),
        StructField("event_timestamp", StringType(), True),
        StructField("ingested_at", StringType(), True),
    ]
)

MERCHANT_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("record_type", StringType(), True),
        StructField("source_system", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("merchant_name", StringType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("merchant_status", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("country", StringType(), True),
        StructField("event_timestamp", StringType(), True),
        StructField("ingested_at", StringType(), True),
    ]
)

LOG_SCHEMA = StructType(
    [
        StructField("source_system", StringType(), True),
        StructField("source_file", StringType(), True),
        StructField("log_line", StringType(), True),
        StructField("ingested_at", StringType(), True),
    ]
)

SUPPORT_TICKET_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("ticket_id", StringType(), True),
        StructField("source_system", StringType(), True),
        StructField("ticket_type", StringType(), True),
        StructField("reference_id", StringType(), True),
        StructField("issue", StringType(), True),
        StructField("priority", StringType(), True),
        StructField("status", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("ingested_at", StringType(), True),
    ]
)
from src.utils.paths import QUARANTINE_BASE_PATH,QUALITY_RESULTS_BASE_PATH

EVENT_PAYMENT_CONTRACT={
    "dataset_name":"event_payment",
    "silver_path":"s3a://sentinelpay-lake/silver/event_payment",
    "quarantine_path":f"{QUARANTINE_BASE_PATH}/event_payment",
    "result_path":f"{QUALITY_RESULTS_BASE_PATH}/event_payment",
    "business_key":"transaction_id",
    "required_columns":[
        "transaction_id",
        "wallet_id",
        "merchant_id",
        "amount",
        "currency",
        "payment_method",
        "transaction_status",
        "reference_number",
        "event_timestamp",
        "kafka_timestamp",
        "bronze_ingested_at",
        "silver_processed_at",
    ],
    "allowed_currencies":[
        "INR",
        "USD",
        "EUR",
        "GBP",
        "JPY",
        "CAD"
    ],
    "allowed_statuses":[
        "SUCCESS",
        "FAILED",
        "PENDING"
    ],
    "allowed_payment_methods":[
        "UPI",
        "Wallet",
        "Credit Card",
        "Debit Card",
        "Net Banking",
    ],
    "amount_range":{
        "min_value":0.01,
        "max_value":1000000.00,
    },
    "late_threshold_minutes":10,
}


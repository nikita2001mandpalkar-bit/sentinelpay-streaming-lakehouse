from src.utils.paths import QUALITY_RESULTS_BASE_PATH, QUARANTINE_BASE_PATH

SETTLEMENTS_CONTRACT = {
    "dataset_name": "settlements",
    "silver_path": "s3a://sentinelpay-lake/silver/settlements",
    "quarantine_path": f"{QUARANTINE_BASE_PATH}/settlements",
    "result_path": f"{QUALITY_RESULTS_BASE_PATH}/settlements",
    "business_key": "settlement_id",
    "required_columns": [
        "settlement_id",
        "merchant_id",
        "settlement_amount",
        "settlement_date",
        "settlement_status",
        "merchant_status",
        "created_at",
        "kafka_timestamp",
        "bronze_ingested_at",
        "silver_processed_at",
    ],
    "allowed_settlement_statuses": [
        "COMPLETED",
        "FAILED",
        "PENDING",
        "PROCESSING",
    ],
    "allowed_merchant_statuses": [
        "ACTIVE",
        "INACTIVE",
    ],
    "amount_range": {
        "min_value": 0.01,
        "max_value": 100000000.00,
    },
}

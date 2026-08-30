from src.utils.paths import QUALITY_RESULTS_BASE_PATH, QUARANTINE_BASE_PATH

BANK_ACCOUNTS_CONTRACT = {
    "dataset_name": "bank_accounts",
    "silver_path": "s3a://sentinelpay-lake/silver/bank_accounts",
    "quarantine_path": f"{QUARANTINE_BASE_PATH}/bank_accounts",
    "result_path": f"{QUALITY_RESULTS_BASE_PATH}/bank_accounts",
    "business_key": "bank_account_id",
    "required_columns": [
        "bank_account_id",
        "customer_id",
        "bank_name",
        "account_number",
        "ifsc_code",
        "account_type",
        "is_primary",
        "account_status",
        "created_at",
        "updated_at",
        "kafka_timestamp",
        "bronze_ingested_at",
        "silver_processed_at",
    ],
    "allowed_account_types": [
        "SAVINGS",
        "CURRENT",
        "SALARY",
    ],
    "allowed_is_primary_values": [
        "TRUE",
        "FALSE",
    ],
    "allowed_account_statuses": [
        "ACTIVE",
        "INACTIVE",
    ],
}

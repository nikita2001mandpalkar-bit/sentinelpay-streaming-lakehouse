from src.utils.paths import QUALITY_RESULTS_BASE_PATH, QUARANTINE_BASE_PATH

DEVICES_CONTRACT = {
    "dataset_name": "devices",
    "silver_path": "s3a://sentinelpay-lake/silver/devices",
    "quarantine_path": f"{QUARANTINE_BASE_PATH}/devices",
    "result_path": f"{QUALITY_RESULTS_BASE_PATH}/devices",
    "business_key": "device_id",
    "required_columns": [
        "device_id",
        "customer_id",
        "device_type",
        "device_os",
        "app_version",
        "registered_at",
        "kafka_timestamp",
        "bronze_ingested_at",
        "silver_processed_at",
    ],
    "allowed_device_types": [
        "DESKTOP",
        "LAPTOP",
        "MOBILE",
        "TABLET",
        "WEB",
    ],
    "allowed_device_os": [
        "ANDROID",
        "IOS",
        "IPADOS",
        "LINUX",
        "MACOS",
        "WINDOWS",
    ],
}

select
    transaction_id,
    wallet_id,
    merchant_id,
    amount,
    currency,
    payment_method,
    transaction_status,
    reference_number,
    event_timestamp,
    kafka_timestamp,
    bronze_ingested_at,
    silver_processed_at,
    is_late
from {{ ref('stg_event_payment') }}

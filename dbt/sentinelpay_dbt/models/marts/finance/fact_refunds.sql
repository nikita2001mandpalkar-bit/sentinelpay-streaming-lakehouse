select
    refund_id,
    transaction_id,
    refund_amount,
    refund_reason,
    refund_status,
    event_timestamp,
    created_at,
    kafka_timestamp,
    bronze_ingested_at,
    silver_processed_at,
    is_late
from {{ ref('stg_event_refund') }}

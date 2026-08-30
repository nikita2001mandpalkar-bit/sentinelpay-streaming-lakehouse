select
    trim(refund_id) as refund_id,
    trim(transaction_id) as transaction_id,
    cast(refund_amount as decimal(18, 2)) as refund_amount,
    trim(refund_reason) as refund_reason,
    upper(trim(refund_status)) as refund_status,
    event_timestamp,
    created_at,
    kafka_timestamp,
    bronze_ingested_at,
    silver_processed_at,
    is_late
from {{ source('silver', 'silver_event_refund') }}

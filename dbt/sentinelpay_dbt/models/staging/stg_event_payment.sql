select
    trim(transaction_id) as transaction_id,
    trim(wallet_id) as wallet_id,
    trim(merchant_id) as merchant_id,
    cast(amount as decimal(18, 2)) as amount,
    upper(trim(currency)) as currency,
    trim(payment_method) as payment_method,
    upper(trim(transaction_status)) as transaction_status,
    trim(reference_number) as reference_number,
    event_timestamp,
    kafka_timestamp,
    bronze_ingested_at,
    silver_processed_at,
    is_late
from {{ source('silver', 'silver_event_payment') }}

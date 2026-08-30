select
    merchant_id,
    currency,
    payment_method,
    transaction_status,
    count(transaction_id) as total_transactions,
    sum(amount) as total_amount,
    avg(amount) as average_amount,
    min(event_timestamp) as first_transaction_timestamp,
    max(event_timestamp) as last_transaction_timestamp
from {{ ref('fact_transactions') }}
group by
    merchant_id,
    currency,
    payment_method,
    transaction_status

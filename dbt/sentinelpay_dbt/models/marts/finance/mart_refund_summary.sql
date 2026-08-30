select
    refund_status,
    refund_reason,
    count(refund_id) as total_refunds,
    sum(refund_amount) as total_refund_amount,
    avg(refund_amount) as average_refund_amount,
    min(event_timestamp) as first_refund_timestamp,
    max(event_timestamp) as last_refund_timestamp
from {{ ref('fact_refunds') }}
group by
    refund_status,
    refund_reason

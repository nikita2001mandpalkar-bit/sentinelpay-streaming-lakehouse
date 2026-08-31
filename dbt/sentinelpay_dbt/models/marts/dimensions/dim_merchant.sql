select
    merchant_id
from {{ ref('fact_transactions') }}
where merchant_id is not null

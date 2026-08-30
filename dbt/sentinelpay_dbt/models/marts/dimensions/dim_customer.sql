select distinct
    wallet_id as customer_id
from {{ ref('fact_transactions') }}
where wallet_id is not null

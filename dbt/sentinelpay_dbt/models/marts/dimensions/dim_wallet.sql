select distinct
    wallet_id
from {{ ref('fact_transactions') }}
where wallet_id is not null

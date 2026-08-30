select *
from {{ source('silver', 'silver_log_support_ticket') }}

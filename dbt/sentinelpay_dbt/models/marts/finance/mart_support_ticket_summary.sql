select
    ticket_type,
    issue,
    priority,
    status,
    count(ticket_id) as total_tickets,
    min(created_at) as first_ticket_created_at,
    max(created_at) as last_ticket_created_at
from {{ ref('stg_support_ticket') }}
group by
    ticket_type,
    issue,
    priority,
    status

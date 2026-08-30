select distinct
    cast(event_timestamp as date) as date_day,
    year(event_timestamp) as year_number,
    month(event_timestamp) as month_number,
    day(event_timestamp) as day_of_month,
    dayofweek(event_timestamp) as day_of_week_number
from {{ ref('fact_transactions') }}
where event_timestamp is not null

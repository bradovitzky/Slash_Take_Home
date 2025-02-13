
with source as (
    select * from {{ ref('card_events') }}
)

select 
    date_trunc('day', timestamp::timestamp) as day,
    count(*) as num_events
from source
group by all
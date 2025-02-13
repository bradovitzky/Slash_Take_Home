select 
    date_trunc('month', day) as month,
    sum(num_events) as monthly_events
from {{ ref('my_first_dbt_model') }}
group by all
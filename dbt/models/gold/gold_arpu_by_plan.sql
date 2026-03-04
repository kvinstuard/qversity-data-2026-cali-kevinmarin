{{ config(materialized='table') }}

select
    plan_type,
    avg(total_revenue) as arpu
from {{ ref('int_revenue_per_subscription') }}
group by 1
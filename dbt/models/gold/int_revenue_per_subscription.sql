{{ config(materialized='table') }}

select
    s.subscription_id,
    s.plan_type,
    coalesce(sum(p.payment_amount), 0) as total_revenue
from {{ ref('silver_subscription') }} s
left join {{ ref('silver_payment_history') }} p
    on s.customer_id = p.customer_id
   and s.phone_number = p.phone_number
group by 1,2



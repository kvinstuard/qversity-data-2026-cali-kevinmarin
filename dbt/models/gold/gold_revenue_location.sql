{{ config(materialized='table') }}

select
    l.country,
    l.city,
    sum(p.payment_amount) as total_revenue
from {{ ref('silver_payment_history') }} p
join {{ ref('silver_subscription') }} s
    on p.customer_id = s.customer_id
   and p.phone_number = s.phone_number
join {{ ref('silver_lines') }} li
    on s.line_id = li.line_id
join {{ ref('silver_customers') }} c
    on li.customer_real_id = c.customer_real_id
join {{ ref('silver_location') }} l
    on c.location_id = l.location_id
group by 1,2
order by total_revenue desc
{{ config(materialized='table') }}

with base as (

    select
        (raw_payload->>'customer_id')::int as customer_id,
        trim(raw_payload->>'phone_number') as phone_number,
        lower(trim(raw_payload->>'email')) as email,

        case
            when raw_payload->>'last_payment_date' ~ '^\d{4}-\d{2}-\d{2}$'
                then (raw_payload->>'last_payment_date')::date
            else null
        end as last_payment_date,

        trim(raw_payload->>'first_name') as first_name,
        trim(raw_payload->>'last_name')  as last_name

    from {{ ref('bronze_mobile_customers_raw') }}
    where raw_payload->>'customer_id' ~ '^\d+$'
),

customers as (

    select *
    from {{ ref('silver_customers') }}

),

joined as (

    select
        b.customer_id,
        b.phone_number,
        b.last_payment_date,
        c.customer_real_id
    from base b
    left join customers c
        on b.first_name = c.first_name
       and b.last_name  = c.last_name
       and b.email      = c.email
)

select distinct
    {{ dbt_utils.generate_surrogate_key([
        'customer_id',
        'phone_number'
    ]) }} as line_id,
    
    customer_id,
    phone_number,
    last_payment_date,
    customer_real_id

from joined
where phone_number is not null
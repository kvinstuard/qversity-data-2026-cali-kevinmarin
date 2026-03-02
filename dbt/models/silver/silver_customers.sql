{{ config(materialized='table') }}

with base as (

    select
        trim(raw_payload->>'first_name')  as first_name,
        trim(raw_payload->>'last_name')   as last_name,
        lower(trim(raw_payload->>'email')) as email,
        (raw_payload->>'credit_score')::numeric as credit_score,
        (raw_payload->>'credit_limit')::numeric as credit_limit,

        case
            when raw_payload->>'registration_date' ~ '^\d{4}-\d{2}-\d{2}$'
                then (raw_payload->>'registration_date')::date
            else null
        end as registration_date,

        trim(raw_payload->>'city')    as city,
        trim(raw_payload->>'country') as country

    from {{ ref('bronze_mobile_customers_raw') }}

),

locations as (

    select *
    from {{ ref('silver_location') }}

),

joined as (

    select
        b.*,
        l.location_id
    from base b
    left join locations l
        on b.city = l.city
       and b.country = l.country

),

ranked as (

    select *,
           row_number() over (
               partition by first_name, last_name, email
               order by registration_date desc nulls last
           ) as rn
    from joined

)

select
    {{ dbt_utils.generate_surrogate_key([
        'first_name',
        'last_name',
        'email'
    ]) }} as customer_real_id,

    first_name,
    last_name,
    email,
    credit_score,
    credit_limit,
    registration_date,
    location_id

from ranked
where rn = 1
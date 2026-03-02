{{ config(materialized='table') }}

with base as (

    select
        (raw_payload->>'customer_id')::int as customer_id,
        trim(raw_payload->>'phone_number') as phone_number,
        raw_payload->>'record_uuid' as record_uuid,
        raw_payload->'contracted_services' as contracted_services

    from {{ ref('bronze_mobile_customers_raw') }}
    where raw_payload->>'customer_id' ~ '^\d+$'
),

subscription_ids as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'customer_id',
            'phone_number',
            'record_uuid'
        ]) }} as subscription_id,
        contracted_services
    from base
),

exploded as (

    select
        s.subscription_id,
        service.value::text as service_name
    from subscription_ids s

    cross join lateral jsonb_array_elements(
        case
            when jsonb_typeof(s.contracted_services) = 'array'
                then s.contracted_services
            else jsonb_build_array(s.contracted_services)
        end
    ) as service(value)
)

select distinct
    subscription_id,
    service_name
from exploded
where service_name is not null
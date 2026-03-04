{{ config(materialized='table') }}

with base as (

    select
        (raw_payload->>'customer_id')::int as customer_id,
        trim(raw_payload->>'phone_number') as phone_number,

        raw_payload->>'status'     as status,
        lower(trim(raw_payload->>'operator'))  as operator,

        case
            when normalized_plan in ('pre', 'prepago') then 'prepago'
            when normalized_plan in ('pos', 'pospago', 'postpago') then 'pospago'
            when normalized_plan in ('control', 'ctrl', 'contrrol') then 'control'
            else 'unknown'
        end as plan_type,

        raw_payload->>'record_uuid' as record_uuid,

        (raw_payload->>'latitude')::numeric  as latitude,
        (raw_payload->>'longitude')::numeric as longitude,
        (raw_payload->>'monthly_data_gb')::numeric as monthly_data_gb,
        (raw_payload->>'monthly_bill_usd')::numeric as monthly_bill_usd,
        raw_payload->>'device_brand' as device_brand,
        raw_payload->>'device_model' as device_model

    from (
        select *,
               regexp_replace(
                   lower(trim(raw_payload->>'plan_type')),
                   '[-_]',
                   '',
                   'g'
               ) as normalized_plan

        from {{ ref('bronze_mobile_customers_raw') }}
        where raw_payload->>'customer_id' ~ '^\d+$'
    ) normalized_source
)

select distinct

    {{ dbt_utils.generate_surrogate_key([
        'customer_id',
        'phone_number',
        'record_uuid'
    ]) }} as subscription_id,

    {{ dbt_utils.generate_surrogate_key([
        'customer_id',
        'phone_number'
    ]) }} as line_id,

    customer_id,
    phone_number,
    status,
    operator,
    plan_type,
    latitude,
    longitude,
    monthly_data_gb,
    monthly_bill_usd,
    device_brand,
    device_model,
    record_uuid

from base
where phone_number is not null

{{ config(materialized='table') }}

with source as (

    select
        raw_payload
    from {{ ref('bronze_mobile_customers_raw') }}
    where raw_payload->>'customer_id' ~ '^\d+$'

),

exploded as (

    select
        (raw_payload->>'customer_id')::int as customer_id,
        trim(raw_payload->>'phone_number') as phone_number,
        raw_payload->>'record_uuid' as record_uuid,
        payment

    from source

    cross join lateral
        jsonb_array_elements(
            case
                when jsonb_typeof(raw_payload->'payment_history') = 'array'
                    then raw_payload->'payment_history'
                else '[]'::jsonb
            end
        ) as payment

),

cleaned as (

    select
        customer_id,
        phone_number,

        case
            when payment->>'amount' ~ '^\d+(\.\d+)?$'
                then (payment->>'amount')::numeric
            else null
        end as payment_amount,

        payment->>'status' as payment_status,

        case
            when payment->>'date' ~ '^\d{4}-\d{2}-\d{2}$'
                then (payment->>'date')::date
            else null
        end as payment_date

    from exploded

)

select
    {{ dbt_utils.generate_surrogate_key([
        'customer_id',
        'phone_number',
        'payment_date',
        'payment_amount'
    ]) }} as payment_id,

    customer_id,
    phone_number,
    payment_date,
    payment_amount,
    payment_status

from cleaned
where payment_date is not null
  and payment_amount is not null
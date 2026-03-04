{{ config(materialized='table') }}

with base as (

    select
        lower(trim(raw_payload->>'city'))    as city,
        lower(trim(raw_payload->>'country')) as country
    from {{ ref('bronze_mobile_customers_raw') }}

),

deduplicated as (

    select distinct
        city,
        country
    from base
    where city is not null
      and country is not null

)

select
    {{ dbt_utils.generate_surrogate_key(['city','country']) }} as location_id,
    city,
    country
from deduplicated
-- gold.dim_zone — статичний довідник, тому table, не incremental.

{{ config(materialized='table') }}

select
    location_id::int as zone_key,
    coalesce(nullif(borough, ''), 'Unknown') as borough,
    coalesce(nullif(zone_name, ''), 'Unknown') as zone_name,
    coalesce(nullif(service_zone, ''), 'Unknown') as service_zone
from {{ ref('seed_taxi_zone') }}

union all

select
    -1 as zone_key,
    'Unknown' as borough,
    'Unknown' as zone_name,
    'Unknown' as service_zone

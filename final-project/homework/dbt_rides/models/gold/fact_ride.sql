-- gold.fact_ride — accumulating snapshot, один рядок на поїздку.
-- Ключа, якого нема у вимірі, не викидаю: складаю в -1 / 'unknown'.

{{ config(
    materialized='incremental',
    unique_key='ride_id',
    incremental_strategy='delete+insert',
) }}

with changed as (
    select *
    from {{ ref('rides') }}
    {% if is_incremental() %}
    where _ingested_at > {{ high_watermark() }}
    {% endif %}
)

select
    rides.ride_id,
    coalesce(pickup.zone_key, -1) as pickup_zone_key,
    coalesce(dropoff.zone_key, -1) as dropoff_zone_key,
    coalesce(drivers.driver_key, 'unknown') as driver_key,
    rides.rider_id,
    rides.status,
    rides.requested_at,
    rides.accepted_at,
    rides.started_at,
    rides.completed_at,
    rides.cancelled_at,
    rides.paid_at,
    {{ utc_date('rides.requested_at') }} as requested_date,
    (date_trunc('hour', rides.requested_at at time zone 'UTC') at time zone 'UTC') as requested_hour,
    rides.wait_seconds,
    rides.trip_seconds,
    rides.distance_km,
    rides.fare_amount,
    rides.surge_multiplier,
    rides.tolls_amount,
    rides.tip_amount,
    rides.total_amount,
    rides.currency,
    rides.payment_method,
    rides.payment_amount,
    rides.cancelled_by,
    rides.cancel_reason,
    rides.cancel_stage,
    rides._ingested_at,
    '{{ run_started_at }}'::timestamptz as _loaded_at
from changed as rides
left join {{ ref('dim_zone') }} as pickup
    on pickup.zone_key = rides.pickup_zone_id
left join {{ ref('dim_zone') }} as dropoff
    on dropoff.zone_key = rides.dropoff_zone_id
left join {{ ref('dim_driver') }} as drivers
    on drivers.driver_key = rides.driver_id

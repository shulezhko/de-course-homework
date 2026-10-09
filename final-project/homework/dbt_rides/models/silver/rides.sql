-- silver.rides — grain: ride_id. Поїздка з'являється, коли є ride_requested.
-- Нова подія перебудовує поїздку з усієї історії, не з одного батча.

{{ config(
    materialized='incremental',
    unique_key='ride_id',
    incremental_strategy='delete+insert',
) }}

with touched as (
    select distinct ride_id
    from {{ ref('events') }}
    {% if is_incremental() %}
    where _ingested_at > {{ high_watermark() }}
    {% endif %}
),

history as (
    select events.*
    from {{ ref('events') }} as events
    inner join touched using (ride_id)
),

folded as (
    select
        ride_id,
        max(payload #>> '{rider,id}') filter (where event_type = 'ride_requested') as rider_id,
        max(payload #>> '{rider,platform}') filter (where event_type = 'ride_requested') as rider_platform,
        max(payload #>> '{rider,app_version}') filter (where event_type = 'ride_requested') as app_version,
        max(payload #>> '{requested_vehicle}') filter (where event_type = 'ride_requested') as requested_vehicle,
        coalesce(
            max(payload #>> '{driver,id}') filter (where event_type = 'ride_accepted'),
            max(payload #>> '{driver,id}') filter (where event_type = 'ride_started'),
            max(payload #>> '{driver,id}') filter (where event_type = 'ride_completed')
        ) as driver_id,
        bool_or(event_type = 'ride_completed') as has_completed,
        bool_or(event_type = 'ride_cancelled') as has_cancelled,
        bool_or(event_type = 'ride_started') as has_started,
        bool_or(event_type = 'ride_accepted') as has_accepted,
        max(occurred_at) filter (where event_type = 'ride_requested') as requested_at,
        max(occurred_at) filter (where event_type = 'ride_accepted') as accepted_at,
        max(occurred_at) filter (where event_type = 'ride_started') as started_at,
        max(occurred_at) filter (where event_type = 'ride_completed') as completed_at,
        max(occurred_at) filter (where event_type = 'ride_cancelled') as cancelled_at,
        max(occurred_at) filter (where event_type = 'payment_captured') as paid_at,
        coalesce(
            max((payload #>> '{pickup,zone_id}')::int) filter (where event_type = 'ride_started'),
            max((payload #>> '{pickup,zone_id}')::int) filter (where event_type = 'ride_requested')
        ) as pickup_zone_id,
        coalesce(
            max((payload #>> '{dropoff,zone_id}')::int) filter (where event_type = 'ride_completed'),
            max((payload #>> '{dropoff,zone_id}')::int) filter (where event_type = 'ride_requested')
        ) as dropoff_zone_id,
        max((payload #>> '{surge_estimate}')::numeric) filter (where event_type = 'ride_requested') as surge_estimate,
        max((payload #>> '{distance_km}')::numeric(8, 2)) filter (where event_type = 'ride_completed') as distance_km,
        max((payload #>> '{fare,surge_multiplier}')::numeric(4, 2)) filter (where event_type = 'ride_completed') as surge_multiplier,
        max((payload #>> '{fare,amount}')::numeric(10, 2)) filter (where event_type = 'ride_completed') as fare_amount,
        max((payload #>> '{fare,tolls}')::numeric(10, 2)) filter (where event_type = 'ride_completed') as tolls_amount,
        -- tip лишається NULL для готівки, не замінюю на 0
        max((payload #>> '{fare,tip}')::numeric(10, 2)) filter (where event_type = 'ride_completed') as tip_amount,
        max((payload #>> '{fare,total}')::numeric(10, 2)) filter (where event_type = 'ride_completed') as total_amount,
        max(payload #>> '{fare,currency}') filter (where event_type = 'ride_completed') as currency,
        max(payload #>> '{cancelled_by}') filter (where event_type = 'ride_cancelled') as cancelled_by,
        max(payload #>> '{reason}') filter (where event_type = 'ride_cancelled') as cancel_reason,
        max(payload #>> '{stage}') filter (where event_type = 'ride_cancelled') as cancel_stage,
        max(payload #>> '{payment,method}') filter (where event_type = 'payment_captured') as payment_method,
        max(payload #>> '{payment,psp_reference}') filter (where event_type = 'payment_captured') as psp_reference,
        max((payload #>> '{payment,amount}')::numeric(10, 2)) filter (where event_type = 'payment_captured') as payment_amount,
        max(_ingested_at) as _ingested_at
    from history
    group by ride_id
    having bool_or(event_type = 'ride_requested')
)

select
    ride_id,
    rider_id,
    rider_platform,
    app_version,
    requested_vehicle,
    driver_id,
    case
        when has_completed then 'completed'
        when has_cancelled then 'cancelled'
        when has_started then 'in_progress'
        when has_accepted then 'accepted'
        else 'requested'
    end as status,
    requested_at,
    accepted_at,
    started_at,
    completed_at,
    -- completed і cancelled разом тест не пускає: завершення виграє
    case when has_completed then null else cancelled_at end as cancelled_at,
    paid_at,
    pickup_zone_id,
    dropoff_zone_id,
    case
        when accepted_at is not null
            then extract(epoch from (accepted_at - requested_at))::int
    end as wait_seconds,
    case
        when completed_at is not null and started_at is not null
            then extract(epoch from (completed_at - started_at))::int
    end as trip_seconds,
    surge_estimate,
    distance_km,
    surge_multiplier,
    fare_amount,
    tolls_amount,
    tip_amount,
    total_amount,
    currency,
    case when has_completed then null else cancelled_by end as cancelled_by,
    case when has_completed then null else cancel_reason end as cancel_reason,
    case when has_completed then null else cancel_stage end as cancel_stage,
    payment_method,
    psp_reference,
    payment_amount,
    _ingested_at,
    '{{ run_started_at }}'::timestamptz as _loaded_at
from folded

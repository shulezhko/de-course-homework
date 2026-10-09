-- gold.dim_driver — поточні атрибути з ride_accepted.
-- Рейтинг береться за occurred_at, тому водія рахую з усієї його історії,
-- а не з нового батча: запізніла стара подія не має затерти новішу.

{{ config(
    materialized='incremental',
    unique_key='driver_key',
    incremental_strategy='delete+insert',
) }}

with accepted as (
    select
        payload #>> '{driver,id}' as driver_key,
        payload #>> '{driver,vehicle,type}' as vehicle_type,
        payload #>> '{driver,vehicle,medallion}' as medallion,
        (payload #>> '{driver,rating}')::numeric as rating,
        occurred_at,
        event_id,
        _ingested_at
    from {{ ref('events') }}
    where event_type = 'ride_accepted'
      and payload #>> '{driver,id}' is not null
),

touched as (
    select distinct driver_key
    from accepted
    {% if is_incremental() %}
    where _ingested_at > {{ high_watermark() }}
    {% endif %}
),

history as (
    select accepted.*
    from accepted
    inner join touched using (driver_key)
),

drivers as (
    select
        driver_key,
        coalesce(
            (array_agg(vehicle_type order by occurred_at desc, event_id desc))[1],
            'unknown'
        ) as vehicle_type,
        (array_agg(medallion order by occurred_at desc, event_id desc))[1] as medallion,
        (array_agg(rating order by occurred_at desc, event_id desc))[1] as latest_rating,
        min(occurred_at) as first_seen_at,
        max(occurred_at) as last_seen_at,
        max(_ingested_at) as _ingested_at,
        '{{ run_started_at }}'::timestamptz as _loaded_at
    from history
    group by driver_key
)

select * from drivers

union all

select
    'unknown' as driver_key,
    'unknown' as vehicle_type,
    null::text as medallion,
    null::numeric as latest_rating,
    null::timestamptz as first_seen_at,
    null::timestamptz as last_seen_at,
    null::timestamptz as _ingested_at,
    '{{ run_started_at }}'::timestamptz as _loaded_at

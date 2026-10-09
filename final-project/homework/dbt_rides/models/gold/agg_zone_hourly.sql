-- gold.agg_zone_hourly — grain (requested_hour, pickup_zone_key).
-- Якщо поїздка змінила зону, стара зона лишилась би із зайвим рядком.
-- Тому перераховую всю годину і прибираю бакети, яких у fact уже немає.

{{ config(
    materialized='incremental',
    unique_key=['requested_hour', 'pickup_zone_key'],
    incremental_strategy='delete+insert',
    post_hook="delete from {{ this }} as agg where not exists (select 1 from {{ ref('fact_ride') }} as fact where fact.requested_hour = agg.requested_hour and fact.pickup_zone_key = agg.pickup_zone_key)"
) }}

with affected_hours as (
    select distinct requested_hour
    from {{ ref('fact_ride') }}
    {% if is_incremental() %}
    where _ingested_at > {{ high_watermark() }}
    {% endif %}
)

select
    fact.requested_hour,
    fact.pickup_zone_key,
    count(*)::int as rides_requested,
    (count(*) filter (where fact.status = 'completed'))::int as rides_completed,
    (count(*) filter (where fact.status = 'cancelled'))::int as rides_cancelled,
    coalesce(sum(fact.total_amount) filter (where fact.status = 'completed'), 0)::numeric(12, 2) as gross_revenue,
    coalesce(sum(fact.tip_amount) filter (where fact.status = 'completed'), 0)::numeric(12, 2) as tips,
    avg(fact.wait_seconds)::numeric(10, 2) as avg_wait_seconds,
    max(fact._ingested_at) as _ingested_at,
    '{{ run_started_at }}'::timestamptz as _loaded_at
from {{ ref('fact_ride') }} as fact
inner join affected_hours using (requested_hour)
group by fact.requested_hour, fact.pickup_zone_key

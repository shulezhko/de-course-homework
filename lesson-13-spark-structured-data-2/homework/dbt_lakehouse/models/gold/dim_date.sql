-- Крок 7: gold.dim_date — безперервний (gapless) календар, згенерований у SQL.
-- Межі min/max рахуємо по фактичних датах, що є FK хоч в одному факті.

with fact_dates as (
    select cast(pushed_at as date) as d from {{ ref('commits') }}
    union all
    select cast(opened_at as date) from {{ ref('pull_requests') }}
    union all
    select cast(merged_at as date) from {{ ref('pull_requests') }}
    union all
    select cast(opened_at as date) from {{ ref('issues') }}
    union all
    select cast(closed_at as date) from {{ ref('issues') }}
),

bounds as (
    select min(d) as min_d, max(d) as max_d
    from fact_dates
    where d is not null
),

calendar as (
    select explode(sequence(min_d, max_d, interval 1 day)) as date_day
    from bounds
)

select
    cast(date_format(date_day, 'yyyyMMdd') as int)   as date_id,
    date_day,
    dayofweek(date_day)                              as day_of_week,
    dayofweek(date_day) in (1, 7)                    as is_weekend,
    weekofyear(date_day)                             as iso_week,
    year(date_day)                                   as year
from calendar

-- Крок 10: gold.fact_repo_activity_daily — багатоджерельний rollup.
-- Грануляція: (repo_id, date_id) — активність репо за добу з 4 silver-джерел.
-- Патерн: денний агрегат на кожне джерело (своя метрика + нулі) → union all → group by.

with commits_daily as (
    select
        repo_name,
        cast(pushed_at as date)                   as day,
        count(*)                                  as commits,
        count(distinct author_email)              as distinct_committers,
        0                                         as prs_opened,
        0                                         as prs_merged,
        0                                         as issues_opened,
        0                                         as issues_closed,
        0                                         as stars,
        0                                         as forks
    from {{ ref('commits') }}
    group by repo_name, cast(pushed_at as date)
),

pr_opened_daily as (
    select
        repo_name, cast(opened_at as date) as day,
        0, 0, count(*), 0, 0, 0, 0, 0
    from {{ ref('pull_requests') }}
    where opened_at is not null
    group by repo_name, cast(opened_at as date)
),

pr_merged_daily as (
    select
        repo_name, cast(merged_at as date) as day,
        0, 0, 0, count(*), 0, 0, 0, 0
    from {{ ref('pull_requests') }}
    where merged_at is not null
    group by repo_name, cast(merged_at as date)
),

issue_opened_daily as (
    select
        repo_name, cast(opened_at as date) as day,
        0, 0, 0, 0, count(*), 0, 0, 0
    from {{ ref('issues') }}
    where opened_at is not null
    group by repo_name, cast(opened_at as date)
),

issue_closed_daily as (
    select
        repo_name, cast(closed_at as date) as day,
        0, 0, 0, 0, 0, count(*), 0, 0
    from {{ ref('issues') }}
    where closed_at is not null
    group by repo_name, cast(closed_at as date)
),

stars_forks_daily as (
    select
        repo_name, cast(created_at as date) as day,
        0, 0, 0, 0, 0, 0,
        count(case when event_type = 'WatchEvent' then 1 end) as stars,
        count(case when event_type = 'ForkEvent' then 1 end)  as forks
    from {{ ref('events') }}
    where event_type in ('WatchEvent', 'ForkEvent')
    group by repo_name, cast(created_at as date)
),

unioned as (
    select * from commits_daily
    union all select * from pr_opened_daily
    union all select * from pr_merged_daily
    union all select * from issue_opened_daily
    union all select * from issue_closed_daily
    union all select * from stars_forks_daily
),

rolled as (
    select
        repo_name,
        day,
        sum(commits)              as commits,
        sum(distinct_committers)  as distinct_committers,
        sum(prs_opened)           as prs_opened,
        sum(prs_merged)           as prs_merged,
        sum(issues_opened)        as issues_opened,
        sum(issues_closed)        as issues_closed,
        sum(stars)                as stars,
        sum(forks)                as forks
    from unioned
    group by repo_name, day
)

select
    md5(concat_ws('|', md5(repo_name), cast(date_format(day, 'yyyyMMdd') as int))) as activity_id,
    md5(repo_name)                               as repo_id,
    cast(date_format(day, 'yyyyMMdd') as int)    as date_id,
    commits,
    distinct_committers,
    prs_opened,
    prs_merged,
    issues_opened,
    issues_closed,
    stars,
    forks
from rolled

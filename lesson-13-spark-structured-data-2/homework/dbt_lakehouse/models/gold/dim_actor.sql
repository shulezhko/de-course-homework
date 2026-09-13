-- Крок 6: gold.dim_actor — conformed dimension, один рядок на актора.
-- Джерело: events, actor_login IS NOT NULL.

select
    md5(actor_login)                            as actor_id,
    actor_login,
    -- is_bot: login закінчується на [bot]
    actor_login like '%[bot]'                   as is_bot,
    min(created_at)                             as first_seen_at,
    max(created_at)                             as last_seen_at,
    count(*)                                    as event_count,
    count(distinct repo_name)                   as distinct_repos
from {{ ref('events') }}
where actor_login is not null
group by actor_login

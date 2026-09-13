-- Крок 5: gold.dim_repo — conformed dimension, один рядок на репозиторій.
-- Джерело: events.

select
    md5(repo_name)                              as repo_id,
    repo_name,
    repo_owner,
    min(created_at)                             as first_seen_at,
    max(created_at)                             as last_seen_at,
    count(*)                                    as event_count,
    -- is_forked: чи була хоч одна подія ForkEvent по цьому репо
    max(case when event_type = 'ForkEvent' then true else false end) as is_forked
from {{ ref('events') }}
group by repo_name, repo_owner

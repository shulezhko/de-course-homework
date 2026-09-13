-- Крок 3: silver.pull_requests — остання версія стану PR (latest-state).
-- Джерело: events, лише PullRequestEvent. from_json(payload, PR_SCHEMA).
-- Грануляція: один рядок на (repo_name, pr_number) — з останньої за часом події.

with parsed as (
    select
        repo_name,
        event_id,
        created_at                                    as event_at,
        from_json(payload, '{{ var("pr_schema") }}')  as p
    from {{ ref('events') }}
    where event_type = 'PullRequestEvent'
),

-- беремо стан з останньої події по кожній PR
latest as (
    select *
    from parsed
    qualify row_number() over (
        partition by repo_name, p.number
        order by event_at desc, event_id desc
    ) = 1
)

select
    repo_name,
    p.number                                          as pr_number,
    p.pull_request.title                              as title,
    p.pull_request.user.login                         as author_login,
    p.pull_request.state                              as state,
    p.pull_request.merged                             as is_merged,
    p.pull_request.draft                              as is_draft,
    to_timestamp(p.pull_request.created_at)           as opened_at,
    to_timestamp(p.pull_request.closed_at)            as closed_at,
    to_timestamp(p.pull_request.merged_at)            as merged_at,
    p.pull_request.additions                          as additions,
    p.pull_request.deletions                          as deletions,
    p.pull_request.changed_files                      as changed_files,
    p.pull_request.commits                            as commits_count,
    p.pull_request.comments                           as comments,
    p.pull_request.review_comments                    as review_comments,
    p.pull_request.author_association                 as author_association,
    p.pull_request.labels.name                        as label_names,
    p.action                                          as last_action,
    event_at                                          as last_event_at,
    (p.pull_request.additions + p.pull_request.deletions) as churn,
    -- hours_open: від opened_at до closed_at (або до last_event_at, якщо ще відкрита)
    (unix_timestamp(coalesce(to_timestamp(p.pull_request.closed_at), event_at))
        - unix_timestamp(to_timestamp(p.pull_request.created_at))) / 3600.0 as hours_open
from latest

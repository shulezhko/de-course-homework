-- Крок 4: silver.issues — злиття IssuesEvent та IssueCommentEvent (latest-state).
-- Обидва типи несуть знімок issue у payload. from_json(payload, ISSUE_SCHEMA).
-- Грануляція: один рядок на (repo_name, issue_number) з останньої події.

with parsed as (
    select
        repo_name,
        event_id,
        event_type,
        created_at                                       as event_at,
        from_json(payload, '{{ var("issue_schema") }}')  as p
    from {{ ref('events') }}
    where event_type in ('IssuesEvent', 'IssueCommentEvent')
),

-- скільки IssueCommentEvent потрапило по кожній issue (для comment_events_seen)
with_counts as (
    select
        *,
        count(case when event_type = 'IssueCommentEvent' then 1 end)
            over (partition by repo_name, p.issue.number) as comment_events_seen
    from parsed
),

latest as (
    select *
    from with_counts
    qualify row_number() over (
        partition by repo_name, p.issue.number
        order by event_at desc, event_id desc
    ) = 1
)

select
    repo_name,
    p.issue.number                                   as issue_number,
    p.issue.title                                    as title,
    p.issue.user.login                               as author_login,
    p.issue.state                                    as state,
    to_timestamp(p.issue.created_at)                 as opened_at,
    to_timestamp(p.issue.closed_at)                  as closed_at,
    p.issue.comments                                 as comments,
    p.issue.labels.name                              as label_names,
    comment_events_seen,
    event_at                                         as last_event_at,
    -- hours_to_close: NULL якщо не закрита
    case
        when p.issue.closed_at is not null then
            (unix_timestamp(to_timestamp(p.issue.closed_at))
                - unix_timestamp(to_timestamp(p.issue.created_at))) / 3600.0
    end                                              as hours_to_close
from latest

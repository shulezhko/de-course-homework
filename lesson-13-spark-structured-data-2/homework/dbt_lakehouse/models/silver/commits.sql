-- Крок 2: silver.commits — розкриття масиву commits (grain = коміт).
-- Джерело: events, лише PushEvent. from_json(payload, PUSH_SCHEMA) → explode.

with pushes as (
    select
        event_id,
        repo_name,
        actor_login                                     as pushed_by,
        created_at                                      as pushed_at,
        from_json(payload, '{{ var("push_schema") }}')  as p
    from {{ ref('events') }}
    where event_type = 'PushEvent'
),

exploded as (
    select
        c.sha                                           as commit_sha,
        repo_name,
        pushed_by,
        -- branch: ref без префікса refs/heads/
        regexp_replace(p.ref, '^refs/heads/', '')       as branch,
        c.author.name                                   as author_name,
        c.author.email                                  as author_email,
        c.message                                       as message,
        c.`distinct`                                    as is_distinct,
        pushed_at,
        event_id,
        -- merge-коміт: message починається з "Merge "
        (c.message like 'Merge %')                      as is_merge_commit,
        -- перший рядок message
        split(c.message, '\n')[0]                       as message_subject,
        length(c.message)                               as message_length
    from pushes
    lateral view explode(p.commits) t as c
),

-- один коміт може прийти в кількох push — лишаємо найраніший pushed_at
-- tie-break за event_id обов'язковий для детермінованості
-- Spark SQL не має QUALIFY → підзапит з row_number
ranked as (
    select
        exploded.*,
        row_number() over (
            partition by commit_sha order by pushed_at, event_id
        ) as rn
    from exploded
)

select
    commit_sha, repo_name, pushed_by, branch,
    author_name, author_email, message, is_distinct,
    pushed_at, is_merge_commit, message_subject, message_length
from ranked
where rn = 1

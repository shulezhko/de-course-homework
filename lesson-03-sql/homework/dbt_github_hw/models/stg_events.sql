{{ config(materialized='view') }}
-- =====================================================================
-- TASK 1 — stg_events (12 балів)
-- Чистий шар поверх сирих GitHub-подій.
-- DQ-фільтри: лише 5 цільових типів подій, без ботів, без порожніх push.
-- Матеріалізація: view (для наскрізного partition pruning у Task 7).
-- =====================================================================

SELECT
    id,
    event_type,
    created_at,
    event_date,
    actor_login,
    repo_name,
    payload_commit_count,
    payload_action,
    payload_ref
FROM read_parquet('{{ var("events_path") }}', hive_partitioning = true)
WHERE
    -- Лише 5 цільових типів подій
    event_type IN ('PushEvent', 'IssuesEvent', 'PullRequestEvent', 'WatchEvent', 'IssueCommentEvent')
    -- Прибрати ботів (actor_login закінчується на [bot])
    AND actor_login NOT LIKE '%[bot]'
    -- Прибрати порожні push (PushEvent з 0 комітів)
    AND NOT (event_type = 'PushEvent' AND payload_commit_count = 0)

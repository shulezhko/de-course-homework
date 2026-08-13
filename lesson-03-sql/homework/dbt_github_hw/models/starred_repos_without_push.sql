-- =====================================================================
-- TASK 5 — starred_repos_without_push (12 балів)
-- Репозиторії, які отримали зірку (WatchEvent), але НЕ мали жодного
-- PushEvent у наборі даних. Anti-join через NOT EXISTS.
-- =====================================================================

SELECT DISTINCT
    w.repo_name
FROM {{ ref('stg_events') }} w
WHERE w.event_type = 'WatchEvent'
  AND NOT EXISTS (
      SELECT 1
      FROM {{ ref('stg_events') }} p
      WHERE p.event_type = 'PushEvent'
        AND p.repo_name = w.repo_name
  )
ORDER BY w.repo_name

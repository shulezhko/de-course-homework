-- =====================================================================
-- TASK 6 — mart_category_daily (20 балів)
-- Широка вітрина: 3-way join stg_events + event_categories + calendar.
-- Грануляція: один рядок на (event_date, category).
-- =====================================================================

SELECT
    e.event_date,
    c.is_weekend,
    cat.category,
    COUNT(*)                       AS events,
    COUNT(DISTINCT e.repo_name)    AS distinct_repos,
    COUNT(DISTINCT e.actor_login)  AS distinct_actors
FROM {{ ref('stg_events') }} e
JOIN {{ ref('event_categories') }} cat
    ON e.event_type = cat.event_type
JOIN {{ ref('calendar') }} c
    ON e.event_date = c.day
GROUP BY e.event_date, c.is_weekend, cat.category
ORDER BY e.event_date, cat.category

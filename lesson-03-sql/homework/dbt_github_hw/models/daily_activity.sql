-- =====================================================================
-- TASK 3 — daily_activity (12 балів)
-- Кількість подій по днях + накопичувальний (running) підсумок.
-- Патерн: агрегація в CTE → SUM() OVER (ORDER BY ...) для running total.
-- =====================================================================

WITH daily AS (
    SELECT
        event_date,
        COUNT(*) AS events
    FROM {{ ref('stg_events') }}
    GROUP BY event_date
)

SELECT
    event_date,
    events,
    SUM(events) OVER (
        ORDER BY event_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_events
FROM daily
ORDER BY event_date

-- =====================================================================
-- TASK 4 — daily_activity_change (12 балів)
-- Зміна кількості подій день-до-дня.
-- Патерн: агрегація по дню → LAG() OVER (ORDER BY event_date).
-- Перший день має NULL у prev_day_events та delta_events.
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
    LAG(events) OVER (ORDER BY event_date) AS prev_day_events,
    events - LAG(events) OVER (ORDER BY event_date) AS delta_events
FROM daily
ORDER BY event_date

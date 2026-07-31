-- =====================================================================
-- TASK 7 — report_category_week (20 балів)
-- Оптимізована версія report_category_week_naive.
--
-- Ключова зміна: join до calendar по СИРІЙ партиційній колоні
-- (e.event_date = c.day) замість strftime-перетворення.
-- Це дозволяє DuckDB пропагувати фільтр iso_week = 2 через join
-- на партиційну колону event_date → partition pruning: 7 партицій замість 14.
-- =====================================================================

SELECT
    c.iso_week,
    cat.category,
    COUNT(*) AS events
FROM {{ ref('stg_events') }} e
JOIN {{ ref('calendar') }} c
    ON e.event_date = c.day
JOIN {{ ref('event_categories') }} cat
    ON e.event_type = cat.event_type
WHERE c.iso_week = 2
GROUP BY c.iso_week, cat.category
ORDER BY cat.category

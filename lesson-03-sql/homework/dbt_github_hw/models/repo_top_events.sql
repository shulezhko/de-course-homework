-- =====================================================================
-- TASK 2 — repo_top_events (12 балів)
-- TOP-5 репозиторіїв за кількістю подій у межах кожного event_type.
-- Патерн: агрегація → ROW_NUMBER() OVER (...) → QUALIFY (DuckDB-спосіб).
-- QUALIFY фільтрує по window function прямо у тому ж SELECT — без підзапиту.
-- =====================================================================

SELECT
    event_type,
    repo_name,
    COUNT(*) AS event_count,
    ROW_NUMBER() OVER (
        PARTITION BY event_type
        ORDER BY COUNT(*) DESC, repo_name
    ) AS type_rank
FROM {{ ref('stg_events') }}
GROUP BY event_type, repo_name
QUALIFY type_rank <= 5
ORDER BY event_type, type_rank

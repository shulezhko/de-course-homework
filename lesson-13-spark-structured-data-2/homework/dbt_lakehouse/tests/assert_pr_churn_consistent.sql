-- Тест: churn у fact_pull_request завжди = additions + deletions.
-- Специфікація: ../../SPEC.md → «Тести». Тест падає, якщо запит поверне рядки.
select *
from {{ ref('fact_pull_request') }}
where churn <> additions + deletions

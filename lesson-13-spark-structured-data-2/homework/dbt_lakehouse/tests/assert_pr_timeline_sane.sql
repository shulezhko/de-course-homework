-- Тест: немає PR, де merged_at < opened_at або closed_at < opened_at.
-- Специфікація: ../../SPEC.md → «Тести». Тест падає, якщо запит поверне рядки.
select *
from {{ ref('pull_requests') }}
where merged_at < opened_at
   or closed_at < opened_at

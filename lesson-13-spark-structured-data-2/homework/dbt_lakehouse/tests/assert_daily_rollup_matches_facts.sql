-- Тест: сума commits у fact_repo_activity_daily = кількість рядків у fact_commit.
-- Специфікація: ../../SPEC.md → «Тести». Тест падає, якщо запит поверне рядки.
select
    (select sum(commits) from {{ ref('fact_repo_activity_daily') }})  as rollup_commits,
    (select count(*) from {{ ref('fact_commit') }})                   as fact_commits
having rollup_commits <> fact_commits

-- Крок 9: gold.fact_pull_request — fact table (grain = PR).
-- Джерело: pull_requests.

select
    md5(concat_ws('|', repo_name, pr_number))          as pr_id,
    md5(repo_name)                                     as repo_id,
    md5(author_login)                                  as author_id,
    cast(date_format(opened_at, 'yyyyMMdd') as int)    as opened_date_id,
    -- merged_date_id: NULL якщо PR не змерджено
    cast(date_format(merged_at, 'yyyyMMdd') as int)    as merged_date_id,
    state,
    is_merged,
    is_draft,
    additions,
    deletions,
    churn,
    changed_files,
    commits_count,
    comments,
    review_comments,
    hours_open,
    -- label_count: size(NULL) = -1, тому через CASE
    case when label_names is null then 0 else size(label_names) end as label_count
from {{ ref('pull_requests') }}

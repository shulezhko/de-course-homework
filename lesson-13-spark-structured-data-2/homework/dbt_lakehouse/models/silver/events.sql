{{ config(materialized='incremental', incremental_strategy='append') }}

-- Крок 1: silver.events — базовий плаский шар подій, payload несемо далі сирим рядком.
-- Джерело: bronze.raw_events. Матеріалізація incremental (append).

with src as (
    select *
    from {{ source('bronze', 'raw_events') }}

    {% if is_incremental() %}
    -- при доливці беремо лише події, новіші за вже завантажені
    where _ingested_at > (select max(_ingested_at) from {{ this }})
    {% endif %}
),

filtered as (
    select
        id                                          as event_id,
        type                                        as event_type,
        actor.login                                 as actor_login,
        repo.name                                   as repo_name,
        split(repo.name, '/')[0]                    as repo_owner,
        to_timestamp(created_at)                    as created_at,
        payload,
        _ingested_at,
        _source_file
    from src
    where type in (
            'PushEvent', 'PullRequestEvent', 'IssuesEvent',
            'IssueCommentEvent', 'WatchEvent', 'ForkEvent'
        )
        -- public = true; NULL відкидаємо свідомо (NULL = true дає NULL → рядок відпадає)
        and public = true
        and id is not null
        and repo.name is not null
        and created_at is not null
)

-- дедуп по event_id
select *
from filtered
qualify row_number() over (partition by event_id order by _ingested_at) = 1

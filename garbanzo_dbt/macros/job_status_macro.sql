{% macro calculate_daily_job_update(table_name) %}

with today as (
    select *
    from {{ ref(table_name) }}
    where
    insert_date = '{{ var("CURRENT_DATE") }}'::date
),

yesterday as (
    select *
    from {{ ref(table_name) }}
    where
    insert_date = ('{{ var("CURRENT_DATE") }}'::date - 1)
)

select
    case
        when today.job_id is null then 'Position closed'
        when yesterday.job_id is null then 'Position opened'
        else 'No change'
    end as job_status
, coalesce(today.job_id, yesterday.job_id) || coalesce(today.insert_date, yesterday.insert_date) as update_job_id
, coalesce(today.absolute_url, yesterday.absolute_url) as absolute_url
, coalesce(today.title , yesterday.title ) as title
, coalesce(today.department , yesterday.department ) as department
, coalesce(today.company , yesterday.company ) as company
, coalesce(today.remote , yesterday.remote ) as remote
, coalesce(today.job_location , yesterday.job_location ) as job_location
, '{{ var("CURRENT_DATE") }}'::date as insert_date
from
today full outer join yesterday
on today.job_id = yesterday.job_id
where today.job_id is null or yesterday.job_id is null

{% endmacro %}

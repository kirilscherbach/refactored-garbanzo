{{
  config(
    materialized='incremental',
    unique_key='clean_job_id',
    description='This table contains a single open position registered as open per day and can be used for further analysis',
    alias='jobs_epic_daily',
    indexes=[
      {'columns': ['clean_job_id'], 'type': 'btree', 'unique': True},
      {'columns': ['insert_date'], 'type': 'btree'},
    ]
  )
}}

with base as (
  select * from {{ source('scraper_results', 'jobs_epic') }}
)

select
    clean_job_id
    , absolute_url
    , education
    , internal_job_id
    , requisition_id
    , title
    , department
    , company
    , remote
    , spotlight
    , type
    , city
    , state
    , country
    , updated_at
    , insert_ts
    , insert_date
from
    (select
        concat(internal_job_id, '-', requisition_id, '-', insert_ts::date) as clean_job_id
        , absolute_url
        , education
        , internal_job_id
        , requisition_id
        , title
        , department
        , company
        , remote
        , spotlight
        , type
        , city
        , state
        , country
        , updated_at
        , insert_ts
        , insert_ts::date as insert_date
        , row_number() over (partition by concat(internal_job_id, '-', requisition_id, '-', insert_ts::date) order by insert_ts desc) rn
    from {{ source('scraper_results', 'jobs_epic') }}
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        -- (uses > to include records whose timestamp occurred since the last run of this model)
        where insert_ts > (select max(insert_ts) from {{ this }})
    {% endif %}
    ) ordered_incr
where rn = 1

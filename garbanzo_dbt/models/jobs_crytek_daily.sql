{{
  config(
    materialized='incremental',
    unique_key='daily_job_id',
    description='This table contains a single open position from Crytek registered as open per day and can be used for further analysis',
    alias='jobs_crytek_daily',
    indexes=[
      {'columns': ['job_id', 'insert_date'], 'type': 'btree', 'unique': True},
      {'columns': ['insert_ts'], 'type': 'btree'},
    ]
  )
}}

select
      daily_job_id
    , absolute_url
    , job_id
    , title
    , department
    , company
    , remote
    , job_location
    , updated_at
    , insert_ts
    , insert_date
from
    (select
          concat(job_id, '-', insert_ts::date) as daily_job_id
        , hosted_url as absolute_url
        , job_id as job_id
        , job_title as title
        , department
        , team as company
        , case when lower(job_location) like '%remote%' then True else False end as remote
        , job_location
        , date_posted as updated_at
        , insert_ts
        , insert_ts::date as insert_date
        , row_number() over (partition by concat(job_id, '-', insert_ts::date) order by insert_ts desc) rn
    from {{ source('scraper_results', 'jobs_crytek') }}
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        -- (uses > to include records whose timestamp occurred since the last run of this model)
        where insert_ts > (select max(insert_ts) from {{ this }})
    {% endif %}
    ) ordered_incr
where rn = 1

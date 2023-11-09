with source as (
      select * from {{ source('scraper_results', 'jobs_epic') }}
),
renamed as (
    select
        {{ adapter.quote("id") }},
        {{ adapter.quote("absolute_url") }},
        {{ adapter.quote("education") }},
        {{ adapter.quote("internal_job_id") }},
        {{ adapter.quote("requisition_id") }},
        {{ adapter.quote("title") }},
        {{ adapter.quote("content") }},
        {{ adapter.quote("department") }},
        {{ adapter.quote("company") }},
        {{ adapter.quote("remote") }},
        {{ adapter.quote("spotlight") }},
        {{ adapter.quote("type") }},
        {{ adapter.quote("city") }},
        {{ adapter.quote("state") }},
        {{ adapter.quote("country") }},
        {{ adapter.quote("filtertext") }},
        {{ adapter.quote("updated_at") }},
        {{ adapter.quote("full_response") }},
        {{ adapter.quote("insert_ts") }}

    from source
)
select * from renamed

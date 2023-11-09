with source as (
      select * from {{ source('scraper_results', 'jobs_crytek') }}
),
renamed as (
    select
        {{ adapter.quote("id") }},
        {{ adapter.quote("job_id") }},
        {{ adapter.quote("additional_plain") }},
        {{ adapter.quote("created_at") }},
        {{ adapter.quote("description_plain") }},
        {{ adapter.quote("lists") }},
        {{ adapter.quote("job_title") }},
        {{ adapter.quote("hosted_url") }},
        {{ adapter.quote("apply_url") }},
        {{ adapter.quote("commitment") }},
        {{ adapter.quote("department") }},
        {{ adapter.quote("job_location") }},
        {{ adapter.quote("team") }},
        {{ adapter.quote("date_posted") }},
        {{ adapter.quote("valid_through") }},
        {{ adapter.quote("full_response") }},
        {{ adapter.quote("insert_ts") }}

    from source
)
select * from renamed

drop table if exists public.jobs_crytek;
CREATE TABLE public.jobs_crytek (
    job_id text,
    additional_plain text,
    created_at timestamp without time zone,
    description_plain text,
    lists JSONB,
    job_title text,
    hosted_url text,
    apply_url text,
    commitment text,
    department text,
    job_location text,
    team text,
    date_posted date,
    valid_through date,
    full_response JSONB,
    insert_ts timestamp without time zone
);

GRANT ALL on public.jobs_crytek to scraper;
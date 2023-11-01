drop table if exists public.jobs;
CREATE TABLE public.jobs (
    absolute_url text,
    education text,
    internal_job_id text,
    requisition_id text,
    title text,
    content text,
    department text,
    company text,
    remote text,
    spotlight text,
    type text,
    city text,
    state text,
    country text,
    filtertext text,
    updated_at timestamp without time zone,
    full_response JSONB,
    insert_ts timestamp without time zone
);

GRANT ALL on public.jobs to scraper
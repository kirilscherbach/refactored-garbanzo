drop table if exists public.jobs_epic;
CREATE TABLE public.jobs_epic (
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

GRANT ALL on public.jobs_epic to scraper;
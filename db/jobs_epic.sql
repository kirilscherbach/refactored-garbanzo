drop table if exists public.jobs_epic;
CREATE TABLE public.jobs_epic (
    id SERIAL PRIMARY KEY,
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

CREATE INDEX insert_ts_btree_epic ON jobs_epic USING btree
(
    insert_ts
);

GRANT ALL on public.jobs_epic to scraper;
GRANT ALL on public.jobs_epic to dbt_user;
GRANT ALL ON SEQUENCE jobs_epic_new_id_seq TO scraper;

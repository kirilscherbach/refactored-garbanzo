import logging

import requests

from garbanzo_scripts.webscrapers.db_utils import db_cursor

logger = logging.getLogger(__name__)


def run_сrytek_jobs_scraper():
    with db_cursor() as cur:
        i = 0
        base_url = "https://www.crytek.com/api/v1/lever-postings"
        logger.info(f"Requesting {base_url}")
        r = requests.get(base_url)
        rd = r.json()
        jobs = rd["postings"]
        position_count = len(jobs)
        for job in jobs:
            url = f"""{base_url}/{job["alias"]}"""
            logger.info(f"""Requesting {base_url}/{job["alias"]}""")
            j = requests.get(url)
            jd = j.json()
            data_dict = {
                "job_id": jd.get("id", "N/A"),
                "additional_plain": jd.get("additional_plain", "N/A"),
                "created_at": jd.get("created_at", "2000-01-01 00:00:00"),
                "description_plain": jd.get("description_plain", "N/A"),
                "lists": jd.get("lists", "N/A"),
                "job_title": jd.get("text", "N/A"),
                "hosted_url": jd.get("hosted_url", "N/A"),
                "apply_url": jd.get("apply_url", "N/A"),
                "commitment": jd.get("commitment", "N/A"),
                "department": jd.get("department", "N/A"),
                "job_location": jd.get("location", "N/A"),
                "team": jd.get("team", "N/A"),
                "date_posted": jd.get("date_posted", "2000-01-01"),
                "valid_through": jd.get("valid_through", "2000-01-01"),
                "full_response": j.text,
            }
            logger.info(f"""Inserting requisition_id {jd.get("id", "N/A")}""")
            query = """
            INSERT INTO jobs_crytek (
                          job_id
                        , additional_plain
                        , created_at
                        , description_plain
                        , lists
                        , job_title
                        , hosted_url
                        , apply_url
                        , commitment
                        , department
                        , job_location
                        , team
                        , date_posted
                        , valid_through
                        , full_response
                        , insert_ts)
            VALUES (
              %(job_id)s
            , %(additional_plain)s
            , %(created_at)s
            , %(description_plain)s
            , %(lists)s
            , %(job_title)s
            , %(hosted_url)s
            , %(apply_url)s
            , %(commitment)s
            , %(department)s
            , %(job_location)s
            , %(team)s
            , %(date_posted)s
            , %(valid_through)s
            , %(full_response)s
            , NOW()::timestamp
            )
            """
            cur.execute(query, data_dict)
            i += 1
        logger.info(f"Inserted {i} out of {position_count} records")


if __name__ == "__main__":
    run_сrytek_jobs_scraper()

import requests
import psycopg2
import os
from time import sleep

base_url = "http://mw-greenhouse-service-prod.debc.live.use1a.on.epicgames.com/api/job"


db_host = "localhost"
db_name = "scraper_db"
db_user = "scraper"
db_password = os.environ.get("KS_PG_PASSWORD")
conn = psycopg2.connect(f"dbname={db_name} user={db_user} host={db_host} password='{db_password}'")
cur = conn.cursor()
try:
    skip = 0
    page = 1
    while True:
        url = f"{base_url}?keyword=data&skip={skip}"
        print(f"Requesting {url}")
        r = requests.get(url)
        rd = r.json()
        position_count = rd["total"]
        for hit in rd["hits"]:
            r_position = requests.get(url=f"""{base_url}/{hit["id"]}""")
            rd_position = r_position.json()
            print(f"""Inserting requisition_id {rd_position.get("requisition_id", "N/A")}""")
            cur.execute(f"""
            INSERT INTO jobs (
                        absolute_url
                        , education
                        , internal_job_id
                        , requisition_id
                        , title, content
                        , department
                        , company
                        , remote
                        , spotlight
                        , type
                        , city
                        , state
                        , country
                        , filterText
                        , updated_at
                        , insert_ts)
            VALUES (
            '{rd_position.get("absolute_url", "N/A")}'
            , '{rd_position.get("education", "N/A")}'
            , '{rd_position.get("internal_job_id", "N/A")}'
            , '{rd_position.get("requisition_id", "N/A")}'
            , '{rd_position.get("title", "N/A")}'
            , '{rd_position.get("content", "N/A")}'
            , '{rd_position.get("department", "N/A")}'
            , '{rd_position.get("company", "N/A")}'
            , '{rd_position.get("remote", "N/A")}'
            , '{rd_position.get("spotlight", "N/A")}'
            , '{rd_position.get("type", "N/A")}'
            , '{rd_position.get("city", "N/A")}'
            , '{rd_position.get("state", "N/A")}'
            , '{rd_position.get("country", "N/A")}'
            , '{rd_position.get("filterText", "N/A")}'
            , '{rd_position.get("updated_at", "N/A")}'
            , NOW()::timestamp
            )
            """)
            conn.commit()
            skip += 1
        print(f"Finished page {page}")
        print(f"Inserted {skip} records")
        if position_count == skip:
            break
        else:
            page += 1
    print(f"Inserted {skip} records")
finally:
    cur.close()
    conn.close()



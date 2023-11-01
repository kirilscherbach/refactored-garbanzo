import requests
import psycopg2
import os

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
            data_dict = {
              "absolute_url": rd_position.get("absolute_url", "N/A")
            , "education": rd_position.get("education", "N/A")
            , "internal_job_id": rd_position.get("internal_job_id", "N/A")
            , "requisition_id": rd_position.get("requisition_id", "N/A")
            , "title": rd_position.get("title", "N/A")
            , "content": rd_position.get("content", "N/A")
            , "department": rd_position.get("department", "N/A")
            , "company": rd_position.get("company", "N/A")
            , "remote": rd_position.get("remote", "N/A")
            , "spotlight": rd_position.get("spotlight", "N/A")
            , "type": rd_position.get("type", "N/A")
            , "city": rd_position.get("city", "N/A")
            , "state": rd_position.get("state", "N/A")
            , "country": rd_position.get("country", "N/A")
            , "filterText": rd_position.get("filterText", "N/A")
            , "updated_at": rd_position.get("updated_at", "N/A")
            , "full_response": r_position.text
            }
            query = """
            INSERT INTO jobs (
                        absolute_url
                        , education
                        , internal_job_id
                        , requisition_id
                        , title
                        , content
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
                        , full_response
                        , insert_ts)
            VALUES ( 
              %(absolute_url)s
            , %(education)s
            , %(internal_job_id)s
            , %(requisition_id)s
            , %(title)s
            , %(content)s
            , %(department)s
            , %(company)s
            , %(remote)s
            , %(spotlight)s
            , %(type)s
            , %(city)s
            , %(state)s
            , %(country)s
            , %(filterText)s
            , %(updated_at)s
            , %(full_response)s
            , NOW()::timestamp
            )
            """
            cur.execute(query, data_dict)
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



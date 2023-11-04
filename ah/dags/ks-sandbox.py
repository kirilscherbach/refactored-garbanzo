from __future__ import annotations
import pendulum
from airflow import DAG
from airflow.decorators import task

with DAG(
    dag_id="01-ks-sandbox",
    catchup=False,
    start_date=pendulum.datetime(2023, 11, 1, tz="UTC"),
    schedule="@daily",
    tags=["produces", "dataset-scheduled"],
) as dag1:

    @task(
        task_id="epic_jobs_scraper2"
    )
    def epic_jobs_scraping():
        from garbanzo_scripts.webscrapers.epic_jobs import run_epic_jobs_scraper
        #from garbanzo_scripts.webscrapers.test import func_func
        run_epic_jobs_scraper()
        #func_func()
        return "Epic Jobs have been scraped successfully"

    task1 = epic_jobs_scraping()
from __future__ import annotations

import logging

import pendulum
from airflow.decorators import dag, task

logger = logging.getLogger(__name__)


@task(task_id="epic_jobs_scraper")
def epic_jobs_scraping():
    from garbanzo_scripts.webscrapers.epic_jobs import run_epic_jobs_scraper

    # from garbanzo_scripts.webscrapers.test import func_func
    run_epic_jobs_scraper()
    # func_func()
    return "Epic Jobs have been scraped successfully"


@task(task_id="crytek_jobs_scraper")
def сrytek_jobs_scraping():
    from garbanzo_scripts.webscrapers.crytek_jobs import run_сrytek_jobs_scraper

    # from garbanzo_scripts.webscrapers.test import func_func
    run_сrytek_jobs_scraper()
    # func_func()
    return "Crytek Jobs have been scraped successfully"


@task(task_id="dbt_model_calc")
def run_dbt_model(**context):
    import os

    os.chdir("/Users/kirils.cherbach/Documents/GitHub/refactored-garbanzo/garbanzo_dbt")
    cur_dt = context["ds"]
    dbt_vars = f"""'CURRENT_DATE': '{ cur_dt }'"""
    logger.info(f"starting with dt {cur_dt}")
    # dbt_vars = """'CURRENT_DATE': '2023-11-12'"""
    from dbt.cli.main import dbtRunner, dbtRunnerResult

    # initialize
    dbt = dbtRunner()

    # create CLI args as a list of strings
    # command is dbt run --vars "{'CURRENT_DATE': '2023-11-09'}"
    cli_args = ["run", "--vars", f"{{{dbt_vars}}}"]
    logger.info(f"Running dbt with args {cli_args}")
    # run the command
    res: dbtRunnerResult = dbt.invoke(cli_args)

    # inspect the results
    for r in res.result:
        logger.info(f"{r.node.name}: {r.status}")
    return "DBT model has run successfully"


@dag(
    dag_id="ks-sandbox",
    catchup=False,
    start_date=pendulum.datetime(year=2023, month=11, day=3, tz="UTC"),
    schedule_interval="30 12 * * *",
    tags=["main"],
)
def my_dag():
    task1 = epic_jobs_scraping()
    task2 = сrytek_jobs_scraping()
    task3 = run_dbt_model()

    task1
    task2


@dag(
    dag_id="ks-backfill-4",
    description="Special dag for backfillign DBT Model",
    catchup=True,
    start_date=pendulum.datetime(year=2023, month=11, day=4, tz="UTC"),
    schedule_interval="@daily",
    tags=["backfill"],
    max_active_runs=1,
)
def backfill_dag():
    task0 = run_dbt_model()


main_dag = my_dag()
backfill_dag = backfill_dag()

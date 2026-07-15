"""Lesson 3 — scheduling and the logical date (THE interview gotcha).

This DAG "processes" one day of data per run, daily at 06:30.
Open any run's log and compare:
  - logical date (ds)   -> WHICH day's data this run is for
  - actual wall clock   -> WHEN the run really executed
The run for Monday executes early TUESDAY, after Monday's data interval closed.

catchup=True + the start_date a few days back means Airflow will create the
missed daily runs the moment you un-pause this DAG. Watch it happen — that IS
backfill behaviour. (This is the only lesson DAG with catchup on, deliberately.)
"""
from datetime import datetime

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator


def process_one_day(ds, logical_date, **context):
    # `ds` is the logical date as 'YYYY-MM-DD' — you used it in every
    # templated string in the project DAG: journal_lines_{{ ds_nodash }}.zip
    print(f"logical date (ds): {ds}")
    print(f"logical_date obj : {logical_date}")
    print(f"wall clock now   : {pendulum.now('UTC')}")
    print(f"=> this run processes the feed file for business date {ds}")


with DAG(
    dag_id="l03_schedule",
    description="Lesson 3: schedule, logical date, catchup/backfill",
    start_date=pendulum.datetime(2026, 7, 10, tz="UTC"),
    schedule="30 6 * * *",   # cron: daily 06:30 UTC
    catchup=True,            # create runs for every interval since start_date
    max_active_runs=1,       # replay missed days one at a time, in order
    tags=["lesson"],
) as dag:

    PythonOperator(
        task_id="process_one_day",
        python_callable=process_one_day,
    )

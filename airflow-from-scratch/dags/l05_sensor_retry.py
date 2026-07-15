"""Lesson 5 — sensors, retries, and failure handling.

Flow: wait for ~/airflow-lab/landing/ready.txt to exist (sensor)
      -> a task that FAILS on its first attempt and succeeds on retry
      -> a notifier that runs only if something failed.

Run it:
  1. Trigger the DAG. The sensor sits in 'up_for_reschedule' — it is NOT
     holding a worker slot while it waits (mode="reschedule"). Talking point.
  2. In another terminal:  mkdir -p ~/airflow-lab/landing && touch ~/airflow-lab/landing/ready.txt
  3. Watch flaky_load go RED (up_for_retry), wait ~30s, watch it retry and go green.
     Open its log for attempt 1 vs attempt 2.
"""
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

LANDING_FILE = Path.home() / "airflow-lab" / "landing" / "ready.txt"


def file_has_arrived() -> bool:
    """A sensor's callable answers one question: 'can we proceed YET?'
    Return False -> Airflow re-checks later. Return True -> downstream runs."""
    print(f"poking for {LANDING_FILE} ...")
    return LANDING_FILE.exists()


def flaky_load(ti, **context):
    """Fails on attempt 1, succeeds on attempt 2 — so you can WATCH a retry.
    Real-world equivalents: a GCP 500, a network blip, BigQuery rate limit."""
    print(f"this is attempt number {ti.try_number}")
    if ti.try_number < 2:
        raise AirflowException("simulated transient failure — retry will fix it")
    print("load succeeded on retry ✔")


def alert(context):
    # on_failure_callback: in production this posts to Slack / PagerDuty / email.
    print(f"ALERT: task {context['task_instance'].task_id} finally failed "
          f"for run {context['ds']}")


with DAG(
    dag_id="l05_sensor_retry",
    description="Lesson 5: sensors, retries, failure callbacks",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["lesson"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
        "on_failure_callback": alert,   # fires only when retries are EXHAUSTED
    },
) as dag:

    wait_for_file = PythonSensor(
        task_id="wait_for_file",
        python_callable=file_has_arrived,
        mode="reschedule",     # release the worker slot between pokes
        poke_interval=15,      # check every 15s
        timeout=60 * 30,       # give up after 30 min -> task fails -> alert
    )

    load = PythonOperator(
        task_id="flaky_load",
        python_callable=flaky_load,
    )

    done = BashOperator(
        task_id="done",
        bash_command="echo 'pipeline complete'",
    )

    wait_for_file >> load >> done

"""Lesson 2 — operators, fan-out/fan-in, and trigger rules.

Shape:                 ┌─ check_csv ─┐
    start ─ download ──┤             ├── report ── cleanup
                       └─ check_zip ─┘
report runs only if BOTH checks succeed (default trigger rule).
cleanup runs NO MATTER WHAT (trigger_rule="all_done") — like a finally block.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def validate(kind: str):
    print(f"pretend-validating the {kind} file")


with DAG(
    dag_id="l02_operators",
    description="Lesson 2: fan-out, fan-in, trigger rules",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["lesson"],
) as dag:

    # EmptyOperator does nothing — used as a visual anchor / join point.
    start = EmptyOperator(task_id="start")

    download = BashOperator(
        task_id="download_file",
        bash_command="echo 'downloading feed...' && sleep 2",
    )

    # Two INDEPENDENT tasks -> Airflow runs them IN PARALLEL.
    check_csv = PythonOperator(
        task_id="check_csv",
        python_callable=validate,
        op_kwargs={"kind": "csv"},
    )
    check_zip = PythonOperator(
        task_id="check_zip",
        python_callable=validate,
        op_kwargs={"kind": "zip"},
    )

    report = BashOperator(
        task_id="report",
        bash_command="echo 'all checks passed'",
        # default trigger_rule="all_success": every upstream must be green.
    )

    cleanup = BashOperator(
        task_id="cleanup",
        bash_command="echo 'cleaning temp files (runs even on failure)'",
        trigger_rule="all_done",  # run when upstreams FINISH, pass or fail
    )

    # A list on either side of >> means fan-out / fan-in.
    start >> download >> [check_csv, check_zip] >> report >> cleanup

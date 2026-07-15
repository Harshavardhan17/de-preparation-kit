"""Lesson 1 — the smallest real DAG.

Three tasks: bash -> python -> bash. Trigger it manually from the UI,
then open each green square and read its log.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def say_hello():
    # Anything printed here lands in the task LOG — your main debugging tool.
    print("Hello from inside a PythonOperator!")
    return "hello"  # return values go to XCom (lesson 4)


with DAG(
    dag_id="l01_hello",
    description="Lesson 1: smallest possible pipeline",
    start_date=datetime(2026, 1, 1),  # required even for manual-only DAGs
    schedule=None,                    # None = runs ONLY when you trigger it
    catchup=False,
    tags=["lesson"],
) as dag:

    print_date = BashOperator(
        task_id="print_date",
        bash_command="date",
    )

    hello = PythonOperator(
        task_id="say_hello",
        python_callable=say_hello,
    )

    goodbye = BashOperator(
        task_id="say_goodbye",
        bash_command="echo 'pipeline finished'",
    )

    # Dependencies: print_date runs first, then hello, then goodbye.
    print_date >> hello >> goodbye

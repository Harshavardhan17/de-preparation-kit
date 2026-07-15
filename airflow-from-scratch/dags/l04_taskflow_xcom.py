"""Lesson 4 — passing data between tasks: XCom and the TaskFlow API.

Modern style: @task functions; return values flow to the next task
automatically (via XCom). After running, open Admin -> XComs in the UI and
find the actual stored values — XCom is just rows in Airflow's metadata DB.

That's exactly why you pass SMALL things (counts, paths, flags) and NEVER
dataframes or file contents: big data goes to storage (GCS/S3/disk); tasks
pass the POINTER to it.
"""
from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="l04_taskflow_xcom",
    description="Lesson 4: TaskFlow API and XCom",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["lesson"],
)
def taskflow_demo():

    @task
    def extract() -> dict:
        # Pretend we downloaded a feed file and counted it.
        # We return METADATA (small!), not the data itself.
        return {"path": "/data/journal_lines.csv", "row_count": 5000}

    @task
    def validate(feed: dict) -> dict:
        print(f"validating {feed['path']}")
        assert feed["row_count"] > 0, "empty feed!"
        feed["validated"] = True
        return feed

    @task
    def load(feed: dict):
        print(f"loading {feed['row_count']} rows from {feed['path']} "
              f"(validated={feed['validated']})")

    # Calling the functions WIRES the dependencies — no >> needed.
    load(validate(extract()))


taskflow_demo()

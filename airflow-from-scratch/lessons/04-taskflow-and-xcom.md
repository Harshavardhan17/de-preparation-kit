# Lesson 4 — Passing data between tasks: XCom & TaskFlow

**Run:** `l04_taskflow_xcom`

## The problem

Tasks run as **separate processes** — possibly on different machines (Composer
workers). A variable set in one task simply doesn't exist in the next. So how
did `validate` receive `extract`'s dictionary?

## XCom ("cross-communication")

**XCom is a small key-value store inside Airflow's metadata database.** When a
task returns a value, Airflow serializes it and writes a row: (dag_id, task_id,
run_id, key, value). A downstream task reads that row back.

Do this now: run the DAG, then open **Admin → XComs** in the UI. Find the
actual rows holding `{"path": ..., "row_count": 5000}`. Once you've *seen*
that XCom is literally database rows, two rules become obvious:

1. **Pass small things**: counts, paths, table names, flags, a manifest dict.
2. **Never pass data itself** — no DataFrames, no file contents. Big data goes
   to storage (GCS, a staging table, disk); tasks pass the *pointer* to it.
   This is exactly why the ledger pipeline writes to `processing/` in the
   bucket and to staging tables in BigQuery, and passes only names forward.

## Two styles, same machinery

**Classic style** (you'll see this in older/company code):
```python
def push(ti):          # ti = task instance
    ti.xcom_push(key="row_count", value=5000)
def pull(ti):
    n = ti.xcom_pull(task_ids="push_task", key="row_count")
```

**TaskFlow style** (Airflow 2, what the lesson DAG uses):
```python
@task
def extract() -> dict: ...
@task
def validate(feed: dict) -> dict: ...
load(validate(extract()))     # calling wires BOTH the XComs and the arrows
```
TaskFlow is just sugar over XCom — return values are pushed, parameters are
pulled, and the function-call chain creates the dependencies. Know both:
her team's DAGs may be classic style; new code is TaskFlow.

## Break it on purpose
- In `extract`, return a huge value: `{"blob": "x" * 10_000_000}`. Run it.
  It may work locally (SQLite is forgiving) but understand: on
  MySQL/Postgres-backed Airflow there are size limits, the metadata DB bloats,
  and the scheduler slows. Say in interviews: *"XCom is for metadata; payloads
  go to storage."* Now revert it.
- Delete the dependency: call `load(...)` with a hardcoded dict instead of the
  chain — watch the Graph view lose its arrows. Calling order IS the wiring.

## Interview checkpoint
- *How do tasks share data?* — via XCom, a key-value store in the metadata DB;
  return values are auto-pushed in TaskFlow style.
- *Would you pass a 2 GB dataset through XCom?* — never; write it to
  GCS/staging and pass the URI. Tasks exchange pointers, not payloads.
- *What's the TaskFlow API?* — the `@task` decorator style where function
  calls define both data flow (XCom) and dependencies.

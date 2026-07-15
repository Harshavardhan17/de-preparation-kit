# Lesson 2 — Operators, fan-out/fan-in, trigger rules

**Run:** `l02_operators`

## Operators are a catalog, not magic

Every operator does one thing and takes parameters. The ones you'll meet most:

| Operator | What it does | Where you've seen it |
|---|---|---|
| `BashOperator` | runs a shell command | ad-hoc scripts, gsutil calls |
| `PythonOperator` | calls a Python function | unzip, validation logic |
| `EmptyOperator` | nothing — a join/anchor point | start/end markers |
| `GCSObjectExistenceSensor` | waits for a bucket object | "is the feed here yet?" |
| `GCSToBigQueryOperator` | loads GCS file → BQ table | the daily load |
| `BigQueryInsertJobOperator` | runs SQL in BigQuery | the transforms |

There are hundreds more (provider packages: Google, AWS, Slack…). You don't
memorize them; you know the pattern: **task_id + parameters, wired with `>>`**.

## Dependencies

```python
a >> b            # b runs after a
a >> [b, c]       # fan-OUT: b and c run in PARALLEL after a
[b, c] >> d       # fan-IN: d waits for BOTH b and c
```

Parallelism is free: if two tasks have no arrow between them, the scheduler
runs them simultaneously. In `l02_operators`, `check_csv` and `check_zip` run
at the same time — watch the Gantt view to see it.

## Trigger rules — when does a task fire?

Default is `all_success`: every upstream must be green. Others you'll use:

| Rule | Fires when | Typical use |
|---|---|---|
| `all_success` (default) | all upstreams succeeded | normal flow |
| `all_done` | all upstreams *finished*, pass or fail | cleanup, "finally" |
| `one_failed` | at least one upstream failed | failure notifier |
| `none_failed_min_one_success` | branches: skipped is OK, failed is not | after a BranchOperator |

## Do this now
1. Trigger the DAG; open **Graph** and **Gantt** views. Confirm the two checks
   overlapped in time.
2. Open `cleanup`'s details — find `trigger_rule=all_done`.

## Break it on purpose
- Make `check_zip` fail: change its callable to raise an exception
  (`raise ValueError("bad zip")`). Trigger again. Observe: `check_csv` still
  green, `report` shows `upstream_failed` (never ran), but `cleanup` **ran
  anyway** because of `all_done`. This exact pattern — a cleanup/alert step
  that runs regardless — is in every production DAG.
- Fix it back.

## Interview checkpoint
- *How do you run tasks in parallel in Airflow?* — Just don't put a dependency
  between them; the scheduler parallelizes independent tasks up to the
  configured parallelism/pool limits.
- *How do you make a cleanup step run even after failures?* —
  `trigger_rule="all_done"`.
- *What operators have you used?* — sensors for file arrival,
  Python/Bash operators for decompression and validation, the GCS→BigQuery
  transfer operator for loads, BigQuery job operators for SQL transforms.

# Lesson 3 — Scheduling, the logical date, catchup & backfill

**Run:** `l03_schedule` — read this lesson FIRST; this DAG has `catchup=True`
on purpose and will do something surprising the moment you un-pause it.

## The single most-failed Airflow interview question

> "Your DAG is scheduled daily. The run stamped July 14 — when does it
> actually execute?"

Answer: **after July 14's data interval ends** — i.e., at the schedule time on
July 15 (for a daily interval). Airflow's model: a scheduled run *represents a
window of data*, and you can only process a window once it's complete.

Two different dates, always:
- **Logical date** (`ds`, previously "execution date"): *which* interval the
  run is for → July 14. You use it to pick the file:
  `journal_lines_{{ ds_nodash }}.zip`.
- **Run date** (wall clock): *when* it executed → early July 15.

Un-pause the DAG, open a run's log: it prints both. Stare at them until the
gap feels obvious, not confusing. Interviewers use this to separate people
who've run Airflow from people who've read about it.

## schedule=

```python
schedule=None            # manual only
schedule="@daily"        # midnight; presets: @hourly @weekly @monthly
schedule="30 6 * * 1-5"  # cron: 06:30 Mon–Fri (the ledger DAG uses this)
schedule=timedelta(hours=4)
```

`start_date` is not "when I deployed it" — it's the beginning of the first
data interval. Best practice: a fixed timezone-aware date in the past, never
`datetime.now()` (moving target ⇒ scheduler confusion).

## catchup and backfill

When you un-paused `l03_schedule`, Airflow created a run for **every daily
interval between `start_date` and now**, oldest first, one at a time
(`max_active_runs=1`). That's `catchup=True`.

- `catchup=False` (what you want ~90% of the time): only schedule from "now"
  onward; no replay of history.
- **Backfill** = deliberately re-running history. Two ways:
  - CLI: `airflow dags backfill l03_schedule -s 2026-07-01 -e 2026-07-05`
  - UI: **Clear** the existing runs/tasks for those dates — cleared tasks
    re-execute. (This is the everyday method on Composer.)

Backfill is only safe because tasks are **idempotent** — re-running the run
for July 14 must produce the same result, not a duplicate. That's why the
project DAG replaces exactly one partition per run. This word — *idempotent* —
should appear in her interview answers repeatedly.

## Do this now
1. Un-pause `l03_schedule`; watch ~4–5 runs get created and execute in order.
2. Open two different runs' logs; compare their `ds` values.
3. Clear one middle run (Grid view → click the task → Clear) and watch just
   that day re-execute. You have now performed a backfill.

## Break it on purpose
- Set `catchup=False`, delete the DAG's runs (Grid → select → delete), restart
  triggering: note that history is NOT recreated now.

## Interview checkpoint
- *Logical vs run date?* — logical identifies the data interval; the run
  executes after the interval closes.
- *A day's load was wrong; how do you fix just that day?* — clear that DAG
  run (or `dags backfill` the range); safe because the load is idempotent —
  it replaces its own partition, never appends blindly.
- *What does catchup=True do on first deploy?* — creates a run for every
  missed interval since start_date; usually you want False plus explicit
  backfills.

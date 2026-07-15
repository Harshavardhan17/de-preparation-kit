# Lesson 6 — Capstone: the ledger pipeline on your laptop

**Run:** `l06_local_ledger_pipeline`

Everything from lessons 1–5, assembled into the real shape of her daily job —
with local stand-ins so no cloud account is needed:

| Real pipeline (HSBC / project DAG) | This capstone |
|---|---|
| GCS `landing/ processing/ archive/` prefixes | `~/airflow-lab/landing/ processing/ archive/` folders |
| `GCSObjectExistenceSensor` | `PythonSensor` globbing the folder |
| `GCSToBigQueryOperator` → partitioned staging | `csv` module → SQLite `stg_journal_lines` |
| BigQuery `ROW_NUMBER()/QUALIFY` dedup | SQLite `DELETE ... NOT IN (SELECT MIN(rowid)...)` |
| `SAFE_CAST` quarantine | Python `try: float(...) except: quarantine` |
| `BigQueryValueCheckOperator` DQ gate | `dq_gate()` raising on unbalanced journals |
| archive to `archive/` prefix | `shutil.move` |

Same six stages: **sense → unzip → validate manifest → load (idempotent,
dedup, quarantine) → DQ gate → archive**. If she can build, run, break, and
explain this DAG, she can whiteboard the production version cold.

## Run it
```bash
# 1. drop a feed into the landing zone
python project/data_generator.py --date $(date +%F) --out ~/airflow-lab/landing

# 2. trigger the DAG in the UI, watch all six stages go green

# 3. inspect the warehouse
sqlite3 ~/airflow-lab/warehouse.db \
  "SELECT business_date, COUNT(*) rows, ROUND(SUM(debit_amount),2) debits,
          ROUND(SUM(credit_amount),2) credits
   FROM stg_journal_lines GROUP BY business_date;"
```
Debits should equal credits per day — the generator injects duplicate lines
that unbalance the raw file, and the dedup step restores balance. Check the
load task's log for `removed N duplicates`.

## The three experiments that make it stick

**1. Idempotency (the big one).** Re-generate the same date's file into
`landing/`, trigger again, re-run the SQL above. Row counts must be identical
— the load replaced the day, it didn't append. Now she can say *"my loads are
idempotent"* and mean it.

**2. The DQ gate fails closed.** Edit one row of a generated CSV before
zipping is hard — easier: temporarily set the gate's tolerance to `-1` (so
everything is a violation), trigger, and watch the DAG halt at
`dq_debits_equal_credits` with `archive` never running. Bad data must not
reach downstream — the graph itself enforces it. Revert.

**3. A late file.** Trigger the DAG with `landing/` empty: the sensor just
waits (reschedule mode). Drop the file five minutes later; the run completes
untouched by humans. That's the answer to "what if the feed is late?"

## Map it back
Open `project/dags/ledger_pipeline_dag.py` side-by-side. Every task in the
capstone has a 1:1 partner there; only the operators change (GCS/BigQuery
instead of folders/SQLite). Lesson 7 walks that file line by line.

## Interview checkpoint
- Narrate the whole capstone out loud in ≤90 seconds, stage by stage, without
  looking. Then answer, from experience now, not theory:
  *what if the file is late? loaded twice? has a corrupt amount? is truncated?*
  (sensor waits · idempotent replace + dedup · quarantine, not crash ·
  manifest validation halts before load)

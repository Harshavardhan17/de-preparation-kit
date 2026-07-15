# GCS → BigQuery Daily Ledger Pipeline

A production-shaped miniature of a bank's daily ledger ingestion: a source
system drops a zipped journal-entry CSV with a control manifest into a GCS
landing bucket; a Cloud Composer (Airflow) DAG senses it, validates,
decompresses, loads it into partitioned BigQuery staging, transforms it into a
published ledger with running balances and an SCD2 account dimension, gates on
data-quality and reconciliation checks, and archives the source file.

**This is the project for the resume.** Everything here is a real pattern used
in real banking data platforms — nothing is toy-shaped except the data volume.

```
                        ┌─────────────────────── Cloud Composer DAG ───────────────────────┐
 source system          │                                                                   │
 journal_lines_*.zip ─▶ landing/ ─▶ sense ─▶ unzip ─▶ validate ─▶ load ─▶ transform ─▶ DQ gate ─▶ reconcile
                        (GCS)                manifest          staging    ledger+SCD2    debits=credits   │
                                                               (BQ)       (BQ)                            ▼
                                                                                              archive/  + forward
```

## Repository layout

| Path | What it is |
|---|---|
| `data_generator.py` | Synthetic daily feed: balanced double-entry journals, ZIP + manifest, with injected duplicates / nulls / bad amounts / late lines. Stdlib only. |
| `dags/ledger_pipeline_dag.py` | The Composer/Airflow 2 DAG. Every design decision is annotated with `# TALKING POINT`. |
| `sql/01_staging_dedup.sql` | Typed staging: ROW_NUMBER + QUALIFY dedup, SAFE_CAST quarantine, partition-replace idempotency. |
| `sql/02_ledger_build.sql` | MERGE publish (idempotent, absorbs late data) + running balances via window functions. |
| `sql/03_reconciliation.sql` | Control-total recon vs manifest + FULL OUTER JOIN per-account break report. |
| `sql/04_scd2_account_dim.sql` | SCD2 account dimension via two-step MERGE. |
| `sql/05_data_quality_checks.sql` | Fail-closed DQ gate: debits=credits, key/nullability, sign, currency, future-date checks. |

## Running it — free tier, no billing

### Level 1 (do this first): SQL only, BigQuery sandbox
The sandbox is free BigQuery with no credit card ([sign up](https://cloud.google.com/bigquery/docs/sandbox)).
1. Generate a few days of data: `python data_generator.py --date 2026-07-14` (repeat for 3–4 dates).
2. Unzip locally; in the BigQuery console create datasets `ledger_staging` and `ledger_published`, then upload each CSV into `ledger_staging.raw_journal_lines` (schema: all amounts as STRING — mirroring the DAG's load step).
3. Run the `sql/` scripts in order in the console, replacing `@business_date` with a literal date (the console can't bind query parameters; the top-level `RUNBOOK.md` §A6 has the exact per-script rewrites). Watch the dedup counts, the quarantine table, the running balances, and the break report actually work.
4. Sandbox limitation: DML (`MERGE`/`DELETE`/`INSERT`) is unavailable in sandbox mode — where a script uses them, rewrite as `CREATE OR REPLACE TABLE ... AS SELECT` while practicing, and be able to explain both forms. (This itself is a good interview story.)

### Level 2: Airflow locally
Run Airflow in Docker (`astro dev init && astro dev start`, or the official
`docker-compose`). Point the DAG at a local folder instead of GCS (swap
`GCSHook` for `shutil`) or use a real free-tier GCS bucket. The goal is to see
the DAG graph, force a failure, watch retries, and practice `airflow dags
backfill`.

### Level 3 (optional): real GCS bucket
GCS has an always-free 5 GB tier. Upload the ZIPs with `gsutil cp`, keep the
`landing/ processing/ archive/` prefix layout, load with `bq load`. Composer
itself is NOT free — do not spin it up; running the same DAG on local Airflow
is what Composer does anyway, and saying exactly that in an interview is a
strong answer.

## The 90-second interview narration

> "Daily, an upstream system drops a zipped journal-entry file with a control
> manifest into a GCS landing bucket. A Composer DAG senses the file, validates
> row counts against the manifest, decompresses it — BigQuery can't load zip —
> and loads it into a date-partitioned staging table, replacing only that day's
> partition so reruns are idempotent. Transformation SQL then deduplicates on
> the natural key with ROW_NUMBER, quarantines unparseable rows with SAFE_CAST,
> MERGEs into the published ledger fact, rebuilds running balances with window
> functions, and maintains an SCD2 account dimension. Before anything is
> forwarded, a data-quality gate enforces debits-equals-credits per journal and
> a reconciliation step ties published totals back to the manifest, writing a
> break report. Only then is the source file archived for audit and downstream
> consumers notified."

Follow-ups you must be able to answer (all annotated in the code):
- *What if the same file is loaded twice?* Partition-replace + MERGE keys ⇒ no duplicates.
- *What if the file never arrives?* Sensor timeout → SLA miss → alert; next day's run is independent.
- *What if one amount is garbage?* SAFE_CAST quarantines the row; the load doesn't fail; recon explains the count difference.
- *What about yesterday's late entries in today's file?* MERGE inserts into the older partition; balances rebuild.
- *Why NUMERIC not FLOAT64?* Exact decimal money math; floats can't represent 0.10.
- *Scale 100×?* Same design; decompress moves to Dataflow/Cloud Function, load stays batch, staging stays partitioned.

## How this maps to the résumé (honest framing)

**Decision made:** the résumé (`~/personal/anusha-resume/`) does **not** list this
as a separate personal project. Instead, the pipeline work is described inside
the current HSBC / LTIMindtree role, de-identified, using deliberately modest
verbs — *support, build and maintain, develop, support the nightly batch*. This
replica is not a résumé line; it is **the vehicle that lets her defend those
lines with first-hand depth**.

So the framing everywhere — résumé, narration, follow-ups — is one consistent
story:

> "I work on this finance ledger platform: I build and maintain the Composer
> ingestion DAGs, develop the BigQuery transformations and data-quality checks,
> and support the daily batch. To understand the whole flow beyond my slice, I
> rebuilt a working version end-to-end myself."

Rules that keep it airtight (see `interview-qa.md` Item 46 and the
`resume-defense.md` guide):
- Never exceed the résumé's verbs in the interview. "Support/build/maintain/
  develop" is the ceiling; "I architected/owned the platform" is not.
- Present the rebuild as *how she learned the platform she supports*, never as a
  separate job or a claim of production authorship.
- Every technical follow-up this project prepares her for is defensible because
  she has personally implemented every stage in this replica.

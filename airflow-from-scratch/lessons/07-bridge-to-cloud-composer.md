# Lesson 7 — The bridge to Cloud Composer

No new DAG to run — this lesson converts everything you've done locally into
fluent Composer/GCP answers, then walks you through the real project DAG.

## Cloud Composer = your Airflow, managed by Google

Everything you ran this week — scheduler, webserver, metadata DB — Composer
runs for you on Google infrastructure (a GKE cluster per *environment*, a
Cloud SQL metadata database, the UI behind Google login). What changes for
the engineer:

| Locally you… | On Composer you… |
|---|---|
| save DAG files into `dags_folder` | upload them to the environment's **GCS dags bucket** (`gs://<env-bucket>/dags/`) — Composer syncs them to workers automatically |
| `pip install` into your venv | declare **PyPI packages** on the environment (Composer installs them on all workers) |
| read logs in the UI | same UI, plus logs also stream to **Cloud Logging** |
| set Variables/Connections in the UI | same, plus secrets usually come from **Secret Manager** |
| `airflow standalone` restart | Google patches/scales the environment; you pick worker size & autoscaling |

Interview line that lands well: *"Composer is just managed Airflow — DAGs are
synced from a GCS bucket, workers are GKE pods, and the metadata DB is Cloud
SQL. Everything I do locally transfers one-to-one; the operational difference
is that deployment is a file upload and dependencies are environment config."*

## How Airflow talks to GCP: Connections & ADC

Locally, GCP operators need credentials. Airflow looks up a **Connection**
named `google_cloud_default`; on Composer it exists automatically (workers run
as a service account — no keys anywhere). Locally, the easiest correct setup
is Application Default Credentials:

```bash
gcloud auth application-default login   # one-time browser login
```

GCP operators then just work. (Full click-by-click GCS setup lives in the
kit's RUNBOOK, Part C.)

## Guided read: `project/dags/ledger_pipeline_dag.py`

Open it now. You already know every concept in it:

| Lines / task | You learned it in |
|---|---|
| `schedule="30 6 * * 1-5"`, `catchup=False`, `{{ ds_nodash }}` | Lesson 3 |
| `default_args` with `retries`, `retry_delay`, `sla` | Lesson 5 |
| `GCSObjectExistenceSensor(mode="reschedule", timeout=4h)` | Lesson 5 (same sensor idea, GCS-flavored) |
| `unzip_to_processing` PythonOperator + `GCSHook` | Lessons 2 & 6 (a **hook** is the reusable client an operator uses internally — use hooks when writing custom Python tasks) |
| `GCSToBigQueryOperator` with `$YYYYMMDD` partition decorator + `WRITE_TRUNCATE` | Lesson 6's idempotent replace, BigQuery-flavored |
| `BigQueryInsertJobOperator` running the `sql/` files (`@business_date` inlined via Jinja — multi-statement scripts can't take query parameters) | Lesson 2's "operators are a catalog" + Lesson 3's `{{ ds }}` |
| `BigQueryValueCheckOperator(pass_value=0)` | Lesson 6's DQ gate |
| fan-out `>> [build_ledger, upsert_dim]` | Lesson 2 |
| `max_active_runs=1` | Lesson 3 (ordered daily runs — running balances need order) |

Read the file top to bottom and check off each row. If any row isn't obvious,
redo that lesson's *break it on purpose* exercise — don't just reread.

## What her team's "inf/enf-style frameworks" are

Banks wrap raw Airflow in internal frameworks: config-driven DAG generation
(a YAML/table describes source → target and a factory generates the DAG),
standard operators with governance baked in, controlled deployment. Interview
framing: *"our ingestion framework generates the DAGs from config; underneath
it's standard Composer — sensors, loads, transforms — which is why I rebuilt
the raw version myself, to own what the framework abstracts."* That answer
turns the framework from a knowledge gap into a strength.

## Final interview checkpoint (the Composer set)
- *Why Composer over self-managed Airflow?* — no scheduler/DB/worker ops,
  IAM-integrated auth, autoscaling workers, versioned upgrades; you trade
  cost and some low-level control.
- *How do you deploy a DAG to Composer?* — copy the file to the environment's
  GCS dags bucket (usually via CI/CD); Composer syncs it within a minute or so.
- *How do DAGs authenticate to BigQuery/GCS?* — the environment's service
  account via the default GCP connection; no key files.
- *A DAG is stuck in queued on Composer — what do you check?* — worker
  autoscaling limits/pool slots, scheduler health in Monitoring, parsing
  errors in the dags folder, and whether the task's queue/resources fit the
  worker size.

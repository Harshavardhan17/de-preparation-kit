"""
Daily ledger ingestion DAG — GCS landing zone -> BigQuery published ledger.

This is the miniature of the real pipeline: a source system drops
journal_lines_<YYYYMMDD>.zip into a landing bucket every day; this DAG senses
it, validates it against its manifest, decompresses it (BigQuery cannot load
ZIP — only gzip is natively supported), loads it into a date-partitioned
staging table, runs the transformation SQL (dedup -> ledger build -> SCD2 dim),
gates on data-quality and reconciliation checks, and only then publishes and
archives the file.

Interview talking points are marked with  # TALKING POINT  throughout.

Airflow 2.x style. Runs on Cloud Composer as-is; locally, run Airflow in
Docker and swap the GCS pieces for the local filesystem (see project README).
"""

from __future__ import annotations

import io
import json
import zipfile
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryValueCheckOperator,
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

# ---------------------------------------------------------------- configuration
GCP_PROJECT = "{{ var.value.gcp_project }}"
BUCKET = "{{ var.value.ledger_bucket }}"          # single bucket, prefix-per-zone layout
DATASET_STG = "ledger_staging"
DATASET_PUB = "ledger_published"

# {{ ds_nodash }} is the DAG-run's LOGICAL date. TALKING POINT: the run that
# processes Monday's file executes early Tuesday, but ds still says Monday —
# logical date identifies the data interval, not the wall clock.
STAMP = "{{ ds_nodash }}"
LANDING_ZIP = f"landing/journal_lines_{STAMP}.zip"
PROCESSING_CSV = f"processing/journal_lines_{STAMP}.csv"
PROCESSING_MANIFEST = f"processing/manifest_{STAMP}.json"
ARCHIVE_ZIP = f"archive/{STAMP}/journal_lines_{STAMP}.zip"

SQL_DIR = Path(__file__).parent.parent / "sql"

default_args = {
    "owner": "ledger-team",
    "retries": 2,                          # TALKING POINT: transient GCP errors
    "retry_delay": timedelta(minutes=5),   # self-heal; real failures page us.
    "execution_timeout": timedelta(minutes=30),
    "sla": timedelta(hours=2),
}


def unzip_to_processing(bucket: str, zip_path: str, **_):
    """Download the ZIP, extract CSV + manifest, upload to processing/ prefix.

    TALKING POINT: BigQuery loads gzip CSV natively but NOT zip archives, so
    decompression is a pipeline responsibility. For files too big for worker
    memory/disk, hand this off to a Cloud Function or Dataflow instead.
    """
    gcs = GCSHook()
    blob = gcs.download(bucket_name=bucket, object_name=zip_path)
    with zipfile.ZipFile(io.BytesIO(blob)) as zf:
        for member in zf.namelist():
            target = ("processing/" + member)
            gcs.upload(bucket_name=bucket, object_name=target, data=zf.read(member))


def validate_manifest(bucket: str, manifest_path: str, csv_path: str, **_):
    """Pre-load gate: does the CSV agree with the sender's control file?

    TALKING POINT: catching a truncated file BEFORE loading is much cheaper
    than reconciling after. Row count now; control totals are re-checked in
    SQL after load (03_reconciliation.sql) so we verify the data as BigQuery
    actually parsed it.

    Returns the manifest dict -> lands in XCom (small metadata, never
    payloads); the load_manifest task pulls it to build feed_manifest.
    """
    gcs = GCSHook()
    manifest = json.loads(gcs.download(bucket_name=bucket, object_name=manifest_path))
    csv_lines = gcs.download(bucket_name=bucket, object_name=csv_path).count(b"\n")
    actual_rows = csv_lines - 1  # header
    if actual_rows != manifest["row_count"]:
        raise AirflowFailException(
            f"Manifest says {manifest['row_count']} rows, file has {actual_rows} — "
            "file is truncated or corrupt; quarantining."
        )
    return manifest


with DAG(
    dag_id="ledger_daily_pipeline",
    description="Daily journal feed: GCS -> staging -> ledger -> reconcile -> publish",
    schedule="30 6 * * 1-5",     # 06:30 on business days, after the upstream cut-off
    start_date=datetime(2026, 7, 1),
    catchup=False,               # TALKING POINT: catchup=True would replay every
    default_args=default_args,   # missed day since start_date on first deploy.
    max_active_runs=1,           # days must publish in order (running balances)
    tags=["ledger", "finance", "gcs", "bigquery"],
) as dag:

    # 1. Wait for the file. reschedule mode frees the worker slot between pokes
    #    instead of holding it for hours (poke mode). Deferrable operators are
    #    the modern equivalent. TALKING POINT.
    wait_for_file = GCSObjectExistenceSensor(
        task_id="wait_for_feed_file",
        bucket=BUCKET,
        object=LANDING_ZIP,
        mode="reschedule",
        poke_interval=300,
        timeout=60 * 60 * 4,     # give up after 4h -> SLA miss alert fires
    )

    # 2. Decompress into processing/
    unzip = PythonOperator(
        task_id="unzip_to_processing",
        python_callable=unzip_to_processing,
        op_kwargs={"bucket": BUCKET, "zip_path": LANDING_ZIP},
    )

    # 3. Structural validation against the control file
    check_manifest = PythonOperator(
        task_id="validate_manifest",
        python_callable=validate_manifest,
        op_kwargs={
            "bucket": BUCKET,
            "manifest_path": PROCESSING_MANIFEST,
            "csv_path": PROCESSING_CSV,
        },
    )

    # 4. Load raw CSV into the staging table's partition for this FEED date.
    #    TALKING POINT — idempotency: writing to the partition decorator
    #    (raw_journal_lines$20260714) with WRITE_TRUNCATE means a rerun replaces
    #    exactly that day, never duplicates it and never touches other days.
    #    The raw table is INGESTION-TIME partitioned (partition = which feed the
    #    row arrived in), NOT column-partitioned on business_date: the feed can
    #    carry late journals stamped with yesterday's date, and a decorator load
    #    into a column-partitioned table rejects any row whose column value
    #    doesn't match the partition. business_date stays a normal column and
    #    drives partitioning from staging onward (01_staging_dedup.sql).
    #    Amounts land as STRING on purpose: SAFE_CAST happens in SQL so one bad
    #    value quarantines a row instead of failing the whole load.
    load_staging = GCSToBigQueryOperator(
        task_id="load_to_staging",
        bucket=BUCKET,
        source_objects=[PROCESSING_CSV],
        destination_project_dataset_table=(
            f"{GCP_PROJECT}.{DATASET_STG}.raw_journal_lines${STAMP}"
        ),
        schema_fields=[
            {"name": "journal_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "line_no", "type": "INT64"},
            {"name": "business_date", "type": "DATE"},
            {"name": "posting_ts", "type": "TIMESTAMP"},
            {"name": "account_id", "type": "STRING"},
            {"name": "account_name", "type": "STRING"},
            {"name": "cost_center", "type": "STRING"},
            {"name": "currency", "type": "STRING"},
            {"name": "debit_amount", "type": "STRING"},
            {"name": "credit_amount", "type": "STRING"},
            {"name": "description", "type": "STRING"},
            {"name": "source_system", "type": "STRING"},
        ],
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        time_partitioning={"type": "DAY"},   # ingestion-time: see comment above
        max_bad_records=0,       # structural badness fails loudly; value badness
    )                            # is handled row-level by SAFE_CAST downstream.

    # 5-7. Transformation SQL, one file per stage (see project/sql/).
    #    The files use @business_date as a placeholder. They are MULTI-STATEMENT
    #    scripts (CREATE + DELETE + INSERT / MERGE), and BigQuery scripts don't
    #    take query parameters — so the DAG inlines the logical date via Jinja
    #    at render time. Safe here because {{ ds }} comes from the scheduler,
    #    not from user input; for single-statement jobs, prefer real
    #    queryParameters. TALKING POINT: logical date -> SQL predicate is what
    #    makes every stage rerunnable for its own day.
    def sql_task(task_id: str, filename: str) -> BigQueryInsertJobOperator:
        query = (SQL_DIR / filename).read_text().replace(
            "@business_date", "DATE('{{ ds }}')"
        )
        return BigQueryInsertJobOperator(
            task_id=task_id,
            configuration={
                "query": {
                    "query": query,
                    "useLegacySql": False,
                }
            },
        )

    dedup_stage = sql_task("dedup_and_type_staging", "01_staging_dedup.sql")
    build_ledger = sql_task("build_ledger", "02_ledger_build.sql")
    upsert_dim = sql_task("scd2_account_dim", "04_scd2_account_dim.sql")

    # 7b. Record the sender's control values in BigQuery so 03_reconciliation
    #     can compare loaded totals against them in pure SQL. The values ride
    #     over from validate_manifest via XCom (metadata-sized — see lesson 4).
    load_manifest = BigQueryInsertJobOperator(
        task_id="load_manifest",
        configuration={
            "query": {
                "query": """
                    CREATE TABLE IF NOT EXISTS ledger_staging.feed_manifest (
                      feed_name      STRING,
                      business_date  DATE,
                      row_count      INT64,
                      total_debit    NUMERIC,
                      total_credit   NUMERIC,
                      loaded_ts      TIMESTAMP
                    );
                    DELETE FROM ledger_staging.feed_manifest
                    WHERE business_date = DATE('{{ ds }}');
                    INSERT INTO ledger_staging.feed_manifest VALUES (
                      'journal_lines',
                      DATE('{{ ds }}'),
                      {{ ti.xcom_pull(task_ids='validate_manifest')['row_count'] }},
                      NUMERIC '{{ ti.xcom_pull(task_ids='validate_manifest')['total_debit'] }}',
                      NUMERIC '{{ ti.xcom_pull(task_ids='validate_manifest')['total_credit'] }}',
                      CURRENT_TIMESTAMP()
                    );
                """,
                "useLegacySql": False,
            }
        },
    )

    # 8. Data-quality gate. TALKING POINT: the fundamental ledger invariant —
    #    total debits equal total credits. If this returns anything but 0
    #    violations, the DAG fails HERE and nothing is forwarded.
    #    (Same Jinja inlining: the check operator has no parameter support.)
    dq_debits_equal_credits = BigQueryValueCheckOperator(
        task_id="dq_debits_equal_credits",
        sql=(SQL_DIR / "05_data_quality_checks.sql").read_text().replace(
            "@business_date", "DATE('{{ ds }}')"
        ),
        pass_value=0,
        use_legacy_sql=False,
    )

    # 9. Reconciliation vs manifest control totals -> writes a break report.
    reconcile = sql_task("reconcile_vs_manifest", "03_reconciliation.sql")

    # 10. Archive the original ZIP immutably (audit trail), remove from landing.
    def archive_file(bucket: str, src: str, dst: str, **_):
        gcs = GCSHook()
        gcs.copy(source_bucket=bucket, source_object=src,
                 destination_bucket=bucket, destination_object=dst)
        gcs.delete(bucket_name=bucket, object_name=src)

    archive = PythonOperator(
        task_id="archive_source_file",
        python_callable=archive_file,
        op_kwargs={"bucket": BUCKET, "src": LANDING_ZIP, "dst": ARCHIVE_ZIP},
    )

    (
        wait_for_file
        >> unzip
        >> check_manifest
        >> load_staging
        >> dedup_stage
        >> [build_ledger, upsert_dim]
    )
    check_manifest >> load_manifest                      # control values -> BQ
    build_ledger >> dq_debits_equal_credits >> reconcile >> archive
    load_manifest >> reconcile                           # recon needs both sides

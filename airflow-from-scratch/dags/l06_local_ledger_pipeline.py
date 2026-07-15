"""Lesson 6 — CAPSTONE: the ledger pipeline, fully local.

Same shape as the real project DAG (and the real HSBC pipeline), with local
stand-ins so it runs on your laptop with zero cloud:

    GCS bucket        ->  ~/airflow-lab/{landing,processing,archive}
    BigQuery          ->  SQLite (~/airflow-lab/warehouse.db)
    GCS sensor        ->  PythonSensor globbing the landing folder
    GCSToBigQuery     ->  csv.DictReader + INSERT

Stages: sense feed -> unzip -> validate manifest -> load staging (idempotent,
dedup, quarantine) -> DQ gate (debits == credits) -> archive.

To run:
  1. Generate a feed into the landing folder:
       python project/data_generator.py --date $(date +%F) --out ~/airflow-lab/landing
  2. Trigger this DAG. Watch each stage. Then run it AGAIN for the same file
     (clear the DAG run) and check the staging counts — the load is idempotent.
  3. Inspect the warehouse:  sqlite3 ~/airflow-lab/warehouse.db
       SELECT COUNT(*) FROM stg_journal_lines;
       SELECT * FROM quarantine LIMIT 5;
"""
import csv
import json
import shutil
import sqlite3
import zipfile
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

LAB = Path.home() / "airflow-lab"
LANDING, PROCESSING, ARCHIVE = LAB / "landing", LAB / "processing", LAB / "archive"
DB = LAB / "warehouse.db"


def newest_feed():
    zips = sorted(LANDING.glob("journal_lines_*.zip"))
    return zips[-1] if zips else None


def feed_arrived() -> bool:
    print(f"poking {LANDING} for journal_lines_*.zip")
    return newest_feed() is not None


def unzip_feed():
    PROCESSING.mkdir(parents=True, exist_ok=True)
    zip_path = newest_feed()
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(PROCESSING)
    print(f"extracted {zip_path.name} -> {PROCESSING}")


def validate_manifest():
    manifest_path = sorted(PROCESSING.glob("manifest_*.json"))[-1]
    csv_path = sorted(PROCESSING.glob("journal_lines_*.csv"))[-1]
    manifest = json.loads(manifest_path.read_text())
    with open(csv_path) as f:
        actual = sum(1 for _ in f) - 1  # minus header
    print(f"manifest row_count={manifest['row_count']}, file has {actual}")
    if actual != manifest["row_count"]:
        raise AirflowFailException("file truncated or corrupt — halting before load")


def load_staging():
    """Idempotent load with dedup and quarantine — the heart of the pipeline."""
    csv_path = sorted(PROCESSING.glob("journal_lines_*.csv"))[-1]
    business_date = csv_path.stem.split("_")[-1]  # YYYYMMDD from filename
    bd = f"{business_date[:4]}-{business_date[4:6]}-{business_date[6:]}"

    con = sqlite3.connect(DB)
    con.executescript("""
      CREATE TABLE IF NOT EXISTS stg_journal_lines (
        journal_id TEXT, line_no INTEGER, business_date TEXT, posting_ts TEXT,
        account_id TEXT, account_name TEXT, cost_center TEXT, currency TEXT,
        debit_amount REAL, credit_amount REAL, description TEXT, source_system TEXT);
      CREATE TABLE IF NOT EXISTS quarantine (raw_line TEXT, reason TEXT, business_date TEXT);
    """)
    # IDEMPOTENCY: replace this business date's rows only (== BigQuery's
    # partition-decorator WRITE_TRUNCATE). Rerun-safe, other days untouched.
    # Late lines for EARLIER dates in this file are handled by the natural-key
    # dedup below, so a rerun can't double-insert them either.
    con.execute("DELETE FROM stg_journal_lines WHERE business_date = ?", (bd,))
    con.execute("DELETE FROM quarantine WHERE business_date = ?", (bd,))

    good, quarantined = [], 0
    with open(csv_path) as f:
        for row in csv.DictReader(f):
            try:  # SAFE_CAST pattern: clean, parse, quarantine on failure
                debit = float(row["debit_amount"].replace(",", ""))
                credit = float(row["credit_amount"].replace(",", ""))
            except ValueError:
                con.execute("INSERT INTO quarantine VALUES (?,?,?)",
                            (json.dumps(row), "unparseable amount", bd))
                quarantined += 1
                continue
            good.append((row["journal_id"], int(row["line_no"]), row["business_date"],
                         row["posting_ts"], row["account_id"], row["account_name"],
                         row["cost_center"] or None, row["currency"], debit, credit,
                         row["description"], row["source_system"]))
    con.executemany("INSERT INTO stg_journal_lines VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", good)

    # DEDUP on the natural key (journal_id, line_no) — SQLite version of
    # the ROW_NUMBER()/QUALIFY pattern you'll write in BigQuery.
    dups = con.execute("""
      DELETE FROM stg_journal_lines WHERE rowid NOT IN (
        SELECT MIN(rowid) FROM stg_journal_lines GROUP BY journal_id, line_no)
    """).rowcount
    con.commit()
    print(f"loaded {len(good)} rows, removed {dups} duplicates, quarantined {quarantined}")
    con.close()


def dq_gate():
    """Fail closed: nothing moves downstream if any journal is unbalanced."""
    con = sqlite3.connect(DB)
    bad = con.execute("""
      SELECT COUNT(*) FROM (
        SELECT journal_id FROM stg_journal_lines
        GROUP BY journal_id
        HAVING ABS(SUM(debit_amount) - SUM(credit_amount)) > 0.005)
    """).fetchone()[0]
    con.close()
    print(f"unbalanced journals: {bad}")
    if bad:
        raise AirflowFailException(f"{bad} journals violate debits==credits — pipeline halted")


def archive_feed():
    ARCHIVE.mkdir(parents=True, exist_ok=True)
    zip_path = newest_feed()
    shutil.move(zip_path, ARCHIVE / zip_path.name)
    for leftover in PROCESSING.glob("*"):
        leftover.unlink()
    print(f"archived {zip_path.name}; processing/ cleaned")


with DAG(
    dag_id="l06_local_ledger_pipeline",
    description="Capstone: local mini ledger pipeline (sense->unzip->validate->load->DQ->archive)",
    start_date=datetime(2026, 1, 1),
    schedule=None,   # trigger manually; the real one runs '30 6 * * 1-5'
    catchup=False,
    tags=["lesson", "capstone"],
) as dag:

    sense = PythonSensor(task_id="wait_for_feed", python_callable=feed_arrived,
                         mode="reschedule", poke_interval=15, timeout=1800)
    unzip = PythonOperator(task_id="unzip_to_processing", python_callable=unzip_feed)
    validate = PythonOperator(task_id="validate_manifest", python_callable=validate_manifest)
    load = PythonOperator(task_id="load_to_staging", python_callable=load_staging)
    dq = PythonOperator(task_id="dq_debits_equal_credits", python_callable=dq_gate)
    archive = PythonOperator(task_id="archive_source_file", python_callable=archive_feed)

    sense >> unzip >> validate >> load >> dq >> archive

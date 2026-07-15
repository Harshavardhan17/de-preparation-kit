# RUNBOOK — every step, in order, assuming zero setup

This is the master index + click-by-click instructions for the whole kit.
Work top to bottom; tick every `[ ]` as you go. Nothing here assumes prior
GCP, Airflow, or terminal experience beyond "can open a terminal and run
Python".

> **Where do I type commands?** In a terminal: macOS → Terminal app;
> Windows → install **WSL2 + Ubuntu** first (Microsoft Store → "Ubuntu",
> then run every command inside the Ubuntu window); Linux → any terminal.
> Lines starting with `#` are comments — don't type them.

---

## Part 0 — Get oriented (15 min)

- [ ] Read the top-level `README.md` (what the kit is, the honesty strategy).
- [ ] Skim `study-plan.md` — it tells you WHICH day you do WHAT. This runbook
      tells you HOW to do each thing.
- [ ] Check Python works: `python3 --version` → need 3.9–3.12.
      If missing: macOS `brew install python`; Ubuntu/WSL
      `sudo apt update && sudo apt install python3 python3-venv python3-pip`.

**Map of the kit**

| # | Folder/File | What it is | How you use it |
|---|---|---|---|
| 1 | `study-plan.md` | 4-week schedule | read Sunday nights |
| 2 | `RUNBOOK.md` | this file | keep open always |
| 3 | `textbook/ledger-pipeline-end-to-end.md` | the theory chapter | read per plan |
| 4 | `sql-practice/` (3 files) | 32 SQL problems + solutions | Part A below |
| 5 | `sql-practice/leetcode-sql50.md` | LeetCode SQL 50 companion | daily warm-up |
| 6 | `airflow-from-scratch/` | 7 hands-on Airflow lessons | Part B below |
| 7 | `project/` | the portfolio pipeline | Parts A (SQL) + C (cloud) |
| 8 | `interview-qa.md` | 50 answered questions | weeks 3–4 |
| 9 | `recently-asked-questions.md` | field-collected recent questions | weeks 3–4 |

---

## Part A — SQL: the BigQuery sandbox (Day 1, ~30 min setup)

The **sandbox** is real BigQuery, free forever, no credit card. Limits that
matter to us: 10 GB storage, 1 TB queries/month (plenty), and **no DML**
(`INSERT`/`UPDATE`/`DELETE`/`MERGE` are blocked — noted below where it bites).

### A1. Create the sandbox
- [ ] Go to <https://console.cloud.google.com/bigquery> in a browser.
- [ ] Sign in with any Gmail account. Accept terms. If asked to create a
      project, name it e.g. `de-prep-sandbox`. **Skip/decline** anything about
      billing or free trial — you want the no-card sandbox. You'll see a
      "Sandbox" badge top-left of the console.
- [ ] You are now in **BigQuery Studio**: left panel = your projects/datasets;
      middle = SQL editor tabs. This is where ALL SQL practice happens.

### A2. First query (prove it works)
- [ ] Click **+ (new tab)**, paste, and press **Run**:
      ```sql
      SELECT 'hello bigquery' AS greeting, CURRENT_DATE() AS today;
      ```

### A3. Create the datasets for the project
A **dataset** is BigQuery's word for a schema/folder of tables.
- [ ] In the left panel, click the **⋮** next to your project id →
      **Create dataset** → ID: `ledger_staging`, location: `US` → Create.
- [ ] Repeat for `ledger_published`.

### A4. Generate the sample data (on your laptop)
- [ ] In a terminal, from the kit folder:
      ```bash
      cd de-interview-prep
      python3 project/data_generator.py --date 2026-07-14 --out ./landing
      python3 project/data_generator.py --date 2026-07-15 --out ./landing
      python3 project/data_generator.py --date 2026-07-16 --out ./landing
      ```
      Each command prints `wrote landing/journal_lines_....zip`. That zip =
      exactly what lands in her team's GCS bucket every morning.
- [ ] Unzip them: `cd landing && for z in *.zip; do unzip -o "$z"; done && cd ..`
      → you now have `journal_lines_*.csv` and `manifest_*.json` files.

### A5. Load a CSV into BigQuery (the manual way first)
- [ ] BigQuery Studio → click `ledger_staging` dataset → **Create table**.
- [ ] Source: **Upload** → pick `landing/journal_lines_20260714.csv`.
- [ ] Table name: `raw_journal_lines`. Schema: tick **Auto detect**, then
      **Edit as text** and replace with (amounts stay STRING on purpose —
      the pipeline cleans them in SQL):
      ```
      journal_id:STRING,line_no:INT64,business_date:DATE,posting_ts:TIMESTAMP,account_id:STRING,account_name:STRING,cost_center:STRING,currency:STRING,debit_amount:STRING,credit_amount:STRING,description:STRING,source_system:STRING
      ```
- [ ] Advanced options → Header rows to skip: `1` → **Create table**.
- [ ] Load the other two CSVs the same way, but choose
      **Append to table** for `raw_journal_lines`.
- [ ] Verify: `SELECT business_date, COUNT(*) FROM ledger_staging.raw_journal_lines GROUP BY 1;`

### A6. Run the project SQL, script by script
The scripts use `@business_date` as a placeholder (the DAG inlines the run's
logical date into it; multi-statement BigQuery scripts can't take real query
parameters). In the console you do the same by hand: **replace every
`@business_date` with a literal** like `DATE '2026-07-14'` (Cmd/Ctrl-F in the
editor).

Sandbox has no DML, so where a script uses `DELETE`/`INSERT`/`MERGE`, use the
sandbox rewrite below — and understand both forms; explaining the difference
is itself interview material.

- [ ] `sql/01_staging_dedup.sql` — sandbox rewrite: skip the `CREATE TABLE IF
      NOT EXISTS` + `DELETE` + `INSERT` dance and run one statement:
      `CREATE OR REPLACE TABLE ledger_staging.stg_journal_lines AS SELECT …`
      (the big SELECT from the script, including `QUALIFY`) — but since
      `CREATE OR REPLACE` rebuilds the WHOLE table, run it ONCE with the date
      filter widened to cover everything you loaded:
      `WHERE business_date >= DATE '2026-07-13'` (the 13th, because the 14th's
      file carries a few late journals stamped one day earlier). Same for the
      quarantine table.
      **Checkpoint:** `SELECT COUNT(*)` on raw vs stg for one date — stg is
      smaller. The difference = injected duplicates + the quarantined journal.
      Find a duplicate: `SELECT journal_id, line_no, COUNT(*) c FROM ledger_staging.raw_journal_lines GROUP BY 1,2 HAVING c>1 LIMIT 5;`
      Then look at the quarantine table — one whole journal with `#REF!`
      amounts (a corrupted spreadsheet export; quarantined in full because a
      partially loaded journal could never balance).
- [ ] `sql/02_ledger_build.sql` — rewrite the `MERGE` as
      `CREATE OR REPLACE TABLE ledger_published.fact_journal_lines AS SELECT …`
      (the MERGE's `USING` subquery, minus the `load_ts` filter), with the
      literal set to your LAST date (`business_date <= DATE '2026-07-16'`) so
      all days publish. The `account_daily_balance` statement runs as-is.
      **Checkpoint:** pick any account and watch its `running_balance`
      accumulate across your three loaded dates.
- [ ] `sql/03_reconciliation.sql` — the DAG's `load_manifest` task builds
      `ledger_staging.feed_manifest` automatically; in the sandbox you create
      it by hand, typing the numbers from your `manifest_*.json` files:
      ```sql
      CREATE OR REPLACE TABLE ledger_staging.feed_manifest AS
      SELECT DATE '2026-07-14' business_date, 5050 row_count, NUMERIC '123.45' total_debit, NUMERIC '123.45' total_credit
      -- ← replace with YOUR manifest values; UNION ALL a row per date
      ;
      ```
      Then run the two report queries (rewrite INSERT as CREATE OR REPLACE),
      one date at a time.
      **Checkpoint:** `row_count` = `EXPLAIN` — the difference is exactly the
      duplicates + quarantined lines + late journals; the debit/credit totals
      = `MATCHED` for your latest date (control totals are business totals per
      date, so they tie out after cleaning). An EARLIER date may show a small
      amount `BREAK` — that's the NEXT day's late journals restating it: find
      them, that's the whole late-data interview story in one query.
- [ ] `sql/04_scd2_account_dim.sql` — MERGE-heavy; in sandbox, read it line
      by line instead of running, then do the SCD2 problems in
      `sql-practice/`. (It runs fine later on any billed project or in an
      interview whiteboard.)
- [ ] `sql/05_data_quality_checks.sql` — runs as-is (pure SELECT). Use your
      LATEST date as the literal (check 5 flags anything dated after it).
      **Checkpoint:** result should be `0`. Make it non-zero: change the
      tolerance `0.005` to `-1` and see every journal "fail".

### A7. Daily SQL practice (rest of the plan)
- [ ] Work through `sql-practice/01…03` in the sandbox — type every query.
- [ ] Warm up daily with `sql-practice/leetcode-sql50.md` (mapping + order).

---

## Part B — Airflow from scratch (evenings, Weeks 1–2)

Everything is in `airflow-from-scratch/`:
- [ ] Do the **One-time setup** in `airflow-from-scratch/README.md`
      (venv → pip install with constraints → `airflow standalone` →
      point `dags_folder` at the track's `dags/` directory).
- [ ] Lessons 1–7 in order: read `lessons/0N-*.md`, run its DAG, do the
      *break it on purpose* exercises, say the *interview checkpoint* aloud.
- [ ] Capstone (lesson 6) gives you the full pipeline running locally with
      zero cloud — this is the minimum bar before interviews.

---

## Part C — Connecting to real GCS (optional, Week 2–3 weekend)

The capstone already proves the pipeline. This part makes the *cloud* part
real: your laptop's Airflow talking to an actual bucket. It needs a billing-
enabled account — GCS has an **always-free 5 GB**, so cost is ₹0 if you stay
tiny, but a card is required. **Skip guilt-free** if you'd rather not add a
card; lesson 7 + the capstone cover the interview.

### C1. Tools & auth
- [ ] Install the Google Cloud CLI: <https://cloud.google.com/sdk/docs/install>
      (macOS: `brew install google-cloud-sdk`).
- [ ] ```bash
      gcloud auth login                        # browser opens, pick your account
      # project ids are GLOBAL — add your own suffix or the create fails
      gcloud projects create de-prep-cloud-<yourname> --set-as-default   # or reuse an existing project
      gcloud auth application-default login    # ADC: what Airflow's GCP operators use
      ```
      That last command is the whole "how does local Airflow authenticate to
      GCP" answer: **Application Default Credentials**. Airflow's
      `google_cloud_default` connection picks them up automatically — no key
      files. (On Composer, the environment's service account plays this role.)

### C2. Bucket with the pipeline's layout
- [ ] ```bash
      gsutil mb -l us-central1 gs://de-prep-ledger-$USER     # bucket names are global — suffix makes it unique
      python3 project/data_generator.py --date 2026-07-17 --out /tmp
      gsutil cp /tmp/journal_lines_20260717.zip gs://de-prep-ledger-$USER/landing/
      gsutil ls -r gs://de-prep-ledger-$USER/                # see landing/ prefix
      ```
- [ ] Install the GCP provider into your Airflow venv:
      `pip install apache-airflow-providers-google` (same constraints flag as
      the original install if it complains).

### C3. Point the project DAG at your bucket
- [ ] Copy `project/dags/ledger_pipeline_dag.py` into your
      `airflow-from-scratch/dags/` folder.
- [ ] In the Airflow UI: **Admin → Variables** → add `gcp_project` = your
      project id, `ledger_bucket` = `de-prep-ledger-<you>` (the DAG reads
      both via `{{ var.value.… }}`).
- [ ] The BigQuery steps need DML (not sandbox): either enable billing on the
      project (staying inside free tier) or stop the run after
      `load_to_staging` — sensing, unzipping and loading from a REAL bucket
      is the experience that matters.
- [ ] Trigger it. When `wait_for_feed_file` goes green against a real GCS
      object, take the win — that is, end to end, what Composer does at work.

### C4. Clean up (avoid any charges)
- [ ] `gsutil rm -r gs://de-prep-ledger-$USER` and delete the project in the
      console if you enabled billing.

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| `airflow: command not found` | activate the venv: `source ~/airflow-lab/venv/bin/activate` |
| DAGs don't appear in UI | wrong `dags_folder` in `$AIRFLOW_HOME/airflow.cfg`; use the FULL path; wait 30s; check `airflow dags list-import-errors` |
| DAG has a red "broken DAG" banner | Python error in the file — the banner shows the traceback |
| Task stuck in `queued` | you stopped `airflow standalone`; restart it |
| `Could not automatically determine credentials` | run `gcloud auth application-default login` (Part C1) |
| BigQuery: `DML ... not supported in sandbox` | expected — use the CREATE OR REPLACE rewrites in A6 |
| Sandbox: table expired after 60 days | the sandbox enforces a 60-day table/partition expiry — just re-run the generator + loads (minutes); the cap only goes away if you upgrade to a billed project |
| Windows: anything at all | make sure you are inside the Ubuntu/WSL terminal, not PowerShell |

## When you're stuck
Rubber-duck it against the textbook chapter first (§ of the failing stage),
then the relevant lesson's *break it on purpose* section — most "bugs" here
are one of the deliberately injected ones. That debugging IS the interview
prep.

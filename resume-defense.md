# Résumé Defense Guide — Anusha (GCP Data Engineer, 4 YoE)

**Purpose:** make every line of the résumé interview-proof. This guide does *not* repeat the deep material already in the kit — it cross-references it. Where the kit is thin (mostly the TCS stack), Part 3 adds genuinely new, technically accurate content.

Kit files referenced throughout:
- `interview-qa.md` — 50 Q&A (Airflow/Composer Q1-14, BigQuery Q15-26, GCS→BQ Q27-33, finance Q34-39, Spark Q40-44, behavioral 45-50).
- `sql-practice/` — 01 window functions, 02 joins/dedup, 03 BigQuery-specific (QUALIFY, MERGE, SAFE_CAST, partitioning).
- `textbook/ledger-pipeline-end-to-end.md` — the full narrative.
- `project/README.md` — the rebuilt replica.

---

## Part 1 — The consistency rule (read this before anything else)

**The interview must never exceed the résumé's verbs.** This is the single rule that keeps a modest, true story from collapsing under cross-examination.

**The verbs on the current role are the ceiling.** The résumé says *support*, *build and maintain*, *develop*, *support the nightly batch*. In the interview those are the strongest words allowed for HSBC work. Banned: "I architected the platform," "I owned the pipeline," "I designed the framework." She has been on this project ~6 months (Dec 2025–present) — six months of genuine, hands-on support work is credible; six months of "I architected a bank's ledger platform" is not, and one follow-up exposes it.

**Where the depth actually comes from — the rebuild.** The credibility that lets her answer hard follow-ups is that she personally rebuilt a working version of the whole pipeline end-to-end (the kit's `project/`) to learn the platform she supports. This is framed *only* as "how I learned the flow beyond my slice" — never as a separate job, a personal side-project line, or a claim of production authorship. The rebuild is why she can explain *why* each design choice was made even for parts that predate her; it is not something she puts a verb like "built the HSBC platform" against. See `interview-qa.md` Item 46 for the exact phrasing.

**"We" vs "I".** Interviewers hear "we built" as "someone else built." Use **"I"** for her own work (the DAGs she maintains, the SQL she writes, the incidents she handles), and name plainly what wasn't hers ("the framework predates me; I've traced it and can explain it, but I didn't build it"). Understanding-without-authorship is a normal senior trait — most engineers inherit systems.

**Bounded, precise claims only.** "I maintain the ingestion DAGs and the validation checks, and I support the daily batch" beats "I run the whole platform." A small scope defended in depth wins; a large scope defended vaguely loses. When she genuinely doesn't know a part: say so, give the accurate summary she does have, and pivot to something adjacent she knows deeply.

---

## Part 2 — Line-by-line defense

### SUMMARY LINE
> "GCP Data Engineer, 4+ yrs … banking finance ledger platform delivering daily general-ledger datasets on GCP … BigQuery, Cloud Composer/Airflow, Python, advanced SQL … full ingestion flow from source feeds in GCS to validated, reconciled data in BigQuery."

**Invites:** "Walk me through what you do day to day" / "What does 'the full ingestion flow' mean for you?"

**Her answer:** "I work on a finance ledger platform at HSBC — we take daily source feeds that land in GCS and turn them into curated general-ledger datasets that downstream finance teams use for reporting and close, so correctness and deadlines are contractual. My hands-on work spans the ingestion flow: I build and maintain the Composer DAGs that sense and load the files, I develop the BigQuery SQL that transforms and validates the data, and I support the nightly batch. Before this I spent three years at TCS building GCP ingestion pipelines across many source systems, which is where most of my Dataflow, Pub/Sub, and Dataproc experience comes from."

**They'll drill:** "4+ years but 6 months here — what's the other 3.5?" → The TCS role (May 2022–Nov 2025); lead with it so the tenure math is transparent. / "'Reconciled' — reconciled against what?" → Source control totals from the feed's manifest; see the reconciliation drill in Part 4.

**Prep in:** `interview-qa.md` Item 45 (the 90-second narration).

---

### SKILLS — Cloud (GCP): BigQuery, GCS, Cloud Composer/Airflow, Pub/Sub, Dataflow, Dataproc, Cloud Functions, Cloud Monitoring
**Invites:** the interviewer picks any one and asks "tell me about your experience with X."
**Rule:** anchor BigQuery/Composer/GCS/Cloud Monitoring to the *current* HSBC work; anchor Pub/Sub/Dataflow/Dataproc/Cloud Functions to the *TCS* work — do not claim the ledger platform uses tools it doesn't. If asked "do you use Dataflow at HSBC?" the honest answer is "not on the ledger batch — batch loads cover it there; my Dataflow and Pub/Sub work was at TCS."
**Prep in:** BigQuery/Composer/GCS → `interview-qa.md` Q1-33; Cloud Monitoring → Q13. Pub/Sub, Dataflow, Cloud Functions, Dataproc → **Part 3 of this guide** (the kit under-serves these).

### SKILLS — Languages: Python (PySpark), advanced SQL (window functions, MERGE, QUALIFY, SAFE_CAST)
**Invites:** "Show me a window function you've written" / "What does QUALIFY do that a subquery doesn't?"
**Her answer:** "Advanced SQL is my strongest area — window functions for running balances and dedup, MERGE for idempotent upserts, QUALIFY to filter on a window result in one pass, SAFE_CAST to quarantine dirty values instead of failing a load. Python is mostly Airflow DAGs and callables, plus PySpark for the Dataproc batch jobs I wrote at TCS."
**They'll drill:** "QUALIFY?" → "It filters on the result of a window function without a wrapping subquery — `QUALIFY ROW_NUMBER() OVER (PARTITION BY key ORDER BY load_ts DESC) = 1` keeps the latest row per key. It's BigQuery/Snowflake syntax, not standard everywhere." / "SAFE_CAST vs CAST?" → "CAST throws and fails the query on a bad value; SAFE_CAST returns NULL, so I can load everything and route the NULLs to a quarantine table."
**Prep in:** `sql-practice/01` (windows), `sql-practice/03` Problems 1 (QUALIFY), 4 (MERGE), 6 (SAFE_CAST).

### SKILLS — Data Engineering: batch & event-driven ingestion, partitioned loads, DQ & reconciliation, SCD Type-2, CI/CD (Jenkins, GitHub)
**Invites:** "You list both batch and event-driven — when do you pick which?" / "Explain SCD Type-2." / "How does your CI/CD work?"
**Prep in:** batch vs event-driven → `interview-qa.md` Q29 **and Part 3.1 here**; partitioned loads → Q17, Q19, Q30; DQ/reconciliation → Q32-33; SCD2 → textbook §4 and `sql-practice/02` Problem 10; **CI/CD → Part 3.5 here** (kit does not cover Jenkins).

### SKILLS — Databases: BigQuery, MySQL, SQL Server
**Invites:** "You've used all three — what trips you up moving between them?"
**Prep in:** **Part 3.6 here** — the dialect cheatsheet (kit does not cover this).

---

### HSBC BULLET 1 — "Support a finance ledger data platform on GCP; daily source feeds ingested into BigQuery and published as curated general-ledger datasets for downstream finance teams."
**Invites:** "What's a general ledger?" / "Who consumes your output and what happens if it's wrong?"
**Her answer:** "A general ledger is the bank's central book of record — every account, accumulating every journal entry, where each entry's debits must equal its credits. Sub-ledgers like loans or payments post summarized journals into it. Daily, those feeds land in GCS, we ingest them into BigQuery, validate and reconcile them, and publish curated GL datasets that downstream finance teams use for reporting and month-end close. If it's wrong, it flows into regulatory filings — so a slow correct number beats a fast wrong one."
**They'll drill:** "Debits always increase?" → "No — depends on account type: debits increase assets and expenses, credits increase liabilities, equity, revenue." / "What is close?" → Part 4 drill + `interview-qa.md` Q37.
**Prep in:** `interview-qa.md` Q34-38; textbook §1.

### HSBC BULLET 2 — "Build and maintain Cloud Composer (Airflow) DAGs — file sensing on GCS landing buckets, decompression and validation, idempotent partition-level loads into date-partitioned BigQuery tables."
**Invites:** "Why sense the file instead of scheduling the load?" / "What makes the load idempotent?" / "Why can't BigQuery just load the ZIP?"
**Her answer:** "The DAG runs daily keyed on the logical date. A `GCSObjectExistenceSensor` — in reschedule/deferrable mode so it doesn't hog a worker — waits for the day's file; sensing the control file guarantees the upload finished. Then I decompress, because BigQuery loads gzip CSV natively but not ZIP, validate the manifest, and load with `WRITE_TRUNCATE` on that date's partition, so a rerun replaces exactly that day rather than appending — that's the idempotency. Everything derives from `{{ ds }}`, never `datetime.now()`, so backfills are safe."
**They'll drill:** "poke vs reschedule vs deferrable?" → Q6. "Sensor times out — then what?" → alert; when the file lands late, clear/rerun that logical date into its partition (Q10). "ZIP vs gzip?" → Q28 (ZIP is an archive format BigQuery won't unpack; gzip it decompresses on the fly).
**Prep in:** `interview-qa.md` Q2 (logical date), Q4 (idempotency), Q6 (sensors), Q28 (ZIP).

### HSBC BULLET 3 — "Develop BigQuery SQL transformations (deduplication, SAFE_CAST, MERGE upserts, window-function balances) and data-quality/reconciliation checks that validate loaded data against source control totals before publication."
**Invites:** "How do you dedup?" / "Walk me through your MERGE." / "What exactly do you reconcile?"
**Her answer:** "Loads go into a stringy staging table so ingestion never fails on a bad value — I SAFE_CAST there and route un-parseable rows to quarantine. I dedup on the natural key — `(source_system, journal_id, line_number)` — with `ROW_NUMBER() OVER (PARTITION BY key ORDER BY load_ts DESC)` in a QUALIFY, keeping the latest arrival. Then a MERGE on that key publishes into the final table so publish-twice equals publish-once. Balances are `SUM(signed_amount) OVER (PARTITION BY account ORDER BY business_date)`. Before anything is forwarded, reconciliation compares loaded row counts and summed debits/credits against the manifest control totals, and asserts debits equal credits — money compared as NUMERIC, never FLOAT."
**They'll drill:** "MERGE matches two source rows to one target — what happens?" → "It errors; that's why staging is deduped first." (Q20) / "Why NUMERIC?" → exact decimal; FLOAT64 can't represent 0.10.
**Prep in:** `sql-practice/01` (windows), `sql-practice/03` Problems 1/4/6; `interview-qa.md` Q20, Q32-33.

### HSBC BULLET 4 — "Support the nightly batch — monitoring, failure triage, safe reruns/backfills via ServiceNow."
**Invites:** "Walk me through a failure you triaged." / "How do you safely rerun a bad day?"
**Her answer:** "When an alert fires I go to the task logs, classify the failure — transient, data, or code. Transient I just clear the task to retry. A data issue, like a load count not matching the manifest, I contain first: hold the downstream forward so bad data can't propagate, raise the ServiceNow ticket, then repair — usually a resend from upstream — and replay that logical date. Because writes are partition-scoped and MERGE-based, replaying is a routine clear-and-rerun, and the reconciliation gate re-runs automatically before forwarding resumes."
**They'll drill:** "Contain-diagnose-fix-verify — in that order?" → yes; containment first because downstream may already consume. / "What if the bad day is 3 weeks old?" → restatement, not just replay (Q33 follow-up).
**Prep in:** `interview-qa.md` Q13 (monitoring), Q33 (safe reprocess), Item 47 (failure story).

---

### TCS BULLET 1 — "Designed/deployed automated pipelines ingesting/transforming data from 10+ source systems into BigQuery."
**Note on verbs:** TCS was 3.5 years of building — "designed/deployed" is defensible here (unlike the current role). Own it.
**Invites:** "10+ sources — how did they differ, and how did you keep it manageable?"
**Her answer:** "The sources ranged from flat-file drops in GCS to database extracts from MySQL and SQL Server. Rather than hand-write each pipeline, I standardized the shape — land, stage as raw types, validate, load to a partitioned BigQuery table — and drove the per-source differences (schema, delimiter, keys) from config, so onboarding a new source was mostly configuration plus its transform SQL."
**They'll drill:** "How did you pull from MySQL/SQL Server?" → scheduled extracts to GCS then load, or (for the straight ones) BigQuery Data Transfer Service — see Part 3.2. / "Raw/staged/processed zones?" → Bullet 4 + `interview-qa.md` Q27.

### TCS BULLET 2 — "Migrated 5+ legacy Dataflow pipelines to GCP Data Transfers, reducing execution time ~25% and lowering cost."
**Invites:** "Why migrate away from Dataflow?" / "What are GCP Data Transfers?" / "Where's the 25% from?"
**Her answer:** "The legacy Dataflow jobs were doing straightforward scheduled loads — pull files on a cadence and land them in BigQuery — with almost no real transformation. That's exactly what the BigQuery Data Transfer Service does as a managed feature, so we replaced the hand-built Beam jobs with scheduled transfers: far less code to maintain, no worker VMs spinning up per run, and the load path is BigQuery-native. The ~25% came mostly from removing Dataflow's per-job worker startup and shuffle overhead for what was really just a bulk load; cost dropped because we stopped paying for Dataflow compute."
**They'll drill:** "What did you give up?" → flexibility — DTS is load-and-schedule, so anything needing genuine transformation stayed on Dataflow or moved into BigQuery SQL. / "Which transfers?" → Part 3.2.
**Prep in:** **Part 3.2 here** (DTS) — kit does not cover it.

### TCS BULLET 3 — "Built event-driven ingestion with Cloud Functions and Pub/Sub (saved 15+ hrs/week); batch PySpark transformations on Dataproc."
**Invites:** "Draw the event-driven flow." / "Pub/Sub is at-least-once — how did you avoid duplicates?" / "Why Dataproc and not BigQuery?"
**Her answer:** "For files that arrived unpredictably, instead of polling I wired GCS object-finalize notifications to a Pub/Sub topic; a Cloud Function subscribed and loaded the file into BigQuery the moment it landed — that's where the 15 hours went, no more manual kicks or wide scheduled windows. Because Pub/Sub is at-least-once, the same event can fire twice, so the load is idempotent — keyed on the object name and generation. For the heavier transformations that were procedural rather than SQL-shaped, I ran PySpark on ephemeral Dataproc clusters reading from and writing to GCS."
**They'll drill:** "Missing file — how does event-driven catch it?" → it can't; you add a scheduled completeness check — absence needs a clock, not an event (Q29). / "Dead-letter?" → Part 3.1. / "Why ephemeral clusters?" → Part 3.4.
**Prep in:** **Part 3.1 (event-driven) and 3.4 (Dataproc) here**; cross-ref `interview-qa.md` Q29, Q40-44.

### TCS BULLET 4 — "Developed SQL for validation/cleansing/transformation across BigQuery, MySQL, SQL Server; raw/staged/processed zones in Cloud Storage."
**Invites:** "SQL differences between the three?" / "What lives in each zone?"
**Her answer:** "Three zones in GCS: raw is the file exactly as received and never touched, staged is cleansed and typed, processed is the modeled output. The SQL differed by engine — BigQuery for the warehouse work, MySQL and SQL Server for source-side extracts and checks — and moving between them you hit dialect differences: TOP vs LIMIT, ISNULL vs IFNULL vs COALESCE, and QUALIFY only existing in BigQuery."
**Prep in:** **Part 3.6 here** (dialect cheatsheet); `interview-qa.md` Q27 (zones).

### TCS BULLET 5 — "Automated deployment/testing with Jenkins CI/CD (+30% release efficiency); monitored with Cloud Logging/Stackdriver; incidents via ServiceNow."
**Invites:** "How does a DAG get from your laptop to production?" / "What do you actually test in CI?"
**Her answer:** "Code lived in GitHub; a Jenkins pipeline on every PR ran linting, unit tests on the Python callables, and a DAG-parse test that imports every DAG to catch import errors and cycles before they ever reach the scheduler. On merge to main, a deploy stage synced the DAG files to the Composer environment's GCS dags bucket, which is how Composer picks them up. The 30% was from killing manual copy-to-bucket deploys and the broken-DAG incidents that used to follow them."
**They'll drill:** "DAG-parse test — why does it matter?" → a DAG that fails to import breaks silently in the scheduler; catching it in CI is cheap. / "How do you test SQL?" → sqlfluff lint plus dry-run against BigQuery to catch bad references without scanning bytes.
**Prep in:** **Part 3.5 here** (kit does not cover Jenkins).

### EDUCATION / CERTS — B.Tech Civil Engineering; HackerRank SQL, Udemy Python
**Invites:** "Civil to data — why?" / (rarely) the certs.
**Her answer:** "Civil is where I learned to reason about systems and precision, but I moved into data early because the analytical and problem-solving side was what I enjoyed, and I've spent all four professional years in GCP data engineering. The HackerRank SQL track through advanced and the Python course are just how I formalized the fundamentals." Keep it short and confident — a non-CS degree with 4 years of relevant work is a non-issue; over-explaining it makes it one.

---

## Part 3 — Gaps the main kit under-serves (study these)

The kit is HSBC/Composer/BigQuery-heavy. These six topics carry her TCS years (3+ of 4) and appear on her skills line — the kit barely touches them, so treat this as new study material.

### 3.1 Event-driven ingestion (Pub/Sub + Cloud Functions) — a headline skill on BOTH roles

**The flow, precisely:** A file lands in GCS → the bucket emits an **`OBJECT_FINALIZE`** notification → delivered to a **Pub/Sub topic** → a **subscription** feeds a consumer (a **Cloud Function**, or Cloud Run for heavier work) → the consumer loads the object into BigQuery. (Modern GCP can also route object events through **Eventarc** directly to Cloud Run, but the Pub/Sub-in-the-middle design is the one to describe, because the delivery semantics are the interesting part.)

**Why this beats schedule+sensor sometimes:** zero waiting, reacts the instant a file lands, scales to any arrival pattern including many-per-day and unpredictable sources. **Why it's sometimes worse:** more moving parts (notification config, topic, subscription, function, retries), and — the killer point — **"the file never arrived" produces no event**, so the failure mode is *silence*. A scheduled sensor, by contrast, encodes an expectation: a timeout tells you the file is *late*. So contractual daily finance feeds → schedule+sensor; ad-hoc/high-frequency → event-driven, always paired with a separate scheduled completeness check.

**At-least-once delivery and idempotency (the question they always ask):** Pub/Sub guarantees **at-least-once** delivery by default — the same message can be delivered more than once (redelivery after a missed ack, duplicate publishes). Therefore the consumer *must* be idempotent. The clean technique: use the GCS object's **name + generation number** (or the Pub/Sub `message_id`) as a dedup key against a small "processed" table, and make the BigQuery write itself idempotent (partition `WRITE_TRUNCATE` or MERGE on business key). Note: Pub/Sub now offers an opt-in **exactly-once delivery** mode per subscription (within a region), but the defensible engineering answer is still "I don't rely on the broker for exactly-once — at-least-once delivery plus idempotent processing gives me effectively-once."

**Ordering:** Pub/Sub does *not* guarantee order by default. If order matters you set a **message ordering key** (messages with the same key are delivered in publish order within a region) — but ordering reduces throughput and most file-load pipelines don't need it, because each file is independent and idempotent.

**Retries and dead-letter topics:** if the consumer keeps nacking/failing, Pub/Sub redelivers with backoff. To stop a poison message from retrying forever, attach a **dead-letter topic (DLQ)** with a `max_delivery_attempts` (e.g., 5); after that many failures the message is routed to the DLQ for inspection instead of blocking the subscription. Always pair a DLQ with an alert on its depth — a growing DLQ is a real incident.

**Likely Qs:**
- *"Push vs pull subscription?"* — Push delivers by POSTing to an HTTPS endpoint (Cloud Function/Cloud Run) — simplest for serverless. Pull has the consumer fetch and ack — more control and batching, better for high throughput.
- *"How is this different from just a GCS-triggered Cloud Function?"* — Under the hood a GCS trigger *is* an event notification; putting Pub/Sub in the middle buys you fan-out to multiple consumers, DLQs, replay, and a buffer if the consumer is down.

### 3.2 BigQuery Data Transfer Service (DTS) / "GCP Data Transfers"

**What it is:** a **managed, scheduled** data-movement service that loads data into BigQuery on a recurring schedule with **no code and no infrastructure**. Sources include Google SaaS (Google Ads, Campaign Manager, YouTube, Google Play), **Cloud Storage** and **Amazon S3 / Azure Blob** (scheduled file loads into BigQuery), warehouse migrations (Teradata, Redshift), and **scheduled queries**. You configure source, destination table, schedule (as frequent as ~15 min for some sources; daily is typical), and write disposition — and it runs itself.

**The migration narrative (her TCS bullet):** "The legacy pipelines were hand-built Dataflow jobs whose only job was 'on a schedule, pick up files and load them into BigQuery' — no meaningful transformation. That's precisely DTS's job as a managed feature. I moved them to scheduled Cloud Storage transfers: the Beam code and the per-run Dataflow workers disappeared, so there was less to maintain, no worker startup latency, and lower cost because we weren't paying Dataflow compute for a glorified `bq load`."

**Trade-offs (name them — it shows judgment):** DTS is **load-and-schedule, not transform**. It's rigid: you get scheduling, backfills, and retries for free, but you cannot express real business logic in it. So the rule she should state: "DTS for straight scheduled ingestion; the moment a pipeline needs genuine transformation, it belongs in BigQuery SQL, Dataflow, or Dataproc — I wouldn't force logic into DTS." Also: transfers are managed by Google, so you trade fine-grained control for zero ops.

**Likely Qs:** *"Why not just a scheduled query or a Composer DAG?"* — Scheduled queries are for SQL you already have in BigQuery; a Composer DAG is right when there are dependencies, sensing, or multi-step orchestration. DTS wins when the *entire* job is "move this source into BigQuery on a schedule" and you want it managed. / *"Backfill?"* — DTS supports scheduled backfill runs over a date range, similar in spirit to Airflow backfill.

### 3.3 Dataflow / Apache Beam basics (on her skills line + a TCS bullet)

**The Beam model:** Beam is a unified programming model for **both batch and streaming**; **Dataflow** is Google's serverless managed runner for Beam (autoscaling, dynamic work rebalancing, no cluster to manage). Core abstractions:
- **PCollection** — a distributed, immutable dataset. **Bounded** = batch (a finite file); **unbounded** = streaming (an endless Pub/Sub stream).
- **PTransform** — an operation on PCollections. The workhorse is **ParDo** (parallel-do): applies a **DoFn** to every element — like a distributed map/flatMap where you write per-element logic. Others: `GroupByKey`, `Combine`, `Flatten`, plus IO connectors (`ReadFromText`, `WriteToBigQuery`, `ReadFromPubSub`).
- **Windowing** — for streaming, you divide an unbounded PCollection into finite windows to aggregate over: **fixed** (tumbling, e.g. every 1 min), **sliding** (overlapping), **session** (gap-based). **Watermarks** track event-time progress and **triggers** decide when to emit a window's result, handling late data.

**The decision framework (Dataflow vs Dataproc vs BigQuery) — the question that ties it together:**
- **BigQuery** — the default when the work is expressible as SQL over structured data. Serverless, cheapest to build/operate for a small team.
- **Dataflow** — serverless Beam; the right tool for **streaming** and for batch transforms that are procedural but you don't want to manage clusters. No Spark ecosystem baggage.
- **Dataproc** — managed Spark/Hadoop; the right tool to **lift-and-shift existing Spark code** or use the Hadoop ecosystem, with more knobs and (with ephemeral clusters) potentially lower cost for steady heavy batch.

The one-liner: *"SQL work → BigQuery; streaming or serverless procedural transforms → Dataflow; existing Spark or Hadoop ecosystem → Dataproc."* (Cross-ref `interview-qa.md` Q40 for the BigQuery-vs-Spark cost model.)

**Likely Qs:** *"Same code for batch and streaming?"* — Yes, that's Beam's headline: bounded vs unbounded PCollection, largely the same transforms. *"Why did you use Dataflow at TCS but not for the ledger batch?"* — "The ledger work is scheduled batch loads that BigQuery handles natively; at TCS I had streaming and procedural transforms where Dataflow earned its place."

### 3.4 Dataproc / PySpark (brief — TCS-specific angle)

The Spark *concepts* — partitioning/shuffle, broadcast joins, file formats, small-files — are in `interview-qa.md` **Q40-44**; study those. What's **TCS-specific** to add:

- **Ephemeral (job-scoped) clusters.** The discipline is spin-up → run the job → tear down, rather than a long-lived cluster billing 24/7. Her line: "I ran PySpark on ephemeral Dataproc clusters — created per job, deleted on completion — so we paid for VM uptime only while the job ran." This is the cost-control answer interviewers want.
- **GCS as the storage layer (not HDFS).** On GCP, Spark reads from and writes to **GCS** via the Cloud Storage connector, so storage is decoupled from the cluster — which is *why* ephemeral clusters work: the data outlives the cluster because it lives in GCS, not on cluster-local HDFS.
- **Dataproc Serverless** is worth a mention — runs Spark batch with no cluster to size at all, the managed evolution of the ephemeral-cluster pattern.

**Likely Q:** *"Why not keep a standing cluster?"* — "Idle clusters burn money; ephemeral clusters plus GCS storage give the same result for a fraction of the cost, and Dataproc Serverless removes cluster sizing entirely."

### 3.5 Jenkins CI/CD for data pipelines (kit does not cover this)

**The GitHub flow:** feature branch → PR → Jenkins CI runs automatically → review → merge to main → Jenkins deploy stage ships to production.

**What CI actually tests for DAGs/SQL:**
1. **Lint** — `flake8`/`black` on Python, `sqlfluff` on SQL.
2. **Unit tests** — `pytest` on the Python callables (the business logic inside `PythonOperator`s / `@task` functions), mocking GCP clients.
3. **DAG-parse / validation test** — import every DAG file and assert it loads with **no import errors and no cycles**, tasks have owners/retries set, etc. This is the highest-value cheap test: a DAG that fails to import breaks *silently* in the scheduler, so catching it in CI prevents a class of production incidents.
4. **SQL dry-run** — run transform SQL as a BigQuery **dry run** (`--dry_run`), which validates syntax and table references and returns bytes-to-be-scanned **without scanning anything or costing money** — catches a bad column reference before deploy.

**Deploy to Composer:** Composer syncs DAGs from a **GCS "dags bucket."** So deployment is literally a **`gsutil rsync`** (or `gcloud composer environments storage dags import`) of the repo's `dags/` folder into that bucket; Composer picks up the change automatically. That's the whole "deploy" — which is why replacing manual copy-to-bucket with an automated Jenkins stage removes both toil and the broken-DAG incidents that manual deploys caused (her "+30% release efficiency").

**Likely Q:** *"How do you roll back?"* — Redeploy the previous commit's DAG files (they're version-controlled), and because loads are idempotent per business date, re-running under the old code is safe.

### 3.6 MySQL / SQL Server ↔ BigQuery dialect cheatsheet (all three on her résumé)

> **Hands-on practice for this lives in `sql-practice/04-mysql-sqlserver-dialects.md`** —
> four finance-framed problems written out in all three dialects. Do those, not
> just this table.

The differences that actually trip people moving between the three:

| Concern | MySQL | SQL Server (T-SQL) | BigQuery (GoogleSQL) |
|---|---|---|---|
| Row limiting | `LIMIT n` | `TOP n` / `OFFSET…FETCH` | `LIMIT n` |
| NULL fallback | `IFNULL(x,y)`, `COALESCE` | `ISNULL(x,y)`, `COALESCE` | `IFNULL(x,y)`, `COALESCE` |
| String concat | `CONCAT()` (`\|\|` off by default) | `+` or `CONCAT()` | `CONCAT()` or `\|\|` |
| Substring | `SUBSTRING`/`SUBSTR` | `SUBSTRING` | `SUBSTR` |
| Current date/time | `NOW()`, `CURDATE()` | `GETDATE()`, `SYSDATETIME()` | `CURRENT_DATE()`, `CURRENT_TIMESTAMP()` |
| Date add | `DATE_ADD(d, INTERVAL 1 DAY)` | `DATEADD(day,1,d)` | `DATE_ADD(d, INTERVAL 1 DAY)` |
| Date diff | `DATEDIFF(a,b)` (days) | `DATEDIFF(day,b,a)` | `DATE_DIFF(a,b,DAY)` |
| Cast | `CAST`/`CONVERT` | `CAST`/`CONVERT` | `CAST` + **`SAFE_CAST`** (returns NULL not error) |
| Filter on window result | subquery needed | subquery needed | **`QUALIFY`** (one pass) |
| Auto-increment key | `AUTO_INCREMENT` | `IDENTITY` | none — no sequences; generate keys yourself |
| PK / FK | enforced | enforced | **declared but NOT enforced** — informational only |
| Upsert | `INSERT … ON DUPLICATE KEY UPDATE` | `MERGE` | `MERGE` |

**The four that come up most in interviews:**
1. **`QUALIFY` is BigQuery-only** — the neat way to filter on a window function without a wrapping subquery.
2. **`SAFE_CAST`** — BigQuery's cast-that-returns-NULL, the backbone of the load-then-quarantine pattern.
3. **BigQuery does not enforce PK/FK** — you can *declare* them (BigQuery added informational primary/foreign key constraints the optimizer may use) but they are **not enforced**, so uniqueness must be guaranteed by your dedup logic, not the database. This is the biggest mental shift coming from MySQL/SQL Server.
4. **`TOP` vs `LIMIT`** — the classic tell that someone came from SQL Server; know both.

---

## Part 4 — 10 rapid-fire "defend this word" drills

Two-to-three sentences each; say them the way she'd say them.

1. **"You wrote 'idempotent' — prove it."** "Every load truncates a single date partition with `WRITE_TRUNCATE`, and publish is a MERGE on the natural key — so running a task twice for the same business date lands the same state as running it once. Airflow *will* rerun tasks on retries and backfills, so I design for it rather than hope it doesn't happen."

2. **"You wrote 'reconciliation' — what exactly do you reconcile and how?"** "Three tightening levels: loaded row count against the manifest's declared count; summed debits and summed credits against the manifest's control totals, compared as NUMERIC; and the double-entry invariant, debits equal credits, which needs no manifest at all. A non-zero delta blocks the downstream forward."

3. **"You wrote 'partitioned loads' — partitioned on what, and why?"** "Partitioned on the accounting business date, not ingestion time, because queries and reprocessing are both by date and because backdated entries must land in an old partition. Partitioning also makes reruns cheap — I truncate exactly one day's partition."

4. **"You wrote 'event-driven' — walk me through it without hand-waving."** "GCS object-finalize notification → Pub/Sub topic → Cloud Function subscriber loads the file into BigQuery. Pub/Sub is at-least-once so the function dedupes on object name plus generation, and because 'no file' emits no event, I pair it with a scheduled completeness check."

5. **"You wrote 'SCD Type-2' — what changes and how do you store it?"** "Account master attributes change over time and finance reports *as of* a date, so each version is a row with `valid_from`, `valid_to`, and `is_current`. A change closes the old row and inserts a new one, and fact-to-dimension joins are range joins on business date — so a March journal reports under March's hierarchy."

6. **"You wrote 'SAFE_CAST' — why not just CAST?"** "CAST throws and fails the whole load on one bad value; SAFE_CAST returns NULL, so the file always lands and I route the NULLs to a quarantine table. In finance you want to *know* about the bad row, not lose the load over it."

7. **"You wrote 'MERGE upserts' — what breaks a MERGE?"** "If the ON clause matches multiple source rows to one target row, the MERGE errors — so I dedup staging first with ROW_NUMBER keeping the latest. And I keep writes to a table on a single path so concurrent MERGEs don't conflict under snapshot isolation."

8. **"You wrote 'reducing execution time ~25%' — where did it come from?"** "The migrated jobs were plain scheduled loads running on Dataflow. Moving them to BigQuery Data Transfer Service removed Dataflow's per-run worker startup and shuffle overhead for what was really a bulk load, so wall-clock dropped and we stopped paying Dataflow compute."

9. **"You wrote 'CI/CD (Jenkins)' — what does the pipeline actually do?"** "On a PR, Jenkins lints, runs unit tests on the Python callables, and does a DAG-parse test so a DAG that fails to import never reaches the scheduler. On merge, it rsyncs the DAGs to the Composer environment's GCS dags bucket, which is how Composer deploys."

10. **"You wrote 'window-function balances' — write the running balance."** "`SUM(signed_amount) OVER (PARTITION BY account_id ORDER BY business_date)` for the full history; at scale I'd do it incrementally — yesterday's close plus today's net movement — which is why strict day-ordering matters for that table. I'm careful with the frame: default `RANGE` with ties can over-count, so for a true running total I use `ROWS` or an explicit frame."

---

## Appendix — where the résumé still leans on claims the kit can't fully back

For your decision on whether to add more material:

1. **BigQuery Data Transfer Service (TCS Bullet 2)** — the kit has *nothing*; Part 3.2 here is the only coverage. The "~25% faster, lower cost" number is Anusha's own claim from her TCS work — I've given her a defensible *mechanism* for it, but the specific figure is hers to stand behind, not something the kit substantiates.
2. **Dataflow / Apache Beam (skills line + TCS Bullet 3)** — kit only mentions Dataflow in passing (Q40). Part 3.3 is now the coverage, but there are **no Beam/Dataflow SQL or coding problems** in `sql-practice/`; if interviewers push on writing a ParDo/DoFn, she has concepts but no rehearsed code.
3. **Pub/Sub delivery semantics (both roles)** — kit's Q29 compares patterns but doesn't drill DLQs, ordering keys, push/pull, or exactly-once mode; Part 3.1 fills it, but this is a *headline* skill on both roles and could justify its own practice Q&A.
4. **Jenkins CI/CD (TCS Bullet 5)** — kit has nothing; Part 3.5 is the only coverage. The "+30% release efficiency" is her claim; the mechanism is defensible but the number is hers.
5. **MySQL / SQL Server hands-on (Databases line + TCS Bullet 4)** — the cheatsheet (Part 3.6) covers dialect, but the kit's SQL practice is 100% BigQuery. If an interviewer asks for *T-SQL-specific* or *MySQL-specific* SQL (e.g., a query using `TOP`, `IDENTITY`, or a MySQL-ism), she has no rehearsed material.
6. **The "10+ source systems" and "5+ pipelines / 15+ hrs/week" quantifiers (TCS bullets)** — these are unverifiable personal metrics; the guide coaches the *story* around them, but nothing in the kit corroborates the numbers, so she must be ready to describe them concretely if pressed.

If you want the kit to fully back the résumé, the highest-leverage additions would be: a short **Pub/Sub + Dataflow Q&A set** (like the existing Q1-50 style) and **2-3 T-SQL/MySQL SQL problems** so the "MySQL, SQL Server" databases line isn't defended by a cheatsheet alone.

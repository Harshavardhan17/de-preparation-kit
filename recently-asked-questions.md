# Recently Asked Questions — Field-Collected (2024–2026)

This file is a bank of questions **actually reported in recent Data Engineer interviews** (2024–2026), collected from public interview reports: Glassdoor-style dumps, Medium/candidate writeups, DataLemur company lists, and interview-guide sites. It complements the main `interview-qa.md` answer bank — that file has the deep answers; this one tells you *what gets asked in the room*.

**Honesty note:** company-tagged questions (`[bank]`, `[HSBC]`, `[JPMC]`, etc.) come from public interview reports by candidates — they are representative of what those companies asked someone, not a guarantee of what you'll get. Untagged questions are recurring items across multiple 2024–2026 question dumps for GCP DE roles at the 3–5 YoE level.

Profile this is tuned for: **4 YoE, GCP stack (GCS → Cloud Composer → BigQuery), finance/ledger data project.**

Legend: `[bank]` = reported at a bank/fintech · `→ covered in <file>` = deep answer exists in this kit.

---

## 1. SQL Round (the live-coding hour)

At 3–5 YoE the SQL round is almost always **write-the-query on a shared editor**, 3–6 problems escalating from joins to window functions. Window functions show up in the large majority of screens. BigQuery dialect shown where SQL is given.

### Ranking / Top-N per group
1. **Second highest salary** (then: "now per department", then: "handle ties"). Hint: `DENSE_RANK() OVER (ORDER BY salary DESC)` then filter `= 2`; per-dept adds `PARTITION BY dept_id`. The naive `MAX(salary) WHERE salary < MAX` version is fine as a warm-up but they push you to the window version.
2. **Nth highest salary as a parameterized idea** — explain difference between `ROW_NUMBER`, `RANK`, `DENSE_RANK` on a tie, on your own query. Asked constantly as the follow-up.
3. **Top 3 products/customers by revenue within each region/month.** Hint: `QUALIFY ROW_NUMBER() OVER (PARTITION BY region ORDER BY revenue DESC) <= 3` — `QUALIFY` is a BigQuery-ism interviewers on GCP teams like to see.
4. **Top 3 departments (≥10 employees) by % of employees earning >100K.** `[bank]` (Wells Fargo SQL list). Hint: `COUNTIF(salary > 100000) / COUNT(*)` per dept, `HAVING COUNT(*) >= 10`, rank on the ratio.

→ covered in `sql-practice/01-window-functions.md`

### Gaps & islands / sequences
5. **Consecutive login days — find users with N+ consecutive active days.** The single most-reported "hard" question at this level. Hint: `DATE_SUB(login_date, INTERVAL ROW_NUMBER() OVER (PARTITION BY user ORDER BY login_date) DAY)` gives a constant per island; group on it.
6. **Find a number that appears at least 3 times consecutively** (LeetCode 180 style). `[bank]` — asked verbatim in a JPMC DE round. Hint: `LAG(num, 1)` and `LAG(num, 2)` both equal `num`, or self-join on `id`.
7. **Gaps in a sequence** — missing transaction IDs / missing dates in a daily feed. Hint: `LEAD(id) OVER (ORDER BY id) - id > 1` exposes the gap edges; or generate a calendar with `GENERATE_DATE_ARRAY` and anti-join.

→ covered in `sql-practice/01-window-functions.md`

### Self-joins / hierarchy
8. **Employees earning more than their manager.** Hint: self-join `e JOIN emp m ON e.manager_id = m.emp_id WHERE e.salary > m.salary`.
9. **Employee hierarchy — report the full manager chain / employees under a given manager.** `[bank]` (JPMC). Hint: recursive CTE (`WITH RECURSIVE` in BigQuery).
10. **Managers with at least 5 direct reports.** Hint: `GROUP BY manager_id HAVING COUNT(*) >= 5`, then join back for the name.

### Dedup & data-quality SQL
11. **Find duplicate rows** (which keys appear more than once). Hint: `GROUP BY key HAVING COUNT(*) > 1`.
12. **Delete/keep-one dedup** — keep the latest record per business key. Hint: `QUALIFY ROW_NUMBER() OVER (PARTITION BY key ORDER BY updated_at DESC) = 1`; in BigQuery you rewrite the table or `MERGE`, you don't `DELETE` dupes row-by-row. Say that — it signals real BQ experience.
13. **Customers with >100 transactions in 30 days OR no activity in 60 days** (activity flagging). `[bank]` (JPMC SQL list). Hint: two filtered aggregations combined with `UNION DISTINCT` or `CASE` flags.

→ covered in `sql-practice/02-joins-aggregation-dedup.md`

### Aggregation over time / growth
14. **Year-over-year (YoY) sales growth per product.** Reported verbatim in a GCP BigQuery round. Hint: `LAG(sales) OVER (PARTITION BY product ORDER BY year)` then `(sales - prev)/prev`; BigQuery: guard with `SAFE_DIVIDE`.
15. **Month-over-month growth / running total of revenue.** Hint: `SUM(rev) OVER (ORDER BY month ROWS UNBOUNDED PRECEDING)` for running total; `LAG` for MoM.
16. **Per-month users, transaction count, and total amount for a given year.** `[bank]` (Wells Fargo). Hint: `GROUP BY DATE_TRUNC(txn_date, MONTH)` with `COUNT(DISTINCT user_id)`, `COUNT(*)`, `SUM(amount)`.
17. **Cards issued: difference between highest- and lowest-issuance month per card.** `[bank]` (JPMC). Hint: `MAX(cnt) - MIN(cnt)` over a monthly aggregate per card.
18. **First-month metric per entity** (e.g., issuance amount in each card's launch month). `[bank]` (JPMC). Hint: `ROW_NUMBER() OVER (PARTITION BY card ORDER BY month) = 1`.
19. **7-day moving average of transactions.** Hint: `AVG(amt) OVER (ORDER BY dt ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)`.
20. **Cumulative sum with a window** — Deloitte GCP DE rounds explicitly reported "windowing + cumulative sum" as their SQL test.

### Misc SQL asked alongside
21. Pivot rows → columns (monthly columns from a month row). Hint: BigQuery `PIVOT` operator, or `MAX(CASE WHEN ...)`.
22. `UNION` vs `UNION ALL`; `WHERE` vs `HAVING`; `RANK` vs `DENSE_RANK` — rapid-fire theory between coding problems.
23. Explain joins on your own query: what happens to row counts on a LEFT JOIN with duplicate keys on the right. (Classic fan-out trap; recon-relevant.)
24. One-to-one vs one-to-many relationships, foreign keys, `INTERSECT` — conceptual fillers reported in bank SQL screens. `[bank]`
25. Write SQL to **convert UTC → IST and aggregate monthly** — reported in a GCP round; hint: `DATETIME(ts, "Asia/Kolkata")`, then `DATE_TRUNC`.
26. Simple filter warm-ups still appear at banks: customers in city X whose name starts with 'A'. `[bank]` Don't overthink the first question.

→ window functions: `sql-practice/01-window-functions.md` · joins/dedup/recon: `sql-practice/02-joins-aggregation-dedup.md`

---

## 2. Airflow / Cloud Composer Round

At 3–5 YoE they assume you *run* DAGs in production; questions are operational, not definitional.

### Reported verbatim in GCP DE interviews
27. How do you create a DAG? Walk through one of yours. `[bank]` (HSBC)
28. Which operators have you actually used? (Expected: `PythonOperator`, `BashOperator`, `GCSToBigQueryOperator`, `BigQueryInsertJobOperator`, `BranchPythonOperator`, `DummyOperator`/`EmptyOperator`, sensors.) `[bank]` (HSBC)
29. Where exactly did you use `PythonOperator` vs a prebuilt Google operator, and why?
30. What is `BranchPythonOperator` and when did you branch a pipeline?
31. Which operator/sensor senses **file arrival in GCS**? (`GCSObjectExistenceSensor`.) Follow-up: poke vs reschedule mode — why reschedule is kinder to worker slots.
32. **A file arrives late in GCS — what happens to your DAG and what do you do?** (Sensor timeout, SLA, rerun strategy.)
33. **How do you prevent DAG failure when the file is missing?** (Sensor `soft_fail`, `trigger_rule`, branch to a skip path.)
34. How do you set task dependencies (`>>`, `set_downstream`, `chain`)?
35. `catchup` parameter — what it does, when you set it False. Follow-up: how `start_date` + `schedule_interval` produce the *previous* interval's run (the execution_date/logical-date gotcha).
36. **How do you backfill 6 months of data?** (`airflow dags backfill`, or clear task instances; plus making the pipeline idempotent so backfill is safe.)
37. Task stuck in `up_for_retry` / `queued` — what does it mean, how do you debug?
38. Task works locally, fails in Composer — debugging steps (task logs in GCS/Cloud Logging, worker dependencies/PyPI packages on the Composer image, network/IAM of the Composer service account).
39. XCom — what it's for, and why you should never push a DataFrame through it (metadata only: paths, IDs, counts).
40. `depends_on_past` — what it restricts; when a ledger pipeline actually wants it.
41. SLA miss — what triggers it, how you alert (email/Slack callback, `on_failure_callback`).
42. Dynamic DAGs / dynamic task mapping (`.expand()`) — one task per file/partition.
43. How to resolve **latency/slowness in Cloud Composer** (scheduler tuning, parallelism/`max_active_runs`, environment size, moving heavy work out of the worker into BigQuery/Dataflow).
44. Composer specifics vs vanilla Airflow: where DAG files live (GCS `dags/` bucket), how you install dependencies, environment upgrades, what the Composer service account needs IAM-wise.
45. Retries and idempotency: "your DAG failed halfway and half of yesterday's data is missing in BigQuery — investigate and recover." Scenario reported in multiple 2025 GCP prep dumps. Answer shape: task logs → identify failed partition → idempotent re-run (`WRITE_TRUNCATE` on the date partition or `MERGE`), never blind re-append.

→ pipeline design & idempotent load pattern: `textbook/ledger-pipeline-end-to-end.md`

---

## 3. BigQuery / GCP Round

### BigQuery internals & storage (2025-vintage favorites)
46. Is BigQuery a database or a data warehouse? Where is data *physically* stored — is it in GCS? Can you see the files? (Colossus, columnar Capacitor format, storage/compute separation, no direct file access.)
47. Loading from GCS vs querying an **external table** on GCS — difference, when data is copied, performance tradeoffs. Directly relevant to your GCS→BQ project.
48. What happens if a node fails mid-query / how is data replicated? (Dremel workers retried; storage replicated — you own neither.)
49. All the ways to load data into BigQuery (batch load jobs from GCS, `bq` CLI, Storage Write API / streaming, Data Transfer Service, federated query + `CTAS`).
50. **Partitioning vs clustering** — differences, how you chose keys on your tables, partition types (time, ingestion-time `_PARTITIONTIME`, integer-range), the per-table partition limit (10,000 today; older reports say 4,000).
51. **"Why does my query still scan all partitions even though the table is partitioned?"** Reported verbatim. (Filter not on the partition column, filter wrapped in a function, or the value comes from a subquery — BigQuery can't prune on a non-constant filter; fix with scripting/declared variables or restructuring.)
52. Follow-up reported verbatim: **partition pruning doesn't work with a subquery filter — what do you do?**
53. `SELECT *` cost question: why it's expensive in a columnar store; how do you estimate query cost before running (dry run, bytes-processed estimate in UI).
54. On-demand vs capacity (slots/editions) pricing; what a slot is; how you cut a team's BigQuery bill. Cost questions are near-universal in 2025 GCP rounds.
55. **Nested/repeated data**: write a DDL with a `STRUCT`/`ARRAY` column (HSBC asked: customer with nested `customer_info(phone_num, email_id)`); then query it with `UNNEST`. `[bank]` (HSBC)
56. Materialized views — limitations (reported verbatim: limited query shapes, base-table restrictions, staleness/refresh); vs scheduled queries.
57. How do you handle **corrupted/bad records while loading** (`max_bad_records`, load to staging as STRING then validate, dead-letter table).
58. `MERGE` statement for upserts — write one. Core for SCD2 and idempotent loads.
59. Streaming inserts vs batch loads — duplicates in the streaming buffer, why batch-from-GCS is the default answer for daily ledgers.
60. Table types: native vs external vs views vs materialized views vs snapshots; time travel (`FOR SYSTEM_TIME AS OF`) for "we deleted data, recover it."

→ covered in `sql-practice/03-bigquery-specific.md`

### Wider GCP
61. GCS: storage classes, lifecycle rules (raw-zone archival), signed URLs, bucket-level vs object-level IAM.
62. When Dataflow vs BigQuery SQL vs Dataproc — justify why your pipeline is ELT (SQL in BQ) instead of Beam. Be ready to defend not using Dataflow.
63. Pub/Sub basics: at-least-once delivery, dead-letter topics, when you'd put it in front of the ledger feed.
64. IAM & security: service accounts per environment, least privilege for the Composer SA (needs `bigquery.jobUser` + dataset-level writer + GCS reader), authorized views / column-level security for PII, CMEK. Banks reliably probe security. `[bank]`
65. **Dev vs prod project access — how do you work across projects?** `[bank]` (HSBC, verbatim: access requirements across development and production projects.)
66. **Multi-region infrastructure** — how do you handle resources/datasets across regions (dataset location pinning, cross-region copies). `[bank]` (HSBC)
67. **Cloud → on-prem migration** (reverse direction!) — HSBC asked how you'd approach migrating data *from* cloud *to* on-premises. `[bank]` Think: export to GCS (Avro/Parquet), transfer appliance/interconnect, reconciliation counts.
68. Batch vs streaming pipeline — differences, and how you'd convert your batch ledger pipeline to near-real-time. (Deloitte GCP rounds reported this.)
69. How do you document a data pipeline / set one up from scratch? (Deloitte, reported.) Have a crisp diagram-in-words of GCS → Composer → staging → MERGE → marts.
70. Monitoring: how do you know a load failed or data is late? (Composer alerting + BigQuery `INFORMATION_SCHEMA.JOBS`, freshness checks, row-count recon vs source.)

→ covered in `sql-practice/03-bigquery-specific.md` and `textbook/ledger-pipeline-end-to-end.md`

---

## 4. Python-for-DE Round

Not LeetCode-hard at 3–5 YoE; it's string/dict/file manipulation plus "prove you write production code." Reported patterns:

71. Reverse a string / check palindrome / **first non-repeating character**.
72. **Count word or character frequency** in a string → return top-k. (Dict/`collections.Counter` — the single most common DE Python ask.)
73. Given a list, return the **duplicate elements** (set/dict pass).
74. Merge two dictionaries; sort a list of dicts by a key; dict comprehension with a filter (`{k: v for k, v in d.items() if v > 0}`).
75. **Replace all characters in a list except a given character.** `[bank]` (JPMC, verbatim.)
76. String-pair completeness across the 26 letters. `[bank]` (JPMC — a set-cover style string puzzle.)
77. **File manipulation**: read a large CSV without loading it all in memory (chunks/generators); parse a log file and aggregate counts; read multiple files into DataFrames, drop columns, filter rows, search strings. `[bank]` (JPMC asked exactly the multi-file DataFrame version.)
78. Flatten a nested list / nested dict (mirrors nested JSON → BQ rows).
79. Generators vs lists; `yield`; why generators matter for large files.
80. `lambda`, `map`/`filter`, `zip`, `enumerate` — rapid-fire; reported verbatim: "lambda function", "what is `__init__`?"
81. Mutable default argument gotcha (`def f(x, acc=[])`) — favorite trap.
82. Shallow vs deep copy; `is` vs `==`.
83. Exception handling in a pipeline task: retry with backoff around a flaky API/GCS call.
84. Write a function to validate a record (schema/date/amount checks) and route bad rows to a reject list — ties directly to your ledger validation stage.
85. How does your DAG code get tested? (pytest for transforms, DAG-integrity test that imports the DAG bag.)

---

## 5. Data Modeling / Warehouse Concepts

Asked in the second tech round at nearly every bank and Big-4 loop:

86. **SCD Types 1, 2, 3 — explain and implement.** `[bank]` (HSBC asked all three verbatim.) Be ready to write the SCD2 `MERGE` in BigQuery (close old row with `end_date`, insert new with `is_current`).
87. **Star vs snowflake schema** — tradeoffs; which your ledger marts use and why (star: fewer joins, BQ prefers denormalized + nested over snowflaking).
88. Fact vs dimension tables; types of facts (additive/semi-additive — **account balance is the classic semi-additive example**, perfect for a ledger project); factless facts.
89. Grain: "what is the grain of your fact table?" — have a one-line answer (e.g., one row per journal line per posting date).
90. Surrogate vs natural keys; how you generate surrogate keys in BigQuery (`GENERATE_UUID()`, `FARM_FINGERPRINT`, or sequence-via-`ROW_NUMBER`).
91. **CDC (Change Data Capture) logic** — how you detect and apply changes without a CDC tool (hash-compare / updated_at watermark → MERGE). `[bank]` (HSBC, verbatim.)
92. Normalization (1NF–3NF) vs denormalization for analytics; why BQ tables are wide.
93. Data lake vs warehouse vs lakehouse; where GCS raw zone fits.
94. Lambda architecture / semantic layer — reported in a 2025 GCP round's modeling section.
95. OLTP vs OLAP; why you don't run analytics on the source ledger DB.
96. Data quality dimensions (completeness, accuracy, timeliness) and how you enforce them in the pipeline — banks frame this as a regulatory question. `[bank]`

→ pipeline + modeling context: `textbook/ledger-pipeline-end-to-end.md` · deep answers: `interview-qa.md`

---

## 6. Behavioral / Project Questions at Banks

Banks weight this round heavily; reports consistently show a full round of project deep-dive:

97. **"Walk me through your project end-to-end"** — asked in every single bank report collected (HSBC twice across rounds). Practice a 3-minute and a 10-minute version of the GCS→Composer→BigQuery ledger story: sources, volumes, schedule, failure handling, recon, consumers. `[bank]`
98. Describe your team structure and your daily responsibilities. `[bank]` (HSBC, verbatim.)
99. A production pipeline failed and business reports are wrong/late — what did you do? (Incident story: detection, comms, fix, post-mortem, prevention.) `[bank]`
100. A business user requests a low-value data element that would hurt pipeline performance — how do you handle it? `[bank]` (JPMC, verbatim — push back with cost/benefit, offer alternative.)
101. You have 3 committed sprint tasks and an urgent unplanned request lands — how do you prioritize? `[bank]` (JPMC, verbatim.)
102. How do you ensure data accuracy for finance/regulatory reporting? (Recon controls, balance checks debit=credit, audit trail, immutable raw zone.) `[bank]`
103. Why this bank / why are you leaving? Salary expectations come up early and bluntly — HSBC reports say they rarely entertain counteroffers, so state your number clearly. `[bank]`
104. Conflict/ownership standards: a time you disagreed with a design decision; a mistake you made in production and what changed after.

---

## Sources

Directly consulted:
- HSBC GCP Data Engineer interview experience, 4+ YoE — https://learn.growdataskills.com/experience/HSBC_%7C_GCP_Data_Engineer_Interview_Experience_%7C_4+_YoE
- JPMorgan Chase Data Engineer interview experience (5 rounds) — https://www.preplaced.in/blog/data-engineer-interview-experience-jp-morgan-chase
- JPMorgan Chase SQL interview questions — https://datalemur.com/blog/jpmorgan-chase-sql-interview-questions
- "Crack GCP BigQuery & Cloud Composer Interviews – Real Questions" (Medium, Mallinath Patil) — https://medium.com/@mallinathnpatil1996/crack-gcp-bigquery-cloud-composer-interviews-real-questions-with-simple-answers-7ecb58f69844
- "GCP BigQuery Interview Questions — must know before 2026" (Medium, Rahul Sounder) — https://medium.com/@sounder.rahul/gcp-bigquery-interview-questions-data-engineer-must-know-before-2026-d828a9833c2e
- 80 SQL Interview Questions for Data Engineers (Datavidhya) — https://datavidhya.com/blog/sql-data-engineering-interview-questions/
- 45 Airflow Interview Questions for Data Engineers (Datavidhya) — https://datavidhya.com/blog/apache-airflow-data-engineering-interview-questions/
- Deloitte GCP Data Engineer interview experience, India, June 2025 — https://www.jointaro.com/interviews/companies/deloitte/experiences/gcp-data-engineer-india-june-16-2025-declined-offer-positive-ec8806cc/

Corroborating (surfaced via search, snippets used):
- Wells Fargo SQL interview questions — https://datalemur.com/blog/wells-fargo-sql-interview-questions
- HSBC Data Engineer interview guide — https://www.interviewquery.com/interview-guides/hsbc-bank-data-engineer
- Barclays Data Engineer interview guide — https://www.interviewquery.com/interview-guides/barclays-data-engineer
- Goldman Sachs Data Engineer interview guide — https://www.interviewquery.com/interview-guides/goldman-sachs-data-engineer
- Deloitte USI GCP DE thread (Glassdoor community) — https://www.glassdoor.co.in/Community/interview-experience/hi-there-what-questions-should-i-expect-for-gcp-data-engineer-role-in-deloitte-usi-deloitte-deloitte-india-suggest-me-any
- GCP DE questions for freshers & experienced 2025 (MyLearnNest) — https://www.mylearnnest.com/gcp-data-engineer-interview-questions-and-answers-for-freshers-experienced-2025/
- 50 Python Interview Questions for Data Engineers (Datavidhya) — https://datavidhya.com/blog/python-data-engineering-interview-questions/

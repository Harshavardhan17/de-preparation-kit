# BigQuery-Specific SQL & Design — Practice Bank

## Core mental model

BigQuery is a **serverless, columnar, distributed** warehouse, and its pricing model shapes every design question: on-demand queries are billed by **bytes scanned** (columns read × rows in the scanned storage blocks), not by rows returned or query time. Therefore the entire optimization game is *scan less*: read fewer **columns** (columnar storage means every column you SELECT is a separate read) and fewer **rows worth of storage** (via **partitioning**, which physically splits a table into segments the engine can skip — *pruning* — and **clustering**, which sorts data within storage blocks so block metadata can skip non-matching ranges).

Two consequences surprise people coming from OLTP databases. First, BigQuery has **no indexes and no enforced primary keys** — uniqueness is *your pipeline's job*, which is why idempotent loads via `MERGE` (or load-then-dedup with `ROW_NUMBER`) are core interview material for anyone running daily Airflow loads. Second, `LIMIT 10` does **not** reduce cost — the full column scan happens before the limit; use the free **table preview** or `TABLESAMPLE` to eyeball data. Capacity-based (slot) pricing changes the *billing* but not the engineering instinct: a **slot** is a unit of compute, and a wasteful query still hogs slots your other DAG tasks need.

The third pillar is BigQuery's **nested data model**: a column can be an `ARRAY` (repeated values in one row) or a `STRUCT` (a nested record), and `UNNEST` flattens arrays into joinable rows. Mastering ARRAY/STRUCT/UNNEST — including the empty-array trap — is what separates "writes SQL in BigQuery" from "engineers on BigQuery."

Scenario throughout: your real stack — daily CSV/ZIP ledger feeds land in GCS, Cloud Composer loads them to BigQuery staging, and MERGE/transform jobs build the general-ledger marts.

---

## Problem 1 — QUALIFY clause (Easy)

**Asked as:** "You keep writing subqueries just to filter on ROW_NUMBER. What BigQuery clause removes that wrapper, and where does it sit in the evaluation order?"

Table `stg_journal_entries(entry_id, account_id, amount, load_ts)` with re-load duplicates.

**Expected output:** latest row per `entry_id`, no subquery.

<details><summary>Solution</summary>

```sql
SELECT entry_id, account_id, amount, load_ts
FROM stg_journal_entries
WHERE TRUE                                    -- see note below
QUALIFY ROW_NUMBER() OVER (
          PARTITION BY entry_id
          ORDER BY load_ts DESC
        ) = 1;
```

**Why it works / what's being tested.** `QUALIFY` is to window functions what `HAVING` is to aggregates: a post-window filter. Evaluation order: `FROM → WHERE → GROUP BY → HAVING → window functions → QUALIFY → ORDER BY/LIMIT`. Because it runs *after* windows are computed, it can reference them directly — no wrapper subquery. You can also filter on a windowed column defined in SELECT by its alias (`SELECT ..., ROW_NUMBER() OVER (...) AS rn ... QUALIFY rn = 1`). Historical quirk worth knowing: older BigQuery required a `WHERE`, `GROUP BY`, or `HAVING` clause alongside `QUALIFY`, so codebases are full of the `WHERE TRUE` idiom; current BigQuery no longer requires it, but recognize it in reviews. Also mention: `QUALIFY` is BigQuery/Snowflake/Teradata/DuckDB dialect, **not** portable ANSI SQL — in Postgres you still need the subquery.

**Common wrong answers:** `WHERE ROW_NUMBER() OVER (...) = 1` (window functions are illegal in WHERE); thinking QUALIFY runs before WHERE (it's last of the filters — WHERE has already reduced the rows the window saw, which matters: filtering in WHERE changes the numbering, filtering in QUALIFY doesn't).
</details>

---

## Problem 2 — Partitioned table design & partition pruning (Medium)

**Asked as:** "Design the DDL for a 5-year, multi-terabyte journal-entries table so that the daily recon query for one date scans gigabytes, not terabytes. How do you *guarantee* nobody full-scans it, and what changes if the table is ingestion-time partitioned?"

<details><summary>Solution</summary>

```sql
-- Column-partitioned design (preferred: partition on the business date)
CREATE TABLE finance.journal_entries
(
  entry_id     STRING    NOT NULL,
  account_id   STRING    NOT NULL,
  entry_date   DATE      NOT NULL,
  amount       NUMERIC   NOT NULL,     -- NUMERIC for money, never FLOAT64
  source_file  STRING,
  load_ts      TIMESTAMP
)
PARTITION BY entry_date
CLUSTER BY account_id
OPTIONS (
  require_partition_filter = TRUE,     -- queries without a WHERE on entry_date are REJECTED
  partition_expiration_days = 1825
);

-- Prunes: the engine touches only the 2026-07-14 partition
SELECT account_id, SUM(amount)
FROM finance.journal_entries
WHERE entry_date = '2026-07-14'
GROUP BY account_id;

-- Does NOT prune (function applied to the partition column):
--   WHERE CAST(entry_date AS STRING) = '2026-07-14'
--   WHERE DATE_ADD(entry_date, INTERVAL 1 DAY) = '2026-07-15'
-- Prunes fine (constant expressions / arithmetic on the *other* side are OK):
--   WHERE entry_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)

-- Ingestion-time partitioned tables have no partition column of their own;
-- you filter the pseudo-column (a TIMESTAMP):
SELECT COUNT(*)
FROM finance.raw_ledger_ingest
WHERE _PARTITIONTIME = TIMESTAMP('2026-07-14');
-- or the DATE-typed alias: WHERE _PARTITIONDATE = '2026-07-14'
```

**Why it works / what's being tested.**
- **Partitioning** physically segments storage by the partition key (DATE/TIMESTAMP/DATETIME column, `_PARTITIONTIME` for ingestion-time, or integer range). A constant filter on that key lets the engine **prune** — skip whole partitions before scanning, which is *the* bytes-billed lever on time-series data. Daily ledger data partitioned by `entry_date` means the daily recon touches 1/1825th of the table.
- `require_partition_filter = TRUE` turns "please filter" into "must filter" — an unfiltered ad-hoc query fails instead of costing a full scan. On a shared finance dataset, set it and say so in interviews.
- The pruning-killer to name: **wrapping the partition column in a function or expression** on the left side of the predicate. Compute on the constant side instead.
- Business-date column partitioning beats ingestion-time when late files arrive (a July 10 file loaded July 12 lands in the July 10 partition, keeping recon queries correct). Ingestion-time partitioning is fine for raw append-only landing zones; remember `_PARTITIONTIME` is a `TIMESTAMP` (midnight UTC of the partition), and it's NULL for rows in the streaming buffer.
- Limits worth knowing: up to 10,000 partitions per table (so day-partitioning is good for ~27 years); too-granular partitions (< ~1 GB each) are an anti-pattern — that's clustering's job.

**Common wrong answers:** partitioning by `account_id` (high-cardinality STRING — not allowed; that's a clustering key); believing `LIMIT` reduces the scan; filtering `DATE(_PARTITIONTIME) = ...` and asserting it always prunes (it does prune, but the idiomatic and safest form is a direct comparison on the pseudo-column).
</details>

---

## Problem 3 — Clustering: what it is and when it helps (Medium)

**Asked as:** "We partitioned by entry_date, but the ops team constantly queries single accounts across a year, and those queries still scan every daily partition fully. What's the next lever, and what are its limits?"

<details><summary>Solution</summary>

```sql
CREATE TABLE finance.journal_entries_v2
PARTITION BY entry_date
CLUSTER BY account_id, cost_centre      -- up to 4 columns, ORDER MATTERS
AS SELECT * FROM finance.journal_entries;

-- Now this scans only the storage blocks whose account_id range can contain A-1001,
-- inside each of the ~365 date partitions it must visit:
SELECT entry_date, SUM(amount)
FROM finance.journal_entries_v2
WHERE entry_date >= '2025-07-15'
  AND account_id = 'A-1001'
GROUP BY entry_date;
```

**Why it works / what's being tested.**
- **Clustering** sorts data *within each partition* by the cluster columns and records min/max metadata per storage block, so equality and range predicates (and prefixes for LIKE 'abc%') on those columns can skip blocks — **block pruning** on top of partition pruning. It's the right tool for high-cardinality keys like `account_id` that partitioning can't handle.
- **Column order matters** like a composite index: filters on `account_id` alone benefit; filters on `cost_centre` alone benefit far less, because data is sorted by account first.
- Honest limits (interviewers probe these): unlike partitioning, clustering gives **no up-front cost guarantee** — the on-demand cost estimate assumes the worst case, and actual bytes billed come in lower after the run; you can't use it with `require_partition_filter`-style enforcement; benefits show up on tables roughly ≥ 1 GB; and BigQuery **re-clusters automatically in the background for free** as inserts accumulate (no manual VACUUM/OPTIMIZE).
- Rule of thumb to recite: **partition on the column you filter by date/lifecycle, cluster on the columns you filter or join by identity** — here, partition `entry_date`, cluster `account_id`.

**Common wrong answers:** "add an index" (BigQuery has none in this sense); clustering on a column you never filter on; putting the rarely-filtered column first in the cluster list; claiming clustering reduces the *estimated* cost shown in the validator (it can't promise, only deliver).
</details>

---

## Problem 4 — MERGE: idempotent daily upsert (Hard)

**Asked as:** "Your Composer DAG loads each day's ledger file into staging, then applies it to the mart. Retries happen; files get re-delivered with corrections; entries can be dropped from a corrected file. Write one statement that makes the apply step idempotent — same input, same final state, run it twice safely."

Tables: `finance.gl_entries` (target, partitioned by `entry_date`), `finance.stg_entries_20260714` (staging: today's file, may contain intra-file duplicates).

<details><summary>Solution</summary>

```sql
MERGE finance.gl_entries AS T
USING (
  -- Dedup the staging file first: MERGE errors if the source matches
  -- one target row with multiple source rows.
  SELECT * EXCEPT(rn)
  FROM (
    SELECT s.*,
           ROW_NUMBER() OVER (PARTITION BY entry_id ORDER BY load_ts DESC) AS rn
    FROM finance.stg_entries_20260714 AS s
  )
  WHERE rn = 1
) AS S
ON  T.entry_id   = S.entry_id
AND T.entry_date = '2026-07-14'          -- restricts the target scan to one partition

WHEN MATCHED AND (T.amount != S.amount OR T.account_id != S.account_id) THEN
  UPDATE SET amount     = S.amount,
             account_id = S.account_id,
             load_ts    = S.load_ts

WHEN NOT MATCHED BY TARGET THEN
  INSERT (entry_id, account_id, entry_date, amount, load_ts)
  VALUES (S.entry_id, S.account_id, S.entry_date, S.amount, S.load_ts)

WHEN NOT MATCHED BY SOURCE AND T.entry_date = '2026-07-14' THEN
  DELETE;                                 -- entry removed in the corrected file
```

**Why it works / what's being tested.**
- The three branches map exactly to upsert semantics: **MATCHED** → correction (UPDATE), **NOT MATCHED BY TARGET** → new entry (INSERT), **NOT MATCHED BY SOURCE** → entry absent from today's corrected file (DELETE). Run it twice with the same file: second run matches everything with equal values, updates nothing (the `AND ... != ...` guard skips no-op updates), inserts nothing, deletes nothing — **idempotent**.
- **The `WHEN NOT MATCHED BY SOURCE` scoping is the make-or-break detail**: without `AND T.entry_date = '2026-07-14'`, that branch would DELETE *every historical row in the table* not present in today's file. Every experienced interviewer checks for this.
- The `AND T.entry_date = '2026-07-14'` predicate in `ON` (and in the DELETE branch) also lets BigQuery prune the target scan to one partition instead of a full-table read — MERGE is billed like any query on the bytes it reads.
- Deduping the source first prevents the runtime error BigQuery throws when one target row matches multiple source rows in an UPDATE/DELETE branch — and daily files re-delivered by upstream systems *do* contain duplicates.
- Alternative idempotency pattern worth naming: for pure snapshot loads, skip MERGE and atomically replace the day's slice — `DELETE WHERE entry_date = @d; INSERT ...` in one transaction, or write to the partition decorator `gl_entries$20260714` with WRITE_TRUNCATE. MERGE wins when you need row-level UPDATE/DELETE semantics and one atomic statement.

**Common wrong answers:** unscoped `WHEN NOT MATCHED BY SOURCE THEN DELETE` (wipes history); plain `INSERT INTO ... SELECT` from staging (retry ⇒ duplicates ⇒ recon breaks); no source dedup (intermittent "UPDATE/MERGE must match at most one source row" failures); `ON T.entry_id = S.entry_id` alone with no date predicate (full-table scan of a multi-TB target every night).
</details>

---

## Problem 5 — ARRAY / STRUCT / UNNEST, and the empty-array trap (Medium)

**Asked as:** "We land journal batches as one row per batch with a repeated field of lines. Sum line amounts per batch — and explain why your first query lost the batches that had no lines."

Table `journal_batches`:

```sql
CREATE TABLE finance.journal_batches (
  batch_id STRING,
  batch_date DATE,
  lines ARRAY<STRUCT<line_no INT64, account_id STRING, amount NUMERIC>>
);
-- B-100: [ (1,'A-1001',500), (2,'A-2001',-500) ]
-- B-101: [ (1,'A-1001',75) ]
-- B-102: []                       -- header arrived, lines file missing
```

**Expected output:** B-100 → 0 (balanced), B-101 → 75, **B-102 → row present** with NULL/0 total, not silently absent.

<details><summary>Solution</summary>

```sql
-- WRONG-ish: comma CROSS JOIN UNNEST drops B-102 (zero array elements ⇒ zero joined rows)
SELECT b.batch_id, SUM(l.amount) AS total
FROM finance.journal_batches AS b,
     UNNEST(b.lines) AS l
GROUP BY b.batch_id;

-- RIGHT: LEFT JOIN UNNEST preserves batches with empty (or NULL) arrays
SELECT
  b.batch_id,
  b.batch_date,
  COUNT(l.line_no)          AS line_count,   -- 0 for B-102
  SUM(l.amount)             AS total_amount  -- NULL for B-102; wrap IFNULL(...,0) if preferred
FROM finance.journal_batches AS b
LEFT JOIN UNNEST(b.lines) AS l
GROUP BY b.batch_id, b.batch_date;

-- Bonus fluency: filter on array contents without flattening the result grain
SELECT batch_id
FROM finance.journal_batches
WHERE EXISTS (SELECT 1 FROM UNNEST(lines) AS l WHERE l.amount > 100000);

-- Building nested data back up:
SELECT batch_date,
       ARRAY_AGG(STRUCT(batch_id, total_amount) ORDER BY total_amount DESC) AS batches
FROM (...) GROUP BY batch_date;
```

**Why it works / what's being tested.**
- `UNNEST(arr)` turns an array into a table of its elements; a **correlated** `JOIN UNNEST(b.lines)` pairs each parent row with its own elements. The comma syntax is an implicit **CROSS JOIN**, and a cross join with zero rows yields zero rows — so empty-array (and NULL-array) parents vanish. `LEFT JOIN UNNEST` is the fix; there's no `ON` needed (the correlation *is* the join condition — BigQuery accepts it without `ON TRUE`).
- Struct fields are reached with plain dot syntax (`l.amount`), and `COUNT(l.line_no)` vs `COUNT(*)` matters after a LEFT JOIN: `COUNT(*)` would return 1 for B-102's null-padded row.
- Why this schema exists at all: nested arrays keep a batch and its lines **physically co-located** — no join at query time, no fan-out risk, one table. It's the idiomatic BigQuery answer to header/detail data, and tying it to "we could land the ledger batch header + lines this way instead of two staging tables" makes it concrete.

**Common wrong answers:** comma-join UNNEST and not noticing missing parents (the entire point of the question); `WHERE ARRAY_LENGTH(lines) > 0` as a "fix" (institutionalizes the data loss); trying `UNNEST(journal_batches)` on the table itself; `SELECT lines[0]` (BigQuery array access is `lines[OFFSET(0)]` 0-based or `lines[ORDINAL(1)]` 1-based, and both error on empty arrays — `lines[SAFE_OFFSET(0)]` returns NULL instead).
</details>

---

## Problem 6 — SAFE_CAST and dirty CSV data (Medium)

**Asked as:** "The ledger CSVs land with everything as STRING: amounts like '1,234.56', '(500.00)' for negatives, dates as '14/07/2026', and occasional garbage like 'N/A'. Type the staging data without the load ever failing, but don't silently swallow bad rows — route them to a rejects output."

Table `stg_raw(entry_id STRING, amount_raw STRING, entry_date_raw STRING)`:

| entry_id | amount_raw | entry_date_raw |
|----------|-----------|----------------|
| E1       | 1,234.56  | 14/07/2026     |
| E2       | (500.00)  | 14/07/2026     |
| E3       | N/A       | 14/07/2026     |
| E4       | 250.00    | 2026-07-14     |

<details><summary>Solution</summary>

```sql
WITH typed AS (
  SELECT
    entry_id,
    -- normalize: strip thousands separators; (x) accounting style ⇒ -x
    SAFE_CAST(
      IF(STARTS_WITH(TRIM(amount_raw), '('),
         '-' || REPLACE(REPLACE(REPLACE(amount_raw, '(', ''), ')', ''), ',', ''),
         REPLACE(amount_raw, ',', ''))
      AS NUMERIC)                                        AS amount,
    COALESCE(
      SAFE.PARSE_DATE('%d/%m/%Y', entry_date_raw),       -- 14/07/2026
      SAFE.PARSE_DATE('%Y-%m-%d', entry_date_raw)        -- 2026-07-14
    )                                                    AS entry_date
  FROM stg_raw
)
-- Clean rows → mart
SELECT * FROM typed
WHERE amount IS NOT NULL AND entry_date IS NOT NULL;

-- Reject rows → quarantine table for ops follow-up
SELECT r.*
FROM stg_raw AS r
JOIN typed  AS t USING (entry_id)
WHERE t.amount IS NULL OR t.entry_date IS NULL;   -- E3 lands here
```

**Why it works / what's being tested.**
- `CAST('N/A' AS NUMERIC)` **aborts the whole query**; `SAFE_CAST` returns **NULL** on failure so one bad cell can't take down the nightly load. Same pattern generalizes: prefixing most functions with `SAFE.` (e.g., `SAFE.PARSE_DATE`, `SAFE_DIVIDE`) converts runtime errors into NULLs.
- The senior half of the answer is the **rejects flow**: SAFE_CAST alone silently converts bad money to NULL, which in finance is unacceptable — you must *count and quarantine* what failed (and your Composer task can fail the DAG or alert if the reject rate exceeds a threshold). "Never fail the load, never lose the failures."
- `PARSE_DATE` with explicit format strings beats `SAFE_CAST(x AS DATE)` for non-ISO formats — `SAFE_CAST` only accepts canonical formats, so '14/07/2026' needs `%d/%m/%Y`. The `COALESCE` of two parse attempts handles mixed-format files, a very real CSV problem.
- Load-time complement worth mentioning: `bq load` options `--max_bad_records` and schema-as-STRING landing (ELT: land raw, type in SQL) — landing everything as STRING and typing in SQL is precisely what this pattern supports.

**Common wrong answers:** plain `CAST` ("it works on my sample"); SAFE_CAST with no reject capture (silent data loss on money); `SAFE_CAST('14/07/2026' AS DATE)` expecting it to guess the format (returns NULL — it isn't a parser); using FLOAT64 for amounts (binary floating point cannot represent 0.1 exactly; money is NUMERIC).
</details>

---

## Problem 7 — TIMESTAMP vs DATETIME vs DATE, and timezone gotchas (Medium)

**Asked as:** "A payment posted at 2026-07-14 23:30 in London shows up in the 2026-07-15 daily report. Walk me through BigQuery's date-time types and find the bug."

Table `payments(payment_id STRING, posted_ts TIMESTAMP)` — `posted_ts = 2026-07-14 22:30:00 UTC` (23:30 London, BST=UTC+1).

<details><summary>Solution</summary>

```sql
-- The bug: DATE() on a TIMESTAMP defaults to UTC
SELECT DATE(posted_ts)                 AS utc_business_date,     -- 2026-07-14
       DATE(posted_ts, 'Europe/London') AS london_business_date; -- 2026-07-14
-- ...so where did 07-15 come from? The reverse mistake: someone stored a London
-- wall-clock string into a TIMESTAMP (interpreted as UTC) or grouped with a
-- hard-coded offset. The fix is always: store the instant, convert at the edge.

-- Correct daily grouping for a London-based ledger:
SELECT
  DATE(posted_ts, 'Europe/London') AS business_date,
  COUNT(*) AS payments,
  SUM(amount) AS total
FROM payments
GROUP BY business_date;
```

**Why it works / what's being tested.** The three types answer different questions:

| Type | What it stores | Timezone? | Use for |
|------|----------------|-----------|---------|
| `TIMESTAMP` | An absolute instant (microseconds since epoch; rendered in UTC) | Aware at the edges: parse/format take a zone, storage is zone-less | Event times: load_ts, posted_ts, audit trails |
| `DATETIME` | A wall-clock reading, e.g. "2026-07-14 23:30:00" | None — it's a civil clock face | "What the local clock said": branch cut-off times |
| `DATE` | A calendar day | None | Business/accounting dates, partition keys |

The gotchas to volunteer: (1) `DATE(ts)` / `EXTRACT(... FROM ts)` / `TIMESTAMP_TRUNC(ts, DAY)` all default to **UTC** — pass the zone (`'Europe/London'`, IANA names, never fixed offsets, because BST/GMT flips twice a year); (2) `CURRENT_DATE()` is also UTC-by-default — a Composer job running at 00:30 London time in summer gets *yesterday's* UTC date; write `CURRENT_DATE('Europe/London')`; (3) casting `DATETIME → TIMESTAMP` silently assumes UTC — the classic way a 23:30 London payment becomes "22:30Z stored as 23:30Z" and drifts a day at the boundary; be explicit with `TIMESTAMP(dt, 'Europe/London')`; (4) daylight-saving means a London business day is not always 24 hours — never implement "London day" as `TIMESTAMP ± INTERVAL 1 HOUR`.

**Common wrong answers:** "TIMESTAMP stores a timezone" (it doesn't — it stores an instant; the zone only exists at parse/format time); fixing the report with `DATETIME_ADD(ts, INTERVAL 1 HOUR)` (breaks every October); partitioning on TIMESTAMP but filtering with a DATE literal and mismatched zone assumptions so day-boundary rows land in the wrong report bucket.
</details>

---

## Problem 8 — Wildcard tables & _TABLE_SUFFIX (Medium)

**Asked as:** "A legacy process created one table per day: `ledger_20260101`, `ledger_20260102`, … Query all of July 2026 in one statement, and tell me why we should migrate away from this design."

<details><summary>Solution</summary>

```sql
SELECT
  PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS entry_date,
  account_id,
  SUM(amount) AS total
FROM `finance.ledger_*`
WHERE _TABLE_SUFFIX BETWEEN '20260701' AND '20260731'
GROUP BY entry_date, account_id;
```

**Why it works / what's being tested.**
- The wildcard `ledger_*` unions every table whose name starts with the prefix; the pseudo-column **`_TABLE_SUFFIX`** holds the part matched by `*`, and a **constant** filter on it prunes which tables are opened — the sharded-table analogue of partition pruning. Two subtleties: the comparison is **lexicographic on STRINGs**, which only works because `YYYYMMDD` sorts like the date (zero-padding matters); and the filter must be constant-foldable — deriving the bound from a subquery or joined column defeats pruning and scans every shard.
- Make the wildcard as long as possible (`ledger_2026*` beats `ledger_*` if a `ledger_backup` table lurks in the dataset — the wildcard matches *every* prefixed table, including ones you forgot).
- The migration argument you're expected to make: date-sharded tables are the **legacy** pattern — schema drift between shards breaks the union (wildcard uses the schema of the most recently created matching table), per-dataset table counts balloon, and there's a limit on tables per query. A single table `PARTITION BY entry_date` gives the same pruning with none of the drift; `require_partition_filter` even replaces the "always filter suffix" discipline with enforcement.

**Common wrong answers:** `WHERE PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) BETWEEN ...` (functions on the pseudo-column can prevent table pruning — filter on the raw string, parse only in SELECT); UNION ALL of 31 table names by hand; assuming the wildcard checks schemas match (it doesn't — it trusts you).
</details>

---

## Problem 9 — Cost optimization: how BigQuery bills and how not to pay for mistakes (Medium)

**Asked as:** "Your team's monthly BigQuery bill doubled. Walk me through how queries are billed and the checklist you'd apply to the recon DAG's queries."

<details><summary>Solution</summary>

**The billing model.** On-demand pricing bills **bytes scanned**: (columns actually referenced) × (their compressed-storage footprint in the partitions/blocks the engine could not prune). Query *runtime* and *rows returned* are irrelevant to the bill. Under **capacity (slot) pricing** you pre-buy compute — **slots** are units of parallel worker capacity, and queries queue for them — so an inefficient query no longer costs marginal dollars but starves concurrent workloads (your other DAG tasks) of slots. Same optimizations, different currency.

**The checklist, in the order it pays off:**

1. **Kill `SELECT *`.** Columnar storage means every column is a separate read; a 40-column journal table queried for 4 columns is a ~10× overpay. `SELECT * EXCEPT(big_json_col)` when you genuinely need "almost all."
2. **Filter on the partition column with a bare predicate** (Problem 2) and set `require_partition_filter=TRUE` on big tables. Verify with a dry run: `bq query --dry_run` or the console validator shows bytes *before* you spend them; the estimate is exact for partition pruning.
3. **Know what does NOT reduce cost:** `LIMIT 10` (full column scan happens first — the one exception is clustered tables, where LIMIT can stop early... don't rely on it); `WHERE` on non-partition/non-cluster columns (rows are filtered *after* blocks are read). To eyeball data use the free **table preview** or `TABLESAMPLE SYSTEM (1 PERCENT)`.
4. **Cluster** the big tables on the identity columns your recon filters/joins by (Problem 3).
5. **Don't re-scan for retries or downstream steps:** results are cached for 24h (free) only for identical, deterministic queries — `CURRENT_TIMESTAMP()` in the SQL busts the cache; materialize intermediate recon results to a table (or use materialized views) instead of re-running a 5-table join in three DAG tasks.
6. **Watch write patterns:** `MERGE` scoped to one partition (Problem 4), not the whole target; avoid single-row DML in loops — batch it.
7. **Guardrails:** per-query `maximum_bytes_billed` (query fails fast if it would scan more), per-project custom quotas, and labels on DAG queries so INFORMATION_SCHEMA.JOBS attributes spend per pipeline: `SELECT user_email, SUM(total_bytes_billed) FROM region-eu.INFORMATION_SCHEMA.JOBS_BY_PROJECT WHERE creation_time > ... GROUP BY 1`.

**What the interviewer is testing:** that you know bytes-scanned is the unit (most candidates say "rows"), that `LIMIT` is a trap, that pruning ≠ filtering, and that you'd *measure* (dry run, JOBS view) before optimizing.

**Common wrong answers:** "add LIMIT"; "create an index"; "SELECT * is fine because the table is partitioned" (partitioning prunes rows-worth-of-blocks, it does nothing about column width).
</details>

---

## Problem 10 — Approximate aggregation: APPROX_COUNT_DISTINCT (Easy)

**Asked as:** "The daily data-quality dashboard shows distinct account counts over 2 billion postings and it's the slowest query in the DAG. Exactness doesn't matter for the trend chart. What do you reach for, and when is it the wrong tool?"

Table `postings(posting_date DATE, account_id STRING, amount NUMERIC)` — billions of rows.

<details><summary>Solution</summary>

```sql
-- Exact: memory-hungry shuffle of every distinct value per group
SELECT posting_date, COUNT(DISTINCT account_id) AS active_accounts
FROM finance.postings
GROUP BY posting_date;

-- Approximate: constant, small memory per group (~1% typical error)
SELECT posting_date, APPROX_COUNT_DISTINCT(account_id) AS active_accounts_approx
FROM finance.postings
GROUP BY posting_date;

-- Composable variant: build sketches once, merge across any slice later
CREATE TABLE finance.daily_account_sketches AS
SELECT posting_date, HLL_COUNT.INIT(account_id) AS acct_sketch
FROM finance.postings
GROUP BY posting_date;

SELECT HLL_COUNT.MERGE(acct_sketch) AS accounts_in_july   -- distinct across the month,
FROM finance.daily_account_sketches                       -- WITHOUT re-reading postings
WHERE posting_date BETWEEN '2026-07-01' AND '2026-07-31';
```

**Why it works / what's being tested.** Exact `COUNT(DISTINCT)` must effectively gather all distinct values per group across workers — memory and shuffle scale with cardinality. `APPROX_COUNT_DISTINCT` uses **HyperLogLog++**: a fixed-size *sketch* summarizes the set, giving typically ~1% error at a fraction of the memory/CPU. The killer feature is that sketches are **mergeable**: daily `HLL_COUNT.INIT` sketches can be `HLL_COUNT.MERGE`d into weekly/monthly distincts — something you can never do with stored daily exact counts, because distinct counts don't add (the same account active on two days would be double-counted). That daily-sketch table is a perfect fit for a DQ dashboard fed by a Composer DAG. Related family: `APPROX_QUANTILES` (percentile latencies/amounts), `APPROX_TOP_COUNT` (top accounts by frequency).

**When it's the wrong tool — say this unprompted:** anything that lands in a financial statement, a regulatory report, or a reconciliation break count must be **exact**. Approximation is for operational monitoring, trends, and capacity numbers where ±1% changes nothing.

**Common wrong answers:** using it for a regulatory or recon figure; storing daily exact distinct counts and SUMming them for the month (double-counts repeat accounts — the exact mistake sketches solve); claiming a fixed error bound (it's probabilistic — typical, not guaranteed).
</details>

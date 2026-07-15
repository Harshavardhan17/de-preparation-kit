# Window Functions — BigQuery Standard SQL

## Core mental model

A **window function** (BigQuery calls them **analytic functions**) computes a value for *every input row* using a set of related rows called the **window**, without collapsing rows the way `GROUP BY` does. `GROUP BY` answers "one row per group"; a window function answers "every row, plus something computed over its group." That distinction is the single most common interview probe.

The window is defined by the `OVER` clause, which has three parts, each optional:

- **`PARTITION BY`** splits rows into independent groups (e.g., one ledger account per partition). The function restarts for each partition.
- **`ORDER BY`** sequences rows *within* the partition. It is required for navigation functions (`LEAD`, `LAG`, `FIRST_VALUE`, `LAST_VALUE`) and numbering functions (`ROW_NUMBER`, `RANK`, `DENSE_RANK`, `NTILE`), and it changes the meaning of aggregates (see frames, next).
- **The frame clause** (`ROWS ...` / `RANGE ...`) restricts an *aggregate* window function to a sliding subset of the partition. Numbering and navigation functions do not accept a frame.

The frame default is the classic trap: when you write `SUM(x) OVER (PARTITION BY a ORDER BY b)` **with an `ORDER BY` but no frame clause**, BigQuery uses **`RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`** — a running total, *but* `RANGE` treats all **peer rows** (rows with equal `ORDER BY` values) as one unit, so tied rows all receive the same combined total. `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` advances strictly one physical row at a time. With no `ORDER BY` at all, the frame is the entire partition. Also remember: in BigQuery, `RANGE` with a numeric offset (`RANGE BETWEEN 6 PRECEDING ...`) requires a **numeric** `ORDER BY` key, so for date-based sliding windows you order by `UNIX_DATE(d)`.

Finally, window functions are evaluated **after** `WHERE`, `GROUP BY`, and `HAVING`, so you cannot filter on them directly in `WHERE`. You either wrap the query in a subquery/CTE or use BigQuery's **`QUALIFY`** clause (covered in depth in file 03).

Scenario used throughout: HSBC-style general-ledger data — journal entries land daily from CSV/ZIP feeds in GCS, get loaded into BigQuery by a Composer DAG, and your team produces balances, reconciliations, and control reports.

---

## Problem 1 — ROW_NUMBER vs RANK vs DENSE_RANK (Easy)

**Asked as:** "Rank accounts by month-end balance, highest first. Two accounts are tied — explain what `ROW_NUMBER`, `RANK`, and `DENSE_RANK` each return for them and the row after."

Table `month_end_balances`:

| account_id | balance    |
|-----------|------------|
| A-1001    | 250000.00  |
| A-1002    | 180000.00  |
| A-1003    | 180000.00  |
| A-1004    | 95000.00   |
| A-1005    | 40000.00   |

**Expected output:** every account with three ranking columns; A-1002 and A-1003 tie, and you must explain the values for A-1004 under each function.

<details><summary>Solution</summary>

```sql
SELECT
  account_id,
  balance,
  ROW_NUMBER() OVER (ORDER BY balance DESC) AS row_num,     -- 1,2,3,4,5 (tie broken arbitrarily)
  RANK()       OVER (ORDER BY balance DESC) AS rnk,         -- 1,2,2,4,5 (gap after tie)
  DENSE_RANK() OVER (ORDER BY balance DESC) AS dense_rnk    -- 1,2,2,3,4 (no gap)
FROM month_end_balances
ORDER BY balance DESC;
```

**Why it works / what's being tested.**
- `ROW_NUMBER` always emits unique, consecutive integers; when `ORDER BY` values tie, the assignment among the tied rows is **non-deterministic** unless you add a tiebreaker column (`ORDER BY balance DESC, account_id`). Say this out loud — interviewers love it.
- `RANK` gives tied rows the same rank and then **skips** (A-1004 gets 4).
- `DENSE_RANK` gives tied rows the same rank with **no gap** (A-1004 gets 3).

**Common wrong answers:** claiming `RANK` and `DENSE_RANK` are interchangeable; forgetting that "top 3 by RANK" can return more than 3 rows when there are ties; not mentioning `ROW_NUMBER`'s non-determinism on ties.
</details>

---

## Problem 2 — Deduplicate a double-loaded feed (Easy)

**Asked as:** "An Airflow retry loaded yesterday's journal file twice, so `journal_entries` has duplicate `entry_id`s with different `load_ts`. Return exactly one row per `entry_id`, keeping the most recently loaded copy."

Table `journal_entries`:

| entry_id | account_id | amount  | entry_date | load_ts                 |
|----------|-----------|---------|------------|-------------------------|
| JE-1     | A-1001    | 500.00  | 2026-07-10 | 2026-07-11 02:00:00 UTC |
| JE-1     | A-1001    | 500.00  | 2026-07-10 | 2026-07-11 02:35:00 UTC |
| JE-2     | A-1002    | -200.00 | 2026-07-10 | 2026-07-11 02:00:00 UTC |
| JE-3     | A-1003    | 75.00   | 2026-07-10 | 2026-07-11 02:00:00 UTC |
| JE-3     | A-1003    | 75.00   | 2026-07-11 02:35:00 UTC | *(dup)*   |

**Expected output:** JE-1, JE-2, JE-3 — one row each, JE-1 and JE-3 being the 02:35 copies.

<details><summary>Solution</summary>

```sql
SELECT *
FROM journal_entries
QUALIFY ROW_NUMBER() OVER (
          PARTITION BY entry_id
          ORDER BY load_ts DESC
        ) = 1;
```

Equivalent portable form (works in engines without `QUALIFY`):

```sql
SELECT * EXCEPT(rn)
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY entry_id ORDER BY load_ts DESC) AS rn
  FROM journal_entries
)
WHERE rn = 1;
```

**Why it works / what's being tested.** This is *the* canonical dedup pattern: partition by the business key, order by a recency column, keep row 1. The interviewer is testing whether you (a) pick a deterministic ordering (add another tiebreaker if `load_ts` can tie), (b) know window functions can't be filtered in `WHERE`, so you need `QUALIFY` or a wrapper.

**Common wrong answers:** `SELECT DISTINCT *` (only removes rows identical in *every* column — the differing `load_ts` defeats it); `GROUP BY entry_id` with `MAX()` on every column (mixes values from different duplicate rows — Frankenstein rows); `WHERE ROW_NUMBER() ... = 1` directly (syntax error — window functions aren't allowed in `WHERE`).
</details>

---

## Problem 3 — Running balance per account: ROWS vs RANGE (Medium)

**Asked as:** "Given signed transaction amounts, produce a running balance per account ordered by transaction time. Then explain what changes if two transactions share the same timestamp."

Table `transactions`:

| txn_id | account_id | txn_ts                  | amount   |
|--------|-----------|-------------------------|----------|
| T1     | A-1001    | 2026-07-01 09:00:00 UTC | 1000.00  |
| T2     | A-1001    | 2026-07-01 12:00:00 UTC | -250.00  |
| T3     | A-1001    | 2026-07-01 12:00:00 UTC | -100.00  |
| T4     | A-1001    | 2026-07-02 08:00:00 UTC | 500.00   |
| T5     | A-1002    | 2026-07-01 10:00:00 UTC | 300.00   |

**Expected output:** each transaction plus `running_balance`; for A-1001 the strict row-by-row balances are 1000, 750, 650, 1150.

<details><summary>Solution</summary>

```sql
SELECT
  txn_id,
  account_id,
  txn_ts,
  amount,
  SUM(amount) OVER (
    PARTITION BY account_id
    ORDER BY txn_ts, txn_id                 -- txn_id breaks timestamp ties deterministically
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS running_balance
FROM transactions;
```

**Why it works / what's being tested.**
- If you omit the frame clause, the default with `ORDER BY` is **`RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`**. Under `RANGE`, T2 and T3 are **peers** (same `txn_ts`), so *both* would show 650 — the sum through the *end of the tie group* — not 750 then 650. That is usually not what a ledger statement wants.
- `ROWS` advances one physical row at a time; combined with the `txn_id` tiebreaker it is fully deterministic. Being able to articulate "default = RANGE, peers collapse, so I write ROWS explicitly with a tiebreaker" is exactly what a senior interviewer wants to hear.

**Common wrong answers:** relying on the default frame and being surprised by tie behavior; a correlated subquery `(SELECT SUM(...) WHERE t2.txn_ts <= t.txn_ts)` — correct-ish but O(n²) and it silently has the same tie problem; forgetting `PARTITION BY account_id` so balances bleed across accounts.
</details>

---

## Problem 4 — Day-over-day change with LAG (Medium)

**Asked as:** "From daily closing balances, show each day's change vs the prior day, absolute and percentage. First day per account should show NULL change, not an error or zero."

Table `daily_balances`:

| account_id | bal_date   | closing_balance |
|-----------|------------|-----------------|
| A-1001    | 2026-07-01 | 1000.00         |
| A-1001    | 2026-07-02 | 1150.00         |
| A-1001    | 2026-07-03 | 900.00          |
| A-1002    | 2026-07-01 | 0.00            |
| A-1002    | 2026-07-02 | 500.00          |

**Expected output:** per account/day: `prev_balance`, `change_amt`, `change_pct` (NULL on the first day; NULL pct when the prior balance is 0).

<details><summary>Solution</summary>

```sql
SELECT
  account_id,
  bal_date,
  closing_balance,
  LAG(closing_balance) OVER w AS prev_balance,
  closing_balance
    - LAG(closing_balance) OVER w AS change_amt,
  SAFE_DIVIDE(
    closing_balance - LAG(closing_balance) OVER w,
    LAG(closing_balance) OVER w
  ) AS change_pct
FROM daily_balances
WINDOW w AS (PARTITION BY account_id ORDER BY bal_date);
```

**Why it works / what's being tested.**
- `LAG(x)` returns NULL for the first row of each partition (no default supplied), and NULL propagates through the subtraction — the correct semantics for "no prior day." `LAG(x, 1, 0)` would fabricate a change equal to the whole balance; know when each is right.
- `SAFE_DIVIDE` returns NULL instead of a division-by-zero error when the prior balance is 0 — A-1002 goes 0 → 500, an infinite % change; NULL is the honest answer.
- The named `WINDOW` clause avoids repeating the OVER spec — a nice fluency signal.
- Bonus point: `LAG` looks back one *row*, not one *calendar day*. If dates have gaps, "prior row" ≠ "yesterday" — say so (that's Problem 5).

**Common wrong answers:** a self-join on `bal_date = DATE_SUB(bal_date, INTERVAL 1 DAY)` (works, but drops the first day entirely and misbehaves on gaps differently); plain `/` division that errors on zero; putting `ORDER BY bal_date DESC` and silently computing next-day change.
</details>

---

## Problem 5 — Detecting gaps in transaction dates (Medium)

**Asked as:** "Your daily ledger feed should post at least one entry every day per account. Find the gaps: for each account, report each stretch of missing days with its start date, end date, and length."

Table `transactions` (distinct posting dates shown):

| account_id | txn_date   |
|-----------|------------|
| A-1001    | 2026-07-01 |
| A-1001    | 2026-07-02 |
| A-1001    | 2026-07-05 |
| A-1001    | 2026-07-06 |
| A-1001    | 2026-07-10 |

**Expected output:**

| account_id | gap_start  | gap_end    | gap_days |
|-----------|------------|------------|----------|
| A-1001    | 2026-07-03 | 2026-07-04 | 2        |
| A-1001    | 2026-07-07 | 2026-07-09 | 3        |

<details><summary>Solution</summary>

```sql
WITH dated AS (
  SELECT DISTINCT account_id, txn_date
  FROM transactions
),
with_prev AS (
  SELECT
    account_id,
    txn_date,
    LAG(txn_date) OVER (PARTITION BY account_id ORDER BY txn_date) AS prev_date
  FROM dated
)
SELECT
  account_id,
  DATE_ADD(prev_date, INTERVAL 1 DAY)              AS gap_start,
  DATE_SUB(txn_date, INTERVAL 1 DAY)               AS gap_end,
  DATE_DIFF(txn_date, prev_date, DAY) - 1          AS gap_days
FROM with_prev
WHERE DATE_DIFF(txn_date, prev_date, DAY) > 1
ORDER BY account_id, gap_start;
```

**Why it works / what's being tested.** A gap exists exactly where consecutive posting dates are more than 1 day apart; `LAG` pairs each date with its predecessor, and simple date arithmetic converts the pair into the missing interval. The `DISTINCT` up front matters — multiple entries on the same day would otherwise create `DATE_DIFF = 0` pairs. This is the light version of gaps-and-islands; Problem 10 does the full version.

**Common wrong answers:** forgetting `PARTITION BY account_id` (gaps computed across accounts); reporting the *bounding* transaction dates instead of the missing range (off-by-one on both ends); `DATE_DIFF(prev_date, txn_date, DAY)` with arguments reversed (BigQuery's `DATE_DIFF` is `(later, earlier, part)` for a positive result).
</details>

---

## Problem 6 — NTILE: bucket accounts into quartiles (Easy)

**Asked as:** "Segment accounts into 4 equal-ish buckets by total July transaction volume so the ops team can prioritize reconciliation on the top quartile."

Table `july_volume`:

| account_id | total_volume |
|-----------|--------------|
| A-1001    | 900000       |
| A-1002    | 750000       |
| A-1003    | 500000       |
| A-1004    | 210000       |
| A-1005    | 90000        |
| A-1006    | 20000        |

**Expected output:** each account with `volume_quartile` 1–4, quartile 1 = highest volume. With 6 rows and 4 buckets, buckets get sizes 2, 2, 1, 1.

<details><summary>Solution</summary>

```sql
SELECT
  account_id,
  total_volume,
  NTILE(4) OVER (ORDER BY total_volume DESC) AS volume_quartile
FROM july_volume;
```

**Why it works / what's being tested.** `NTILE(n)` deals rows into `n` buckets as evenly as possible; when the count doesn't divide evenly, **earlier buckets get the extra rows** (6 rows / 4 buckets → 2,2,1,1). Contrast with `PERCENT_RANK`/`CUME_DIST`, which return a *relative position* rather than a bucket number, and note that `NTILE` can split rows with **equal values into different buckets** — if that's unacceptable, bucket on a computed rank or use explicit value thresholds.

**Common wrong answers:** assuming equal bucket sizes always; assuming ties are kept together; writing `NTILE(4) OVER (PARTITION BY account_id ...)` (a partition per account = every account is in bucket 1).
</details>

---

## Problem 7 — FIRST_VALUE / LAST_VALUE and the frame pitfall (Medium)

**Asked as:** "For each account and month, add columns for the opening balance (first day's) and closing balance (last day's) to every daily row. A junior wrote `LAST_VALUE(balance) OVER (PARTITION BY account, month ORDER BY day)` and got wrong numbers — why?"

Table `daily_balances`:

| account_id | bal_date   | balance |
|-----------|------------|---------|
| A-1001    | 2026-07-01 | 100     |
| A-1001    | 2026-07-02 | 180     |
| A-1001    | 2026-07-03 | 150     |

**Expected output:** each daily row with `opening_balance = 100` and `closing_balance = 150` on *every* row of the month.

<details><summary>Solution</summary>

```sql
SELECT
  account_id,
  bal_date,
  balance,
  FIRST_VALUE(balance) OVER (
    PARTITION BY account_id, DATE_TRUNC(bal_date, MONTH)
    ORDER BY bal_date
  ) AS opening_balance,
  LAST_VALUE(balance) OVER (
    PARTITION BY account_id, DATE_TRUNC(bal_date, MONTH)
    ORDER BY bal_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING   -- the fix
  ) AS closing_balance
FROM daily_balances;
```

**Why it works / what's being tested.** With `ORDER BY` present, the default frame ends at `CURRENT ROW`. `FIRST_VALUE` doesn't care (the first row is always inside the frame), but `LAST_VALUE` under the default frame returns... **the current row's own value** — the junior's column just echoed `balance`. Extending the frame to `UNBOUNDED FOLLOWING` makes the whole partition visible. Alternative idiom the interviewer will accept: `FIRST_VALUE(balance) OVER (... ORDER BY bal_date DESC)` for the closing balance — same result, no frame clause needed.

**Common wrong answers:** not knowing why `LAST_VALUE` "doesn't work" (this is a top-5 window-function interview question); using `MAX(bal_date)` + a join when a frame fix is one line; using `RANGE` instead of `ROWS` for the fix (here it happens to work, but `ROWS` is the precise statement of intent).
</details>

---

## Problem 8 — Top-N per group (Medium)

**Asked as:** "Show the 3 largest transactions by absolute amount for each account each month. Discuss what happens on ties."

Table `transactions`: `txn_id, account_id, txn_date, amount` (assume plenty of rows).

**Expected output:** at most 3 rows per (account, month) with `ROW_NUMBER`; possibly more with `RANK` on ties — you should be able to defend a choice.

<details><summary>Solution</summary>

```sql
SELECT
  account_id,
  DATE_TRUNC(txn_date, MONTH) AS txn_month,
  txn_id,
  amount
FROM transactions
QUALIFY ROW_NUMBER() OVER (
          PARTITION BY account_id, DATE_TRUNC(txn_date, MONTH)
          ORDER BY ABS(amount) DESC, txn_id      -- deterministic
        ) <= 3
ORDER BY account_id, txn_month, ABS(amount) DESC;
```

**Why it works / what's being tested.** Top-N per group is the bread-and-butter window pattern: number rows within each partition by the ranking criterion, keep `<= N`. The design decision to articulate: `ROW_NUMBER` gives *exactly* 3 (ties broken arbitrarily unless you add a tiebreaker); `RANK` gives *at least* 3 when row 3 ties with row 4 (often the right call for a finance control report — you don't want to silently drop a tied-for-third transaction); `DENSE_RANK <= 3` returns everything in the top three *distinct amounts*, which can be many rows.

**Common wrong answers:** `GROUP BY account_id ... ORDER BY amount LIMIT 3` (LIMIT applies to the whole result, not per group); correlated subquery counting larger rows (quadratic); forgetting `DATE_TRUNC` and partitioning only by account.
</details>

---

## Problem 9 — Gaps and islands: longest consecutive active streak (Hard)

**Asked as:** "An account is 'active' on a day if it posts at least one transaction. For each account, find its longest streak of consecutive active days, with the streak's start and end dates."

Table `transactions` (distinct active dates):

| account_id | txn_date   |
|-----------|------------|
| A-1001    | 2026-07-01 |
| A-1001    | 2026-07-02 |
| A-1001    | 2026-07-03 |
| A-1001    | 2026-07-06 |
| A-1001    | 2026-07-07 |
| A-1002    | 2026-07-04 |

**Expected output:**

| account_id | streak_start | streak_end | streak_days |
|-----------|--------------|------------|-------------|
| A-1001    | 2026-07-01   | 2026-07-03 | 3           |
| A-1002    | 2026-07-04   | 2026-07-04 | 1           |

<details><summary>Solution</summary>

```sql
WITH active_days AS (
  SELECT DISTINCT account_id, txn_date
  FROM transactions
),
grouped AS (
  SELECT
    account_id,
    txn_date,
    -- Consecutive dates minus a consecutive row counter = a constant per island
    DATE_SUB(
      txn_date,
      INTERVAL ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY txn_date) DAY
    ) AS island_key
  FROM active_days
),
islands AS (
  SELECT
    account_id,
    island_key,
    MIN(txn_date) AS streak_start,
    MAX(txn_date) AS streak_end,
    COUNT(*)      AS streak_days
  FROM grouped
  GROUP BY account_id, island_key
)
SELECT account_id, streak_start, streak_end, streak_days
FROM islands
QUALIFY ROW_NUMBER() OVER (
          PARTITION BY account_id
          ORDER BY streak_days DESC, streak_start
        ) = 1
ORDER BY account_id;
```

**Why it works / what's being tested.** The classic trick: along a run of consecutive dates, the date advances by 1 per row and `ROW_NUMBER` advances by 1 per row, so `date − row_number` is **constant within an island** and jumps whenever a day is skipped. Group by that constant and each group *is* one streak. The `DISTINCT` in step 1 is load-bearing — duplicate dates would advance `ROW_NUMBER` without advancing the date and shatter the islands. Interviewers use this to separate people who've internalized window functions from people who've memorized `LAG`.

**Common wrong answers:** trying to chain `LAG` comparisons without the second pass (LAG tells you *where* islands break, but you still need a running `SUM` of break flags — that variant is fine too, just more steps); forgetting `DISTINCT`; grouping by `island_key` across accounts without `account_id`.
</details>

---

## Problem 10 — Consecutive missing feed files (Hard)

**Asked as:** "Your Composer DAG expects one ledger file per source system per calendar day in `feed_files`. For July 1–10, list each run of consecutive missing days per source — the SRE team wants outage windows, not individual dates."

Table `feed_files`:

| source_system | file_date  | file_name              |
|---------------|------------|------------------------|
| GL-EU         | 2026-07-01 | gl_eu_20260701.csv.zip |
| GL-EU         | 2026-07-02 | gl_eu_20260702.csv.zip |
| GL-EU         | 2026-07-05 | gl_eu_20260705.csv.zip |
| GL-EU         | 2026-07-08 | gl_eu_20260708.csv.zip |
| GL-EU         | 2026-07-09 | gl_eu_20260709.csv.zip |
| GL-EU         | 2026-07-10 | gl_eu_20260710.csv.zip |

**Expected output:**

| source_system | missing_from | missing_to | days_missing |
|---------------|--------------|------------|--------------|
| GL-EU         | 2026-07-03   | 2026-07-04 | 2            |
| GL-EU         | 2026-07-06   | 2026-07-07 | 2            |

<details><summary>Solution</summary>

```sql
WITH calendar AS (
  SELECT source_system, cal_date
  FROM (SELECT DISTINCT source_system FROM feed_files)
  CROSS JOIN UNNEST(GENERATE_DATE_ARRAY('2026-07-01', '2026-07-10')) AS cal_date
),
missing AS (
  SELECT c.source_system, c.cal_date
  FROM calendar AS c
  LEFT JOIN feed_files AS f
    ON f.source_system = c.source_system
   AND f.file_date     = c.cal_date
  WHERE f.file_date IS NULL              -- anti-join: expected but not received
),
islands AS (
  SELECT
    source_system,
    cal_date,
    DATE_SUB(
      cal_date,
      INTERVAL ROW_NUMBER() OVER (PARTITION BY source_system ORDER BY cal_date) DAY
    ) AS island_key
  FROM missing
)
SELECT
  source_system,
  MIN(cal_date) AS missing_from,
  MAX(cal_date) AS missing_to,
  COUNT(*)      AS days_missing
FROM islands
GROUP BY source_system, island_key
ORDER BY source_system, missing_from;
```

**Why it works / what's being tested.** You cannot find missing rows by looking only at rows that exist — you must **generate the expected universe first**. `GENERATE_DATE_ARRAY` + `UNNEST` builds the calendar, the LEFT JOIN ... IS NULL anti-join isolates the missing days, and then the same islands trick from Problem 9 compresses them into runs. This mirrors a real data-engineering control: "did every expected file arrive?" — great to tie back to your GCS/Composer pipeline story.

**Common wrong answers:** applying `LAG` directly to `feed_files` and reporting *gaps between received files* — correct here but breaks the moment the *first or last* expected day is missing (no bounding row to LAG against; the calendar approach has no such edge case); hard-coding the calendar as literal rows; cross-joining the calendar without per-source pairing.
</details>

---

## Problem 11 — 7-day moving average (Medium)

**Asked as:** "Compute a 7-day moving average of total daily posting amount. Then: what's wrong with `ROWS BETWEEN 6 PRECEDING AND CURRENT ROW` if some days have no postings?"

Table `daily_postings` (pre-aggregated, but weekend days absent):

| post_date  | total_amount |
|------------|--------------|
| 2026-07-06 | 1000         |
| 2026-07-07 | 1200         |
| 2026-07-08 | 900          |
| 2026-07-09 | 1100         |
| 2026-07-10 | 1300         |
| 2026-07-13 | 800          |

**Expected output:** each day with `ma_7d`. On 2026-07-13 the window must cover **calendar** days 07-07…07-13 (five rows present), not the last 7 physical rows.

<details><summary>Solution</summary>

```sql
-- Version 1: last 7 physical rows — only correct if every calendar day has a row
SELECT
  post_date,
  total_amount,
  AVG(total_amount) OVER (
    ORDER BY post_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS ma_7row
FROM daily_postings;

-- Version 2: true 7 *calendar* days — RANGE over a numeric day key
SELECT
  post_date,
  total_amount,
  AVG(total_amount) OVER (
    ORDER BY UNIX_DATE(post_date)
    RANGE BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS ma_7day
FROM daily_postings;
```

**Why it works / what's being tested.**
- `ROWS 6 PRECEDING` counts *rows*. With weekend rows absent, on 07-13 it would reach back 7 business days — a longer calendar span than intended. `RANGE 6 PRECEDING` bounds the frame by *ORDER-BY value distance* instead.
- BigQuery requires a **numeric** ordering key for `RANGE` with an offset, so convert the date with `UNIX_DATE()` (days since 1970-01-01) — a distinctly BigQuery detail worth volunteering.
- Third acceptable design: densify with a calendar (as in Problem 10, treating missing days as 0) and then `ROWS` is fine — the right choice depends on whether a no-posting day should count as 0 in the average or be excluded. Saying that trade-off out loud is the senior answer.

**Common wrong answers:** `ROWS` on sparse dates without noticing; `RANGE BETWEEN INTERVAL 6 DAY PRECEDING` (not BigQuery syntax); averaging `AVG(total_amount)` after grouping by week (that's a tumbling window, not a moving one).
</details>

---

## Problem 12 — Percent of total (Medium)

**Asked as:** "For the monthly expense report, show each cost centre's spend and its share of the month's total, without a self-join."

Table `expenses`:

| expense_month | cost_centre | spend  |
|---------------|-------------|--------|
| 2026-06-01    | CC-OPS      | 40000  |
| 2026-06-01    | CC-TECH     | 35000  |
| 2026-06-01    | CC-RISK     | 25000  |
| 2026-07-01    | CC-OPS      | 50000  |
| 2026-07-01    | CC-TECH     | 50000  |

**Expected output:** each row plus `pct_of_month` (e.g., CC-OPS June = 0.40; July rows = 0.50 each).

<details><summary>Solution</summary>

```sql
SELECT
  expense_month,
  cost_centre,
  spend,
  SAFE_DIVIDE(
    spend,
    SUM(spend) OVER (PARTITION BY expense_month)
  ) AS pct_of_month
FROM expenses
ORDER BY expense_month, pct_of_month DESC;
```

**Why it works / what's being tested.** `SUM(spend) OVER (PARTITION BY expense_month)` — **no `ORDER BY`** — makes the frame the *entire partition*, so every row sees the month's grand total; divide and done. Adding `ORDER BY` inside that OVER would turn it into a running total (default frame kicks in) and silently produce garbage percentages — the exact bug interviewers fish for. `SAFE_DIVIDE` guards a zero-total month. Note for people coming from Oracle/Snowflake: BigQuery has **no `RATIO_TO_REPORT`**; this is the idiom.

**Common wrong answers:** joining to a `GROUP BY` subquery of totals (works, but the question said windowed); `SUM(spend) OVER (PARTITION BY expense_month ORDER BY spend)` (running total bug above); computing percent of *grand* total by omitting the partition when the question asked per month.
</details>

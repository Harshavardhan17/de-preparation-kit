# Cross-Dialect SQL — BigQuery vs MySQL vs SQL Server

Her résumé lists **BigQuery, MySQL, and SQL Server**, but the other 32 practice
problems are BigQuery-only. Interviewers who see three databases *will* ask her
to write the same query in whichever one their shop uses — or, more sneakily,
ask "how would this differ in SQL Server?" This file drills the handful of
differences that actually trip people, using the same ledger framing.

**The core mental model:** the *logic* (window functions, joins, aggregation)
is nearly identical across all three modern engines. What differs is a small,
predictable surface: **row-limiting syntax, the `QUALIFY` shortcut, string/date
functions, and NULL helpers.** Learn those five and she can move between
dialects on demand — which is exactly the fluency the résumé implies.

## The cheat-sheet (memorize this table)

| Need | BigQuery | MySQL (8.0+) | SQL Server (T-SQL) |
|---|---|---|---|
| Limit rows | `LIMIT n` / `LIMIT n OFFSET m` | `LIMIT n` / `LIMIT m, n` | `TOP n` or `OFFSET m ROWS FETCH NEXT n ROWS ONLY` |
| Filter on a window fn | `QUALIFY` | ❌ none — wrap in subquery/CTE | ❌ none — wrap in subquery/CTE |
| String concat (agg) | `STRING_AGG(x, ',')` | `GROUP_CONCAT(x SEPARATOR ',')` | `STRING_AGG(x, ',')` (2017+) |
| NULL → default | `IFNULL` / `COALESCE` | `IFNULL` / `COALESCE` | `ISNULL` / `COALESCE` |
| Days between dates | `DATE_DIFF(d2, d1, DAY)` | `DATEDIFF(d2, d1)` | `DATEDIFF(DAY, d1, d2)` |
| Safe cast | `SAFE_CAST(x AS INT64)` | `CAST` (no safe variant) | `TRY_CAST(x AS INT)` |
| Current date | `CURRENT_DATE()` | `CURDATE()` | `CAST(GETDATE() AS DATE)` |
| Conditional count | `COUNTIF(cond)` | `SUM(cond)` (bool→1/0) | `SUM(CASE WHEN cond THEN 1 ELSE 0 END)` |

Two traps worth saying out loud in an interview:
- **`DATEDIFF` argument order is reversed** between MySQL (`end, start`) and SQL
  Server (`DAY, start, end`) — and SQL Server takes a *unit* as the first arg.
  Getting this right unprompted signals real multi-DB experience.
- **`QUALIFY` is BigQuery-only** (also Snowflake/DuckDB). In MySQL/SQL Server the
  identical logic needs a CTE — same window function, one extra wrapper.

---

## Problem 1 — Second-highest posting amount per account
**Difficulty: Medium.** *Asked as:* "For each account, find the second-largest
single posting amount. Now write it for SQL Server instead of BigQuery."

Table `postings(account_id, posting_id, amount)`.

<details><summary>Solution</summary>

**BigQuery** — `QUALIFY` filters the window function directly:
```sql
SELECT account_id, posting_id, amount
FROM postings
QUALIFY DENSE_RANK() OVER (PARTITION BY account_id ORDER BY amount DESC) = 2;
```

**MySQL 8.0+** — no `QUALIFY`, so wrap it:
```sql
WITH ranked AS (
  SELECT account_id, posting_id, amount,
         DENSE_RANK() OVER (PARTITION BY account_id ORDER BY amount DESC) AS rnk
  FROM postings
)
SELECT account_id, posting_id, amount FROM ranked WHERE rnk = 2;
```

**SQL Server** — identical to MySQL (CTE + window); T-SQL has had window
functions since 2012:
```sql
WITH ranked AS (
  SELECT account_id, posting_id, amount,
         DENSE_RANK() OVER (PARTITION BY account_id ORDER BY amount DESC) AS rnk
  FROM postings
)
SELECT account_id, posting_id, amount FROM ranked WHERE rnk = 2;
```

**What the interviewer is testing:** do you know `QUALIFY` is a BigQuery
convenience, not standard SQL? The CTE form is the portable answer. Use
`DENSE_RANK` (not `ROW_NUMBER`) so ties for first are handled correctly — a
common wrong answer returns nothing when the top amount is tied.
</details>

---

## Problem 2 — Keep only the latest version of each journal line
**Difficulty: Medium.** *Asked as:* "Upstream re-sends corrected rows. Keep the
newest per `(journal_id, line_no)`. Show me MySQL."

Table `journal_lines(journal_id, line_no, amount, updated_at)`.

<details><summary>Solution</summary>

**BigQuery:**
```sql
SELECT * FROM journal_lines
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY journal_id, line_no ORDER BY updated_at DESC) = 1;
```

**MySQL / SQL Server** (same portable shape):
```sql
WITH latest AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY journal_id, line_no
                       ORDER BY updated_at DESC) AS rn
  FROM journal_lines
)
SELECT journal_id, line_no, amount, updated_at
FROM latest WHERE rn = 1;
```

**SQL Server bonus — physically delete the older duplicates** (they love this
one, because the CTE is *updatable* in T-SQL):
```sql
WITH latest AS (
  SELECT ROW_NUMBER() OVER (PARTITION BY journal_id, line_no
                            ORDER BY updated_at DESC) AS rn
  FROM journal_lines
)
DELETE FROM latest WHERE rn > 1;
```

**Testing:** the keep-latest-per-key pattern is *the* dedup question. Say
explicitly why `ROW_NUMBER` (exactly one row) not `RANK`/`DENSE_RANK` (ties
would keep several). The updatable-CTE `DELETE` is a T-SQL signature move.
</details>

---

## Problem 3 — Settlement lag, and the DATEDIFF trap
**Difficulty: Easy–Medium.** *Asked as:* "Compute days between posting date and
settlement date. Careful — write it correctly in MySQL and in SQL Server."

Table `trades(trade_id, posting_date, settle_date)`.

<details><summary>Solution</summary>

**BigQuery** — explicit unit, `(end, start)` order:
```sql
SELECT trade_id, DATE_DIFF(settle_date, posting_date, DAY) AS settle_lag
FROM trades;
```

**MySQL** — `DATEDIFF(end, start)`, returns days, no unit arg:
```sql
SELECT trade_id, DATEDIFF(settle_date, posting_date) AS settle_lag
FROM trades;
```

**SQL Server** — unit FIRST, then `(start, end)` — the reverse of MySQL:
```sql
SELECT trade_id, DATEDIFF(DAY, posting_date, settle_date) AS settle_lag
FROM trades;
```

**Testing:** pure dialect awareness. The three engines all spell "days between"
differently, and the MySQL-vs-SQL-Server argument-order flip is the classic
"gotcha" — `DATEDIFF(posting_date, settle_date)` in SQL Server isn't even valid
(first arg must be a datepart). Calling that out unprompted is a strong signal.
</details>

---

## Problem 4 — Debit/credit pivot per currency, with an account list
**Difficulty: Medium.** *Asked as:* "Per currency, show total debits, total
credits, count of postings, and a comma-list of distinct accounts. Do it in
BigQuery, then tell me what changes in MySQL and SQL Server."

Table `postings(currency, account_id, debit_amount, credit_amount)`.

<details><summary>Solution</summary>

**BigQuery:**
```sql
SELECT
  currency,
  SUM(debit_amount)                         AS total_debits,
  SUM(credit_amount)                        AS total_credits,
  COUNTIF(debit_amount > 0)                 AS debit_postings,
  STRING_AGG(DISTINCT account_id, ',')      AS accounts,
  IFNULL(SUM(debit_amount), 0)              AS debits_nonnull
FROM postings
GROUP BY currency;
```

**MySQL** — `GROUP_CONCAT` instead of `STRING_AGG`; no `COUNTIF` (a boolean sums
as 1/0, which is a neat MySQL-ism):
```sql
SELECT
  currency,
  SUM(debit_amount)                         AS total_debits,
  SUM(credit_amount)                        AS total_credits,
  SUM(debit_amount > 0)                     AS debit_postings,
  GROUP_CONCAT(DISTINCT account_id SEPARATOR ',') AS accounts,
  IFNULL(SUM(debit_amount), 0)              AS debits_nonnull
FROM postings
GROUP BY currency;
```

**SQL Server** — `STRING_AGG` (2017+) but it takes no `DISTINCT`, so dedupe
first; no `COUNTIF`, use `SUM(CASE …)`; `ISNULL` not `IFNULL`:
```sql
SELECT
  p.currency,
  SUM(p.debit_amount)                                   AS total_debits,
  SUM(p.credit_amount)                                  AS total_credits,
  SUM(CASE WHEN p.debit_amount > 0 THEN 1 ELSE 0 END)   AS debit_postings,
  (SELECT STRING_AGG(account_id, ',')
   FROM (SELECT DISTINCT account_id FROM postings d
         WHERE d.currency = p.currency) x)              AS accounts,
  ISNULL(SUM(p.debit_amount), 0)                        AS debits_nonnull
FROM postings p
GROUP BY p.currency;
```

**Testing:** four dialect surfaces at once — conditional count, string
aggregation, `DISTINCT`-in-aggregation support, and the NULL helper. The honest,
senior answer is "the aggregation logic is the same; I'd swap `COUNTIF`→
`SUM(CASE…)`, `STRING_AGG`→`GROUP_CONCAT` on MySQL, and handle that SQL Server's
`STRING_AGG` has no `DISTINCT`." That sentence alone demonstrates the
three-database fluency the résumé claims.
</details>

---

## How to practise this
Pick any 5 problems from `01`–`03`, and for each, write the BigQuery answer,
then re-write it in MySQL and SQL Server from the cheat-sheet — out loud,
naming what changed. Twenty minutes of this and the dialect questions become
free points instead of stumbles. She does not need deep MySQL/T-SQL admin
knowledge for a GCP DE role; she needs to *translate query logic on demand*,
which is what this drills.

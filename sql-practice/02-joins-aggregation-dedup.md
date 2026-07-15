# Joins, Aggregation & Deduplication — BigQuery Standard SQL

## Core mental model

A **join** is row matching: for each row on one side, find the rows on the other side whose join predicate evaluates to TRUE. Three consequences of that definition drive almost every interview trap. First, **NULL never matches anything** — `NULL = NULL` is not TRUE but UNKNOWN — so NULLable join keys silently drop rows in inner joins and poison `NOT IN`. Second, join **cardinality multiplies**: if a key matches 1 row on the left and 3 on the right you get 3 output rows, and if it matches 2 and 3 you get 6 — this **fan-out** is why amounts "mysteriously double" after adding a join, and why you aggregate *before* joining, not after. Third, an **outer join** (`LEFT`/`RIGHT`/`FULL`) preserves unmatched rows by padding the other side with NULLs — which is exactly the raw material for **anti-joins** ("in A but not B") and **reconciliation** ("in A, in B, or in both with different values").

On the aggregation side, keep the logical evaluation order straight: `FROM/JOIN → WHERE → GROUP BY → HAVING → SELECT (window functions) → QUALIFY → ORDER BY → LIMIT`. **`WHERE` filters rows before grouping; `HAVING` filters groups after aggregation.** Anything expressible in `WHERE` should stay in `WHERE` — it shrinks the data earlier and, in BigQuery, is where partition pruning happens. BigQuery's **conditional aggregation** primitives — `COUNTIF(cond)` and `SUM(IF(cond, x, 0))` — let you pivot categorical rows into columns in a single scan, which is how you build debit/credit or per-status summary layouts without multiple passes.

Scenario throughout: general-ledger data at a bank — a source system feed (`src_*`), the warehouse copy (`gl_*`), and the daily reconciliation between them.

---

## Problem 1 — Self-join: matching debit and credit legs (Medium)

**Asked as:** "Every journal batch should balance: each debit leg has a matching credit leg in the same batch with the opposite amount. Find batches where a debit has no matching credit."

Table `journal_lines`:

| line_id | batch_id | side  | amount  |
|---------|----------|-------|---------|
| L1      | B-100    | DEBIT | 500.00  |
| L2      | B-100    | CREDIT| 500.00  |
| L3      | B-101    | DEBIT | 750.00  |
| L4      | B-101    | CREDIT| 700.00  |
| L5      | B-102    | DEBIT | 200.00  |
| L6      | B-102    | CREDIT| 200.00  |

**Expected output:** L3 (batch B-101) — a debit with no equal-amount credit in its batch.

<details><summary>Solution</summary>

```sql
SELECT d.line_id, d.batch_id, d.amount
FROM journal_lines AS d
LEFT JOIN journal_lines AS c
  ON  c.batch_id = d.batch_id
  AND c.side     = 'CREDIT'
  AND c.amount   = d.amount
WHERE d.side = 'DEBIT'
  AND c.line_id IS NULL;
```

**Why it works / what's being tested.** A **self-join** treats one table as two logical tables via aliases; here the same table plays "debits" and "candidate credits." The interviewer is testing (a) that you alias both sides and never write the unqualified column, (b) that you push role filters into the right place — `d.side = 'DEBIT'` in `WHERE` (it filters the preserved side) but `c.side = 'CREDIT'` in the **ON** clause (in `WHERE` it would turn the LEFT JOIN back into an INNER join, because NULL-padded rows fail the predicate), and (c) awareness that amount-equality matching is 1-to-many if a batch has two 500.00 credits — for real pairing you'd match on a shared reference and dedup.

**Common wrong answers:** putting `c.side = 'CREDIT'` in `WHERE` (kills the unmatched rows you're looking for); INNER self-join then "find what's missing" (you can't — the missing rows are gone); comparing `c.amount = -d.amount` when the schema stores both sides positive with a `side` flag (read the schema first, say your sign assumption out loud).
</details>

---

## Problem 2 — Anti-join three ways, and the NOT IN null trap (Medium)

**Asked as:** "List source-feed entries that never made it into the warehouse table. Show me three ways, and tell me which one you'd never use and why."

Tables:

`src_entries` — | entry_id | amount | — rows: E1, E2, E3, E4
`gl_entries` — | entry_id | amount | — rows: E1, E3, **NULL** *(a bad load wrote a NULL key)*

**Expected output:** E2 and E4.

<details><summary>Solution</summary>

```sql
-- 1) NOT EXISTS — the recommended default
SELECT s.*
FROM src_entries AS s
WHERE NOT EXISTS (
  SELECT 1 FROM gl_entries AS g WHERE g.entry_id = s.entry_id
);

-- 2) LEFT JOIN ... IS NULL — equivalent result, join-shaped
SELECT s.*
FROM src_entries AS s
LEFT JOIN gl_entries AS g USING (entry_id)
WHERE g.entry_id IS NULL;

-- 3) NOT IN — the trap
SELECT s.*
FROM src_entries AS s
WHERE s.entry_id NOT IN (SELECT entry_id FROM gl_entries);
-- Returns ZERO rows here, because gl_entries contains a NULL entry_id.
```

**Why it works / what's being tested.**
- `NOT IN (a, b, NULL)` expands to `x <> a AND x <> b AND x <> NULL`. The last term is **UNKNOWN**, so the whole conjunction can never be TRUE — one NULL in the subquery and the query returns **no rows at all**, silently. This is the trap; naming it precisely (three-valued logic) is the point of the question.
- `NOT EXISTS` is null-safe: it only asks "did any row match?", and `g.entry_id = s.entry_id` being UNKNOWN simply means "no match," which is what you want. Prefer it.
- `LEFT JOIN ... IS NULL` is also null-safe and fine; its risk is picking a test column that is legitimately NULL on matched rows — test the join key itself (or a guaranteed NOT NULL column), not an arbitrary attribute.
- Fixed NOT IN, if forced: `NOT IN (SELECT entry_id FROM gl_entries WHERE entry_id IS NOT NULL)`.

**Common wrong answers:** using `NOT IN` and asserting it's equivalent; in form 2, testing `WHERE g.amount IS NULL` (a matched row with a NULL amount would be misreported as missing); `EXCEPT DISTINCT` on full rows when only key membership was asked (also dedups, changing semantics).
</details>

---

## Problem 3 — FULL OUTER JOIN reconciliation break report (Hard)

**Asked as:** "Build the daily recon: compare source vs warehouse by entry_id and classify each break as MISSING_IN_TARGET, MISSING_IN_SOURCE, or AMOUNT_MISMATCH, with both amounts and the difference."

Tables:

`src_entries` (from the CSV feed):

| entry_id | amount  |
|----------|---------|
| E1       | 100.00  |
| E2       | 250.00  |
| E3       | 75.00   |

`gl_entries` (warehouse):

| entry_id | amount  |
|----------|---------|
| E1       | 100.00  |
| E3       | 80.00   |
| E4       | 60.00   |

**Expected output:**

| entry_id | src_amount | tgt_amount | diff   | break_type        |
|----------|------------|------------|--------|-------------------|
| E2       | 250.00     | NULL       | 250.00 | MISSING_IN_TARGET |
| E3       | 75.00      | 80.00      | -5.00  | AMOUNT_MISMATCH   |
| E4       | NULL       | 60.00      | -60.00 | MISSING_IN_SOURCE |

(E1 matches and is excluded.)

<details><summary>Solution</summary>

```sql
SELECT
  COALESCE(s.entry_id, g.entry_id) AS entry_id,
  s.amount                          AS src_amount,
  g.amount                          AS tgt_amount,
  IFNULL(s.amount, 0) - IFNULL(g.amount, 0) AS diff,
  CASE
    WHEN g.entry_id IS NULL THEN 'MISSING_IN_TARGET'
    WHEN s.entry_id IS NULL THEN 'MISSING_IN_SOURCE'
    ELSE 'AMOUNT_MISMATCH'
  END AS break_type
FROM src_entries AS s
FULL OUTER JOIN gl_entries AS g
  ON g.entry_id = s.entry_id
WHERE s.entry_id IS NULL
   OR g.entry_id IS NULL
   OR s.amount <> g.amount
ORDER BY entry_id;
```

**Why it works / what's being tested.** `FULL OUTER JOIN` is the only join that preserves unmatched rows from **both** sides in one pass, which is exactly the shape of a reconciliation. The details that separate candidates: `COALESCE(s.entry_id, g.entry_id)` because whichever side is missing contributes a NULL key; classifying via **NULL-ness of the join key**, not of amount columns; and knowing that `s.amount <> g.amount` alone would *not* catch missing rows (UNKNOWN, filtered out) — hence the explicit `IS NULL` disjuncts. For NUMERIC money this is exact; if the columns were FLOAT64, compare with a tolerance (`ABS(s.amount - g.amount) > 0.005`) and say why. Real-world upgrade: aggregate both sides per entry_id first if either side can hold multiple rows per key, or the join fans out (Problem 9).

**Common wrong answers:** two LEFT JOIN queries UNIONed (works but scans twice and often double-counts the mismatch class); `WHERE s.amount <> g.amount` only (drops both MISSING classes); selecting `s.entry_id` instead of the COALESCE (NULL ids in the report).
</details>

---

## Problem 4 — Semi-join: accounts with activity (Easy)

**Asked as:** "Return accounts from the master that had at least one posting in July. I don't want duplicates and I don't want any columns from the postings table — what's the right join type?"

Tables: `accounts(account_id, account_name)`; `postings(posting_id, account_id, posting_date, amount)` — an account may have thousands of postings.

**Expected output:** each qualifying account exactly once.

<details><summary>Solution</summary>

```sql
SELECT a.account_id, a.account_name
FROM accounts AS a
WHERE EXISTS (
  SELECT 1
  FROM postings AS p
  WHERE p.account_id   = a.account_id
    AND p.posting_date BETWEEN '2026-07-01' AND '2026-07-31'
);

-- Equivalent: WHERE a.account_id IN (SELECT account_id FROM postings WHERE ...)
```

**Why it works / what's being tested.** This is a **semi-join**: filter left-side rows by *existence* of a match, returning left columns only, each row at most once. `EXISTS`/`IN` express it directly and the optimizer can stop probing at the first match. The tempting alternative — `JOIN postings ... ` then `SELECT DISTINCT` — first multiplies each account by its posting count and then pays to deduplicate; on a postings table with millions of rows that's real money in BigQuery. Saying "join + DISTINCT is a fan-out I then have to undo; EXISTS never fans out" is the answer the interviewer wants. (Unlike `NOT IN`, plain `IN` is safe with NULLs in the subquery for the positive case — NULLs just never match.)

**Common wrong answers:** `INNER JOIN` + `DISTINCT` presented as equally good without mentioning the cost; putting the date filter on the outer query instead of inside the EXISTS; `LEFT JOIN ... WHERE p.account_id IS NOT NULL` (an inner join wearing a costume, still fans out).
</details>

---

## Problem 5 — Conditional aggregation: debit/credit pivot (Medium)

**Asked as:** "One row per account: total debits, total credits, net movement, count of each, and how many postings are still in status 'PENDING'. Single scan, no joins."

Table `postings`:

| posting_id | account_id | side   | amount | status  |
|------------|-----------|--------|--------|---------|
| P1         | A-1001    | DEBIT  | 500    | POSTED  |
| P2         | A-1001    | CREDIT | 200    | POSTED  |
| P3         | A-1001    | DEBIT  | 300    | PENDING |
| P4         | A-1002    | CREDIT | 900    | POSTED  |

**Expected output:** A-1001 → debits 800 (2), credits 200 (1), net 600, pending 1; A-1002 → debits 0 (0), credits 900 (1), net −900, pending 0.

<details><summary>Solution</summary>

```sql
SELECT
  account_id,
  SUM(IF(side = 'DEBIT',  amount, 0)) AS total_debits,
  SUM(IF(side = 'CREDIT', amount, 0)) AS total_credits,
  SUM(IF(side = 'DEBIT',  amount, 0))
    - SUM(IF(side = 'CREDIT', amount, 0)) AS net_movement,
  COUNTIF(side = 'DEBIT')  AS debit_count,
  COUNTIF(side = 'CREDIT') AS credit_count,
  COUNTIF(status = 'PENDING') AS pending_count
FROM postings
GROUP BY account_id;
```

**Why it works / what's being tested.** **Conditional aggregation** turns row categories into columns in one pass: the `IF` routes each row's amount into the right bucket (0 otherwise), and `COUNTIF(cond)` is BigQuery shorthand for `SUM(IF(cond,1,0))`. Two nuances worth voicing: with `SUM(IF(cond, amount, 0))` an account with no debits gets **0**; with `SUM(IF(cond, amount, NULL))` it gets **NULL** (aggregates ignore NULLs, and an all-NULL SUM is NULL) — for money reports 0 is usually right, but know you chose it. And this is exactly what a `PIVOT` clause does under the hood; hand-rolled conditional aggregation is more flexible (mixed measures, computed columns like `net_movement`).

**Common wrong answers:** `WHERE side = 'DEBIT'` + a second query + join (two scans for no reason); `COUNT(IF(cond, 1, 0))` (counts *all* rows — `IF` returns 0, which is non-NULL and thus counted; you need `COUNT(IF(cond, 1, NULL))` or just `COUNTIF`); grouping by side and expecting columns.
</details>

---

## Problem 6 — HAVING vs WHERE (Easy)

**Asked as:** "Find accounts whose net posted movement in July exceeds 1,000,000, excluding reversal entries. Which conditions go in WHERE and which in HAVING, and why?"

Table `postings(account_id, posting_date, amount, is_reversal BOOL)` — `amount` signed.

**Expected output:** account_id + net movement, only where |net| > 1,000,000.

<details><summary>Solution</summary>

```sql
SELECT
  account_id,
  SUM(amount) AS net_movement
FROM postings
WHERE posting_date BETWEEN '2026-07-01' AND '2026-07-31'   -- row-level: filter before grouping
  AND NOT is_reversal
GROUP BY account_id
HAVING ABS(SUM(amount)) > 1000000;                          -- group-level: needs the aggregate
```

**Why it works / what's being tested.** `WHERE` runs **before** grouping and may not reference aggregates; `HAVING` runs **after** grouping and exists precisely to filter on them. The performance corollary is the real point: any predicate that *can* go in `WHERE` *must* go in `WHERE` — it shrinks the rows fed into the aggregation, and in BigQuery a date predicate in `WHERE` is what enables **partition pruning** on a date-partitioned table. Putting `NOT is_reversal` in `HAVING` (via a hack like `HAVING COUNTIF(is_reversal) = 0`) would also change semantics — it filters whole accounts, not individual reversal rows.

**Common wrong answers:** `WHERE SUM(amount) > 1000000` (invalid — aggregate in WHERE); filtering the date range in HAVING (correct result, but aggregates a year to keep a month, and kills pruning); forgetting `ABS` when the question said "exceeds" a magnitude on signed data.
</details>

---

## Problem 7 — Find duplicates three ways (Medium)

**Asked as:** "Audit found duplicate journal entries. Show me (1) which business keys are duplicated and how many copies, (2) the full duplicate rows, and (3) how you'd flag every row with its copy count without collapsing anything."

Table `journal_entries(entry_id, account_id, entry_date, amount, load_ts)` — business key is `entry_id`.

<details><summary>Solution</summary>

```sql
-- (1) Which keys are duplicated: GROUP BY ... HAVING
SELECT entry_id, COUNT(*) AS copies
FROM journal_entries
GROUP BY entry_id
HAVING COUNT(*) > 1;

-- (2) The full offending rows: ROW_NUMBER (also lets you *pick which copy to keep*)
SELECT *
FROM journal_entries
QUALIFY ROW_NUMBER() OVER (PARTITION BY entry_id ORDER BY load_ts DESC) > 1;
-- rn > 1 = the extra copies (delete-list); rn = 1 = the survivors.

-- (3) Every row annotated, nothing collapsed: COUNT(*) OVER
SELECT
  *,
  COUNT(*) OVER (PARTITION BY entry_id) AS copies_of_key
FROM journal_entries;
-- WHERE-able via QUALIFY: ... QUALIFY COUNT(*) OVER (PARTITION BY entry_id) > 1
```

**Why it works / what's being tested.** Three tools, three questions: `GROUP BY/HAVING` answers "*which keys* and how many" but loses the rows; `ROW_NUMBER` gives you an ordered lineup within each key so you can distinguish keeper vs extras (the only form that directly supports "keep latest, delete rest"); `COUNT(*) OVER` keeps every row and every column while attaching group context — ideal for an audit extract. A strong candidate also asks "**duplicate by what definition?**" — same `entry_id` (business-key dup) vs identical entire row (full dup) lead to different `PARTITION BY`/`GROUP BY` lists.

**Common wrong answers:** `SELECT DISTINCT` as a "find duplicates" answer (it hides them, doesn't find them); form (2) with `ORDER BY` omitted then claiming "rn=1 keeps the latest" (without ORDER BY it keeps an arbitrary copy); joining (1) back to the table to get full rows when `QUALIFY COUNT(*) OVER (...) > 1` does it in one pass.
</details>

---

## Problem 8 — GROUP BY ROLLUP: trial-balance subtotals (Medium)

**Asked as:** "The trial balance needs totals at three levels in one result: per (entity, account_class), per entity, and grand total. One query — and label the subtotal rows properly, because some entity names could theoretically be NULL."

Table `gl_balances`:

| entity | account_class | balance |
|--------|---------------|---------|
| UK     | ASSET         | 1000    |
| UK     | LIABILITY     | -400    |
| IN     | ASSET         | 600     |
| IN     | LIABILITY     | -100    |

**Expected output:** 4 detail rows, 2 entity subtotals (UK 600, IN 500), 1 grand total (1100), each tagged with its level.

<details><summary>Solution</summary>

```sql
SELECT
  entity,
  account_class,
  SUM(balance) AS total_balance,
  CASE
    WHEN GROUPING(entity) = 1        THEN 'GRAND TOTAL'
    WHEN GROUPING(account_class) = 1 THEN 'ENTITY SUBTOTAL'
    ELSE 'DETAIL'
  END AS level_label
FROM gl_balances
GROUP BY ROLLUP(entity, account_class)
ORDER BY GROUPING(entity), entity, GROUPING(account_class), account_class;
```

**Why it works / what's being tested.** `ROLLUP(a, b)` aggregates at every *prefix* of the column list: `(a, b)`, `(a)`, and `()` — a hierarchy of subtotals in one scan, exactly the shape of a trial balance. In subtotal rows the rolled-up columns come back as **NULL**, which is ambiguous if the column itself can hold NULL data — that's what **`GROUPING(col)`** solves: it returns 1 when the NULL is a rollup placeholder and 0 when it's real data. Using `GROUPING()` instead of `IFNULL(entity, 'TOTAL')` is the difference between a correct answer and a fragile one. (Related: BigQuery also supports `GROUPING SETS` and `CUBE` when you need non-hierarchical combinations.)

**Common wrong answers:** three queries glued with `UNION ALL` (works, three scans, and interviewers ask for ROLLUP by name); labeling subtotals with `entity IS NULL` checks (breaks on genuinely NULL entities); expecting `ROLLUP(a, b)` to also produce the `(b)`-only subtotal (that's `CUBE`, not `ROLLUP`).
</details>

---

## Problem 9 — Many-to-many fan-out: why did the amounts double? (Hard)

**Asked as:** "Finance says July's expense total doubled after we joined postings to the approvals table 'to add the approver'. Total in `postings` is 1,500 but the report says 2,300. Diagnose and fix."

Tables:

`postings`:

| posting_id | amount |
|------------|--------|
| P1         | 800    |
| P2         | 700    |

`approvals` (a posting can be approved multiple times — resubmissions):

| approval_id | posting_id | approver | approved_at |
|-------------|-----------|----------|-------------|
| AP1         | P1        | alice    | 2026-07-01  |
| AP2         | P1        | bob      | 2026-07-02  |
| AP3         | P2        | carol    | 2026-07-01  |

Buggy query: `SELECT SUM(p.amount) FROM postings p JOIN approvals a ON a.posting_id = p.posting_id;` → returns **2,300** (P1's 800 counted twice).

<details><summary>Solution</summary>

```sql
-- Diagnosis: the join key is 1-to-MANY, so each posting row is replicated
-- once per approval before SUM sees it. Verify the grain:
SELECT posting_id, COUNT(*) AS approval_rows
FROM approvals
GROUP BY posting_id
HAVING COUNT(*) > 1;   -- P1 → 2  ⇒ fan-out confirmed

-- Fix A (preferred): collapse the many-side to the join grain BEFORE joining
SELECT SUM(p.amount) AS total_amount
FROM postings AS p
LEFT JOIN (
  SELECT
    posting_id,
    ARRAY_AGG(approver ORDER BY approved_at DESC LIMIT 1)[OFFSET(0)] AS latest_approver
  FROM approvals
  GROUP BY posting_id
) AS a
  ON a.posting_id = p.posting_id;   -- 1-to-1 now; SUM = 1,500 and you still get the approver

-- Fix B: aggregate the measure at its own grain, then attach dimensions
-- Fix C (band-aid): SUM(DISTINCT ...) — WRONG in general: two different
-- postings of 700 would collapse into one. Never fix fan-out with DISTINCT on money.
```

**Why it works / what's being tested.** This is a **grain** question. A join's output grain is the finer of the two sides; joining a measure table to a many-rows-per-key table replicates the measure, and every downstream aggregate is inflated by the match count. The professional workflow being tested: (1) *suspect* fan-out whenever totals change after adding a join, (2) *prove* it by counting rows per key on the many side, (3) *fix* it by pre-aggregating the many side to the join key's grain — never by sprinkling `DISTINCT`. Also note the LEFT JOIN in the fix: an INNER join would additionally drop unapproved postings, a second, quieter way joins corrupt totals.

**Common wrong answers:** `SUM(DISTINCT p.amount)` (collapses legitimately equal amounts); `SELECT DISTINCT` before the SUM (drops legitimate duplicate-amount postings the same way); declaring the approvals table "bad data" — resubmission history is a perfectly valid grain, the query just ignored it.
</details>

---

## Problem 10 — Latest record per key: slowly changing account attributes (Medium)

**Asked as:** "The account-master feed appends a full snapshot row every time an attribute changes. Give me the current view: exactly one row per account, the latest version. Show two idioms and compare their cost."

Table `account_master_hist`:

| account_id | account_name    | risk_rating | updated_at              |
|-----------|-----------------|-------------|-------------------------|
| A-1001    | Ops Payables    | LOW         | 2026-05-01 10:00:00 UTC |
| A-1001    | Ops Payables    | MEDIUM      | 2026-06-15 09:00:00 UTC |
| A-1001    | Ops Payables EU | MEDIUM      | 2026-07-01 11:00:00 UTC |
| A-1002    | Payroll Control | LOW         | 2026-04-20 08:00:00 UTC |

**Expected output:** A-1001 (Ops Payables EU / MEDIUM / 2026-07-01), A-1002 (Payroll Control / LOW / 2026-04-20).

<details><summary>Solution</summary>

```sql
-- Idiom 1: ROW_NUMBER + QUALIFY — the readable default
SELECT *
FROM account_master_hist
QUALIFY ROW_NUMBER() OVER (
          PARTITION BY account_id
          ORDER BY updated_at DESC
        ) = 1;

-- Idiom 2: ARRAY_AGG(... LIMIT 1) — aggregation-shaped, often cheaper on skewed keys
SELECT
  latest.*
FROM (
  SELECT
    ARRAY_AGG(t ORDER BY t.updated_at DESC LIMIT 1)[OFFSET(0)] AS latest
  FROM account_master_hist AS t
  GROUP BY t.account_id
);
```

**Why it works / what's being tested.** "Latest row per key" is the SCD workhorse — it's how you derive a current dimension from an append-only history (and the SELECT half of the MERGE pattern in file 03). Idiom 1 sorts each partition and keeps rank 1. Idiom 2 aggregates each group into a 1-element array of whole-row STRUCTs (`t` = the row) and unwraps it with `[OFFSET(0)]`; because `ARRAY_AGG ... LIMIT 1` only ever *retains* one row per group, BigQuery can process it as a plain aggregation — frequently faster and less memory-hungry than a full per-partition sort when some keys have huge histories. Either way, name your tie-breaker: if two versions share `updated_at`, add another `ORDER BY` column or the result is non-deterministic.

**Common wrong answers:** `GROUP BY account_id` with `MAX(updated_at)` *and* `MAX(account_name)` (mixes attributes from different versions — the classic Frankenstein row); join back on `(account_id, MAX(updated_at))` (correct but returns *two* rows on a timestamp tie, and it's a second scan); `SELECT DISTINCT account_id, ...` (does nothing of the sort).
</details>

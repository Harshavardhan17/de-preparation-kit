# LeetCode "Top SQL 50" — companion guide

The official free study plan: **<https://leetcode.com/studyplan/top-sql-50/>**
(50 problems, no premium needed). It is the single most common screening set —
many companies lift their SQL round questions straight from it. Use it as the
**daily warm-up** (2–3 problems before the kit's own practice files), Weeks 1–3.

## How it maps to this kit

| LeetCode SQL 50 category | Kit file that goes deeper | Note |
|---|---|---|
| Select / Basic Joins | `02-joins-aggregation-dedup.md` | LC is MySQL-flavored; our file is BigQuery — solving both dialects is a plus |
| Basic Aggregate Functions | `02-joins-aggregation-dedup.md` (COUNTIF/pivot, HAVING) | |
| Sorting & Grouping | `02-joins-aggregation-dedup.md` (ROLLUP) | |
| Advanced Select & Joins | `01-window-functions.md` + `02` (self-join, anti-join, fan-out) | |
| Subqueries | `01-window-functions.md` (most subquery problems die to a window function — solve BOTH ways) | |
| String / Regex / Clause | quick wins; BigQuery equivalents: `REGEXP_CONTAINS`, `FORMAT_DATE` | |

Dialect note: LeetCode runs MySQL by default. Accept the dialect differences —
`GROUP_CONCAT` vs `STRING_AGG`, `IFNULL` vs `COALESCE`/`IFNULL`, no `QUALIFY`.
Being able to say "in BigQuery I'd write this with QUALIFY" *while* solving it
in MySQL is exactly the fluency interviewers reward.

## The 15 that actually get asked (do these even on a busy day)

| Problem | The pattern it drills | Kit twin |
|---|---|---|
| 176. Second Highest Salary | Nth value, NULL when absent | 01 §ranking |
| 185. Department Top Three Salaries | top-N per group (DENSE_RANK) | 01 §top-N |
| 180. Consecutive Numbers | consecutiveness via LAG or gaps-and-islands | 01 §islands |
| 550. Game Play Analysis IV | first-event self-join / window, retention | 01 §LEAD/LAG |
| 570. Managers with ≥5 Direct Reports | self-join + HAVING | 02 §self-join |
| 585. Investments in 2016 | dedup semantics, multi-condition counts | 02 §duplicates |
| 602. Friend Requests II | UNION ALL then aggregate | 02 |
| 626. Exchange Seats | window trickery / CASE on row parity | 01 |
| 1045. Customers Who Bought All Products | relational division (HAVING COUNT DISTINCT = total) | 02 |
| 1164. Product Price at a Given Date | as-of / point-in-time lookup — SCD2 in disguise! | 03 §MERGE + project SCD2 |
| 1204. Last Person to Fit in the Bus | running SUM with cutoff | 01 §running balance — *her ledger pattern* |
| 1321. Restaurant Growth | moving window average with date frames | 01 §moving average |
| 1341. Movie Rating | two aggregations, UNION, tie-breaking | 02 |
| 1907. Count Salary Categories | conditional aggregation with zero-filled buckets | 02 §COUNTIF |
| 1934. Confirmation Rate | LEFT JOIN + AVG(CASE) rate pattern | 02 §pivot |

## Cadence
- **Weeks 1–2:** 3/day in study-plan order of topics (joins → aggregates →
  windows). First attempt timeboxed to 15 min, then read the editorial, then
  **re-solve from a blank editor**.
- **Week 3:** finish the set; redo every problem you needed the editorial for.
- **Week 4:** speed round — 10 random ones, 8 min each, out loud, as in a
  screen-share interview.

Track progress in the study plan page itself (it has built-in checkmarks).

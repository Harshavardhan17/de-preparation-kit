-- 03_reconciliation.sql
-- Reconcile what we PUBLISHED against what the sender SAID they sent
-- (manifest control totals), and produce a per-account break report.
--
-- Interview concepts on display:
--   * control-total reconciliation (row counts, sum of debits, sum of credits)
--   * FULL OUTER JOIN break report — rows on either side that don't match
--   * this is the pattern behind every "source vs target" recon question.
--
-- The manifest JSON is loaded to ledger_staging.feed_manifest by the DAG's
-- load_manifest task (one row per business_date: row_count, total_debit,
-- total_credit). Manifest semantics: row_count is the PHYSICAL file line
-- count; the totals are the sender's BUSINESS totals for this business date.

CREATE TABLE IF NOT EXISTS ledger_published.recon_results (
  business_date  DATE,
  check_name     STRING,
  source_value   NUMERIC,
  target_value   NUMERIC,
  difference     NUMERIC,
  status         STRING,
  checked_ts     TIMESTAMP
);

DELETE FROM ledger_published.recon_results WHERE business_date = @business_date;

INSERT INTO ledger_published.recon_results
WITH target AS (
  SELECT
    COUNT(*)                            AS row_count,
    SUM(debit_amount)                   AS total_debit,
    SUM(credit_amount)                  AS total_credit
  FROM ledger_staging.stg_journal_lines
  WHERE business_date = @business_date
),
source AS (
  SELECT row_count, total_debit, total_credit
  FROM ledger_staging.feed_manifest
  WHERE business_date = @business_date
),
checks AS (
  SELECT 'row_count'    AS check_name, CAST(s.row_count AS NUMERIC) AS source_value, CAST(t.row_count AS NUMERIC) AS target_value FROM source s CROSS JOIN target t
  UNION ALL
  SELECT 'total_debit',  s.total_debit,  t.total_debit  FROM source s CROSS JOIN target t
  UNION ALL
  SELECT 'total_credit', s.total_credit, t.total_credit FROM source s CROSS JOIN target t
)
SELECT
  @business_date,
  check_name,
  source_value,
  target_value,
  source_value - target_value AS difference,
  -- Count differences are EXPECTED: staging deduplicates resent lines,
  -- quarantines unparseable amounts, and routes late journals to yesterday's
  -- partition. The recon's job is to make the difference VISIBLE and
  -- EXPLAINED, not to force it to zero. Amounts must tie out at run time —
  -- though tomorrow's late journals may legitimately RESTATE today's totals
  -- afterwards (rerun this recon for a date to see history move).
  CASE
    WHEN check_name = 'row_count' AND source_value - target_value <> 0
      THEN 'EXPLAIN'          -- = dedup + quarantine + late-journal line counts
    WHEN source_value - target_value = 0 THEN 'MATCHED'
    ELSE 'BREAK'
  END AS status,
  CURRENT_TIMESTAMP()
FROM checks;

-- Per-account break report: accounts whose staged totals differ from published
-- totals. FULL OUTER JOIN so an account missing from EITHER side surfaces.
CREATE OR REPLACE TABLE ledger_published.recon_account_breaks AS
WITH staged AS (
  SELECT account_id, currency,
         SUM(debit_amount) AS debits, SUM(credit_amount) AS credits
  FROM ledger_staging.stg_journal_lines
  WHERE business_date = @business_date
  GROUP BY account_id, currency
),
published AS (
  SELECT account_id, currency,
         SUM(debit_amount) AS debits, SUM(credit_amount) AS credits
  FROM ledger_published.fact_journal_lines
  WHERE business_date = @business_date
  GROUP BY account_id, currency
)
SELECT
  @business_date                                   AS business_date,
  COALESCE(s.account_id, p.account_id)             AS account_id,
  COALESCE(s.currency, p.currency)                 AS currency,
  s.debits                                         AS staged_debits,
  p.debits                                         AS published_debits,
  s.credits                                        AS staged_credits,
  p.credits                                        AS published_credits,
  CASE
    WHEN s.account_id IS NULL THEN 'MISSING_IN_STAGING'
    WHEN p.account_id IS NULL THEN 'MISSING_IN_PUBLISHED'
    WHEN COALESCE(s.debits,0)  <> COALESCE(p.debits,0)
      OR COALESCE(s.credits,0) <> COALESCE(p.credits,0) THEN 'AMOUNT_BREAK'
  END                                              AS break_type
FROM staged s
FULL OUTER JOIN published p
  ON  s.account_id = p.account_id
  AND s.currency   = p.currency
WHERE s.account_id IS NULL
   OR p.account_id IS NULL
   OR COALESCE(s.debits,0)  <> COALESCE(p.debits,0)
   OR COALESCE(s.credits,0) <> COALESCE(p.credits,0);

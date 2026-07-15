-- 05_data_quality_checks.sql
-- The gate the DAG runs BEFORE anything is forwarded downstream.
-- Executed by BigQueryValueCheckOperator with pass_value = 0:
-- the query must return a single row with a single value; anything but 0
-- fails the task and halts the pipeline.
--
-- Interview concepts on display:
--   * the fundamental double-entry invariant: per journal, debits = credits
--   * conditional aggregation (COUNTIF) to pack several checks into one scan
--   * "fail closed": bad data stops the pipeline rather than flowing onward.

WITH violations AS (
  -- Check 1: unbalanced journals (the invariant that defines a ledger)
  SELECT COUNT(*) AS n
  FROM (
    SELECT journal_id
    FROM ledger_staging.stg_journal_lines
    WHERE business_date = @business_date
    GROUP BY journal_id
    HAVING ABS(SUM(debit_amount) - SUM(credit_amount)) > 0.005  -- rounding tolerance
  )

  UNION ALL

  -- Check 2: keys and amounts present
  SELECT COUNTIF(
       journal_id IS NULL
    OR account_id IS NULL
    OR (debit_amount IS NULL AND credit_amount IS NULL)
  )
  FROM ledger_staging.stg_journal_lines
  WHERE business_date = @business_date

  UNION ALL

  -- Check 3: a line must be a debit OR a credit, never both, never negative
  SELECT COUNTIF(
       (debit_amount > 0 AND credit_amount > 0)
    OR debit_amount  < 0
    OR credit_amount < 0
  )
  FROM ledger_staging.stg_journal_lines
  WHERE business_date = @business_date

  UNION ALL

  -- Check 4: currency must be a known ISO code
  SELECT COUNTIF(currency NOT IN ('USD','GBP','INR','HKD','EUR','SGD','JPY','CNY'))
  FROM ledger_staging.stg_journal_lines
  WHERE business_date = @business_date

  UNION ALL

  -- Check 5: no future-dated postings
  SELECT COUNTIF(business_date > @business_date)
  FROM ledger_staging.stg_journal_lines
)
SELECT SUM(n) AS total_violations FROM violations;

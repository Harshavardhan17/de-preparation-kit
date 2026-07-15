-- 01_staging_dedup.sql
-- Raw partition -> typed, deduplicated staging for one business date.
--
-- Interview concepts on display:
--   * ROW_NUMBER() dedup on a natural key (the single most-asked SQL pattern)
--   * QUALIFY (BigQuery: filter on a window function without a subquery)
--   * SAFE_CAST quarantine pattern — one bad value must not kill the load
--   * Idempotent write: replacing a fixed window of partitions, rerun-safe
--   * late-data window: today's feed may restate yesterday's partition

-- Good rows -> typed staging
CREATE TABLE IF NOT EXISTS ledger_staging.stg_journal_lines (
  journal_id     STRING NOT NULL,
  line_no        INT64,
  business_date  DATE,
  posting_ts     TIMESTAMP,
  account_id     STRING,
  account_name   STRING,
  cost_center    STRING,
  currency       STRING,
  debit_amount   NUMERIC,     -- NUMERIC, never FLOAT64, for money. Talking point.
  credit_amount  NUMERIC,
  description    STRING,
  source_system  STRING,
  load_ts        TIMESTAMP
)
PARTITION BY business_date
CLUSTER BY account_id, journal_id;

-- Late-data window: today's feed may carry complete journals stamped with
-- YESTERDAY's business date (adjustments posted after the cut-off). So each
-- run rebuilds a 2-day window of partitions — yesterday absorbs the late
-- journals, today is built fresh, and the DELETE+INSERT pair over the same
-- window keeps the rerun idempotent.
DELETE FROM ledger_staging.stg_journal_lines
WHERE business_date BETWEEN DATE_SUB(@business_date, INTERVAL 1 DAY) AND @business_date;

INSERT INTO ledger_staging.stg_journal_lines
SELECT
  journal_id,
  line_no,
  business_date,
  posting_ts,
  account_id,
  account_name,
  NULLIF(TRIM(cost_center), '')                            AS cost_center,
  currency,
  SAFE_CAST(REPLACE(debit_amount,  ',', '') AS NUMERIC)    AS debit_amount,
  SAFE_CAST(REPLACE(credit_amount, ',', '') AS NUMERIC)    AS credit_amount,
  description,
  source_system,
  CURRENT_TIMESTAMP()                                      AS load_ts
FROM ledger_staging.raw_journal_lines
WHERE business_date BETWEEN DATE_SUB(@business_date, INTERVAL 1 DAY) AND @business_date
  -- Good rows only: anything whose amounts fail the cast goes to quarantine
  -- below, NOT into staging — a half-parsed journal could never balance.
  AND SAFE_CAST(REPLACE(debit_amount,  ',', '') AS NUMERIC) IS NOT NULL
  AND SAFE_CAST(REPLACE(credit_amount, ',', '') AS NUMERIC) IS NOT NULL
-- Dedup: upstream resends produce exact duplicate lines. Natural key is
-- (journal_id, line_no); keep the latest posting_ts deterministically.
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY journal_id, line_no
  ORDER BY posting_ts DESC
) = 1;

-- Quarantine rows whose amounts failed SAFE_CAST — reviewed by ops, never lost.
CREATE TABLE IF NOT EXISTS ledger_staging.quarantine_journal_lines
LIKE ledger_staging.raw_journal_lines;

DELETE FROM ledger_staging.quarantine_journal_lines
WHERE business_date BETWEEN DATE_SUB(@business_date, INTERVAL 1 DAY) AND @business_date;

INSERT INTO ledger_staging.quarantine_journal_lines
SELECT *
FROM ledger_staging.raw_journal_lines
WHERE business_date BETWEEN DATE_SUB(@business_date, INTERVAL 1 DAY) AND @business_date
  AND (SAFE_CAST(REPLACE(debit_amount,  ',', '') AS NUMERIC) IS NULL
    OR SAFE_CAST(REPLACE(credit_amount, ',', '') AS NUMERIC) IS NULL);

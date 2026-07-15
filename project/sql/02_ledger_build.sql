-- 02_ledger_build.sql
-- Staging -> published ledger fact + running account balances.
--
-- Interview concepts on display:
--   * MERGE for an idempotent publish (rerun-safe, no duplicates ever)
--   * window functions: SUM() OVER (PARTITION BY ... ORDER BY ...) running balance
--   * late-arriving data: the feed can contain lines stamped with an earlier
--     business date; MERGE absorbs them, and balances are rebuilt from the
--     earliest affected date.

CREATE TABLE IF NOT EXISTS ledger_published.fact_journal_lines (
  journal_id     STRING NOT NULL,
  line_no        INT64  NOT NULL,
  business_date  DATE,
  posting_ts     TIMESTAMP,
  account_id     STRING,
  cost_center    STRING,
  currency       STRING,
  debit_amount   NUMERIC,
  credit_amount  NUMERIC,
  net_amount     NUMERIC,          -- debit positive, credit negative
  source_system  STRING,
  published_ts   TIMESTAMP
)
PARTITION BY business_date
CLUSTER BY account_id;

-- Idempotent publish. A rerun of the same day UPDATEs rather than duplicating;
-- late-arriving lines for earlier dates INSERT into their own partitions.
MERGE ledger_published.fact_journal_lines AS tgt
USING (
  SELECT
    journal_id, line_no, business_date, posting_ts, account_id,
    cost_center, currency, debit_amount, credit_amount,
    debit_amount - credit_amount AS net_amount,
    source_system
  FROM ledger_staging.stg_journal_lines
  WHERE business_date <= @business_date   -- includes late lines from this file
    AND load_ts >= TIMESTAMP(@business_date)
) AS src
ON  tgt.journal_id = src.journal_id
AND tgt.line_no    = src.line_no
WHEN MATCHED THEN UPDATE SET
  business_date = src.business_date,
  posting_ts    = src.posting_ts,
  account_id    = src.account_id,
  cost_center   = src.cost_center,
  currency      = src.currency,
  debit_amount  = src.debit_amount,
  credit_amount = src.credit_amount,
  net_amount    = src.net_amount,
  source_system = src.source_system,
  published_ts  = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
  journal_id, line_no, business_date, posting_ts, account_id, cost_center,
  currency, debit_amount, credit_amount, net_amount, source_system, published_ts
) VALUES (
  src.journal_id, src.line_no, src.business_date, src.posting_ts, src.account_id,
  src.cost_center, src.currency, src.debit_amount, src.credit_amount,
  src.net_amount, src.source_system, CURRENT_TIMESTAMP()
);

-- Daily balance snapshot: net movement per account per day, then a running
-- (cumulative) balance. The classic window-function interview question,
-- doing real work.
CREATE OR REPLACE TABLE ledger_published.account_daily_balance
PARTITION BY business_date
CLUSTER BY account_id AS
WITH daily_net AS (
  SELECT
    account_id,
    currency,
    business_date,
    SUM(debit_amount)  AS day_debits,
    SUM(credit_amount) AS day_credits,
    SUM(net_amount)    AS day_net
  FROM ledger_published.fact_journal_lines
  GROUP BY account_id, currency, business_date
)
SELECT
  account_id,
  currency,
  business_date,
  day_debits,
  day_credits,
  day_net,
  -- Default frame with ORDER BY is RANGE UNBOUNDED PRECEDING..CURRENT ROW;
  -- one row per (account, currency, date) here, so ROWS vs RANGE is equivalent —
  -- but say ROWS explicitly to show you know the difference.
  SUM(day_net) OVER (
    PARTITION BY account_id, currency
    ORDER BY business_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS running_balance
FROM daily_net;

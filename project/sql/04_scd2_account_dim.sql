-- 04_scd2_account_dim.sql
-- Slowly Changing Dimension Type 2 for account master data.
--
-- Interview concepts on display:
--   * SCD2: full history of attribute changes with effective-date ranges
--   * the two-step MERGE pattern (expire the old row, insert the new version)
--   * why finance needs SCD2: "what cost center did account X map to on the
--     date this journal posted?" must be answerable forever (audit).
--
-- dim_account rows: one CURRENT row per account (is_current = TRUE) plus the
-- full history. Point-in-time joins use:
--   ON f.account_id = d.account_id
--  AND f.business_date >= d.effective_from
--  AND f.business_date <  d.effective_to

CREATE TABLE IF NOT EXISTS ledger_published.dim_account (
  account_sk      STRING,      -- surrogate key (GENERATE_UUID at insert)
  account_id      STRING NOT NULL,
  account_name    STRING,
  account_type    STRING,      -- ASSET / LIABILITY / EQUITY / REVENUE / EXPENSE
  effective_from  DATE,
  effective_to    DATE,        -- 9999-12-31 for the current row
  is_current      BOOL
);

-- Today's distinct account attributes as seen in the feed.
CREATE TEMP TABLE todays_accounts AS
SELECT DISTINCT
  account_id,
  account_name,
  CASE SUBSTR(account_id, 1, 1)
    WHEN '1' THEN 'ASSET'
    WHEN '2' THEN 'LIABILITY'
    WHEN '3' THEN 'EQUITY'
    WHEN '4' THEN 'REVENUE'
    ELSE 'EXPENSE'
  END AS account_type
FROM ledger_staging.stg_journal_lines
WHERE business_date = @business_date;

-- Step 1: EXPIRE current rows whose attributes changed in today's feed.
MERGE ledger_published.dim_account AS tgt
USING todays_accounts AS src
ON tgt.account_id = src.account_id AND tgt.is_current
WHEN MATCHED AND (
       COALESCE(tgt.account_name, '') <> COALESCE(src.account_name, '')
    OR COALESCE(tgt.account_type, '') <> COALESCE(src.account_type, '')
) THEN UPDATE SET
  effective_to = @business_date,
  is_current   = FALSE;

-- Step 2: INSERT a fresh current row for new accounts AND for accounts just
-- expired in step 1 (they no longer have an is_current row, so the anti-join
-- picks them up too).
INSERT INTO ledger_published.dim_account
SELECT
  GENERATE_UUID(),
  src.account_id,
  src.account_name,
  src.account_type,
  @business_date,
  DATE '9999-12-31',
  TRUE
FROM todays_accounts AS src
WHERE NOT EXISTS (
  SELECT 1
  FROM ledger_published.dim_account d
  WHERE d.account_id = src.account_id AND d.is_current
);

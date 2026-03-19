-- Silver Layer: Salesforce Accounts
-- Cleans raw accounts, casts types, deduplicates, and adds business metrics.

CREATE OR REFRESH MATERIALIZED VIEW silver_sf_accounts
COMMENT 'Cleaned Salesforce accounts with ARR tiers and SA metadata'
CLUSTER BY (account_id)
AS
SELECT
  account_id,
  account_name,
  COALESCE(type, 'Unknown')      AS account_type,
  COALESCE(industry, 'Unknown')  AS industry,
  COALESCE(vertical, 'Unknown')  AS vertical,
  COALESCE(arr, 0.0)             AS arr,
  COALESCE(t3m_arr, 0.0)         AS t3m_arr,
  number_of_employees,
  billing_city,
  billing_state,
  billing_country,
  CONCAT_WS(', ',
    NULLIF(billing_city, ''),
    NULLIF(billing_state, ''),
    NULLIF(billing_country, '')
  )                              AS billing_location,
  owner_name,
  last_sa_engaged_name,
  CASE
    WHEN arr >= 1000000 THEN 'Strategic (≥$1M)'
    WHEN arr >= 500000  THEN 'Enterprise ($500K–$1M)'
    WHEN arr >= 100000  THEN 'Growth ($100K–$500K)'
    WHEN arr > 0        THEN 'Emerging (<$100K)'
    ELSE 'No ARR'
  END                            AS arr_tier,
  _extracted_at                  AS last_synced_at
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY _extracted_at DESC) AS rn
  FROM LIVE.raw_sf_accounts
)
WHERE rn = 1
  AND account_id IS NOT NULL
  AND account_name IS NOT NULL;

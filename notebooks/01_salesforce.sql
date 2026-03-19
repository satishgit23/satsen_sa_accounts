-- =============================================================================
-- Notebook 01 — Salesforce Accounts & Use Cases
-- Purpose : Query account and UCO data, flag stale records, show health
-- Catalog : satsen_catalog   Schema: satsen_sa_accounts
-- =============================================================================

-- ── 1. Account Overview ───────────────────────────────────────────────────────
SELECT
  account_name,
  arr_tier,
  ROUND(arr / 1000, 0)        AS arr_k,
  industry,
  vertical,
  billing_location,
  last_synced_at
FROM satsen_catalog.satsen_sa_accounts.silver_sf_accounts
ORDER BY arr DESC NULLS LAST;

-- ── 2. UCO Stage Distribution per Account ─────────────────────────────────────
SELECT
  account_name,
  stage,
  COUNT(*)             AS uco_count,
  MAX(last_modified_date) AS last_updated
FROM satsen_catalog.satsen_sa_accounts.silver_sf_use_cases
GROUP BY account_name, stage
ORDER BY account_name, stage_order;

-- ── 3. Stale UCOs (not updated in 30+ days) ───────────────────────────────────
SELECT
  uco_id,
  account_name,
  uco_name,
  stage,
  use_case_type,
  implementation_status,
  DATEDIFF(CURRENT_DATE(), CAST(last_modified_date AS DATE)) AS days_since_update,
  last_modified_date
FROM satsen_catalog.satsen_sa_accounts.silver_sf_use_cases
WHERE is_stale = TRUE
ORDER BY days_since_update DESC;

-- ── 4. UCOs with Go-Live in Next 30 Days ──────────────────────────────────────
SELECT
  uco_id,
  account_name,
  uco_name,
  stage,
  go_live_date,
  estimated_project_go_live,
  DATEDIFF(CAST(COALESCE(go_live_date, estimated_project_go_live) AS DATE), CURRENT_DATE()) AS days_until_go_live
FROM satsen_catalog.satsen_sa_accounts.silver_sf_use_cases
WHERE go_live_soon = TRUE
ORDER BY days_until_go_live;

-- ── 5. Account Health Scorecard ───────────────────────────────────────────────
SELECT
  account_name,
  arr_tier,
  ROUND(arr / 1000, 0)   AS arr_k,
  total_ucos,
  ucos_early,
  ucos_mid,
  ucos_live,
  stale_ucos,
  urgent_emails,
  meetings_this_week,
  slack_messages_14d,
  health_score
FROM satsen_catalog.satsen_sa_accounts.gold_account_health
ORDER BY health_score DESC;

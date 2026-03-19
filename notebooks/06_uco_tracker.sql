-- =============================================================================
-- Notebook 06 — UCO Tracker
-- Purpose : Full UCO pipeline health, progress tracking, and stage insights
-- Catalog : satsen_catalog   Schema: satsen_sa_accounts
-- =============================================================================

-- ── 1. UCO Pipeline Overview ──────────────────────────────────────────────────
SELECT
  stage,
  COUNT(*)                                  AS uco_count,
  COUNT(DISTINCT account_name)              AS accounts,
  COUNT(CASE WHEN is_stale    THEN 1 END)   AS stale,
  COUNT(CASE WHEN go_live_soon THEN 1 END)  AS go_live_soon
FROM satsen_catalog.satsen_sa_accounts.silver_sf_use_cases
GROUP BY stage
ORDER BY stage_order;

-- ── 2. Full UCO List with Status ──────────────────────────────────────────────
SELECT
  account_name,
  uco_name,
  stage,
  use_case_type,
  implementation_status,
  go_live_date,
  estimated_project_go_live,
  DATEDIFF(CURRENT_DATE(), CAST(last_modified_date AS DATE)) AS days_since_update,
  is_stale,
  go_live_soon,
  assigned_sa_name
FROM satsen_catalog.satsen_sa_accounts.silver_sf_use_cases
ORDER BY account_name, stage_order;

-- ── 3. Stage Progression Summary ──────────────────────────────────────────────
WITH stage_counts AS (
  SELECT
    account_name,
    SUM(CASE WHEN stage IN ('U1','U2') THEN 1 ELSE 0 END) AS early_stage,
    SUM(CASE WHEN stage IN ('U3','U4') THEN 1 ELSE 0 END) AS mid_stage,
    SUM(CASE WHEN stage IN ('U5','U6') THEN 1 ELSE 0 END) AS live_stage,
    COUNT(*)                                               AS total_ucos
  FROM satsen_catalog.satsen_sa_accounts.silver_sf_use_cases
  GROUP BY account_name
)
SELECT
  sc.*,
  ROUND(100.0 * live_stage / NULLIF(total_ucos, 0), 1) AS pct_live,
  a.arr_tier,
  a.health_score
FROM stage_counts sc
JOIN satsen_catalog.satsen_sa_accounts.gold_account_health a ON a.account_name = sc.account_name
ORDER BY pct_live DESC;

-- ── 4. UCOs by Implementation Status ─────────────────────────────────────────
SELECT
  implementation_status,
  COUNT(*)                  AS uco_count,
  COUNT(DISTINCT account_name) AS accounts
FROM satsen_catalog.satsen_sa_accounts.silver_sf_use_cases
WHERE implementation_status IS NOT NULL
GROUP BY implementation_status
ORDER BY uco_count DESC;

-- ── 5. Go-Live Forecast (Next 90 Days) ────────────────────────────────────────
SELECT
  account_name,
  uco_name,
  stage,
  COALESCE(go_live_date, estimated_project_go_live)                          AS target_go_live,
  DATEDIFF(
    CAST(COALESCE(go_live_date, estimated_project_go_live) AS DATE),
    CURRENT_DATE()
  )                                                                           AS days_out,
  CASE
    WHEN COALESCE(go_live_date, estimated_project_go_live) < CURRENT_TIMESTAMP() THEN 'OVERDUE'
    WHEN DATEDIFF(
           CAST(COALESCE(go_live_date, estimated_project_go_live) AS DATE),
           CURRENT_DATE()) <= 30                                              THEN 'THIS_MONTH'
    ELSE 'NEXT_QUARTER'
  END                                                                         AS forecast_bucket
FROM satsen_catalog.satsen_sa_accounts.silver_sf_use_cases
WHERE COALESCE(go_live_date, estimated_project_go_live) IS NOT NULL
  AND COALESCE(go_live_date, estimated_project_go_live)
      <= CURRENT_TIMESTAMP() + INTERVAL 90 DAYS
ORDER BY COALESCE(go_live_date, estimated_project_go_live);

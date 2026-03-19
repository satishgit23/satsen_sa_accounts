-- Silver Layer: Salesforce Use Cases (UCOs)
-- Cleans raw UCOs, normalizes stages, adds urgency flags.

CREATE OR REFRESH MATERIALIZED VIEW silver_sf_use_cases
COMMENT 'Cleaned UCOs with stage normalization and health signals'
CLUSTER BY (account_id, stage)
AS
SELECT
  uco_id,
  account_id,
  account_name,
  uco_name,
  COALESCE(stage, 'Unknown')                AS stage,
  COALESCE(description, '')                 AS description,
  COALESCE(use_case_type, 'Unknown')        AS use_case_type,
  COALESCE(implementation_status, 'Unknown') AS implementation_status,
  go_live_date,
  estimated_project_go_live,
  last_modified_date,
  created_date,
  assigned_sa_name,
  -- Stage numeric sort order
  CASE stage
    WHEN 'U1' THEN 1
    WHEN 'U2' THEN 2
    WHEN 'U3' THEN 3
    WHEN 'U4' THEN 4
    WHEN 'U5' THEN 5
    WHEN 'U6' THEN 6
    ELSE 99
  END                                       AS stage_order,
  -- Flag UCOs not touched in 30+ days
  CASE
    WHEN last_modified_date < CURRENT_TIMESTAMP() - INTERVAL 30 DAYS THEN TRUE
    ELSE FALSE
  END                                       AS is_stale,
  -- Flag go-live within 30 days
  CASE
    WHEN COALESCE(go_live_date, estimated_project_go_live) IS NOT NULL
      AND COALESCE(go_live_date, estimated_project_go_live)
          BETWEEN CURRENT_TIMESTAMP()
              AND CURRENT_TIMESTAMP() + INTERVAL 30 DAYS
    THEN TRUE
    ELSE FALSE
  END                                       AS go_live_soon,
  _extracted_at                             AS last_synced_at
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY uco_id ORDER BY _extracted_at DESC) AS rn
  FROM LIVE.raw_sf_use_cases
)
WHERE rn = 1
  AND uco_id IS NOT NULL;

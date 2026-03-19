-- =============================================================================
-- Notebook 05 — SA Daily Activity Dashboard
-- Purpose : Unified daily view of all customer touchpoints for Satish
-- Catalog : satsen_catalog   Schema: satsen_sa_accounts
-- =============================================================================

-- ── 1. Today's Action Items ────────────────────────────────────────────────────
SELECT
  account_name,
  arr_tier,
  activity_type,
  activity_title,
  activity_detail,
  priority,
  days_old,
  is_unread,
  needs_response,
  meeting_link
FROM satsen_catalog.satsen_sa_accounts.gold_sa_activity
WHERE activity_date = CURRENT_DATE()
   OR (activity_type = 'Email' AND needs_response = TRUE AND days_old <= 3)
   OR (activity_type = 'Meeting' AND priority = 'HIGH')
ORDER BY
  CASE priority WHEN 'URGENT' THEN 1 WHEN 'HIGH' THEN 2 WHEN 'MEDIUM' THEN 3 ELSE 4 END,
  account_name;

-- ── 2. This Week's Full Activity Timeline ─────────────────────────────────────
SELECT
  activity_date,
  account_name,
  activity_type,
  activity_title,
  activity_detail,
  priority
FROM satsen_catalog.satsen_sa_accounts.gold_sa_activity
WHERE activity_date BETWEEN DATE_SUB(CURRENT_DATE(), 7) AND DATE_ADD(CURRENT_DATE(), 7)
ORDER BY activity_date, account_name, activity_type;

-- ── 3. Account Health Scorecard (Exec View) ───────────────────────────────────
SELECT
  account_name,
  arr_tier,
  ROUND(arr / 1000, 0)    AS arr_k,
  health_score,
  total_ucos,
  ucos_live,
  stale_ucos,
  urgent_emails,
  meetings_this_week,
  slack_messages_14d,
  ucos_go_live_soon
FROM satsen_catalog.satsen_sa_accounts.gold_account_health
ORDER BY health_score ASC;

-- ── 4. Accounts Needing Attention (Low Health Score) ──────────────────────────
SELECT
  account_name,
  arr_tier,
  health_score,
  CASE
    WHEN stale_ucos > 0                THEN 'Stale UCOs need update'
    WHEN urgent_emails > 0             THEN 'Overdue email responses'
    WHEN total_ucos = 0                THEN 'No active use cases'
    WHEN meetings_this_week = 0
         AND slack_messages_14d = 0    THEN 'No engagement this week'
    ELSE 'Review recommended'
  END                                  AS attention_reason,
  stale_ucos,
  urgent_emails,
  total_ucos,
  meetings_this_week
FROM satsen_catalog.satsen_sa_accounts.gold_account_health
WHERE health_score < 65
ORDER BY health_score;

-- ── 5. Cross-Source Activity Summary by Account ───────────────────────────────
SELECT
  a.account_name,
  a.arr_tier,
  a.health_score,
  COUNT(DISTINCT CASE WHEN g.activity_type = 'Email'   THEN g.activity_id END) AS email_actions,
  COUNT(DISTINCT CASE WHEN g.activity_type = 'Meeting' THEN g.activity_id END) AS meetings,
  COUNT(DISTINCT CASE WHEN g.activity_type = 'Slack'   THEN g.activity_id END) AS slack_threads
FROM satsen_catalog.satsen_sa_accounts.gold_account_health a
LEFT JOIN satsen_catalog.satsen_sa_accounts.gold_sa_activity g
       ON g.account_name = a.account_name
      AND g.activity_date >= DATE_SUB(CURRENT_DATE(), 14)
GROUP BY a.account_name, a.arr_tier, a.health_score
ORDER BY a.health_score ASC;

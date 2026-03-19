-- =============================================================================
-- Notebook 04 — Slack Messages
-- Purpose : Show customer Slack activity and high-engagement threads
-- Catalog : satsen_catalog   Schema: satsen_sa_accounts
-- =============================================================================

-- ── 1. Recent High-Engagement Messages ────────────────────────────────────────
SELECT
  account_name,
  arr_tier,
  channel_name,
  LEFT(message_text, 200)      AS message_preview,
  CAST(sent_at AS DATE)        AS date,
  days_since_sent,
  reply_count,
  reaction_count,
  engagement_level
FROM satsen_catalog.satsen_sa_accounts.silver_slack_messages
WHERE engagement_level IN ('HIGH', 'MEDIUM')
ORDER BY
  CASE engagement_level WHEN 'HIGH' THEN 1 ELSE 2 END,
  sent_at DESC;

-- ── 2. Slack Activity by Customer (Last 14 Days) ──────────────────────────────
SELECT
  account_name,
  arr_tier,
  channel_name,
  COUNT(*)                                               AS total_messages,
  COUNT(CASE WHEN is_reply = FALSE THEN 1 END)           AS top_level_messages,
  SUM(reply_count)                                       AS total_replies,
  COUNT(CASE WHEN engagement_level = 'HIGH' THEN 1 END)  AS high_engagement,
  MAX(CAST(sent_at AS DATE))                             AS last_message_date
FROM satsen_catalog.satsen_sa_accounts.silver_slack_messages
GROUP BY account_name, arr_tier, channel_name
ORDER BY total_messages DESC;

-- ── 3. Customer Channels Summary ──────────────────────────────────────────────
SELECT
  account_name,
  COLLECT_SET(channel_name)                             AS channels,
  COUNT(*)                                              AS total_messages,
  COUNT(DISTINCT DATE_FORMAT(sent_at, 'yyyy-MM-dd'))    AS active_days,
  MAX(CAST(sent_at AS DATE))                            AS most_recent
FROM satsen_catalog.satsen_sa_accounts.silver_slack_messages
GROUP BY account_name
ORDER BY most_recent DESC;

-- ── 4. Top Threads with Most Activity ─────────────────────────────────────────
SELECT
  account_name,
  channel_name,
  LEFT(message_text, 150)       AS thread_starter,
  CAST(sent_at AS DATE)         AS date,
  reply_count,
  reaction_count
FROM satsen_catalog.satsen_sa_accounts.silver_slack_messages
WHERE is_reply = FALSE
  AND reply_count > 0
ORDER BY reply_count DESC
LIMIT 20;

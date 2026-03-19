-- Gold Layer: SA Daily Activity Summary
-- Unified activity timeline across all sources (Gmail, Calendar, Slack)
-- joined with Salesforce account + UCO context.

CREATE OR REFRESH MATERIALIZED VIEW gold_sa_activity
COMMENT 'Unified SA daily activity: emails, meetings, Slack, and UCO health — one row per activity'
AS

-- ── Emails needing a response ────────────────────────────────────────────────
SELECT
  e.message_id                                AS activity_id,
  e.account_name,
  e.arr_tier,
  CAST(e.received_date AS DATE)               AS activity_date,
  'Email'                                     AS activity_type,
  CONCAT('Email: ', e.subject)                AS activity_title,
  CONCAT('From: ', e.from_name,
         ' | Status: ', e.action_status)      AS activity_detail,
  e.action_status                             AS priority,
  e.days_since_received                       AS days_old,
  e.is_unread,
  e.needs_response,
  NULL                                        AS meeting_link,
  e.last_synced_at

FROM LIVE.silver_gmail_emails e
WHERE e.needs_response = TRUE

UNION ALL

-- ── Upcoming and recent meetings ─────────────────────────────────────────────
SELECT
  c.event_id                                  AS activity_id,
  c.account_name,
  c.arr_tier,
  CAST(c.start_time AS DATE)                  AS activity_date,
  'Meeting'                                   AS activity_type,
  CONCAT('Meeting: ', c.event_title)          AS activity_title,
  CONCAT(
    c.timing_label, ' | ',
    COALESCE(CAST(CAST(c.duration_minutes AS INT) AS STRING), '?'), ' min | ',
    CAST(c.attendee_count AS STRING), ' attendees'
  )                                           AS activity_detail,
  CASE c.timing_label
    WHEN 'THIS_WEEK' THEN 'HIGH'
    WHEN 'UPCOMING'  THEN 'MEDIUM'
    ELSE 'LOW'
  END                                         AS priority,
  DATEDIFF(CURRENT_DATE(), CAST(c.start_time AS DATE)) AS days_old,
  FALSE                                       AS is_unread,
  FALSE                                       AS needs_response,
  c.video_link                                AS meeting_link,
  c.last_synced_at

FROM LIVE.silver_calendar_events c
WHERE c.timing_label IN ('THIS_WEEK', 'UPCOMING', 'PAST')

UNION ALL

-- ── High-engagement Slack messages ───────────────────────────────────────────
SELECT
  s.message_ts                                AS activity_id,
  s.account_name,
  s.arr_tier,
  CAST(s.sent_at AS DATE)                     AS activity_date,
  'Slack'                                     AS activity_type,
  CONCAT('Slack #', s.channel_name)           AS activity_title,
  CONCAT(
    LEFT(s.message_text, 120),
    ' | Replies: ', CAST(s.reply_count AS STRING),
    ' | Reactions: ', CAST(s.reaction_count AS STRING)
  )                                           AS activity_detail,
  s.engagement_level                          AS priority,
  s.days_since_sent                           AS days_old,
  FALSE                                       AS is_unread,
  FALSE                                       AS needs_response,
  NULL                                        AS meeting_link,
  s.last_synced_at

FROM LIVE.silver_slack_messages s
WHERE s.engagement_level IN ('HIGH', 'MEDIUM')
  AND s.days_since_sent <= 14;


-- ── Gold: Account health summary (UCO scorecards) ────────────────────────────
CREATE OR REFRESH MATERIALIZED VIEW gold_account_health
COMMENT 'Per-account health scorecard: ARR, UCO stages, and engagement signals'
AS
SELECT
  a.account_id,
  a.account_name,
  a.arr,
  a.t3m_arr,
  a.arr_tier,
  a.industry,
  a.vertical,
  a.billing_location,

  -- UCO metrics
  COUNT(DISTINCT u.uco_id)                                          AS total_ucos,
  COUNT(DISTINCT CASE WHEN u.stage IN ('U1','U2') THEN u.uco_id END) AS ucos_early,
  COUNT(DISTINCT CASE WHEN u.stage IN ('U3','U4') THEN u.uco_id END) AS ucos_mid,
  COUNT(DISTINCT CASE WHEN u.stage IN ('U5','U6') THEN u.uco_id END) AS ucos_live,
  COUNT(DISTINCT CASE WHEN u.is_stale             THEN u.uco_id END) AS stale_ucos,
  COUNT(DISTINCT CASE WHEN u.go_live_soon          THEN u.uco_id END) AS ucos_go_live_soon,

  -- Email signals
  COUNT(DISTINCT CASE WHEN e.needs_response AND e.action_status = 'URGENT'
                      THEN e.message_id END)                         AS urgent_emails,
  COUNT(DISTINCT CASE WHEN e.needs_response
                      THEN e.message_id END)                         AS pending_emails,

  -- Meeting signals (next 7 days)
  COUNT(DISTINCT CASE WHEN c.timing_label = 'THIS_WEEK'
                      THEN c.event_id END)                           AS meetings_this_week,

  -- Slack engagement (last 14 days)
  COUNT(DISTINCT CASE WHEN s.days_since_sent <= 14
                      THEN s.message_ts END)                         AS slack_messages_14d,

  -- Overall health score (0–100)
  LEAST(100, GREATEST(0,
    50
    + CASE WHEN COUNT(DISTINCT u.uco_id) > 0 THEN 10 ELSE 0 END
    - LEAST(20, COUNT(DISTINCT CASE WHEN u.is_stale THEN u.uco_id END) * 5)
    + LEAST(15, COUNT(DISTINCT CASE WHEN c.timing_label = 'THIS_WEEK' THEN c.event_id END) * 5)
    + LEAST(10, COUNT(DISTINCT CASE WHEN s.days_since_sent <= 7 THEN s.message_ts END) * 2)
    - LEAST(15, COUNT(DISTINCT CASE WHEN e.action_status = 'URGENT' THEN e.message_id END) * 5)
  ))                                                                 AS health_score,

  a.last_synced_at

FROM LIVE.silver_sf_accounts       a
LEFT JOIN LIVE.silver_sf_use_cases   u ON u.account_id   = a.account_id
LEFT JOIN LIVE.silver_gmail_emails   e ON e.account_name = a.account_name
LEFT JOIN LIVE.silver_calendar_events c ON c.account_name = a.account_name
LEFT JOIN LIVE.silver_slack_messages  s ON s.account_name = a.account_name
GROUP BY ALL;

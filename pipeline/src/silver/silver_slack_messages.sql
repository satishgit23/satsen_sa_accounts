-- Silver Layer: Slack Messages
-- Cleans raw Slack messages and enriches with account context.

CREATE OR REFRESH MATERIALIZED VIEW silver_slack_messages
COMMENT 'Cleaned Slack messages from customer channels with account context'
CLUSTER BY (account_name, sent_at)
AS
SELECT
  s.message_ts,
  s.channel_id,
  s.channel_name,
  s.account_name,
  a.arr_tier,
  s.user_id,
  s.message_text,
  s.sent_at,
  -- Days since message
  DATEDIFF(CURRENT_DATE(), CAST(s.sent_at AS DATE)) AS days_since_sent,
  s.thread_ts,
  s.is_reply,
  s.reply_count,
  s.reactions,
  SIZE(s.reactions)                                 AS reaction_count,
  -- Engagement signal
  CASE
    WHEN s.reply_count >= 5 OR SIZE(s.reactions) >= 3 THEN 'HIGH'
    WHEN s.reply_count >= 2 OR SIZE(s.reactions) >= 1 THEN 'MEDIUM'
    ELSE 'LOW'
  END                                               AS engagement_level,
  s._extracted_at                                   AS last_synced_at
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY message_ts ORDER BY _extracted_at DESC) AS rn
  FROM LIVE.raw_slack_messages
) s
LEFT JOIN LIVE.silver_sf_accounts a ON a.account_name = s.account_name
WHERE s.rn = 1
  AND s.message_ts IS NOT NULL
  AND TRIM(s.message_text) != '';

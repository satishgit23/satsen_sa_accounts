-- Silver Layer: Gmail Customer Emails
-- Cleans raw emails, enriches with account ARR, and flags actionable items.

CREATE OR REFRESH MATERIALIZED VIEW silver_gmail_emails
COMMENT 'Cleaned customer emails with response priority and account context'
CLUSTER BY (account_name, received_date)
AS
SELECT
  e.message_id,
  e.thread_id,
  e.account_name,
  a.arr_tier,
  e.from_email,
  e.from_name,
  e.to_emails,
  e.cc_emails,
  e.subject,
  e.body_snippet,
  e.received_date,
  e.is_unread,
  e.needs_response,
  e.has_attachment,
  -- Days since email received
  DATEDIFF(CURRENT_DATE(), CAST(e.received_date AS DATE)) AS days_since_received,
  -- Priority label
  CASE
    WHEN e.needs_response AND e.is_unread
         AND DATEDIFF(CURRENT_DATE(), CAST(e.received_date AS DATE)) >= 2
    THEN 'URGENT'
    WHEN e.needs_response AND e.is_unread  THEN 'PENDING'
    WHEN e.needs_response AND NOT e.is_unread THEN 'READ_PENDING'
    ELSE 'NO_ACTION'
  END                                                    AS action_status,
  e._extracted_at                                        AS last_synced_at
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY message_id ORDER BY _extracted_at DESC) AS rn
  FROM LIVE.raw_gmail_emails
)  e
LEFT JOIN LIVE.silver_sf_accounts a ON a.account_name = e.account_name
WHERE e.rn = 1
  AND e.message_id IS NOT NULL;

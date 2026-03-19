-- =============================================================================
-- Notebook 02 — Gmail Customer Emails
-- Purpose : Surface emails needing a response, prioritized by urgency
-- Catalog : satsen_catalog   Schema: satsen_sa_accounts
-- =============================================================================

-- ── 1. All Emails Needing a Response ──────────────────────────────────────────
SELECT
  account_name,
  arr_tier,
  subject,
  from_name,
  from_email,
  CAST(received_date AS DATE)   AS date,
  days_since_received,
  action_status,
  is_unread,
  has_attachment
FROM satsen_catalog.satsen_sa_accounts.silver_gmail_emails
WHERE needs_response = TRUE
ORDER BY
  CASE action_status
    WHEN 'URGENT'       THEN 1
    WHEN 'PENDING'      THEN 2
    WHEN 'READ_PENDING' THEN 3
    ELSE 4
  END,
  received_date DESC;

-- ── 2. Urgent Emails (unread + 2+ days old) ───────────────────────────────────
SELECT
  account_name,
  subject,
  from_email,
  CAST(received_date AS DATE) AS received,
  days_since_received,
  body_snippet
FROM satsen_catalog.satsen_sa_accounts.silver_gmail_emails
WHERE action_status = 'URGENT'
ORDER BY days_since_received DESC;

-- ── 3. Email Volume by Customer (Last 30 Days) ────────────────────────────────
SELECT
  account_name,
  arr_tier,
  COUNT(*)                                                      AS total_emails,
  COUNT(CASE WHEN needs_response = TRUE  THEN 1 END)            AS needs_response,
  COUNT(CASE WHEN action_status = 'URGENT' THEN 1 END)          AS urgent,
  COUNT(CASE WHEN is_unread = TRUE        THEN 1 END)            AS unread,
  MAX(CAST(received_date AS DATE))                              AS last_email_date
FROM satsen_catalog.satsen_sa_accounts.silver_gmail_emails
GROUP BY account_name, arr_tier
ORDER BY needs_response DESC, total_emails DESC;

-- ── 4. Email Thread Summary ────────────────────────────────────────────────────
SELECT
  account_name,
  subject,
  COUNT(*)                                           AS emails_in_thread,
  MIN(CAST(received_date AS DATE))                   AS thread_start,
  MAX(CAST(received_date AS DATE))                   AS last_reply,
  BOOL_OR(needs_response)                            AS has_pending_response,
  COLLECT_SET(from_email)                            AS participants
FROM satsen_catalog.satsen_sa_accounts.silver_gmail_emails
GROUP BY account_name, thread_id, subject
HAVING COUNT(*) > 1
ORDER BY last_reply DESC;

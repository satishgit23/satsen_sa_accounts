-- =============================================================================
-- Notebook 03 — Google Calendar Meetings
-- Purpose : Show upcoming and past meetings per customer account
-- Catalog : satsen_catalog   Schema: satsen_sa_accounts
-- =============================================================================

-- ── 1. This Week's Meetings ────────────────────────────────────────────────────
SELECT
  account_name,
  arr_tier,
  event_title,
  CAST(start_time AS DATE)      AS date,
  DATE_FORMAT(start_time, 'HH:mm') AS time,
  CAST(duration_minutes AS INT) AS duration_min,
  attendee_count,
  video_link,
  organizer_email
FROM satsen_catalog.satsen_sa_accounts.silver_calendar_events
WHERE timing_label = 'THIS_WEEK'
ORDER BY start_time;

-- ── 2. All Upcoming Meetings ───────────────────────────────────────────────────
SELECT
  account_name,
  arr_tier,
  event_title,
  CAST(start_time AS DATE)      AS date,
  event_day_of_week,
  DATE_FORMAT(start_time, 'HH:mm') AS start_time,
  CAST(duration_minutes AS INT) AS duration_min,
  attendee_count,
  video_link,
  timing_label
FROM satsen_catalog.satsen_sa_accounts.silver_calendar_events
WHERE timing_label IN ('THIS_WEEK', 'UPCOMING')
ORDER BY start_time;

-- ── 3. Meeting Frequency by Customer (Last 30 Days) ───────────────────────────
SELECT
  account_name,
  arr_tier,
  COUNT(*)                                                              AS total_meetings,
  ROUND(AVG(duration_minutes), 0)                                       AS avg_duration_min,
  SUM(attendee_count)                                                   AS total_attendees,
  MIN(CAST(start_time AS DATE))                                         AS first_meeting,
  MAX(CAST(start_time AS DATE))                                         AS last_meeting
FROM satsen_catalog.satsen_sa_accounts.silver_calendar_events
WHERE timing_label = 'PAST'
  AND start_time >= CURRENT_TIMESTAMP() - INTERVAL 30 DAYS
GROUP BY account_name, arr_tier
ORDER BY total_meetings DESC;

-- ── 4. Meeting Prep Notes (Upcoming + Account Context) ────────────────────────
SELECT
  c.account_name,
  c.event_title,
  CAST(c.start_time AS DATE)                           AS meeting_date,
  c.duration_minutes,
  c.attendees,
  a.arr_tier,
  a.arr,
  COUNT(DISTINCT u.uco_id)                             AS active_ucos,
  COUNT(DISTINCT CASE WHEN u.is_stale THEN u.uco_id END) AS stale_ucos,
  COUNT(DISTINCT CASE WHEN e.needs_response THEN e.message_id END) AS pending_emails
FROM satsen_catalog.satsen_sa_accounts.silver_calendar_events   c
JOIN satsen_catalog.satsen_sa_accounts.silver_sf_accounts       a ON a.account_name = c.account_name
LEFT JOIN satsen_catalog.satsen_sa_accounts.silver_sf_use_cases u ON u.account_id   = a.account_id
LEFT JOIN satsen_catalog.satsen_sa_accounts.silver_gmail_emails e ON e.account_name = c.account_name
WHERE c.timing_label IN ('THIS_WEEK', 'UPCOMING')
GROUP BY ALL
ORDER BY c.start_time;

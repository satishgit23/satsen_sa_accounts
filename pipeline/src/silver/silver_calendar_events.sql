-- Silver Layer: Google Calendar Events
-- Cleans raw events, calculates duration, labels past vs. upcoming.

CREATE OR REFRESH MATERIALIZED VIEW silver_calendar_events
COMMENT 'Cleaned calendar events with timing, duration, and account context'
CLUSTER BY (account_name, start_time)
AS
SELECT
  e.event_id,
  e.account_name,
  a.arr_tier,
  e.event_title,
  e.description,
  e.location,
  e.start_time,
  e.end_time,
  -- Duration in minutes
  CASE
    WHEN e.start_time IS NOT NULL AND e.end_time IS NOT NULL
    THEN ROUND((UNIX_TIMESTAMP(e.end_time) - UNIX_TIMESTAMP(e.start_time)) / 60.0, 0)
    ELSE NULL
  END                                               AS duration_minutes,
  -- Day of week for the event
  DATE_FORMAT(e.start_time, 'EEEE')                AS event_day_of_week,
  -- Past vs. upcoming classification
  CASE
    WHEN e.start_time < CURRENT_TIMESTAMP()        THEN 'PAST'
    WHEN e.start_time < CURRENT_TIMESTAMP()
         + INTERVAL 7 DAYS                         THEN 'THIS_WEEK'
    WHEN e.start_time < CURRENT_TIMESTAMP()
         + INTERVAL 30 DAYS                        THEN 'UPCOMING'
    ELSE 'FUTURE'
  END                                               AS timing_label,
  e.attendees,
  SIZE(e.attendees)                                 AS attendee_count,
  e.video_link,
  e.status,
  e.organizer_email,
  e.html_link,
  e._extracted_at                                   AS last_synced_at
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY _extracted_at DESC) AS rn
  FROM LIVE.raw_calendar_events
) e
LEFT JOIN LIVE.silver_sf_accounts a ON a.account_name = e.account_name
WHERE e.rn = 1
  AND e.event_id IS NOT NULL
  AND e.status != 'cancelled';

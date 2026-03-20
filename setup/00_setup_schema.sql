-- Databricks notebook source

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # SA Tracker — Schema Setup
-- MAGIC **Notebook:** `00_setup_schema`
-- MAGIC
-- MAGIC Creates the catalog schema, all Delta tables, staging tables, and the unified activity view.
-- MAGIC Run this notebook **once** before any pipeline notebooks, or on every run (all statements are idempotent).
-- MAGIC
-- MAGIC **Catalog:** `satsen_catalog`
-- MAGIC **Schema:**  `satsen_sa_accounts`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1 · Create Schema

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS satsen_catalog.satsen_sa_accounts
COMMENT 'SA Activity Tracker — Salesforce, Gmail, Google Calendar, and Slack data for Satish Senapathy';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2 · Salesforce — Staging (populated by fetch_salesforce_to_databricks.py)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS satsen_catalog.satsen_sa_accounts.sf_accounts_staging (
  account_id STRING, account_name STRING, type STRING, industry STRING, vertical STRING,
  arr DOUBLE, t3m_arr DOUBLE, number_of_employees INT, billing_city STRING, billing_state STRING,
  billing_country STRING, owner_name STRING, last_sa_engaged_id STRING, last_sa_engaged_name STRING,
  _extracted_at TIMESTAMP
) USING DELTA
COMMENT 'Staging for SF accounts — populated by local fetch_salesforce_to_databricks.py';

CREATE TABLE IF NOT EXISTS satsen_catalog.satsen_sa_accounts.sf_use_cases_staging (
  uco_id STRING, account_id STRING, account_name STRING, uco_name STRING, stage STRING,
  description STRING, use_case_type STRING, implementation_status STRING,
  go_live_date TIMESTAMP, estimated_project_go_live TIMESTAMP, last_modified_date TIMESTAMP,
  created_date TIMESTAMP, assigned_sa_id STRING, assigned_sa_name STRING, _extracted_at TIMESTAMP
) USING DELTA
COMMENT 'Staging for SF use cases — populated by local fetch_salesforce_to_databricks.py';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3 · Salesforce — Accounts (legacy)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS satsen_catalog.satsen_sa_accounts.sf_accounts (
  account_id            STRING    NOT NULL  COMMENT 'Salesforce Account ID (18-char)',
  account_name          STRING              COMMENT 'Account / Company name',
  type                  STRING              COMMENT 'Account type (Top 25, AWS, etc.)',
  industry              STRING              COMMENT 'Industry classification',
  vertical              STRING              COMMENT 'Databricks vertical (Mfg & Automotive, etc.)',
  arr                   DOUBLE              COMMENT 'Annual Recurring Revenue in USD',
  t3m_arr               DOUBLE              COMMENT 'Trailing 3-month ARR in USD',
  number_of_employees   INT                 COMMENT 'Employee headcount',
  billing_city          STRING              COMMENT 'Billing city',
  billing_state         STRING              COMMENT 'Billing state / province',
  billing_country       STRING              COMMENT 'Billing country',
  owner_name            STRING              COMMENT 'Account AE / Owner name',
  last_sa_engaged_id    STRING              COMMENT 'Salesforce User ID of last SA engaged',
  last_sa_engaged_name  STRING              COMMENT 'Full name of last SA engaged',
  ingested_at           TIMESTAMP           COMMENT 'UTC timestamp of last ingestion',
  source_system         STRING              COMMENT 'Source system (salesforce)'
)
USING DELTA
COMMENT 'Salesforce accounts where Satish Senapathy is Last SA Engaged'
TBLPROPERTIES (
  'delta.enableChangeDataFeed'        = 'true',
  'delta.autoOptimize.optimizeWrite'  = 'true'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4 · Salesforce — Use Cases (UCOs)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS satsen_catalog.satsen_sa_accounts.sf_use_cases (
  uco_id                  STRING    NOT NULL  COMMENT 'Salesforce UseCase__c ID',
  account_id              STRING              COMMENT 'Parent Salesforce Account ID',
  account_name            STRING              COMMENT 'Parent account name',
  uco_name                STRING              COMMENT 'Use Case name / title',
  stage                   STRING              COMMENT 'UCO lifecycle stage (U1 – U6)',
  description             STRING              COMMENT 'Use case description',
  use_case_type           STRING              COMMENT 'Type of use case (ML, Streaming, BI, etc.)',
  implementation_status   STRING              COMMENT 'Current implementation status',
  go_live_date            TIMESTAMP           COMMENT 'Expected or actual go-live date',
  full_production_date    TIMESTAMP           COMMENT 'Full production date',
  last_modified_date      TIMESTAMP           COMMENT 'Last modified date in Salesforce',
  created_date            TIMESTAMP           COMMENT 'Record created date in Salesforce',
  assigned_sa_id          STRING              COMMENT 'Assigned SA Salesforce user ID',
  assigned_sa_name        STRING              COMMENT 'Assigned SA full name',
  ingested_at             TIMESTAMP           COMMENT 'UTC timestamp of last ingestion',
  source_system           STRING              COMMENT 'Source system (salesforce)'
)
USING DELTA
COMMENT 'Salesforce Use Cases (UCOs) for accounts managed by Satish Senapathy'
TBLPROPERTIES (
  'delta.enableChangeDataFeed'        = 'true',
  'delta.autoOptimize.optimizeWrite'  = 'true'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5 · Gmail — Customer Emails

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS satsen_catalog.satsen_sa_accounts.gmail_customer_emails (
  message_id        STRING          NOT NULL  COMMENT 'Gmail message ID (unique)',
  thread_id         STRING                    COMMENT 'Gmail thread ID (groups replies)',
  account_name      STRING                    COMMENT 'Matched customer account name',
  customer_label    STRING                    COMMENT 'Customers/* Gmail label matched',
  from_email        STRING                    COMMENT 'Sender email address',
  from_name         STRING                    COMMENT 'Sender display name',
  to_emails         ARRAY<STRING>             COMMENT 'Primary recipient email addresses',
  cc_emails         ARRAY<STRING>             COMMENT 'CC email addresses',
  subject           STRING                    COMMENT 'Email subject line',
  body_snippet      STRING                    COMMENT 'Short snippet of email body (500 chars)',
  body_text         STRING                    COMMENT 'Full plain-text email body (4 000 chars)',
  received_date     TIMESTAMP                 COMMENT 'Date and time email was received (UTC)',
  labels            ARRAY<STRING>             COMMENT 'All Gmail labels applied to message',
  is_unread         BOOLEAN                   COMMENT 'True if email has not been read',
  needs_response    BOOLEAN                   COMMENT 'True if SA action / reply is required',
  has_attachment    BOOLEAN                   COMMENT 'True if email has file attachments',
  ingested_at       TIMESTAMP                 COMMENT 'UTC timestamp of last ingestion',
  source_system     STRING                    COMMENT 'Source system (gmail)'
)
USING DELTA
COMMENT 'Gmail emails tagged with Customers/* labels for each SA account'
TBLPROPERTIES (
  'delta.enableChangeDataFeed'        = 'true',
  'delta.autoOptimize.optimizeWrite'  = 'true'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6 · Google Calendar — Meeting Events

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS satsen_catalog.satsen_sa_accounts.calendar_events (
  event_id            STRING    NOT NULL  COMMENT 'Google Calendar event ID',
  account_name        STRING              COMMENT 'Matched customer account name',
  event_title         STRING              COMMENT 'Event / meeting title',
  agenda              STRING              COMMENT 'Event description / agenda (4 000 chars)',
  start_datetime      TIMESTAMP           COMMENT 'Event start date-time (UTC)',
  end_datetime        TIMESTAMP           COMMENT 'Event end date-time (UTC)',
  duration_minutes    INT                 COMMENT 'Meeting duration in minutes',
  attendees           ARRAY<STRUCT<
                        email:           STRING,
                        name:            STRING,
                        response_status: STRING
                      >>                  COMMENT 'Attendees with name, email, and RSVP status',
  organizer_email     STRING              COMMENT 'Organizer email address',
  organizer_name      STRING              COMMENT 'Organizer display name',
  meeting_link        STRING              COMMENT 'Google Meet / Zoom / Teams video link',
  location            STRING              COMMENT 'Physical or virtual location',
  event_status        STRING              COMMENT 'confirmed | tentative | cancelled',
  sa_response_status  STRING              COMMENT 'SA RSVP: accepted | declined | tentative',
  is_recurring        BOOLEAN             COMMENT 'True if this is part of a recurring series',
  calendar_id         STRING              COMMENT 'Google Calendar ID (primary, etc.)',
  ingested_at         TIMESTAMP           COMMENT 'UTC timestamp of last ingestion',
  source_system       STRING              COMMENT 'Source system (google_calendar)'
)
USING DELTA
COMMENT 'Google Calendar events matched to SA customer accounts'
TBLPROPERTIES (
  'delta.enableChangeDataFeed'        = 'true',
  'delta.autoOptimize.optimizeWrite'  = 'true'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7 · Slack — Messages

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS satsen_catalog.satsen_sa_accounts.slack_messages (
  message_ts          STRING    NOT NULL  COMMENT 'Slack message timestamp (acts as unique ID)',
  channel_id          STRING              COMMENT 'Slack channel ID',
  channel_name        STRING              COMMENT 'Slack channel name',
  account_name        STRING              COMMENT 'Customer account matched to channel / message text',
  sender_id           STRING              COMMENT 'Slack user ID of message sender',
  sender_name         STRING              COMMENT 'Slack display name of message sender',
  message_text        STRING              COMMENT 'Full message text (4 000 chars)',
  thread_ts           STRING              COMMENT 'Parent thread timestamp (null if root message)',
  is_thread_reply     BOOLEAN             COMMENT 'True if this message is a thread reply',
  reply_count         INT                 COMMENT 'Number of replies to this message',
  reactions           ARRAY<STRING>       COMMENT 'Emoji reaction names on the message',
  has_files           BOOLEAN             COMMENT 'True if message has file attachments',
  message_date        TIMESTAMP           COMMENT 'Parsed message timestamp (UTC)',
  ingested_at         TIMESTAMP           COMMENT 'UTC timestamp of last ingestion',
  source_system       STRING              COMMENT 'Source system (slack)'
)
USING DELTA
COMMENT 'Slack messages from customer-related channels where SA is a participant'
TBLPROPERTIES (
  'delta.enableChangeDataFeed'        = 'true',
  'delta.autoOptimize.optimizeWrite'  = 'true'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 8 · Ingestion Run Log

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS satsen_catalog.satsen_sa_accounts.ingestion_run_log (
  run_id        STRING     COMMENT 'Unique run identifier (UUID)',
  source        STRING     COMMENT 'Source system (salesforce | gmail | calendar | slack)',
  status        STRING     COMMENT 'SUCCESS | FAILED',
  rows_ingested BIGINT     COMMENT 'Number of rows upserted in this run',
  run_at        TIMESTAMP  COMMENT 'UTC timestamp when this run started',
  duration_sec  DOUBLE     COMMENT 'Run duration in seconds',
  error_detail  STRING     COMMENT 'Error message if status = FAILED'
)
USING DELTA
COMMENT 'Observability log for every ingestion run'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 9 · Unified Activity View

-- COMMAND ----------

CREATE OR REPLACE VIEW satsen_catalog.satsen_sa_accounts.sa_daily_activity
COMMENT 'Unified SA activity timeline across Gmail, Calendar, and Slack — ordered by most recent'
AS
SELECT
  'email'          AS activity_type,
  account_name,
  received_date    AS activity_date,
  subject          AS title,
  from_name        AS contact,
  body_snippet     AS summary,
  CASE
    WHEN needs_response = true  THEN 'ACTION REQUIRED'
    WHEN is_unread      = true  THEN 'UNREAD'
    ELSE                             'READ'
  END              AS status,
  message_id       AS source_id,
  ingested_at
FROM satsen_catalog.satsen_sa_accounts.gmail_customer_emails

UNION ALL

SELECT
  'meeting'        AS activity_type,
  account_name,
  start_datetime   AS activity_date,
  event_title      AS title,
  organizer_name   AS contact,
  COALESCE(agenda, '') AS summary,
  UPPER(event_status)  AS status,
  event_id         AS source_id,
  ingested_at
FROM satsen_catalog.satsen_sa_accounts.calendar_events

UNION ALL

SELECT
  'slack'          AS activity_type,
  account_name,
  message_date     AS activity_date,
  CONCAT('[', channel_name, '] ', LEFT(message_text, 120)) AS title,
  sender_name      AS contact,
  message_text     AS summary,
  'MESSAGE'        AS status,
  message_ts       AS source_id,
  ingested_at
FROM satsen_catalog.satsen_sa_accounts.slack_messages

ORDER BY activity_date DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 10 · Verify Setup

-- COMMAND ----------

SHOW TABLES IN satsen_catalog.satsen_sa_accounts;

-- COMMAND ----------

-- Quick row-count sanity check across all tables
SELECT 'sf_accounts'           AS table_name, COUNT(*) AS rows FROM satsen_catalog.satsen_sa_accounts.sf_accounts
UNION ALL
SELECT 'sf_use_cases',                         COUNT(*) FROM satsen_catalog.satsen_sa_accounts.sf_use_cases
UNION ALL
SELECT 'gmail_customer_emails',                COUNT(*) FROM satsen_catalog.satsen_sa_accounts.gmail_customer_emails
UNION ALL
SELECT 'calendar_events',                      COUNT(*) FROM satsen_catalog.satsen_sa_accounts.calendar_events
UNION ALL
SELECT 'slack_messages',                       COUNT(*) FROM satsen_catalog.satsen_sa_accounts.slack_messages
UNION ALL
SELECT 'ingestion_run_log',                    COUNT(*) FROM satsen_catalog.satsen_sa_accounts.ingestion_run_log
ORDER BY table_name;

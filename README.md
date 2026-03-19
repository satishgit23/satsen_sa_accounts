# SATSEN SA Accounts — Data Tracker

A Databricks-native pipeline that ingests Salesforce, Gmail, Google Calendar, and Slack data for Solution Architect Satish Senapathy and surfaces it as a unified daily activity dashboard.

---

## Architecture

```
External Sources          Extract (Python)         Transform (SQL DLT)       Consume (SQL Notebooks)
──────────────────        ─────────────────        ───────────────────        ───────────────────────
Salesforce  ──────┐
Gmail       ──────┤──► extract/*.py  ──► bronze  ──► silver  ──► gold  ──► notebooks/*.sql
Google Cal  ──────┤       (DLT)
Slack       ──────┘
```

### Medallion Layers

| Layer  | Location                | Format     | Description                             |
|--------|-------------------------|------------|-----------------------------------------|
| Bronze | `pipeline/src/bronze/`  | Python DLT | Raw API extraction → Delta tables       |
| Silver | `pipeline/src/silver/`  | SQL DLT    | Cleaned, validated, enriched            |
| Gold   | `pipeline/src/gold/`    | SQL DLT    | Business-ready materialized views       |

---

## Project Structure

```
satsen_sa_accounts/
├── setup/
│   └── 00_setup_schema.sql          ← DDL: create catalog, schema, tables
├── extract/                         ← Python API extraction (used by DLT bronze)
│   ├── salesforce_extract.py        ← Salesforce accounts & UCOs
│   ├── gmail_extract.py             ← Gmail customer-tagged emails
│   ├── calendar_extract.py          ← Google Calendar events
│   └── slack_extract.py             ← Slack customer channel messages
├── pipeline/
│   ├── pipeline.json                ← Lakeflow pipeline config
│   └── src/
│       ├── bronze/                  ← Python DLT: API → raw Delta tables
│       ├── silver/                  ← SQL DLT: clean + transform
│       └── gold/                    ← SQL DLT: business views
├── notebooks/                       ← SQL analysis notebooks
│   ├── 01_salesforce.sql            ← Account overview, UCO health
│   ├── 02_gmail.sql                 ← Emails needing response
│   ├── 03_calendar.sql              ← Meetings + prep context
│   ├── 04_slack.sql                 ← Customer Slack engagement
│   ├── 05_daily_activity.sql        ← Unified daily dashboard
│   └── 06_uco_tracker.sql           ← UCO pipeline + go-live forecast
├── job/
│   └── databricks.yml               ← Databricks Asset Bundle job config
└── requirements.txt
```

---

## Databricks Tables (satsen_catalog.satsen_sa_accounts)

### Bronze (raw)
| Table                  | Source            |
|------------------------|-------------------|
| `raw_sf_accounts`      | Salesforce        |
| `raw_sf_use_cases`     | Salesforce        |
| `raw_gmail_emails`     | Gmail             |
| `raw_calendar_events`  | Google Calendar   |
| `raw_slack_messages`   | Slack             |

### Silver (cleaned)
| Table                    | Description                              |
|--------------------------|------------------------------------------|
| `silver_sf_accounts`     | ARR tiers, location, SA metadata         |
| `silver_sf_use_cases`    | Stage ordering, stale/go-live flags      |
| `silver_gmail_emails`    | Action status (URGENT/PENDING), days old |
| `silver_calendar_events` | Duration, THIS_WEEK/UPCOMING labels      |
| `silver_slack_messages`  | Engagement scoring HIGH/MEDIUM/LOW       |

### Gold (business-ready)
| Table                 | Description                                        |
|-----------------------|----------------------------------------------------|
| `gold_sa_activity`    | Unified activity timeline (email + meeting + Slack)|
| `gold_account_health` | Per-account scorecard with 0–100 health score      |

---

## Pipeline

**Name:** SATSEN Accounts Ingest Pipeline  
**Pipeline ID:** `28b9d78f-1fc8-444b-89df-556670175352`  
**Schedule:** Daily at 7 AM CST  
**Catalog/Schema:** `satsen_catalog.satsen_sa_accounts`

### Run on-demand
```bash
databricks pipelines start-update 28b9d78f-1fc8-444b-89df-556670175352 --full-refresh
```

### Secrets required (scope: `satsen-sa-tracker`)
| Key                       | Description                         |
|---------------------------|-------------------------------------|
| `google_oauth_credentials`| Google OAuth JSON (auto-refreshes)  |
| `sf_access_token`         | Salesforce session token            |
| `sf_instance_url`         | Salesforce instance URL             |
| `slack_token`             | Slack Bot OAuth token (optional)    |

---

## Setup

```bash
pip install -r requirements.txt
databricks pipelines start-update 28b9d78f-1fc8-444b-89df-556670175352
```

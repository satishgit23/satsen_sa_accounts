# Databricks notebook source
# COMMAND ----------
"""
Bronze Layer — Salesforce
Reads from staging tables populated by fetch_salesforce_to_databricks.py (run locally in Cursor).
No direct Salesforce connection from Databricks — you fetch from SF locally and push to staging.
"""

import dlt

CATALOG = "satsen_catalog"
SCHEMA  = "satsen_sa_accounts"


# ── Bronze: raw Salesforce accounts (from staging) ───────────────────────────
@dlt.table(
    name    = "raw_sf_accounts",
    comment = "Raw Salesforce accounts — populated by local fetch_salesforce_to_databricks.py",
)
def raw_sf_accounts():
    return spark.table(f"{CATALOG}.{SCHEMA}.sf_accounts_staging")


# ── Bronze: raw Salesforce use cases (from staging) ──────────────────────────
@dlt.table(
    name    = "raw_sf_use_cases",
    comment = "Raw Salesforce UCOs — populated by local fetch_salesforce_to_databricks.py",
)
def raw_sf_use_cases():
    return spark.table(f"{CATALOG}.{SCHEMA}.sf_use_cases_staging")

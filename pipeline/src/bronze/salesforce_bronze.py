# Databricks notebook source
# COMMAND ----------
import subprocess, sys
subprocess.run([sys.executable, "-m", "pip", "install", "--quiet", "--disable-pip-version-check", "simple-salesforce"], check=True)

# COMMAND ----------
"""
Bronze Layer — Salesforce
Reads credentials from Databricks Secrets, calls Salesforce REST API,
and materializes raw accounts and use cases as pipeline-managed Delta tables.
"""

import base64
from datetime import datetime, timezone

import dlt
from databricks.sdk import WorkspaceClient
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField,
    StructType, TimestampType,
)

SECRET_SCOPE = spark.conf.get("secret_scope", "satsen-sa-tracker")
SA_EMAIL     = spark.conf.get("sa_email", "satish.senapathy@databricks.com")

SF_ACCOUNTS_QUERY = """
    SELECT Id, Name, Type, Industry, Vertical_New__c,
           ARR__c, T3M_ARR__c, NumberOfEmployees,
           BillingCity, BillingState, BillingCountry,
           Owner.Name,
           Last_SA_Engaged__c, Last_SA_Engaged__r.Name
    FROM Account
    WHERE Last_SA_Engaged__r.Email = '{email}'
    ORDER BY ARR__c DESC NULLS LAST
""".format(email=SA_EMAIL)

SF_UCO_QUERY = """
    SELECT Id, Name, Account__c, Account__r.Name,
           Stages__c, Description__c, Use_Case_Type__c,
           Implementation_Status__c,
           Go_Live_Date__c, Estimated_Project_Go_live__c,
           LastModifiedDate, CreatedDate,
           SAOwner__c, SAOwner__r.Name
    FROM UseCase__c
    WHERE Account__c IN (
        SELECT Id FROM Account WHERE Last_SA_Engaged__r.Email = '{email}'
    )
    ORDER BY LastModifiedDate DESC
""".format(email=SA_EMAIL)


def _get_secret(scope, key):
    w   = WorkspaceClient()
    raw = w.secrets.get_secret(scope, key).value
    try:
        return base64.b64decode(raw).decode("utf-8")
    except Exception:
        return raw


def _sf_client():
    from simple_salesforce import Salesforce

    # Databricks Salesforce enforces SSO — username/password auth is blocked.
    # Authenticate with the stored OAuth session_id (sf_access_token).
    # If the token has expired, run refresh_sf_token.py locally to update it.
    session_id   = _get_secret(SECRET_SCOPE, "sf_access_token")
    instance_url = _get_secret(SECRET_SCOPE, "sf_instance_url")

    if not session_id:
        raise RuntimeError(
            "sf_access_token secret is empty. "
            "Run refresh_sf_token.py to get a fresh token and update the secret."
        )

    return Salesforce(session_id=session_id, instance_url=instance_url)


def _to_dt(val):
    if not val:
        return None
    try:
        return datetime.fromisoformat(val.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


# ── Bronze: raw Salesforce accounts ─────────────────────────────────────────
@dlt.table(
    name    = "raw_sf_accounts",
    comment = "Raw Salesforce accounts — Last SA Engaged = Satish Senapathy",
)
def raw_sf_accounts():
    sf      = _sf_client()
    records = sf.query_all(SF_ACCOUNTS_QUERY).get("records", [])
    now     = datetime.now(timezone.utc)

    rows = [
        (
            r["Id"],
            r.get("Name"),
            r.get("Type"),
            r.get("Industry"),
            r.get("Vertical_New__c"),
            float(r["ARR__c"])          if r.get("ARR__c")           else None,
            float(r["T3M_ARR__c"])      if r.get("T3M_ARR__c")       else None,
            int(r["NumberOfEmployees"]) if r.get("NumberOfEmployees") else None,
            r.get("BillingCity"),
            r.get("BillingState"),
            r.get("BillingCountry"),
            (r.get("Owner") or {}).get("Name"),
            r.get("Last_SA_Engaged__c"),
            (r.get("Last_SA_Engaged__r") or {}).get("Name"),
            now,
        )
        for r in records
    ]

    schema = StructType([
        StructField("account_id",            StringType()),
        StructField("account_name",          StringType()),
        StructField("type",                  StringType()),
        StructField("industry",              StringType()),
        StructField("vertical",              StringType()),
        StructField("arr",                   DoubleType()),
        StructField("t3m_arr",               DoubleType()),
        StructField("number_of_employees",   IntegerType()),
        StructField("billing_city",          StringType()),
        StructField("billing_state",         StringType()),
        StructField("billing_country",       StringType()),
        StructField("owner_name",            StringType()),
        StructField("last_sa_engaged_id",    StringType()),
        StructField("last_sa_engaged_name",  StringType()),
        StructField("_extracted_at",         TimestampType()),
    ])

    return spark.createDataFrame(rows, schema)


# ── Bronze: raw Salesforce use cases (UCOs) ─────────────────────────────────
@dlt.table(
    name    = "raw_sf_use_cases",
    comment = "Raw Salesforce UCOs for SA-managed accounts",
)
def raw_sf_use_cases():
    sf      = _sf_client()
    records = sf.query_all(SF_UCO_QUERY).get("records", [])
    now     = datetime.now(timezone.utc)

    rows = [
        (
            r["Id"],
            r.get("Account__c"),
            (r.get("Account__r") or {}).get("Name"),
            r.get("Name"),
            r.get("Stages__c"),
            r.get("Description__c"),
            r.get("Use_Case_Type__c"),
            r.get("Implementation_Status__c"),
            _to_dt(r.get("Go_Live_Date__c")),
            _to_dt(r.get("Estimated_Project_Go_live__c")),
            _to_dt(r.get("LastModifiedDate")),
            _to_dt(r.get("CreatedDate")),
            r.get("SAOwner__c"),
            (r.get("SAOwner__r") or {}).get("Name"),
            now,
        )
        for r in records
    ]

    schema = StructType([
        StructField("uco_id",                StringType()),
        StructField("account_id",            StringType()),
        StructField("account_name",          StringType()),
        StructField("uco_name",              StringType()),
        StructField("stage",                 StringType()),
        StructField("description",           StringType()),
        StructField("use_case_type",         StringType()),
        StructField("implementation_status", StringType()),
        StructField("go_live_date",               TimestampType()),
        StructField("estimated_project_go_live",  TimestampType()),
        StructField("last_modified_date",    TimestampType()),
        StructField("created_date",          TimestampType()),
        StructField("assigned_sa_id",        StringType()),
        StructField("assigned_sa_name",      StringType()),
        StructField("_extracted_at",         TimestampType()),
    ])

    return spark.createDataFrame(rows, schema)

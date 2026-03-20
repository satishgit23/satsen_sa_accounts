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
import json
import urllib.parse
import urllib.request
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


def _secret_exists(scope, key):
    """Return True if the secret exists in the scope."""
    try:
        w = WorkspaceClient()
        for s in w.secrets.list_secrets(scope=scope):
            if s.key == key:
                return True
        return False
    except Exception:
        return False


def _get_secret_optional(scope, key):
    """Return secret value or None if the secret does not exist."""
    if not _secret_exists(scope, key):
        return None
    try:
        return _get_secret(scope, key)
    except Exception:
        return None


def _refresh_access_token():
    """Exchange refresh_token for a new access_token via OAuth 2.0."""
    if not _secret_exists(SECRET_SCOPE, "sf_refresh_token"):
        return None
    refresh_token = _get_secret(SECRET_SCOPE, "sf_refresh_token")
    client_id     = _get_secret_optional(SECRET_SCOPE, "sf_client_id")
    client_secret = _get_secret_optional(SECRET_SCOPE, "sf_client_secret")
    instance_url  = _get_secret_optional(SECRET_SCOPE, "sf_instance_url") or "https://databricks.my.salesforce.com"

    if not all([refresh_token, client_id, client_secret]):
        return None

    token_url = f"{instance_url.rstrip('/')}/services/oauth2/token"
    data = urllib.parse.urlencode({
        "grant_type":    "refresh_token",
        "refresh_token": refresh_token,
        "client_id":     client_id,
        "client_secret": client_secret,
    }).encode()

    req = urllib.request.Request(token_url, data=data, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    with urllib.request.urlopen(req) as resp:
        result = json.loads(resp.read().decode())
    return result.get("access_token"), result.get("instance_url", instance_url)


def _sf_client(access_token=None, instance_url=None):
    from simple_salesforce import Salesforce

    if access_token and instance_url:
        return Salesforce(session_id=access_token, instance_url=instance_url)

    # 1. Try refresh token first — get a fresh access token every time.
    try:
        tokens = _refresh_access_token()
        if tokens:
            access_token, instance_url = tokens
            if access_token:
                return Salesforce(session_id=access_token, instance_url=instance_url)
    except Exception:
        pass

    # 2. Fallback: use stored session_id (expires ~2h; run refresh_sf_token.py to get refresh token).
    session_id   = _get_secret(SECRET_SCOPE, "sf_access_token")
    instance_url = _get_secret(SECRET_SCOPE, "sf_instance_url")

    if not session_id:
        raise RuntimeError(
            "No valid Salesforce auth. Either:\n"
            "  A) Run refresh_sf_token.py with a Connected App to store sf_refresh_token, sf_client_id, sf_client_secret\n"
            "  B) Or store sf_access_token (session) manually — it expires in ~2 hours."
        )

    return Salesforce(session_id=session_id, instance_url=instance_url)


def _sf_query_all(query):
    """Run a Salesforce query with automatic retry on session expiry."""
    from simple_salesforce.exceptions import SalesforceExpiredSession

    def _is_session_expired(ex):
        if isinstance(ex, SalesforceExpiredSession):
            return True
        return "INVALID_SESSION_ID" in str(getattr(ex, "content", [])) or "Session expired" in str(ex)

    sf = _sf_client()
    try:
        return sf.query_all(query).get("records", [])
    except Exception as ex:
        if not _is_session_expired(ex):
            raise
        # Session expired — refresh token and retry with a new session.
        tokens = _refresh_access_token()
        if not tokens:
            raise RuntimeError(
                "Session expired and no refresh token configured. "
                "Run refresh_sf_token.py to store sf_refresh_token, sf_client_id, sf_client_secret."
            )
        access_token, instance_url = tokens
        sf = _sf_client(access_token=access_token, instance_url=instance_url)
        return sf.query_all(query).get("records", [])


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
    records = _sf_query_all(SF_ACCOUNTS_QUERY)
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
    records = _sf_query_all(SF_UCO_QUERY)
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

#!/usr/bin/env python3
"""
Fetch Salesforce data from your local machine and push to Databricks.

Runs in Cursor/IDE — you authenticate to Salesforce via browser (SID), then this
script pushes the data to Databricks. No need for Databricks to connect to Salesforce.

Usage:
    python3 fetch_salesforce_to_databricks.py SID
    python3 fetch_salesforce_to_databricks.py   # prompts for SID
    SF_SID=xxx python3 fetch_salesforce_to_databricks.py

Prerequisites:
    pip install simple-salesforce databricks-sdk
    databricks configure (or DATABRICKS_HOST + DATABRICKS_TOKEN)
"""

import json
import os
import subprocess
import sys
from datetime import datetime, timezone

SF_INSTANCE_URL = "https://databricks.my.salesforce.com"
SA_EMAIL = "satish.senapathy@databricks.com"
CATALOG = "satsen_catalog"
SCHEMA = "satsen_sa_accounts"

SF_ACCOUNTS_QUERY = f"""
    SELECT Id, Name, Type, Industry, Vertical_New__c,
           ARR__c, T3M_ARR__c, NumberOfEmployees,
           BillingCity, BillingState, BillingCountry,
           Owner.Name,
           Last_SA_Engaged__c, Last_SA_Engaged__r.Name
    FROM Account
    WHERE Last_SA_Engaged__r.Email = '{SA_EMAIL}'
    ORDER BY ARR__c DESC NULLS LAST
"""

SF_UCO_QUERY = f"""
    SELECT Id, Name, Account__c, Account__r.Name,
           Stages__c, Description__c, Use_Case_Type__c,
           Implementation_Status__c,
           Go_Live_Date__c, Estimated_Project_Go_live__c,
           LastModifiedDate, CreatedDate,
           SAOwner__c, SAOwner__r.Name
    FROM UseCase__c
    WHERE Account__c IN (
        SELECT Id FROM Account WHERE Last_SA_Engaged__r.Email = '{SA_EMAIL}'
    )
    ORDER BY LastModifiedDate DESC
"""


def _get_sid():
    sid = (sys.argv[1] if len(sys.argv) > 1 else os.environ.get("SF_SID", "")).strip()
    if sid:
        return sid
    print("Get SID: Log in to Salesforce → F12 → Application → Cookies → sid")
    return input("Paste SID: ").strip()


def _fetch_salesforce(session_id):
    from simple_salesforce import Salesforce

    sf = Salesforce(session_id=session_id, instance_url=SF_INSTANCE_URL)
    accounts = sf.query_all(SF_ACCOUNTS_QUERY).get("records", [])
    use_cases = sf.query_all(SF_UCO_QUERY).get("records", [])
    return accounts, use_cases


def _to_sql_str(val):
    if val is None:
        return "NULL"
    if isinstance(val, (int, float)):
        return str(val) if val is not None else "NULL"
    s = str(val).replace("\\", "\\\\").replace("'", "''")
    return f"'{s}'"


def _to_ts(val):
    if not val:
        return None
    try:
        dt = datetime.fromisoformat(str(val).replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def _run_sql(warehouse_id, statement):
    import tempfile
    payload = {
        "warehouse_id": warehouse_id,
        "statement": statement,
        "wait_timeout": "50s",
    }
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(payload, f)
        path = f.name
    try:
        r = subprocess.run(
            ["databricks", "api", "post", "/api/2.0/sql/statements", "--json", f"@{path}"],
            capture_output=True,
            text=True,
        )
    finally:
        os.unlink(path)
    if r.returncode != 0:
        raise RuntimeError(f"SQL failed: {r.stderr}")
    out = json.loads(r.stdout)
    if out.get("status", {}).get("state") == "FAILED":
        raise RuntimeError(out.get("status", {}).get("error", {}).get("message", "Unknown error"))
    return out


def _get_warehouse_id():
    r = subprocess.run(
        ["databricks", "api", "get", "/api/2.0/sql/warehouses"],
        capture_output=True,
        text=True,
    )
    data = json.loads(r.stdout)
    for w in data.get("warehouses", []):
        if w.get("state") == "RUNNING":
            return w["id"]
    return data.get("warehouses", [{}])[0].get("id")


def _push_to_databricks(accounts, use_cases):
    wh = _get_warehouse_id()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # Create staging tables
    create_accounts = f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.sf_accounts_staging (
        account_id STRING, account_name STRING, type STRING, industry STRING, vertical STRING,
        arr DOUBLE, t3m_arr DOUBLE, number_of_employees INT, billing_city STRING, billing_state STRING,
        billing_country STRING, owner_name STRING, last_sa_engaged_id STRING, last_sa_engaged_name STRING,
        _extracted_at TIMESTAMP
    ) USING DELTA
    """
    create_ucos = f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.sf_use_cases_staging (
        uco_id STRING, account_id STRING, account_name STRING, uco_name STRING, stage STRING,
        description STRING, use_case_type STRING, implementation_status STRING,
        go_live_date TIMESTAMP, estimated_project_go_live TIMESTAMP, last_modified_date TIMESTAMP,
        created_date TIMESTAMP, assigned_sa_id STRING, assigned_sa_name STRING, _extracted_at TIMESTAMP
    ) USING DELTA
    """
    _run_sql(wh, create_accounts)
    _run_sql(wh, create_ucos)

    # Truncate and insert accounts
    _run_sql(wh, f"TRUNCATE TABLE {CATALOG}.{SCHEMA}.sf_accounts_staging")
    for r in accounts:
        arr = float(r["ARR__c"]) if r.get("ARR__c") else None
        t3m = float(r["T3M_ARR__c"]) if r.get("T3M_ARR__c") else None
        emp = int(r["NumberOfEmployees"]) if r.get("NumberOfEmployees") else None
        vals = (
            _to_sql_str(r.get("Id")),
            _to_sql_str(r.get("Name")),
            _to_sql_str(r.get("Type")),
            _to_sql_str(r.get("Industry")),
            _to_sql_str(r.get("Vertical_New__c")),
            str(arr) if arr is not None else "NULL",
            str(t3m) if t3m is not None else "NULL",
            str(emp) if emp is not None else "NULL",
            _to_sql_str(r.get("BillingCity")),
            _to_sql_str(r.get("BillingState")),
            _to_sql_str(r.get("BillingCountry")),
            _to_sql_str((r.get("Owner") or {}).get("Name")),
            _to_sql_str(r.get("Last_SA_Engaged__c")),
            _to_sql_str((r.get("Last_SA_Engaged__r") or {}).get("Name")),
            f"TIMESTAMP('{now}')",
        )
        stmt = f"INSERT INTO {CATALOG}.{SCHEMA}.sf_accounts_staging VALUES ({','.join(vals)})"
        _run_sql(wh, stmt)

    # Truncate and insert use cases
    _run_sql(wh, f"TRUNCATE TABLE {CATALOG}.{SCHEMA}.sf_use_cases_staging")
    for r in use_cases:
        go_live = _to_ts(r.get("Go_Live_Date__c"))
        est_go = _to_ts(r.get("Estimated_Project_Go_live__c"))
        last_mod = _to_ts(r.get("LastModifiedDate"))
        created = _to_ts(r.get("CreatedDate"))
        vals = (
            _to_sql_str(r.get("Id")),
            _to_sql_str(r.get("Account__c")),
            _to_sql_str((r.get("Account__r") or {}).get("Name")),
            _to_sql_str(r.get("Name")),
            _to_sql_str(r.get("Stages__c")),
            _to_sql_str(r.get("Description__c")),
            _to_sql_str(r.get("Use_Case_Type__c")),
            _to_sql_str(r.get("Implementation_Status__c")),
            f"TIMESTAMP('{go_live}')" if go_live else "NULL",
            f"TIMESTAMP('{est_go}')" if est_go else "NULL",
            f"TIMESTAMP('{last_mod}')" if last_mod else "NULL",
            f"TIMESTAMP('{created}')" if created else "NULL",
            _to_sql_str(r.get("SAOwner__c")),
            _to_sql_str((r.get("SAOwner__r") or {}).get("Name")),
            f"TIMESTAMP('{now}')",
        )
        stmt = f"INSERT INTO {CATALOG}.{SCHEMA}.sf_use_cases_staging VALUES ({','.join(vals)})"
        _run_sql(wh, stmt)


def main():
    print("=" * 60)
    print("  Salesforce → Databricks (local fetch)")
    print("=" * 60)

    sid = _get_sid()
    if not sid:
        print("ERROR: No SID provided.")
        sys.exit(1)

    print("\n1. Fetching from Salesforce...")
    accounts, use_cases = _fetch_salesforce(sid)
    print(f"   Accounts: {len(accounts)}, Use cases: {len(use_cases)}")

    print("\n2. Pushing to Databricks...")
    _push_to_databricks(accounts, use_cases)

    print("\n✓ Done. Run the pipeline to process the data, or it will pick up on next scheduled run.")


if __name__ == "__main__":
    main()

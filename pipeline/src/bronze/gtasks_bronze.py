# Databricks notebook source

"""
Bronze Layer — Google Tasks (Full Refresh)
Fetches all pending tasks from all Google Task lists and performs a full
refresh each run. The silver layer cleans and normalises the data.

Full refresh is appropriate because:
  - Tasks are typically few in number (< 500)
  - Completion / deletion are important state changes that need to be reflected
  - No reliable cursor/incremental field is available across all task lists
"""

import base64 as b64lib
import json
import re
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Optional

import dlt
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

SECRET_SCOPE = spark.conf.get("secret_scope", "satsen-sa-tracker")

ACCOUNT_KEYWORDS = {
    "Milacron":            ["milacron"],
    "Analog Devices":      ["analog devices", "adi", "analogdevices"],
    "Timken Company":      ["timken"],
    "Dana Incorporated":   ["dana inc", "dana incorporated", "dana-inc"],
    "Wabtec":              ["wabtec"],
    "Hillenbrand":         ["hillenbrand"],
    "Ciena":               ["ciena"],
    "Penske":              ["penske"],
    "Air Products":        ["air products", "air-products"],
}

_SCHEMA = StructType([
    StructField("task_id",        StringType()),
    StructField("parent_task_id", StringType()),
    StructField("tasklist_id",    StringType()),
    StructField("tasklist_name",  StringType()),
    StructField("title",          StringType()),
    StructField("notes",          StringType()),
    StructField("status",         StringType()),
    StructField("due_date",       StringType()),
    StructField("account_name",   StringType()),
    StructField("updated",        StringType()),
    StructField("_extracted_at",  TimestampType()),
])


def _get_secret(scope: str, key: str) -> str:
    from databricks.sdk import WorkspaceClient
    w   = WorkspaceClient()
    raw = w.secrets.get_secret(scope, key).value
    try:
        return b64lib.b64decode(raw).decode("utf-8")
    except Exception:
        return raw


def _get_token() -> str:
    raw_creds = _get_secret(SECRET_SCOPE, "google_oauth_credentials")
    creds = json.loads(raw_creds)
    data  = urllib.parse.urlencode({
        "client_id":     creds["client_id"],
        "client_secret": creds["client_secret"],
        "refresh_token": creds["refresh_token"],
        "grant_type":    "refresh_token",
    }).encode()
    req = urllib.request.Request(
        "https://oauth2.googleapis.com/token",
        data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())["access_token"]


def _match_account(text: str) -> Optional[str]:
    text_lower = text.lower()
    for account, keywords in ACCOUNT_KEYWORDS.items():
        if any(kw in text_lower for kw in keywords):
            return account
    return None


def _fetch_tasks():
    now   = datetime.now(timezone.utc)
    token = _get_token()
    hdrs  = {
        "Authorization":      f"Bearer {token}",
        "x-goog-user-project": "gcp-sandbox-field-eng",
    }

    def _get(url: str):
        with urllib.request.urlopen(urllib.request.Request(url, headers=hdrs)) as r:
            return json.loads(r.read())

    task_lists = _get("https://tasks.googleapis.com/tasks/v1/users/@me/lists")
    rows = []
    for tl in task_lists.get("items", []):
        tl_id   = tl["id"]
        tl_name = tl.get("title", "My Tasks")
        # showHidden=true ensures subtasks are returned alongside parent tasks
        tasks   = _get(
            f"https://tasks.googleapis.com/tasks/v1/lists/{tl_id}/tasks"
            f"?showCompleted=false&showHidden=true&maxResults=100"
        )
        for t in tasks.get("items", []):
            if t.get("status") == "completed":
                continue
            title   = t.get("title", "") or ""
            notes   = t.get("notes", "") or ""
            account = _match_account(f"{title} {notes}")
            rows.append((
                t["id"],
                t.get("parent") or None,
                tl_id,
                tl_name,
                title,
                notes,
                t.get("status", "needsAction"),
                (t.get("due") or "")[:10],
                account,
                t.get("updated", ""),
                now,
            ))
    return spark.createDataFrame(rows, _SCHEMA)


@dlt.table(
    name    = "raw_gtasks",
    comment = "Pending Google Tasks — full refresh on every pipeline run",
    schema  = _SCHEMA,
)
def raw_gtasks():
    return _fetch_tasks()

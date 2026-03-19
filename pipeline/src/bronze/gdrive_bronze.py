# Databricks notebook source

"""
Bronze Layer — Google Drive Account Docs (Full Refresh)
Reads all Google Docs from the Accounts folder in Google Drive and stores
the first 4000 characters of each document's text for action-item extraction.

The Accounts folder is structured as:
  Accounts/
    <Account Name>/
      <Document>.gdoc   ← fetched here
      <Sub-folder>/
        <Document>.gdoc ← fetched recursively

Full refresh on every run (documents change infrequently and the set is small).
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

SECRET_SCOPE   = spark.conf.get("secret_scope", "satsen-sa-tracker")
ACCOUNTS_ROOT  = "15P6JnTjpWG9FOCludzm1ivtkv_jzZOpa"
ACCOUNT_MAP    = {
    "Tenneco":        "Tenneco",
    "DRiV":           "Tenneco",
    "Analog Devices": "Analog Devices",
    "Wabtec":         "Wabtec",
    "Timken":         "Timken Company",
    "Penske":         "Penske",
    "Milacron":       "Milacron",
    "Ciena":          "Ciena",
    "Air Products":   "Air Products",
}

_SCHEMA = StructType([
    StructField("file_id",       StringType()),
    StructField("file_name",     StringType()),
    StructField("account_name",  StringType()),
    StructField("modified_time", StringType()),
    StructField("content",       StringType()),
    StructField("_extracted_at", TimestampType()),
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


def _get_account(folder_name: str) -> str:
    for k, v in ACCOUNT_MAP.items():
        if k.lower() in folder_name.lower():
            return v
    return folder_name


def _fetch_docs():
    now   = datetime.now(timezone.utc)
    token = _get_token()
    hdrs  = {
        "Authorization":       f"Bearer {token}",
        "x-goog-user-project": "gcp-sandbox-field-eng",
    }

    def _get(url: str):
        with urllib.request.urlopen(urllib.request.Request(url, headers=hdrs)) as r:
            return json.loads(r.read())

    def _export_text(file_id: str) -> str:
        url = (f"https://www.googleapis.com/drive/v3/files/{file_id}"
               f"/export?mimeType=text/plain")
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=hdrs)) as r:
                raw = r.read().decode("utf-8", errors="replace")
            return re.sub(r'\n{3,}', '\n\n', raw).strip()[:4000]
        except Exception:
            return ""

    def _list_recursive(folder_id: str, account_name: str):
        q   = urllib.parse.quote(f"'{folder_id}' in parents and trashed=false")
        res = _get(
            f"https://www.googleapis.com/drive/v3/files"
            f"?q={q}&fields=files(id,name,mimeType,modifiedTime)&pageSize=100"
        )
        items = []
        for f in res.get("files", []):
            mime = f.get("mimeType", "")
            if mime == "application/vnd.google-apps.folder":
                items.extend(_list_recursive(f["id"], account_name))
            elif mime == "application/vnd.google-apps.document":
                items.append((f["id"], f["name"], account_name,
                               f.get("modifiedTime", "")[:10]))
        return items

    # List account subfolders
    q = urllib.parse.quote(
        f"'{ACCOUNTS_ROOT}' in parents "
        f"and mimeType='application/vnd.google-apps.folder' and trashed=false"
    )
    subfolders = _get(
        f"https://www.googleapis.com/drive/v3/files"
        f"?q={q}&fields=files(id,name)&pageSize=50"
    )

    rows = []
    for sf in subfolders.get("files", []):
        acct  = _get_account(sf["name"])
        for file_id, file_name, account, modified in _list_recursive(sf["id"], acct):
            content = _export_text(file_id)
            if not content:
                continue
            rows.append((file_id, file_name, account, modified, content, now))

    return spark.createDataFrame(rows, _SCHEMA)


@dlt.table(
    name    = "raw_gdrive_docs",
    comment = "Google Docs text content from Account folders — full refresh",
    schema  = _SCHEMA,
)
def raw_gdrive_docs():
    return _fetch_docs()

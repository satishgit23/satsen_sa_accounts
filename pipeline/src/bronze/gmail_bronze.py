# Databricks notebook source
# MAGIC %pip install google-auth google-auth-httplib2 requests

# COMMAND ----------

"""
Bronze Layer — Gmail (Incremental Append)
Fetches only the PREVIOUS DAY's customer-tagged emails via Gmail API and
appends them to a persistent streaming table. Re-runs are safe — the silver
layer deduplicates by message_id using ROW_NUMBER().

Lookback: newer_than:2d (yesterday + 1-day safety buffer for late-arriving mail)
Volume:   up to 50 emails per customer label per run (9 labels = max 450/day)
"""

import base64 as b64lib
import json
import re
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from email.utils import parseaddr, parsedate_to_datetime

import dlt
from databricks.sdk import WorkspaceClient
from pyspark.sql.types import (
    ArrayType, BooleanType, StringType,
    StructField, StructType, TimestampType,
)

SECRET_SCOPE  = spark.conf.get("secret_scope", "satsen-sa-tracker")
SA_EMAIL      = spark.conf.get("sa_email", "satish.senapathy@databricks.com")
QUOTA_PROJECT = "gcp-sandbox-field-eng"
GMAIL_BASE    = "https://gmail.googleapis.com/gmail/v1/users/me"

CUSTOMER_LABELS = {
    "Label_1119997633843409654": "Milacron",
    "Label_2823845897392377105": "Analog Devices",
    "Label_3759231143514332816": "Timken Company",
    "Label_5065395456085932653": "Dana Incorporated",
    "Label_5122355398108776028": "Wabtec",
    "Label_566547237843183472":  "Hillenbrand",
    "Label_6364007740675531536": "Ciena",
    "Label_6598175870726516929": "Penske",
    "Label_7917940092878100372": "Air Products",
}

NO_REPLY_RE = re.compile(
    r"noreply|no-reply|notifications@|alerts@|jira@|confluence@|"
    r"drive-shares|gemini-notes|calendar-notification|sfdc\.user",
    re.I,
)
NO_REPLY_SUBJECTS = [
    "invitation:", "updated invitation:", "canceled event",
    "icymi", "all hands", "outage", "survey", "shared with you",
]

_SCHEMA = StructType([
    StructField("message_id",     StringType()),
    StructField("thread_id",      StringType()),
    StructField("account_name",   StringType()),
    StructField("customer_label", StringType()),
    StructField("from_email",     StringType()),
    StructField("from_name",      StringType()),
    StructField("to_emails",      ArrayType(StringType())),
    StructField("cc_emails",      ArrayType(StringType())),
    StructField("subject",        StringType()),
    StructField("body_snippet",   StringType()),
    StructField("body_text",      StringType()),
    StructField("received_date",  TimestampType()),
    StructField("labels",         ArrayType(StringType())),
    StructField("is_unread",      BooleanType()),
    StructField("needs_response", BooleanType()),
    StructField("has_attachment", BooleanType()),
    StructField("_extracted_at",  TimestampType()),
])


def _get_secret(scope, key):
    w   = WorkspaceClient()
    raw = w.secrets.get_secret(scope, key).value
    try:
        return b64lib.b64decode(raw).decode("utf-8")
    except Exception:
        return raw


def _google_token():
    creds_json = _get_secret(SECRET_SCOPE, "google_oauth_credentials")
    creds_dict = json.loads(creds_json)

    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request

    creds = Credentials(
        token         = None,
        refresh_token = creds_dict["refresh_token"],
        token_uri     = "https://oauth2.googleapis.com/token",
        client_id     = creds_dict["client_id"],
        client_secret = creds_dict["client_secret"],
    )
    creds.refresh(Request())
    return creds.token


def _gmail(token, path, params=None):
    url = f"{GMAIL_BASE}/{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={
        "Authorization":       f"Bearer {token}",
        "x-goog-user-project": QUOTA_PROJECT,
    })
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())


def _body(payload):
    if payload.get("mimeType") == "text/plain":
        data = payload.get("body", {}).get("data", "")
        if data:
            return b64lib.urlsafe_b64decode(data + "==").decode("utf-8", errors="replace")
    for part in payload.get("parts", []):
        text = _body(part)
        if text:
            return text
    return ""


def _needs_response(hmap):
    _, fe = parseaddr(hmap.get("from", ""))
    if SA_EMAIL.lower() in fe.lower():
        return False
    if NO_REPLY_RE.search(fe):
        return False
    subj = hmap.get("subject", "").lower()
    if any(kw in subj for kw in NO_REPLY_SUBJECTS):
        return False
    return True


def _fetch_emails():
    """Fetch yesterday's emails (newer_than:2d for safety buffer) across all customer labels."""
    token = _google_token()
    now   = datetime.now(timezone.utc)
    rows  = []
    seen  = set()

    for label_id, account_name in CUSTOMER_LABELS.items():
        try:
            resp    = _gmail(token, "messages", {
                "labelIds":  label_id,
                "q":         "newer_than:2d",   # yesterday + 1-day safety buffer
                "maxResults": 50,               # max 50 per label per run
            })
            msg_ids = [m["id"] for m in resp.get("messages", [])]
        except Exception:
            continue

        for mid in msg_ids:
            if mid in seen:
                continue
            seen.add(mid)
            try:
                data = _gmail(token, f"messages/{mid}", {"format": "full"})
            except Exception:
                continue

            headers = data.get("payload", {}).get("headers", [])
            hmap    = {h["name"].lower(): h["value"] for h in headers}
            labels  = data.get("labelIds", [])

            fn, fe  = parseaddr(hmap.get("from", ""))
            to_list = [parseaddr(x.strip())[1] for x in hmap.get("to", "").split(",") if x.strip()]
            cc_list = [parseaddr(x.strip())[1] for x in hmap.get("cc", "").split(",") if x.strip()]

            try:
                recv = parsedate_to_datetime(hmap.get("date", "")).astimezone(timezone.utc)
            except Exception:
                recv = now

            body    = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", _body(data.get("payload", {})))).strip()[:4000]
            has_att = any(p.get("filename") for p in data.get("payload", {}).get("parts", []))

            rows.append((
                mid,
                data.get("threadId"),
                account_name,
                account_name,
                fe,
                fn or fe,
                to_list,
                cc_list,
                hmap.get("subject", "(no subject)"),
                data.get("snippet", "")[:500],
                body,
                recv,
                labels,
                "UNREAD" in labels,
                _needs_response(hmap),
                has_att,
                now,
            ))

    return spark.createDataFrame(rows, _SCHEMA)


# ── Persistent streaming table (accumulates history over time) ───────────────
dlt.create_streaming_live_table(
    name    = "raw_gmail_emails",
    comment = "Raw Gmail customer emails — incremental daily append, deduplicated in silver",
    schema  = _SCHEMA,
)

# ── Daily append flow: fetches only the previous day's emails ────────────────
@dlt.append_flow(target="raw_gmail_emails")
def gmail_incremental():
    return _fetch_emails()

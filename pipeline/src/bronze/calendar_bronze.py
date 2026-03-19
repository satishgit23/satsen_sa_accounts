# Databricks notebook source
# MAGIC %pip install google-auth google-auth-httplib2 requests

# COMMAND ----------

"""
Bronze Layer — Google Calendar
Reads Google OAuth credentials from Databricks Secrets, fetches events
matched to customer accounts, and materializes as a raw pipeline-managed table.
"""

import base64 as b64lib
import json
import re
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Optional

import dlt
from databricks.sdk import WorkspaceClient
from pyspark.sql.types import (
    ArrayType, StringType, StructField,
    StructType, TimestampType,
)

SECRET_SCOPE   = spark.conf.get("secret_scope", "satsen-sa-tracker")
QUOTA_PROJECT  = "gcp-sandbox-field-eng"
CALENDAR_BASE  = "https://www.googleapis.com/calendar/v3"
LOOKBACK_DAYS  = 30
LOOKAHEAD_DAYS = 60

ACCOUNT_KEYWORDS = {
    "Milacron":            ["milacron"],
    "Analog Devices":      ["analog devices", "adi"],
    "Timken Company":      ["timken"],
    "Dana Incorporated":   ["dana inc", "dana incorporated", "dana"],
    "Wabtec":              ["wabtec"],
    "Hillenbrand":         ["hillenbrand"],
    "Ciena":               ["ciena"],
    "Penske":              ["penske"],
    "Air Products":        ["air products"],
}

VIDEO_LINK_RE = re.compile(
    r"https?://(?:meet\.google\.com|zoom\.us/j|teams\.microsoft\.com)[^\s\"<>]*",
    re.I,
)


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


def _calendar_get(token, path, params=None):
    url = f"{CALENDAR_BASE}/{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={
        "Authorization":       f"Bearer {token}",
        "x-goog-user-project": QUOTA_PROJECT,
    })
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())


def _match_account(text: str) -> Optional[str]:
    text_lower = text.lower()
    for account, keywords in ACCOUNT_KEYWORDS.items():
        if any(kw in text_lower for kw in keywords):
            return account
    return None


def _video_link(event: dict) -> Optional[str]:
    for source in [
        event.get("location", "") or "",
        event.get("description", "") or "",
        event.get("hangoutLink", "") or "",
    ]:
        m = VIDEO_LINK_RE.search(source)
        if m:
            return m.group(0)
    return None


def _parse_dt(val: Optional[str]) -> Optional[datetime]:
    if not val:
        return None
    try:
        return datetime.fromisoformat(val.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        try:
            return datetime.fromisoformat(val).astimezone(timezone.utc)
        except Exception:
            return None


# ── Bronze: raw calendar events ─────────────────────────────────────────────
@dlt.table(
    name    = "raw_calendar_events",
    comment = "Raw Google Calendar events matched to customer accounts",
)
def raw_calendar_events():
    token = _google_token()
    now   = datetime.now(timezone.utc)

    time_min = (now - timedelta(days=LOOKBACK_DAYS)).isoformat()
    time_max = (now + timedelta(days=LOOKAHEAD_DAYS)).isoformat()

    params     = {
        "timeMin":      time_min,
        "timeMax":      time_max,
        "maxResults":   250,
        "singleEvents": True,
        "orderBy":      "startTime",
    }

    events, page_token = [], None
    while True:
        if page_token:
            params["pageToken"] = page_token
        resp = _calendar_get(token, "calendars/primary/events", params)
        events.extend(resp.get("items", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    rows = []
    for ev in events:
        title   = ev.get("summary", "") or ""
        desc    = ev.get("description", "") or ""
        loc     = ev.get("location", "") or ""
        account = _match_account(f"{title} {desc} {loc}")

        if not account:
            for att in (ev.get("attendees") or []):
                domain = (att.get("email", "") or "").split("@")[-1].lower()
                account = _match_account(domain)
                if account:
                    break

        if not account:
            continue

        attendee_list = [
            f"{a.get('email', '')}|{a.get('responseStatus', '')}|{a.get('displayName', '')}"
            for a in (ev.get("attendees") or [])
        ]
        start_raw = ev.get("start", {})
        end_raw   = ev.get("end", {})
        start_dt  = _parse_dt(start_raw.get("dateTime") or start_raw.get("date"))
        end_dt    = _parse_dt(end_raw.get("dateTime")   or end_raw.get("date"))

        rows.append((
            ev.get("id"),
            account,
            title,
            desc[:2000] if desc else None,
            loc or None,
            start_dt,
            end_dt,
            attendee_list,
            _video_link(ev),
            ev.get("status"),
            (ev.get("organizer") or {}).get("email"),
            ev.get("htmlLink"),
            now,
        ))

    schema = StructType([
        StructField("event_id",        StringType()),
        StructField("account_name",    StringType()),
        StructField("event_title",     StringType()),
        StructField("description",     StringType()),
        StructField("location",        StringType()),
        StructField("start_time",      TimestampType()),
        StructField("end_time",        TimestampType()),
        StructField("attendees",       ArrayType(StringType())),
        StructField("video_link",      StringType()),
        StructField("status",          StringType()),
        StructField("organizer_email", StringType()),
        StructField("html_link",       StringType()),
        StructField("_extracted_at",   TimestampType()),
    ])

    return spark.createDataFrame(rows, schema)

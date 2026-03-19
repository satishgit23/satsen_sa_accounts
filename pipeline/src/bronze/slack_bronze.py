# Databricks notebook source
# MAGIC %pip install slack-sdk

# COMMAND ----------

"""
Bronze Layer — Slack (Incremental Append)
Fetches only the PREVIOUS DAY's customer-channel messages via Slack API and
appends them to a persistent streaming table. Re-runs are safe — the silver
layer deduplicates by message_ts using ROW_NUMBER().

Lookback: 2 days (yesterday + 1-day safety buffer for late-arriving messages)
"""

import base64 as b64lib
from datetime import datetime, timedelta, timezone
from typing import Optional

import dlt
from pyspark.sql.types import (
    ArrayType, BooleanType, IntegerType, StringType,
    StructField, StructType, TimestampType,
)

SECRET_SCOPE = spark.conf.get("secret_scope", "satsen-sa-tracker")

ACCOUNT_KEYWORDS = {
    "Milacron":            ["milacron"],
    "Analog Devices":      ["analog devices", "adi", "analogdevices"],
    "Timken Company":      ["timken"],
    "Dana Incorporated":   ["dana-inc", "dana_inc", "dana"],
    "Wabtec":              ["wabtec"],
    "Hillenbrand":         ["hillenbrand"],
    "Ciena":               ["ciena"],
    "Penske":              ["penske"],
    "Air Products":        ["air-products", "air_products", "airproducts"],
}

_SCHEMA = StructType([
    StructField("message_ts",    StringType()),
    StructField("channel_id",    StringType()),
    StructField("channel_name",  StringType()),
    StructField("account_name",  StringType()),
    StructField("user_id",       StringType()),
    StructField("message_text",  StringType()),
    StructField("sent_at",       TimestampType()),
    StructField("thread_ts",     StringType()),
    StructField("is_reply",      BooleanType()),
    StructField("reply_count",   IntegerType()),
    StructField("reactions",     ArrayType(StringType())),
    StructField("_extracted_at", TimestampType()),
])


def _get_secret(scope, key):
    from databricks.sdk import WorkspaceClient
    w   = WorkspaceClient()
    raw = w.secrets.get_secret(scope, key).value
    try:
        return b64lib.b64decode(raw).decode("utf-8")
    except Exception:
        return raw


def _slack_client():
    try:
        from slack_sdk import WebClient
        token = _get_secret(SECRET_SCOPE, "slack_token")
        return WebClient(token=token)
    except Exception:
        return None


def _match_account(text: str) -> Optional[str]:
    text_lower = text.lower()
    for account, keywords in ACCOUNT_KEYWORDS.items():
        if any(kw in text_lower for kw in keywords):
            return account
    return None


def _fetch_messages():
    """Fetch messages from the last 2 days across all customer-related channels."""
    client = _slack_client()
    now    = datetime.now(timezone.utc)

    if not client:
        return spark.createDataFrame([], _SCHEMA)

    # Yesterday + 1-day safety buffer
    oldest = (now - timedelta(days=2)).timestamp()
    rows   = []
    seen   = set()

    channels_cursor = None
    all_channels    = []
    while True:
        resp = client.conversations_list(
            types  = "public_channel,private_channel",
            limit  = 200,
            cursor = channels_cursor,
        )
        all_channels.extend(resp.get("channels", []))
        channels_cursor = (resp.get("response_metadata") or {}).get("next_cursor")
        if not channels_cursor:
            break

    for ch in all_channels:
        ch_name    = ch.get("name", "") or ""
        ch_purpose = ((ch.get("purpose") or {}).get("value") or "")
        account    = _match_account(f"{ch_name} {ch_purpose}")
        if not account:
            continue

        try:
            hist_cursor = None
            while True:
                hist = client.conversations_history(
                    channel = ch["id"],
                    oldest  = oldest,
                    limit   = 100,
                    cursor  = hist_cursor,
                )
                for msg in hist.get("messages", []):
                    mid = msg.get("ts")
                    if not mid or mid in seen:
                        continue
                    seen.add(mid)
                    sent   = datetime.fromtimestamp(float(mid), tz=timezone.utc)
                    thread = msg.get("thread_ts")
                    rxns   = [r["name"] for r in (msg.get("reactions") or [])]
                    rows.append((
                        mid,
                        ch["id"],
                        ch_name,
                        account,
                        msg.get("user") or msg.get("bot_id"),
                        (msg.get("text") or "")[:2000],
                        sent,
                        thread,
                        bool(thread and thread != mid),
                        int(msg.get("reply_count") or 0),
                        rxns,
                        now,
                    ))

                hist_cursor = (hist.get("response_metadata") or {}).get("next_cursor")
                if not hist_cursor:
                    break
        except Exception:
            continue

    return spark.createDataFrame(rows, _SCHEMA)


# ── Persistent streaming table (accumulates history over time) ───────────────
dlt.create_streaming_live_table(
    name    = "raw_slack_messages",
    comment = "Raw Slack messages from customer channels — incremental daily append, deduplicated in silver",
    schema  = _SCHEMA,
)

# ── Daily append flow: fetches only the previous day's messages ──────────────
@dlt.append_flow(target="raw_slack_messages")
def slack_incremental():
    return _fetch_messages()

#!/usr/bin/env python3
"""
Obtain Salesforce OAuth tokens (access + refresh) and store them in Databricks Secrets.

Run this ONCE with a Connected App configured. The refresh token allows the pipeline
to automatically get new access tokens on every run — no more manual refreshes.

Prerequisites:
  1. Create a Connected App in Salesforce (Setup → App Manager → New Connected App)
  2. Enable OAuth Settings
  3. Add Callback URL: http://localhost:8765/callback
  4. Under "Selected OAuth Scopes", add: Access and manage your data (api),
     Perform requests at any time (refresh_token, offline_access)
  5. Copy the Consumer Key and Consumer Secret into SF_CLIENT_ID and SF_CLIENT_SECRET below

Usage:
    python3 refresh_sf_token.py
"""

import http.server
import json
import subprocess
import threading
import urllib.parse
import urllib.request
import webbrowser
from datetime import datetime

# ── Configuration ────────────────────────────────────────────────────────────
SF_INSTANCE_URL = "https://databricks.my.salesforce.com"
DATABRICKS_SCOPE = "satsen-sa-tracker"

# REQUIRED: Paste your Connected App credentials here
SF_CLIENT_ID     = ""   # Consumer Key from Connected App
SF_CLIENT_SECRET = ""   # Consumer Secret from Connected App

CALLBACK_PORT = 8765
CALLBACK_URL  = f"http://localhost:{CALLBACK_PORT}/callback"

# ── OAuth 2.0 Web Server Flow ────────────────────────────────────────────────
_captured = {}


class _CallbackHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)

        if "code" in params:
            code = params["code"][0]
            token_url = f"{SF_INSTANCE_URL}/services/oauth2/token"
            data = urllib.parse.urlencode({
                "grant_type":    "authorization_code",
                "code":          code,
                "client_id":     SF_CLIENT_ID,
                "client_secret": SF_CLIENT_SECRET,
                "redirect_uri":  CALLBACK_URL,
            }).encode()

            req = urllib.request.Request(token_url, data=data, method="POST")
            req.add_header("Content-Type", "application/x-www-form-urlencoded")
            try:
                with urllib.request.urlopen(req) as resp:
                    result = json.loads(resp.read().decode())
                _captured["access_token"]  = result.get("access_token", "")
                _captured["refresh_token"]  = result.get("refresh_token", "")
                _captured["instance_url"]  = result.get("instance_url", SF_INSTANCE_URL)
                html = "<h2>Tokens captured! You can close this window.</h2>"
            except urllib.error.HTTPError as e:
                body = e.read().decode() if e.fp else ""
                html = f"<h2>Token exchange failed</h2><pre>{body[:500]}</pre>"
            except Exception as ex:
                html = f"<h2>Error: {ex}</h2>"
        else:
            html = "<h2>No authorization code received. Please try again.</h2>"

        self.send_response(200)
        self.end_headers()
        self.wfile.write(html.encode())

    def log_message(self, fmt, *args):
        pass


def _oauth_flow():
    params = {
        "response_type": "code",
        "client_id":     SF_CLIENT_ID,
        "redirect_uri":  CALLBACK_URL,
        "scope":         "api refresh_token offline_access",
    }
    auth_url = f"{SF_INSTANCE_URL}/services/oauth2/authorize?" + urllib.parse.urlencode(params)

    server = http.server.HTTPServer(("localhost", CALLBACK_PORT), _CallbackHandler)
    thread = threading.Thread(target=lambda: server.handle_request(), daemon=True)
    thread.start()

    print("\nOpening browser for Salesforce SSO login...")
    print("  Log in and approve the app when prompted.\n")
    webbrowser.open(auth_url)
    thread.join(timeout=120)
    server.server_close()

    return _captured


def _store_secret(key, value):
    result = subprocess.run(
        ["databricks", "secrets", "put-secret", DATABRICKS_SCOPE, key, "--string-value", value],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        print(f"  ERROR storing {key}: {result.stderr}")
        return False
    return True


def main():
    print("=" * 60)
    print("  Salesforce OAuth — Refresh Token Setup")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    if not SF_CLIENT_ID or not SF_CLIENT_SECRET:
        print("""
ERROR: SF_CLIENT_ID and SF_CLIENT_SECRET must be set.

1. In Salesforce: Setup → App Manager → New Connected App (or edit existing)
2. Enable "OAuth Settings"
3. Callback URL: http://localhost:8765/callback
4. OAuth Scopes: api, refresh_token, offline_access
5. Copy Consumer Key → SF_CLIENT_ID
6. Copy Consumer Secret → SF_CLIENT_SECRET
7. Edit this script and paste the values at the top
8. Run again: python3 refresh_sf_token.py
""")
        return

    tokens = _oauth_flow()

    if not tokens.get("access_token"):
        print("\nERROR: No access token captured. Please try again.")
        return

    if not tokens.get("refresh_token"):
        print("\nWARNING: No refresh token in response. Ensure your Connected App")
        print("  has 'refresh_token' and 'offline_access' in OAuth Scopes.")

    print("\nStoring tokens in Databricks Secrets...")

    ok = True
    ok &= _store_secret("sf_access_token",  tokens.get("access_token", ""))
    ok &= _store_secret("sf_instance_url",  tokens.get("instance_url", SF_INSTANCE_URL))
    if tokens.get("refresh_token"):
        ok &= _store_secret("sf_refresh_token", tokens["refresh_token"])
        ok &= _store_secret("sf_client_id",     SF_CLIENT_ID)
        ok &= _store_secret("sf_client_secret", SF_CLIENT_SECRET)

    if ok:
        print(f"  ✓ Secrets updated in scope '{DATABRICKS_SCOPE}'")
        if tokens.get("refresh_token"):
            print("\n  The pipeline will now auto-refresh access tokens on every run.")
        print("\nDone! Re-run the pipeline to verify.")
    else:
        print("\nSome secrets failed to store. Check errors above.")


if __name__ == "__main__":
    main()

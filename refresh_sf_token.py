#!/usr/bin/env python3
"""
Store Salesforce credentials in Databricks Secrets for the pipeline.

Three modes:

  1. SECURITY TOKEN (recommended if no SID): Username + password + security token.
     Pipeline logs in fresh on every run. Works if your org allows API login (not SSO-only).
     python3 refresh_sf_token.py --security-token
     python3 refresh_sf_token.py USERNAME PASSWORD SECURITY_TOKEN

  2. MANUAL SID: Pass the 'sid' cookie from browser. Token expires in ~2 hours.
     python3 refresh_sf_token.py YOUR_SID_HERE

  3. OAUTH: Set SF_CLIENT_ID/SF_CLIENT_SECRET (Connected App). Auto-refresh.
"""

import http.server
import json
import os
import sys
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


def _security_token_flow():
    """Get username, password, security_token from args or env. No browser needed."""
    if len(sys.argv) >= 4 and sys.argv[1] != "--security-token":
        return sys.argv[1].strip(), sys.argv[2].strip(), sys.argv[3].strip()
    username = os.environ.get("SF_USERNAME", "").strip()
    password = os.environ.get("SF_PASSWORD", "").strip()
    sec_token = os.environ.get("SF_SECURITY_TOKEN", "").strip()
    if username and password:
        return username, password, sec_token
    print("""
=================================================================
SECURITY TOKEN (username + password + security token)
=================================================================

Get your security token: Salesforce → Setup → My Personal Information → Reset My Security Token

Pass as args:  python3 refresh_sf_token.py USERNAME PASSWORD SECURITY_TOKEN
Or env vars:   SF_USERNAME=... SF_PASSWORD=... SF_SECURITY_TOKEN=... python3 refresh_sf_token.py --security-token

Note: If your org uses SSO-only, this may fail with INVALID_SSO_GATEWAY_URL.
=================================================================
""")
    u = input("Salesforce username (email): ").strip()
    p = input("Salesforce password: ").strip()
    t = input("Security token (from Reset My Security Token): ").strip()
    return u, p, t


def _manual_sid_flow():
    """Get sid from arg, env, or prompt. Works in read-only terminals when passed as arg."""
    sid = (sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] != "--security-token" else os.environ.get("SF_SID", "")).strip()
    if sid:
        return sid
    print("""
=================================================================
MANUAL SESSION (sid cookie)
=================================================================

1. Open a browser and go to: https://databricks.my.salesforce.com
2. Log in with your Databricks SSO.
3. Open Developer Tools (F12) → Application → Cookies → databricks.my.salesforce.com
4. Find the cookie named "sid" and copy its value (looks like: 00D...!AQ...)

Pass it as:  python3 refresh_sf_token.py YOUR_SID_HERE

=================================================================
""")
    return input("Paste the 'sid' cookie value here: ").strip()


def main():
    print("=" * 60)
    print("  Salesforce Token Setup")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    use_security_token = "--security-token" in sys.argv or (
        len(sys.argv) >= 4 and sys.argv[1] != "--security-token"
    )

    if use_security_token:
        # Security token flow: username + password + security token
        print("\nUsing Security Token (username + password + security token)...")
        username, password, sec_token = _security_token_flow()
        if not username or not password:
            print("\nERROR: Username and password are required.")
            return
        print("\nStoring in Databricks Secrets...")
        ok = _store_secret("sf_username", username)
        ok &= _store_secret("sf_password", password)
        ok &= _store_secret("sf_security_token", sec_token or "")
        ok &= _store_secret("sf_instance_url", SF_INSTANCE_URL)
        if ok:
            print(f"  ✓ sf_username, sf_password, sf_security_token, sf_instance_url updated in scope '{DATABRICKS_SCOPE}'")
            print("\n  The pipeline will log in fresh on every run. No expiration.")
            print("\nDone! Re-run the pipeline to verify.")
        else:
            print("\nFailed to store. Check errors above.")

    elif SF_CLIENT_ID and SF_CLIENT_SECRET:
        # OAuth flow (Connected App)
        print("\nUsing OAuth 2.0 (Connected App)...")
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
    else:
        # Manual sid flow (no Connected App)
        print("\nUsing manual method (sid cookie). Token expires in ~2 hours.\n")
        sid = _manual_sid_flow()

        if not sid:
            print("\nERROR: No value entered.")
            return

        print("\nStoring in Databricks Secrets...")
        ok = _store_secret("sf_access_token", sid)
        ok &= _store_secret("sf_instance_url", SF_INSTANCE_URL)

        if ok:
            print(f"  ✓ sf_access_token and sf_instance_url updated in scope '{DATABRICKS_SCOPE}'")
            print("\nDone! Re-run the pipeline. The token will expire in ~2 hours.")
        else:
            print("\nFailed to store. Check errors above.")


if __name__ == "__main__":
    main()

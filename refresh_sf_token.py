#!/usr/bin/env python3
"""
Refresh the Salesforce access token stored in Databricks Secrets.

Run this script locally whenever the pipeline fails with SalesforceExpiredSession.
It opens a browser window where you log in via SSO, captures the new token, and
stores it in the 'satsen-sa-tracker' secret scope automatically.

Usage:
    python3 refresh_sf_token.py
"""

import http.server
import json
import subprocess
import threading
import urllib.parse
import webbrowser
from datetime import datetime

# ── Configuration ────────────────────────────────────────────────────────────
SF_INSTANCE_URL = "https://databricks.my.salesforce.com"
DATABRICKS_SCOPE = "satsen-sa-tracker"
SF_TOKEN_SECRET  = "sf_access_token"

# Salesforce Connected App credentials for internal tooling.
# If you don't have these, use the MANUAL method below.
#
# To find your Connected App:
#   Salesforce → Setup → App Manager → find a Connected App with "oauth" enabled
#   Copy the Consumer Key and Consumer Secret.
SF_CLIENT_ID     = ""   # paste Consumer Key here, or leave blank for MANUAL mode
SF_CLIENT_SECRET = ""   # paste Consumer Secret here

# OAuth callback
CALLBACK_PORT = 8765
CALLBACK_URL  = f"http://localhost:{CALLBACK_PORT}/callback"

# ── OAuth 2.0 Web Server Flow ────────────────────────────────────────────────
_captured_token = {}


class _CallbackHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)

        if "code" in params:
            # Exchange authorization code for access token
            code = params["code"][0]
            token_url = f"{SF_INSTANCE_URL}/services/oauth2/token"
            data = {
                "grant_type":    "authorization_code",
                "code":          code,
                "client_id":     SF_CLIENT_ID,
                "client_secret": SF_CLIENT_SECRET,
                "redirect_uri":  CALLBACK_URL,
            }
            import urllib.request
            req = urllib.request.Request(
                token_url,
                data=urllib.parse.urlencode(data).encode(),
                method="POST",
            )
            with urllib.request.urlopen(req) as resp:
                result = json.loads(resp.read())
            _captured_token["access_token"] = result.get("access_token", "")
            _captured_token["instance_url"] = result.get("instance_url", SF_INSTANCE_URL)
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"<h2>Token captured! You can close this window.</h2>")
        else:
            # Fragment-based response (Implicit flow): token is in the URL fragment.
            # Browser won't send the fragment to the server, so we use JS to relay it.
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"""
<html><body>
<script>
  var hash = window.location.hash.slice(1);
  var params = new URLSearchParams(hash);
  var token = params.get('access_token');
  if (token) {
    fetch('/token?access_token=' + encodeURIComponent(token))
      .then(() => document.body.innerHTML = '<h2>Token captured! You can close this window.</h2>');
  } else {
    document.body.innerHTML = '<h2>No token found. Please try again.</h2>';
  }
</script>
</body></html>""")

    def do_GET_token(self):
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)
        if "access_token" in params:
            _captured_token["access_token"] = params["access_token"][0]
        self.send_response(200)
        self.end_headers()

    def log_message(self, fmt, *args):
        pass  # suppress server logs


def _start_server():
    server = http.server.HTTPServer(("localhost", CALLBACK_PORT), _CallbackHandler)
    server.handle_request()  # one request only


def _oauth_flow():
    params = {
        "response_type": "code" if SF_CLIENT_ID else "token",
        "client_id":     SF_CLIENT_ID or "SalesforceOfficeIntegration",
        "redirect_uri":  CALLBACK_URL,
        "scope":         "api refresh_token offline_access",
    }
    auth_url = f"{SF_INSTANCE_URL}/services/oauth2/authorize?" + urllib.parse.urlencode(params)

    thread = threading.Thread(target=_start_server, daemon=True)
    thread.start()

    print(f"\nOpening browser for Salesforce SSO login...")
    print(f"  {auth_url}\n")
    webbrowser.open(auth_url)
    thread.join(timeout=120)

    return _captured_token.get("access_token", "")


# ── Manual Token Method ──────────────────────────────────────────────────────
def _manual_method():
    print("""
=================================================================
MANUAL TOKEN REFRESH
=================================================================

1. Open a browser and go to:
   https://databricks.my.salesforce.com

2. Log in with your Databricks SSO.

3. After logging in, open the browser Developer Tools (F12).

4. Go to the "Application" (Chrome) or "Storage" (Firefox) tab.

5. Under "Cookies" → "https://databricks.my.salesforce.com",
   find the cookie named "sid".

6. Copy the cookie value (it looks like: 00D...!AQ...)

=================================================================
""")
    token = input("Paste the 'sid' cookie value here: ").strip()
    return token


# ── Store in Databricks Secrets ──────────────────────────────────────────────
def _store_secret(key, value):
    result = subprocess.run(
        ["databricks", "secrets", "put-secret", DATABRICKS_SCOPE, key, "--string-value", value],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        print(f"  ERROR storing secret: {result.stderr}")
        return False
    return True


def main():
    print("=" * 60)
    print("  Salesforce Token Refresher")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    if SF_CLIENT_ID:
        print("\nUsing OAuth 2.0 Web Server Flow (Connected App)...")
        token = _oauth_flow()
    else:
        print("\nNo Connected App configured. Using manual method...")
        token = _manual_method()

    if not token:
        print("\nERROR: No token captured. Please try again.")
        return

    print(f"\nToken captured (length: {len(token)} chars)")
    print("Storing in Databricks Secrets...")

    if _store_secret(SF_TOKEN_SECRET, token):
        print(f"  ✓ Secret '{SF_TOKEN_SECRET}' updated in scope '{DATABRICKS_SCOPE}'")
        print("\nDone! You can now re-run the pipeline.")
        print("  databricks api post /api/2.0/pipelines/28b9d78f-1fc8-444b-89df-556670175352/updates --json '{\"full_refresh\": false}'")
    else:
        print("\nFailed to update secret. Please store it manually:")
        print(f"  databricks secrets put-secret {DATABRICKS_SCOPE} {SF_TOKEN_SECRET} --string-value '<token>'")


if __name__ == "__main__":
    main()

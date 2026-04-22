"""
Fyers API V3 - Access Token Generator
Run daily before market hours to get fresh access token.
"""

import json
import requests
import pyotp
import time
from urllib.parse import parse_qs, urlparse
from fyers_apiv3 import fyersModel

# ── Credentials ───────────────────────────────────────────
APP_ID = "LPXLEAXXE1"
APP_TYPE = "200"
SECRET_KEY = "wNUzhKBPHFDmMKbz"
CLIENT_ID = f"{APP_ID}-{APP_TYPE}"
FY_ID = "DP02418"
TOTP_KEY = "FN66WGMJWRQR2HJNHJUHHXB4ONCY2Q2M"
PIN = "0510"
REDIRECT_URI = "https://trade.fyers.in/api-login/redirect-uri/index.html"

# ── Endpoints ─────────────────────────────────────────────
BASE = "https://api-t2.fyers.in/vagator/v2"
BASE2 = "https://api-t1.fyers.in/api/v3"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Content-Type": "application/json"
}

# ── Token Generation ──────────────────────────────────────
def generate_token():
    # Step 1: Send OTP
    r1 = requests.post(f"{BASE}/send_login_otp", json={"fy_id": FY_ID, "app_id": "2"}, headers=HEADERS)
    assert r1.status_code == 200, f"Send OTP failed: {r1.text}"
    request_key = r1.json()["request_key"]
    print("✓ OTP sent")

    # Step 2: Verify TOTP
    totp = pyotp.TOTP(TOTP_KEY).now()
    r2 = requests.post(f"{BASE}/verify_otp", json={"request_key": request_key, "otp": totp}, headers=HEADERS)
    assert r2.status_code == 200, f"Verify TOTP failed: {r2.text}"
    request_key_2 = r2.json()["request_key"]
    print("✓ TOTP verified")

    # Step 3: Verify PIN
    r3 = requests.post(f"{BASE}/verify_pin", json={
        "request_key": request_key_2, "identity_type": "pin", "identifier": PIN
    }, headers=HEADERS)
    assert r3.status_code == 200, f"Verify PIN failed: {r3.text}"
    trade_token = r3.json()["data"]["access_token"]
    print("✓ PIN verified")

    # Step 4: Get auth code
    r4 = requests.post(f"{BASE2}/token", json={
        "fyers_id": FY_ID, "app_id": APP_ID, "redirect_uri": REDIRECT_URI,
        "appType": APP_TYPE, "code_challenge": "", "state": "sample_state",
        "scope": "", "nonce": "", "response_type": "code", "create_cookie": True
    }, headers={**HEADERS, "Authorization": f"Bearer {trade_token}"})
    auth_code = parse_qs(urlparse(r4.json()["Url"]).query)["auth_code"][0]
    print("✓ Auth code received")

    # Step 5: Generate access token
    session = fyersModel.SessionModel(
        client_id=CLIENT_ID, secret_key=SECRET_KEY,
        redirect_uri=REDIRECT_URI, response_type="code", grant_type="authorization_code"
    )
    session.set_token(auth_code)
    response = session.generate_token()
    access_token = response["access_token"]
    print(f"✅ Access Token: {access_token}")
    return access_token


if __name__ == "__main__":
    token = generate_token()

"""
Fyers API V3 - Automated Login & Trading Script (FIXED)
Balfund Trading Private Limited
================================================
"""

import json
import requests
import pyotp
import base64
import sys
import time
from urllib.parse import parse_qs, urlparse
from fyers_apiv3 import fyersModel

# ============================================================
# CREDENTIALS
# ============================================================
APP_ID = "KWSRQDLSF2"
APP_TYPE = "200"
SECRET_KEY = "WRKnlBpWq5TrosWM"
CLIENT_ID = f"{APP_ID}-{APP_TYPE}"  # KWSRQDLSF2-200

FY_ID = "YN04712"
TOTP_KEY = "67LK3GUYYZLAH5266PQFX5XJJWE3ZROX"
PIN = "2825"

REDIRECT_URI = "https://trade.fyers.in/api-login/redirect-uri/index.html"
APP_ID_TYPE = "2"  # 2 = web login

# ============================================================
# API ENDPOINTS (CORRECTED)
# ============================================================
BASE_URL = "https://api-t2.fyers.in/vagator/v2"
BASE_URL_2 = "https://api-t1.fyers.in/api/v3"

URL_SEND_LOGIN_OTP = BASE_URL + "/send_login_otp"       # NOT send_login_otp_v3
URL_VERIFY_TOTP = BASE_URL + "/verify_otp"
URL_VERIFY_PIN = BASE_URL + "/verify_pin"                # NOT verify_pin_v2
URL_TOKEN = BASE_URL_2 + "/token"
URL_VALIDATE_AUTH_CODE = BASE_URL_2 + "/validate-authcode"

# Browser-like headers to avoid rejection
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Content-Type": "application/json"
}


# ============================================================
# LOGIN FUNCTIONS
# ============================================================

def send_login_otp(fy_id, app_id):
    """Step 1: Send login OTP request to get request_key"""
    try:
        payload = {"fy_id": fy_id, "app_id": app_id}
        result = requests.post(url=URL_SEND_LOGIN_OTP, json=payload, headers=HEADERS)
        print(f"  [DEBUG] send_login_otp status: {result.status_code}")
        print(f"  [DEBUG] send_login_otp response: {result.text[:200]}")
        
        if result.status_code != 200:
            return None, f"HTTP {result.status_code}: {result.text}"
        data = result.json()
        if "request_key" in data:
            return data["request_key"], None
        else:
            return None, f"No request_key in response: {data}"
    except Exception as e:
        return None, str(e)


def generate_totp(totp_key):
    """Step 2: Generate TOTP from secret key"""
    try:
        totp = pyotp.TOTP(totp_key).now()
        return totp, None
    except Exception as e:
        return None, str(e)


def verify_totp(request_key, totp):
    """Step 3: Verify TOTP and get new request_key"""
    try:
        payload = {"request_key": request_key, "otp": totp}
        result = requests.post(url=URL_VERIFY_TOTP, json=payload, headers=HEADERS)
        print(f"  [DEBUG] verify_totp status: {result.status_code}")
        print(f"  [DEBUG] verify_totp response: {result.text[:200]}")
        
        if result.status_code != 200:
            return None, f"HTTP {result.status_code}: {result.text}"
        data = result.json()
        if "request_key" in data:
            return data["request_key"], None
        else:
            return None, f"No request_key: {data}"
    except Exception as e:
        return None, str(e)


def verify_pin(request_key, pin):
    """Step 4: Verify PIN and get access token"""
    try:
        payload = {
            "request_key": request_key,
            "identity_type": "pin",
            "identifier": pin
        }
        result = requests.post(url=URL_VERIFY_PIN, json=payload, headers=HEADERS)
        print(f"  [DEBUG] verify_pin status: {result.status_code}")
        print(f"  [DEBUG] verify_pin response: {result.text[:200]}")
        
        if result.status_code != 200:
            return None, f"HTTP {result.status_code}: {result.text}"
        data = result.json()
        if "data" in data and "access_token" in data["data"]:
            return data["data"]["access_token"], None
        else:
            return None, f"No access_token: {data}"
    except Exception as e:
        return None, str(e)


def get_auth_code(fy_id, app_id, redirect_uri, app_type, access_token):
    """Step 5: Get auth code from trade access token"""
    try:
        payload = {
            "fyers_id": fy_id,
            "app_id": app_id,
            "redirect_uri": redirect_uri,
            "appType": app_type,
            "code_challenge": "",
            "state": "sample_state",
            "scope": "",
            "nonce": "",
            "response_type": "code",
            "create_cookie": True
        }
        headers = {
            **HEADERS,
            "Authorization": f"Bearer {access_token}"
        }
        result = requests.post(url=URL_TOKEN, json=payload, headers=headers)
        print(f"  [DEBUG] get_auth_code status: {result.status_code}")
        print(f"  [DEBUG] get_auth_code response: {result.text[:200]}")

        if result.status_code not in [200, 308]:
            return None, f"HTTP {result.status_code}: {result.text}"

        data = result.json()
        if "Url" in data:
            url = data["Url"]
            auth_code = parse_qs(urlparse(url).query)["auth_code"][0]
            return auth_code, None
        elif "url" in data:
            url = data["url"]
            auth_code = parse_qs(urlparse(url).query)["auth_code"][0]
            return auth_code, None
        else:
            return None, f"No Url in response: {data}"
    except Exception as e:
        return None, str(e)


def generate_access_token(auth_code):
    """Step 6: Generate final API access token"""
    try:
        session = fyersModel.SessionModel(
            client_id=CLIENT_ID,
            secret_key=SECRET_KEY,
            redirect_uri=REDIRECT_URI,
            response_type="code",
            grant_type="authorization_code"
        )
        session.set_token(auth_code)
        response = session.generate_token()
        print(f"  [DEBUG] generate_token response keys: {list(response.keys()) if isinstance(response, dict) else response}")

        if "access_token" in response:
            return response["access_token"], None
        else:
            return None, f"Token Error: {response}"
    except Exception as e:
        return None, str(e)


# ============================================================
# MAIN AUTO-LOGIN FLOW
# ============================================================

def auto_login():
    """Complete automated login flow → returns access_token"""
    print("=" * 60)
    print("  FYERS API V3 - Automated Login")
    print("  Balfund Trading Private Limited")
    print("=" * 60)

    # Step 1: Send Login OTP
    print("\n[1/6] Sending login OTP...")
    request_key, err = send_login_otp(FY_ID, APP_ID_TYPE)
    if err:
        print(f"  ✗ FAILED: {err}")
        return None
    print(f"  ✓ Request key received")

    # Step 2: Generate TOTP
    print("\n[2/6] Generating TOTP...")
    totp, err = generate_totp(TOTP_KEY)
    if err:
        print(f"  ✗ FAILED: {err}")
        return None
    print(f"  ✓ TOTP generated: {totp}")

    # Step 3: Verify TOTP (retry up to 3 times)
    print("\n[3/6] Verifying TOTP...")
    request_key_2 = None
    for attempt in range(1, 4):
        request_key_2, err = verify_totp(request_key, totp)
        if request_key_2:
            break
        print(f"  Attempt {attempt} failed: {err}")
        time.sleep(1)
        totp, _ = generate_totp(TOTP_KEY)

    if not request_key_2:
        print(f"  ✗ TOTP verification failed after 3 attempts")
        return None
    print(f"  ✓ TOTP verified")

    # Step 4: Verify PIN
    print("\n[4/6] Verifying PIN...")
    trade_access_token, err = verify_pin(request_key_2, PIN)
    if err:
        print(f"  ✗ FAILED: {err}")
        return None
    print(f"  ✓ PIN verified, trade token received")

    # Step 5: Get Auth Code
    print("\n[5/6] Getting auth code...")
    auth_code, err = get_auth_code(FY_ID, APP_ID, REDIRECT_URI, APP_TYPE, trade_access_token)
    if err:
        print(f"  ✗ FAILED: {err}")
        return None
    print(f"  ✓ Auth code received")

    # Step 6: Generate Final Access Token
    print("\n[6/6] Generating API access token...")
    access_token, err = generate_access_token(auth_code)
    if err:
        print(f"  ✗ FAILED: {err}")
        return None

    print("\n" + "=" * 60)
    print("  ✅ LOGIN SUCCESSFUL!")
    print("=" * 60)
    print(f"\nAccess Token: {access_token[:60]}...")

    return access_token


# ============================================================
# FYERS CLIENT HELPER
# ============================================================

def get_fyers_client(access_token=None):
    """Get authenticated FyersModel client"""
    if access_token is None:
        access_token = auto_login()
        if not access_token:
            print("Login failed! Cannot create client.")
            return None

    fyers = fyersModel.FyersModel(
        client_id=CLIENT_ID,
        is_async=False,
        token=access_token,
        log_path=""
    )
    return fyers


# ============================================================
# QUICK TEST
# ============================================================

def test_connection(fyers):
    """Test API connection"""
    print("\n" + "-" * 60)
    print("  Testing API Connection")
    print("-" * 60)

    # Profile
    print("\n📋 Profile:")
    profile = fyers.get_profile()
    print(f"   Response: {profile}")

    # Funds
    print("\n💰 Funds:")
    funds = fyers.funds()
    if funds.get("s") == "ok":
        for item in funds.get("fund_limit", []):
            if item["title"] in ["Total Balance", "Available Balance", "Used Margin"]:
                print(f"   {item['title']}: ₹{item['equityAmount']}")
    else:
        print(f"   Response: {funds}")

    print("\n" + "-" * 60)


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":
    access_token = auto_login()

    if access_token:
        fyers = get_fyers_client(access_token)
        test_connection(fyers)
    else:
        print("\n❌ Login failed. Check debug output above.")

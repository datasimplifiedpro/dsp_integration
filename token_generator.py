import http.client
import os, json, http.client

import numpy as np
import pandas as pd

from configparser import ConfigParser
from datetime import datetime, timezone, timedelta


# -------------------- Config --------------------
BASE = os.path.dirname(os.path.abspath(__file__))
CFG_PATH = os.path.join(BASE, "config.ini")

config = ConfigParser(interpolation=None)
config.read(CFG_PATH)

def issue_token(config_path=None):
    """
    - Reads API key/site/creds from config.ini
    - POSTs to /public/v6/usertoken/issue
    - Saves AccessToken -> [token] authorization
    - Saves Expires     -> [token] timestamp
    """
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.ini")

    cfg = ConfigParser(interpolation=None)
    cfg.read(config_path)

    api_key  = cfg.get("mb", "api_key")
    site_id  = cfg.get("mb", "site_id")
    version  = cfg.get("mb", "version", fallback="6")
    host     = cfg.get("mb", "host",    fallback="api.mindbodyonline.com")
    username = cfg.get("credentials", "username")
    password = cfg.get("credentials", "password")

    # Request
    conn = http.client.HTTPSConnection(host, timeout=30)
    url = "/public/v{}/usertoken/issue".format(version)
    headers = {
        "Content-Type": "application/json",
        "Api-Key": api_key,
        "SiteId": site_id,
    }
    body = json.dumps({"username": username, "password": password})

    conn.request("POST", url, body=body, headers=headers)
    resp = conn.getresponse()
    raw  = resp.read()
    conn.close()

    # Parse only what we need
    payload = json.loads(raw.decode("utf-8"))
    token   = payload["AccessToken"]
    expires = payload["Expires"]

    # Write only AccessToken and Expires
    if not cfg.has_section("token"):
        cfg.add_section("token")
    cfg.set("token", "authorization", token)
    cfg.set("token", "timestamp", expires)
    print("[issue_token] timestamp:", expires)

    with open(config_path, "w") as f:
        cfg.write(f)

    return token

def get_valid_token():
    auth = config.get("token", "authorization")
    ts   = config.get("token", "timestamp")

    now_utc = str(datetime.now(timezone.utc))[:10]
    exp_utc = ts[:10]

    print(f"[token] Now={now_utc}  Expires(UTC)={exp_utc}")
    if now_utc >= exp_utc:
        print("[token] Expired → issuing new token...")
        issue_token(CFG_PATH)
        config.read(CFG_PATH)
        auth = config.get("token", "authorization")
        ts   = config.get("token", "timestamp")
        exp_utc = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        print(f"[token] Refreshed. New expiry(UTC)={exp_utc.isoformat()}")
    else:
        print("[token] Still valid.")

    return auth


#for manual testing
# Function: Run this block only when the file is executed directly (e.g., `python token_generator.py`),
# not when it's imported from another module.
if __name__ == "__main__":
    try:
        # Attempt to call the function once to obtain and persist a fresh token.
        issue_token()

        # Simple confirmation that the write to config.ini happened.
        print("[issue_token] Saved AccessToken and timestamp.")
    except Exception as e:
        # Catch any error (HTTP, JSON parsing, file write, etc.) and print a concise message.
        print("[issue_token] ERROR:", repr(e))
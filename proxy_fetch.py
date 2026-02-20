import os
import json
import subprocess
import urllib.parse
import requests
from flask import Flask, jsonify

app = Flask(__name__)

API_KEY = (os.environ.get("DATA_API_KEY") or "").strip()
RESOURCE_ID = (os.environ.get("DATA_RESOURCE_ID") or "").strip()

OPEND_DATASTORE_SQL = "https://opend.data.go.th/get-ckan/datastore_search_sql"

HEADERS_API = {
    "api-key": API_KEY,
    "x-api-key": API_KEY,
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "th-TH,th;q=0.9,en-US;q=0.8,en;q=0.7",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Referer": "https://data.go.th/",
    "Origin": "https://data.go.th",
}

def _build_url(url, params):
    q = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
    return f"{url}?{q}" if q else url

def _curl_get_json(url, headers, params, timeout=60):
    full_url = _build_url(url, params)
    cmd = ["curl", "-sS", "-L", "--compressed", "--connect-timeout", str(timeout), "--max-time", str(timeout), full_url]
    for k, v in headers.items():
        if v:
            cmd.insert(-1, f"{k}: {v}")
            cmd.insert(-1, "-H")
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        return {"error": f"curl rc={p.returncode}", "stderr": p.stderr[:300]}
    try:
        return json.loads(p.stdout)
    except Exception:
        return {"error": "non-json", "body": p.stdout[:300]}

@app.route("/egp")
def egp():
    if not API_KEY or not RESOURCE_ID:
        return jsonify({"error": "missing env", "need": ["DATA_API_KEY", "DATA_RESOURCE_ID"]}), 500

    sql = f"""
    SELECT *
    FROM "{RESOURCE_ID}"
    WHERE project_name LIKE '%อินทร์บุรี%'
       OR prov_name LIKE '%สิงห์บุรี%'
    LIMIT 200
    """

    # ลอง requests ก่อน
    try:
        r = requests.get(OPEND_DATASTORE_SQL, headers=HEADERS_API, params={"sql": sql}, timeout=60)
        if r.status_code < 400:
            return jsonify(r.json())
        # ถ้าโดนบล็อก -> ลอง curl
        data = _curl_get_json(OPEND_DATASTORE_SQL, HEADERS_API, {"sql": sql}, timeout=70)
        return jsonify(data), (200 if "result" in data else 502)
    except Exception as e:
        return jsonify({"error": "exception", "detail": str(e)}), 500

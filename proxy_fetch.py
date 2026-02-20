# proxy_fetch.py
import os
import requests
from flask import Flask, jsonify, request

app = Flask(__name__)

API_KEY = os.environ.get("DATA_API_KEY")
RESOURCE_ID = os.environ.get("DATA_RESOURCE_ID")

@app.route("/egp")
def egp():

    sql = f"""
    SELECT *
    FROM "{RESOURCE_ID}"
    WHERE project_name LIKE '%อินทร์บุรี%'
       OR prov_name LIKE '%สิงห์บุรี%'
    LIMIT 200
    """

    url = "https://opend.data.go.th/get-ckan/datastore_search_sql"

    headers = {
        "api-key": API_KEY,
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://data.go.th/"
    }

    resp = requests.get(url, headers=headers, params={"sql": sql}, timeout=60)

    return jsonify(resp.json())

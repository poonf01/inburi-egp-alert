import os
import json
import time
import subprocess
import requests
from typing import Any, Dict, List, Optional

# ======================
# ENV
# ======================
LINE_TOKEN = os.environ.get("LINE_TOKEN", "").strip()
API_KEY = os.environ.get("DATA_API_KEY", "").strip()
RESOURCE_ID = os.environ.get("DATA_RESOURCE_ID", "").strip()

# ======================
# CONSTANTS
# ======================
OPEND_DATASTORE_SQL = "https://opend.data.go.th/get-ckan/datastore_search_sql"
OPEND_DATASTORE_SEARCH = "https://opend.data.go.th/get-ckan/datastore_search"

LINE_BROADCAST_URL = "https://api.line.me/v2/bot/message/broadcast"
DATA_JSON_PATH = "data.json"

# ======================
# HEADERS
# ======================
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

HEADERS_LINE = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {LINE_TOKEN}",
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0",
}

# ======================
# HELPERS
# ======================
def require_env() -> bool:
    missing = []
    if not API_KEY: missing.append("DATA_API_KEY")
    if not LINE_TOKEN: missing.append("LINE_TOKEN")
    if not RESOURCE_ID: missing.append("DATA_RESOURCE_ID")
    if missing:
        print("❌ Missing environment variables:", ", ".join(missing))
        return False
    return True

def _short(text: str, n: int = 250) -> str:
    t = (text or "").replace("\n", " ").replace("\r", " ").strip()
    return t[:n]

# แก้ให้ใช้ POST ยัดใส่ Payload แทน URL เพื่อหลบ WAF และหลีกเลี่ยง curl URL error
def _curl_post_json(url: str, headers: Dict[str, str], payload: Dict[str, Any], timeout: int = 70) -> Dict[str, Any]:
    cmd = [
        "curl", "-sS", "-L", "--compressed",
        "--connect-timeout", str(timeout),
        "--max-time", str(timeout),
        "-X", "POST",
        "-H", "Content-Type: application/json"
    ]
    for k, v in headers.items():
        if v:
            cmd.extend(["-H", f"{k}: {v}"])
    
    cmd.extend(["-d", json.dumps(payload), url])

    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"curl failed rc={p.returncode}: {_short(p.stderr)}")

    try:
        return json.loads(p.stdout)
    except Exception:
        raise RuntimeError(f"curl returned non-json: {_short(p.stdout)}")

def _requests_post_json(url: str, headers: Dict[str, str], payload: Dict[str, Any], retries: int = 3, timeout: int = 45) -> Dict[str, Any]:
    session = requests.Session()
    last_err: Optional[Exception] = None

    for i in range(retries):
        try:
            resp = session.post(url, headers=headers, json=payload, timeout=timeout)
            if resp.status_code < 400:
                return resp.json()

            body = _short(resp.text)
            if resp.status_code in (403, 429, 500, 502, 503, 504):
                last_err = RuntimeError(f"HTTP {resp.status_code}: {body}")
                wait = 2 ** i
                print(f"⚠️ HTTP {resp.status_code} retry in {wait}s ...")
                time.sleep(wait)
                continue

            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last_err = e
            wait = 2 ** i
            print(f"⚠️ Request error retry in {wait}s ...")
            time.sleep(wait)

    raise RuntimeError(f"requests failed after retries: {last_err}")

def get_json_with_fallback(url: str, headers: Dict[str, str], payload: Dict[str, Any]) -> Dict[str, Any]:
    try:
        return _requests_post_json(url, headers, payload)
    except Exception as e1:
        print(f"⚠️ requests failed -> trying curl fallback ...")
    return _curl_post_json(url, headers, payload)

def load_old_data(path: str = DATA_JSON_PATH) -> List[Dict[str, Any]]:
    if not os.path.exists(path): return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except Exception:
        return []

def save_data(items: List[Dict[str, Any]], path: str = DATA_JSON_PATH) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)

def send_line_notify(project_name: str, budget: str, department: str) -> None:
    if not LINE_TOKEN: return
    message = (
        "📢 มีประกาศจัดซื้อจัดจ้างใหม่ในพื้นที่!\n\n"
        f"🏢 หน่วยงาน: {department}\n"
        f"📌 โครงการ: {project_name}\n"
        f"💰 งบประมาณ: {budget} บาท\n\n"
        "(by Alieninburi)"
    )
    payload = {"messages": [{"type": "text", "text": message}]}
    try:
        requests.post(LINE_BROADCAST_URL, headers=HEADERS_LINE, json=payload, timeout=30)
    except Exception as e:
        print(f"⚠️ LINE notify exception: {e}")

# ======================
# FETCH
# ======================
def fetch_records_sql(resource_id: str) -> List[Dict[str, Any]]:
    # ยิง POST พร้อม payload ไม่แปะบน URL แล้ว
    sql = f"""SELECT * FROM "{resource_id}" WHERE project_name LIKE '%อินทร์บุรี%' OR prov_name LIKE '%สิงห์บุรี%' OR dept_name LIKE '%อินทร์บุรี%' LIMIT 200"""
    data = get_json_with_fallback(OPEND_DATASTORE_SQL, headers=HEADERS_API, payload={"sql": sql})
    return data.get("result", {}).get("records", []) or []

def fetch_records_search(resource_id: str) -> List[Dict[str, Any]]:
    out, seen = [], set()
    for q in ["อินทร์บุรี", "สิงห์บุรี"]:
        payload = {"resource_id": resource_id, "q": q, "limit": 200}
        data = get_json_with_fallback(OPEND_DATASTORE_SEARCH, headers=HEADERS_API, payload=payload)
        for r in (data.get("result", {}).get("records", []) or []):
            pid = str(r.get("project_id", "")).strip()
            key = pid if pid else json.dumps(r, ensure_ascii=False, sort_keys=True)
            if key not in seen:
                seen.add(key)
                out.append(r)
    return out

def fetch_records_any(resource_id: str) -> List[Dict[str, Any]]:
    try:
        return fetch_records_sql(resource_id)
    except Exception as e:
        print(f"⚠️ SQL failed: {e}")

    try:
        return fetch_records_search(resource_id)
    except Exception as e:
        print(f"⚠️ datastore_search failed: {e}")

    raise RuntimeError("All fetch methods failed (SQL, datastore_search).")

# ======================
# MAIN
# ======================
def main() -> None:
    if not require_env(): return

    print("✅ Environment OK")
    print("📥 กำลังดึงข้อมูลจาก opend.data.go.th ...")

    try:
        records = fetch_records_any(RESOURCE_ID)
        print(f"✅ ดึงสำเร็จ: {len(records)} รายการ")
    except Exception as e:
        print(f"❌ ดึงข้อมูลล้มเหลว: {e}")
        return

    old_data = load_old_data(DATA_JSON_PATH)
    old_ids = {str(x.get("project_id")) for x in old_data if isinstance(x, dict)}

    new_projects = [p for p in records if str(p.get("project_id", "")).strip() and str(p.get("project_id", "")).strip() not in old_ids]

    if new_projects:
        print(f"🆕 พบโครงการใหม่ {len(new_projects)} รายการ กำลังส่งแจ้งเตือน ...")
        for proj in new_projects:
            send_line_notify(
                str(proj.get("project_name", "ไม่ระบุชื่อโครงการ")),
                str(proj.get("sum_price_agree", "ไม่ระบุ")),
                str(proj.get("dept_name", "ไม่ระบุหน่วยงาน")),
            )
            print(f"📨 แจ้งแล้ว: {proj.get('project_name')}")
        save_data(new_projects + old_data, DATA_JSON_PATH)
        print("✅ อัปเดต data.json เรียบร้อย")
    else:
        if not os.path.exists(DATA_JSON_PATH): save_data([], DATA_JSON_PATH)
        print("😴 วันนี้ไม่มีโครงการใหม่")

if __name__ == "__main__":
    main()

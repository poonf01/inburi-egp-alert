import os
import json
import time
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
LINE_BROADCAST_URL = "https://api.line.me/v2/bot/message/broadcast"
DATA_JSON_PATH = "data.json"


# ======================
# HEADERS (สำคัญมากกับ data.go.th/opend.data.go.th)
# ======================
HEADERS_API = {
    "api-key": API_KEY,
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0",
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
    if not API_KEY:
        missing.append("DATA_API_KEY")
    if not LINE_TOKEN:
        missing.append("LINE_TOKEN")
    if not RESOURCE_ID:
        missing.append("DATA_RESOURCE_ID")

    if missing:
        print("❌ Missing environment variables:", ", ".join(missing))
        print("👉 ไปที่ GitHub Repo > Settings > Secrets and variables > Actions")
        print("   แล้วเพิ่ม Secrets ให้ครบ: LINE_TOKEN, DATA_API_KEY, DATA_RESOURCE_ID")
        return False
    return True


def http_get_with_retry(
    url: str,
    headers: Dict[str, str],
    params: Dict[str, Any],
    retries: int = 5,
    timeout: int = 40,
) -> requests.Response:
    last_exc: Optional[Exception] = None

    for i in range(retries):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=timeout)

            # เจอ status ที่มักเป็นชั่วคราว/โดน rate-limit -> backoff แล้วลองใหม่
            if resp.status_code in (403, 429, 500, 502, 503, 504):
                wait = 2 ** i
                print(f"⚠️ HTTP {resp.status_code} retry in {wait}s ...")
                time.sleep(wait)
                continue

            resp.raise_for_status()
            return resp

        except Exception as e:
            last_exc = e
            wait = 2 ** i
            print(f"⚠️ Request error retry in {wait}s ... ({e})")
            time.sleep(wait)

    raise RuntimeError(f"Request failed after retries: {last_exc}")


def load_old_data(path: str = DATA_JSON_PATH) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        return []
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
    # กันพัง ถ้า token หาย
    if not LINE_TOKEN:
        return

    message = (
        "📢 มีประกาศจัดซื้อจัดจ้างใหม่ในพื้นที่!\n\n"
        f"🏢 หน่วยงาน: {department}\n"
        f"📌 โครงการ: {project_name}\n"
        f"💰 งบประมาณ: {budget} บาท\n\n"
        "(by Alieninburi)"
    )

    payload = {"messages": [{"type": "text", "text": message}]}

    try:
        resp = requests.post(
            LINE_BROADCAST_URL,
            headers=HEADERS_LINE,
            json=payload,
            timeout=30,
        )
        if resp.status_code >= 400:
            print(f"⚠️ LINE notify failed: HTTP {resp.status_code} {resp.text[:200]}")
    except Exception as e:
        print(f"⚠️ LINE notify exception: {e}")


# ======================
# MAIN LOGIC
# ======================
def fetch_records_sql(resource_id: str) -> List[Dict[str, Any]]:
    """
    ดึงข้อมูลด้วย datastore_search_sql (มักรอดจาก 403 มากกว่า datastore_search)
    """
    # ปรับ WHERE ได้ตามต้องการ
    sql = f"""
    SELECT *
    FROM "{resource_id}"
    WHERE project_name LIKE '%อินทร์บุรี%'
       OR prov_name LIKE '%สิงห์บุรี%'
       OR dept_name LIKE '%อินทร์บุรี%'
    LIMIT 200
    """

    params = {"sql": sql}

    resp = http_get_with_retry(OPEND_DATASTORE_SQL, headers=HEADERS_API, params=params)
    data = resp.json()
    return data.get("result", {}).get("records", []) or []


def main() -> None:
    if not require_env():
        return

    print("✅ Environment OK")
    print(f"🔧 Using DATA_RESOURCE_ID: {RESOURCE_ID}")

    print("📥 กำลังดึงข้อมูลจาก opend.data.go.th (SQL) ...")
    try:
        records = fetch_records_sql(RESOURCE_ID)
        print(f"✅ ดึงสำเร็จ: {len(records)} รายการ")
    except Exception as e:
        print(f"❌ ดึงข้อมูลล้มเหลว: {e}")
        return

    # โหลด data เก่า
    old_data = load_old_data(DATA_JSON_PATH)
    old_ids = {str(x.get("project_id")) for x in old_data if isinstance(x, dict)}

    # หาโครงการใหม่
    new_projects: List[Dict[str, Any]] = []
    for proj in records:
        proj_id = str(proj.get("project_id", "")).strip()
        if not proj_id:
            continue
        if proj_id not in old_ids:
            new_projects.append(proj)

    if new_projects:
        print(f"🆕 พบโครงการใหม่ {len(new_projects)} รายการ กำลังส่งแจ้งเตือน ...")
        for proj in new_projects:
            send_line_notify(
                str(proj.get("project_name", "ไม่ระบุชื่อโครงการ")),
                str(proj.get("sum_price_agree", "ไม่ระบุ")),
                str(proj.get("dept_name", "ไม่ระบุหน่วยงาน")),
            )
            print(f"📨 แจ้งแล้ว: {proj.get('project_name')}")

        # เก็บใหม่ไว้บนสุด
        save_data(new_projects + old_data, DATA_JSON_PATH)
        print("✅ อัปเดต data.json เรียบร้อย")
    else:
        if not os.path.exists(DATA_JSON_PATH):
            save_data([], DATA_JSON_PATH)
        print("😴 วันนี้ไม่มีโครงการใหม่ของอินทร์บุรี")


if __name__ == "__main__":
    main()

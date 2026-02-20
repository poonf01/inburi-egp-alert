import os
import json
import time
from curl_cffi import requests
from typing import Any, Dict, List

# ======================
# ENV
# ======================
LINE_TOKEN = os.environ.get("LINE_TOKEN", "").strip()
API_KEY = os.environ.get("DATA_API_KEY", "").strip()
RESOURCE_ID = os.environ.get("DATA_RESOURCE_ID", "").strip()

# ======================
# CONSTANTS & HEADERS
# ======================
OPEND_DATASTORE_SQL = "https://opend.data.go.th/get-ckan/datastore_search_sql"
OPEND_DATASTORE_SEARCH = "https://opend.data.go.th/get-ckan/datastore_search"
DATA_JSON_PATH = "data.json"

HEADERS_API = {
    "api-key": API_KEY,
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://data.go.th/"
}

HEADERS_LINE = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {LINE_TOKEN}"
}

# ======================
# CORE FUNCTIONS
# ======================
def fetch_like_a_human(url: str, payload: dict) -> dict:
    """ ฟังก์ชันปลอมตัวเป็น Google Chrome เพื่อเจาะระบบป้องกัน WAF """
    for i in range(3):
        try:
            # impersonate="chrome116" คือหัวใจสำคัญในการหลบ WAF
            resp = requests.post(url, headers=HEADERS_API, json=payload, impersonate="chrome116", timeout=45)
            if resp.status_code == 200:
                return resp.json()
            print(f"⚠️ HTTP {resp.status_code} โดนขวาง, รอ {2**i} วิ แล้วลองใหม่...")
            time.sleep(2**i)
        except Exception as e:
            print(f"⚠️ Error {e}, รอ {2**i} วิ แล้วลองใหม่...")
            time.sleep(2**i)
    
    raise RuntimeError("ทะลวงไม่สำเร็จ ระบบรัฐบาลบล็อก IP ของ GitHub แน่นหนามาก")

def get_records() -> List[Dict[str, Any]]:
    # 1. ลองวิธี SQL ก่อน (ดีและแม่นยำสุด)
    sql = f"""SELECT * FROM "{RESOURCE_ID}" WHERE project_name LIKE '%อินทร์บุรี%' OR prov_name LIKE '%สิงห์บุรี%' OR dept_name LIKE '%อินทร์บุรี%' LIMIT 200"""
    try:
        print("🕵️ กำลังดึงข้อมูล (แบบ SQL)...")
        data = fetch_like_a_human(OPEND_DATASTORE_SQL, {"sql": sql})
        return data.get("result", {}).get("records", [])
    except Exception as e:
        print(f"❌ SQL พลาด: {e}")

    # 2. ถ้า SQL พัง ลองแบบ Search คำค้นหาแทน
    print("🕵️ กำลังดึงข้อมูล (แบบ Search ทั่วไป)...")
    out, seen = [], set()
    for q in ["อินทร์บุรี", "สิงห์บุรี"]:
        try:
            data = fetch_like_a_human(OPEND_DATASTORE_SEARCH, {"resource_id": RESOURCE_ID, "q": q, "limit": 200})
            for r in (data.get("result", {}).get("records", []) or []):
                pid = str(r.get("project_id", "")).strip()
                if pid and pid not in seen:
                    seen.add(pid)
                    out.append(r)
        except Exception:
            continue
            
    if not out:
        raise RuntimeError("หมดหนทาง รัฐบาลน่าจะแบน IP ของ GitHub Actions 100% ครับ")
    return out

def load_old_data() -> List[Dict[str, Any]]:
    if not os.path.exists(DATA_JSON_PATH): return []
    try:
        with open(DATA_JSON_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []

def save_data(items: List[Dict[str, Any]]) -> None:
    with open(DATA_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)

def send_line(project_name: str, budget: str, dept: str) -> None:
    msg = f"📢 ประกาศจัดซื้อจัดจ้างใหม่!\n\n🏢 {dept}\n📌 {project_name}\n💰 {budget} บาท\n\n(by Alieninburi)"
    try:
        requests.post("https://api.line.me/v2/bot/message/broadcast", headers=HEADERS_LINE, json={"messages": [{"type": "text", "text": msg}]})
    except Exception as e:
        print(f"⚠️ LINE Error: {e}")

# ======================
# MAIN
# ======================
def main():
    if not all([API_KEY, LINE_TOKEN, RESOURCE_ID]):
        print("❌ ตั้งค่า Secrets ไม่ครบครับ")
        return

    try:
        records = get_records()
        print(f"✅ ทะลวงกำแพงสำเร็จ! ได้มา {len(records)} รายการ")
    except Exception as e:
        print(f"❌ ล้มเหลว: {e}")
        return

    old_data = load_old_data()
    old_ids = {str(x.get("project_id")) for x in old_data if isinstance(x, dict)}

    new_projects = [p for p in records if str(p.get("project_id", "")) and str(p.get("project_id", "")) not in old_ids]

    if new_projects:
        print(f"🆕 เจอของใหม่ {len(new_projects)} งาน ส่ง LINE ด่วน!")
        for p in new_projects:
            send_line(p.get("project_name", "-"), p.get("sum_price_agree", "-"), p.get("dept_name", "-"))
        save_data(new_projects + old_data)
    else:
        if not os.path.exists(DATA_JSON_PATH): save_data([])
        print("😴 ไม่มีประกาศใหม่ครับ")

if __name__ == "__main__":
    main()

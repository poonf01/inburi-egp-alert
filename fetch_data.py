import os
import json
import time
import requests
from typing import Any, Dict, List, Optional

LINE_TOKEN = os.environ.get("LINE_TOKEN", "").strip()
API_KEY = os.environ.get("DATA_API_KEY", "").strip()
RESOURCE_ID = os.environ.get("DATA_RESOURCE_ID", "").strip()

# --- Common headers ---
HEADERS_BROWSER = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    )
}

HEADERS_API = {
    **HEADERS_BROWSER,
    "api-key": API_KEY,
}

HEADERS_LINE = {
    **HEADERS_BROWSER,
    "Content-Type": "application/json",
    "Authorization": f"Bearer {LINE_TOKEN}",
}

OPEND_DATASTORE_SEARCH = "https://opend.data.go.th/get-ckan/datastore_search"
LINE_BROADCAST_URL = "https://api.line.me/v2/bot/message/broadcast"


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
        print("👉 แก้โดยไปที่ GitHub > Settings > Secrets and variables > Actions")
        print("   แล้วเพิ่ม Secrets ให้ครบ: LINE_TOKEN, DATA_API_KEY, DATA_RESOURCE_ID")
        return False
    return True


def http_get_with_retry(
    url: str,
    headers: Dict[str, str],
    params: Dict[str, Any],
    retries: int = 4,
    timeout: int = 30,
) -> requests.Response:
    last_exc: Optional[Exception] = None
    for i in range(retries):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=timeout)
            # ถ้าโดน 403/429 ให้ลองใหม่แบบถอยหลัง
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


def send_line_notify(project_name: str, budget: str, department: str) -> None:
    # ถ้า token ว่าง อย่าส่ง
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
        # ไม่ให้สคริปต์ล้มเพราะ LINE อย่างเดียว
        if resp.status_code >= 400:
            print(f"⚠️ LINE notify failed: HTTP {resp.status_code} {resp.text[:200]}")
    except Exception as e:
        print(f"⚠️ LINE notify exception: {e}")


def load_old_data(path: str = "data.json") -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except Exception:
        return []


def save_data(items: List[Dict[str, Any]], path: str = "data.json") -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)


def main() -> None:
    if not require_env():
        return

    print("✅ Environment OK")
    print(f"🔧 Using DATA_RESOURCE_ID: {RESOURCE_ID}")

    # ดึงข้อมูลเฉพาะ keyword "อินทร์บุรี" (ปรับได้)
    params = {
        "resource_id": RESOURCE_ID,
        "q": "อินทร์บุรี",
        "limit": 200,
    }

    print("📥 กำลังดึงข้อมูลจาก opend.data.go.th ...")
    try:
        resp = http_get_with_retry(OPEND_DATASTORE_SEARCH, headers=HEADERS_API, params=params)
        data = resp.json()
        records = data.get("result", {}).get("records", []) or []
        print(f"✅ ดึงสำเร็จ: {len(records)} รายการ (จาก query อินทร์บุรี)")
    except Exception as e:
        print(f"❌ ดึงข้อมูลล้มเหลว: {e}")
        return

    # filter เพิ่มอีกชั้น เผื่อบาง record ไม่มี prov_name แต่มีข้อความ
    inburi_projects = [
        r for r in records
        if ("อินทร์บุรี" in str(r)) or ("สิงห์บุรี" in str(r.get("prov_name", "")))
    ]

    old_data = load_old_data("data.json")
    old_ids = {str(x.get("project_id")) for x in old_data if isinstance(x, dict)}

    new_projects: List[Dict[str, Any]] = []
    for proj in inburi_projects:
        proj_id = str(proj.get("project_id", "")).strip()
        if not proj_id:
            # ถ้าไม่มี id ก็ข้าม เพื่อไม่ให้แจ้งซ้ำ
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

        # เก็บใหม่ไว้บนสุด
        save_data(new_projects + old_data, "data.json")
        print("✅ อัปเดต data.json เรียบร้อย")
    else:
        # ให้มีไฟล์ data.json เสมอ
        if not os.path.exists("data.json"):
            save_data([], "data.json")
        print("😴 วันนี้ไม่มีโครงการใหม่ของอินทร์บุรี")


if __name__ == "__main__":
    main()

import os
import json
import time
import subprocess
import urllib.parse
import requests
from typing import Any, Dict, List, Optional, Tuple

# ======================
# ENV
# ======================
LINE_TOKEN = os.environ.get("LINE_TOKEN", "").strip()
API_KEY = os.environ.get("DATA_API_KEY", "").strip()
RESOURCE_ID = os.environ.get("DATA_RESOURCE_ID", "").strip()

# (ทางเลือก) ถ้าจะใช้ proxy ที่รันไว้ที่อื่น (Render/Railway/VPS)
PROXY_URL = os.environ.get("PROXY_URL", "").strip().rstrip("/")

# ======================
# CONSTANTS
# ======================
OPEND_DATASTORE_SQL = "https://opend.data.go.th/get-ckan/datastore_search_sql"
OPEND_DATASTORE_SEARCH = "https://opend.data.go.th/get-ckan/datastore_search"

LINE_BROADCAST_URL = "https://api.line.me/v2/bot/message/broadcast"
DATA_JSON_PATH = "data.json"

# ======================
# HEADERS (กัน WAF ให้เต็ม)
# ======================
# NOTE: ใส่ทั้ง api-key และ x-api-key เผื่อ gateway บางตัวเช็คคนละชื่อ
HEADERS_API = {
    "api-key": API_KEY,
    "x-api-key": API_KEY,
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "th-TH,th;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Connection": "keep-alive",
    # UA ให้เหมือน Chrome จริง (หลาย WAF บล็อก UA สั้น ๆ)
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
    if not API_KEY:
        missing.append("DATA_API_KEY")
    if not LINE_TOKEN:
        missing.append("LINE_TOKEN")
    if not RESOURCE_ID:
        missing.append("DATA_RESOURCE_ID")

    if missing:
        print("❌ Missing environment variables:", ", ".join(missing))
        print("👉 GitHub Repo > Settings > Secrets and variables > Actions")
        print("   เพิ่ม Secrets ให้ครบ: LINE_TOKEN, DATA_API_KEY, DATA_RESOURCE_ID")
        return False
    return True


def _short(text: str, n: int = 250) -> str:
    t = (text or "").replace("\n", " ").replace("\r", " ").strip()
    return t[:n]


def _build_url(url: str, params: Dict[str, Any]) -> str:
    q = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
    return f"{url}?{q}" if q else url


def _curl_get_json(url: str, headers: Dict[str, str], params: Dict[str, Any], timeout: int = 60) -> Dict[str, Any]:
    full_url = _build_url(url, params)

    cmd = [
        "curl",
        "-sS",
        "-L",
        "--compressed",
        "--connect-timeout",
        str(timeout),
        "--max-time",
        str(timeout),
        full_url,
    ]
    for k, v in headers.items():
        if v:
            cmd.insert(-1, f"{k}: {v}")
            cmd.insert(-1, "-H")

    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"curl failed rc={p.returncode}: {_short(p.stderr)}")

    try:
        return json.loads(p.stdout)
    except Exception:
        raise RuntimeError(f"curl returned non-json: {_short(p.stdout)}")


def _requests_get_json(
    url: str,
    headers: Dict[str, str],
    params: Dict[str, Any],
    retries: int = 4,
    timeout: int = 45,
) -> Dict[str, Any]:
    """
    requests ก่อน -> ถ้าโดน 403/429/5xx จะ retry และสุดท้าย throw error ที่มีรายละเอียดจริง
    """
    session = requests.Session()
    last_err: Optional[Exception] = None

    for i in range(retries):
        try:
            resp = session.get(url, headers=headers, params=params, timeout=timeout)

            # OK
            if resp.status_code < 400:
                return resp.json()

            body = _short(resp.text)
            # 403: มักโดน WAF บล็อก -> ไม่ต้องรอให้ครบทุก retry ก็ได้ แต่ให้ลองอีกครั้ง 1-2 รอบเผื่อชั่วคราว
            if resp.status_code in (403, 429, 500, 502, 503, 504):
                last_err = RuntimeError(f"HTTP {resp.status_code}: {body}")
                wait = 2 ** i
                print(f"⚠️ HTTP {resp.status_code} retry in {wait}s ... ({body})")
                time.sleep(wait)
                continue

            resp.raise_for_status()
            return resp.json()

        except Exception as e:
            last_err = e
            wait = 2 ** i
            print(f"⚠️ Request error retry in {wait}s ... ({e})")
            time.sleep(wait)

    raise RuntimeError(f"requests failed after retries: {last_err}")


def get_json_with_fallback(url: str, headers: Dict[str, str], params: Dict[str, Any]) -> Dict[str, Any]:
    """
    1) requests (มี retry)
    2) ถ้ายังพัง -> curl (มักผ่าน WAF ได้)
    """
    try:
        return _requests_get_json(url, headers, params)
    except Exception as e1:
        print(f"⚠️ requests failed -> trying curl fallback ... ({e1})")

    # curl fallback (ไม่ retry ยาว ๆ ให้เสียเวลา)
    return _curl_get_json(url, headers, params, timeout=70)


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
            print(f"⚠️ LINE notify failed: HTTP {resp.status_code} {_short(resp.text)}")
    except Exception as e:
        print(f"⚠️ LINE notify exception: {e}")


# ======================
# FETCH
# ======================
def fetch_records_sql(resource_id: str) -> List[Dict[str, Any]]:
    sql = f"""
    SELECT *
    FROM "{resource_id}"
    WHERE project_name LIKE '%อินทร์บุรี%'
       OR prov_name LIKE '%สิงห์บุรี%'
       OR dept_name LIKE '%อินทร์บุรี%'
    LIMIT 200
    """
    data = get_json_with_fallback(OPEND_DATASTORE_SQL, headers=HEADERS_API, params={"sql": sql})
    return data.get("result", {}).get("records", []) or []


def fetch_records_search(resource_id: str) -> List[Dict[str, Any]]:
    """
    fallback อีกชั้น: datastore_search (ไม่ใช้ SQL)
    ทำ 2 ครั้งแล้ว union กัน เพื่อเลี่ยงปัญหา AND/OR ใน q
    """
    out: List[Dict[str, Any]] = []
    seen: set = set()

    for q in ["อินทร์บุรี", "สิงห์บุรี"]:
        data = get_json_with_fallback(
            OPEND_DATASTORE_SEARCH,
            headers=HEADERS_API,
            params={"resource_id": resource_id, "q": q, "limit": 200},
        )
        recs = data.get("result", {}).get("records", []) or []
        for r in recs:
            pid = str(r.get("project_id", "")).strip()
            key = pid if pid else json.dumps(r, ensure_ascii=False, sort_keys=True)
            if key not in seen:
                seen.add(key)
                out.append(r)

    return out


def fetch_records_via_proxy() -> List[Dict[str, Any]]:
    """
    ต้องมี PROXY_URL ตั้งไว้ เช่น https://xxx.onrender.com
    endpoint: /egp -> คืน JSON แบบเดียวกับ CKAN
    """
    if not PROXY_URL:
        return []

    url = f"{PROXY_URL}/egp"
    print(f"🌐 Using PROXY_URL: {url}")

    # proxy เราไม่ต้องส่ง api-key ก็ได้ (เพราะ proxy เก็บ key เอง)
    try:
        data = get_json_with_fallback(url, headers={"Accept": "application/json"}, params={})
        return data.get("result", {}).get("records", []) or []
    except Exception as e:
        print(f"⚠️ proxy fetch failed: {e}")
        return []


def fetch_records_any(resource_id: str) -> List[Dict[str, Any]]:
    # 1) SQL ก่อน (ดีที่สุด)
    try:
        return fetch_records_sql(resource_id)
    except Exception as e:
        print(f"⚠️ SQL failed: {e}")

    # 2) datastore_search
    try:
        return fetch_records_search(resource_id)
    except Exception as e:
        print(f"⚠️ datastore_search failed: {e}")

    # 3) proxy (ถ้ามี)
    recs = fetch_records_via_proxy()
    if recs:
        return recs

    # ถ้ายังไม่มา แปลว่าโดนบล็อกหนัก/คีย์ไม่ผ่านจริง ๆ
    raise RuntimeError("All fetch methods failed (SQL, datastore_search, proxy).")


# ======================
# MAIN
# ======================
def main() -> None:
    if not require_env():
        return

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

        save_data(new_projects + old_data, DATA_JSON_PATH)
        print("✅ อัปเดต data.json เรียบร้อย")
    else:
        if not os.path.exists(DATA_JSON_PATH):
            save_data([], DATA_JSON_PATH)
        print("😴 วันนี้ไม่มีโครงการใหม่ของอินทร์บุรี")


if __name__ == "__main__":
    main()

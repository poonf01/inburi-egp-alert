import os
import json
import requests

LINE_TOKEN = os.environ.get("LINE_TOKEN")
API_KEY = os.environ.get("DATA_API_KEY")

# Allow users to manually specify a resource ID via environment to avoid
# package_search lookups. Useful if the search endpoint is unavailable.
MANUAL_RESOURCE_ID = os.environ.get("DATA_RESOURCE_ID")

# Header for API requests that require an API key
headers_api = {"api-key": API_KEY}
headers_line = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {LINE_TOKEN}"
}

# เพิ่มหน้ากาก User-Agent เพื่อหลอกระบบเว็บรัฐว่าเราคือ Google Chrome ไม่ใช่บอท
headers_browser = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
}

def get_latest_egp_resource_id() -> str | None:
    """
    Search for the most recently updated e-GP dataset on data.go.th and
    return the identifier of its downloadable resource (CSV/API/JSON).

    Uses the `package_search` endpoint on the opend.data.go.th service,
    which requires an API key. Combining the API key header with a
    browser-like User-Agent helps bypass basic firewall and rate limits.

    Returns:
        The resource ID string if found, otherwise None.
    """
    print("🔍 กำลังค้นหาไฟล์ข้อมูลจัดซื้อจัดจ้างเดือนล่าสุด...")
    # Use the opend data portal which expects an API key in the header.
    search_url = "https://opend.data.go.th/get-ckan/package_search"
    params = {
        "q": "จัดซื้อจัดจ้าง",
        # sort by most recently modified metadata first
        "sort": "metadata_modified desc",
        "rows": 3,
    }
    try:
        # Merge API key and browser headers so both are sent
        search_headers = {}
        if headers_browser:
            search_headers.update(headers_browser)
        if headers_api:
            search_headers.update(headers_api)

        # Perform the search
        response = requests.get(search_url, headers=search_headers, params=params)
        response.raise_for_status()
        data = response.json()

        # Iterate through returned packages and pick the newest resource
        for pkg in data.get("result", {}).get("results", []):
            resources = pkg.get("resources", [])
            if resources:
                # Reverse iterate to pick the last defined resource first
                for res in reversed(resources):
                    fmt = (res.get("format") or "").lower()
                    if fmt in {"csv", "api", "json"}:
                        resource_id = res.get("id")
                        print(
                            f"✅ เจอไฟล์ล่าสุดแล้ว: {res.get('name')} (รหัส: {resource_id})"
                        )
                        return resource_id
        print("⚠️ ไม่พบ Resource ที่มีรูปแบบ CSV/API/JSON ในแพ็คเกจที่ค้นพบ")
    except Exception as e:
        # Log detailed error and return None so the caller can handle it
        print(f"❌ ค้นหา Resource ID อัตโนมัติไม่สำเร็จ: {e}")
    return None

def send_line_notify(project_name, budget, department):
    url = "https://api.line.me/v2/bot/message/broadcast"
    message = f"📢 มีประกาศจัดซื้อจัดจ้างใหม่ในพื้นที่!\n\n🏢 หน่วยงาน: {department}\n📌 โครงการ: {project_name}\n💰 งบประมาณ: {budget} บาท\n\n(by Alieninburi)"
    
    data = {"messages": [{"type": "text", "text": message}]}
    requests.post(url, headers=headers_line, json=data)

def main():
    # Use the manually provided resource ID if available, otherwise search for it.
    resource_id = None
    if MANUAL_RESOURCE_ID:
        print(
            f"🔧 ใช้ Resource ID ที่กำหนดผ่าน environment: {MANUAL_RESOURCE_ID}"
        )
        resource_id = MANUAL_RESOURCE_ID
    else:
        resource_id = get_latest_egp_resource_id()
        if not resource_id:
            print("ไม่สามารถหา Resource ID ได้ สคริปต์หยุดทำงาน")
            return

    API_URL = "https://opend.data.go.th/get-ckan/datastore_search"
    PAYLOAD = {
        "resource_id": resource_id,
        "q": "อินทร์บุรี",
        "limit": 100
    }

    print("กำลังดึงข้อมูลโครงการของอินทร์บุรี...")
    try:
        response = requests.get(API_URL, headers=headers_api, params=PAYLOAD)
        response.raise_for_status()
        data = response.json()
        records = data.get("result", {}).get("records", [])
    except Exception as e:
        print(f"เกิดข้อผิดพลาดในการดึงข้อมูล: {e}")
        return

    inburi_projects = [r for r in records if "สิงห์บุรี" in str(r.get("prov_name", "")) or "อินทร์บุรี" in str(r)]

    old_data = []
    if os.path.exists("data.json"):
        with open("data.json", "r", encoding="utf-8") as f:
            try:
                old_data = json.load(f)
            except json.JSONDecodeError:
                old_data = []

    old_project_ids = {str(item.get("project_id")) for item in old_data}
    new_projects = []

    for proj in inburi_projects:
        proj_id = str(proj.get("project_id"))
        if proj_id not in old_project_ids:
            new_projects.append(proj)
            send_line_notify(
                proj.get("project_name", "ไม่ระบุชื่อโครงการ"), 
                proj.get("sum_price_agree", "ไม่ระบุ"), 
                proj.get("dept_name", "ไม่ระบุหน่วยงาน")
            )
            print(f"ส่งแจ้งเตือน: {proj.get('project_name')}")

    if new_projects:
        all_projects = new_projects + old_data
        with open("data.json", "w", encoding="utf-8") as f:
            json.dump(all_projects, f, ensure_ascii=False, indent=2)
        print(f"อัปเดตไฟล์ data.json เรียบร้อย (เพิ่ม {len(new_projects)} รายการ)")
    else:
        if not os.path.exists("data.json"):
             with open("data.json", "w", encoding="utf-8") as f:
                 json.dump([], f)
        print("วันนี้ไม่มีโครงการใหม่ของอินทร์บุรี")

if __name__ == "__main__":
    main()

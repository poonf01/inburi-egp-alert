import os
import json
import requests

# ดึงกุญแจจากตู้เซฟ GitHub
LINE_TOKEN = os.environ.get("LINE_TOKEN")
API_KEY = os.environ.get("DATA_API_KEY")

headers_api = {"api-key": API_KEY}
headers_line = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {LINE_TOKEN}"
}

def get_latest_egp_resource_id():
    """ฟังก์ชันนักล่า: ค้นหา Resource ID ของเดือนล่าสุดอัตโนมัติ จะได้ไม่ต้องแก้โค้ดเอง"""
    print("🔍 กำลังค้นหาไฟล์ข้อมูลจัดซื้อจัดจ้างเดือนล่าสุด...")
    search_url = "https://opend.data.go.th/get-ckan/package_search"
    # ค้นหาคำว่า จัดซื้อจัดจ้าง และเรียงตามวันที่อัปเดตล่าสุด
    params = {
        "q": "จัดซื้อจัดจ้าง", 
        "sort": "metadata_modified desc",
        "rows": 3
    }
    
    try:
        response = requests.get(search_url, headers=headers_api, params=params)
        response.raise_for_status()
        data = response.json()
        
        # วนลูปหาแพ็กเกจข้อมูลที่เจอ
        for pkg in data.get("result", {}).get("results", []):
            resources = pkg.get("resources", [])
            if resources:
                # ดึงไฟล์ล่าสุดมาใช้
                for res in reversed(resources):
                    if res.get("format", "").lower() in ["csv", "api", "json"]:
                        print(f"✅ เจอไฟล์ล่าสุดแล้ว: {res.get('name')} (รหัส: {res.get('id')})")
                        return res.get("id")
    except Exception as e:
        print(f"❌ ค้นหา Resource ID อัตโนมัติไม่สำเร็จ: {e}")
        return None

def send_line_notify(project_name, budget, department):
    """ส่งข้อความเข้า LINE OA"""
    url = "https://api.line.me/v2/bot/message/broadcast"
    message = f"📢 มีประกาศจัดซื้อจัดจ้างใหม่ในพื้นที่!\n\n🏢 หน่วยงาน: {department}\n📌 โครงการ: {project_name}\n💰 งบประมาณ: {budget} บาท\n\n(by Alieninburi)"
    
    data = {"messages": [{"type": "text", "text": message}]}
    requests.post(url, headers=headers_line, json=data)

def main():
    # 1. ให้บอทไปล่า Resource ID ล่าสุดมาก่อนเลย
    resource_id = get_latest_egp_resource_id()
    if not resource_id:
        print("ไม่สามารถหา Resource ID ได้ สคริปต์หยุดทำงาน")
        return

    # 2. นำ ID ที่ล่ามาได้ ไปดึงข้อมูลเฉพาะของอินทร์บุรี
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

    # กรองเอาแค่สิงห์บุรี/อินทร์บุรี
    inburi_projects = [r for r in records if "สิงห์บุรี" in str(r.get("prov_name", "")) or "อินทร์บุรี" in str(r)]

    # 3. ตรวจสอบข้อมูลเก่า
    old_data = []
    if os.path.exists("data.json"):
        with open("data.json", "r", encoding="utf-8") as f:
            try:
                old_data = json.load(f)
            except json.JSONDecodeError:
                old_data = []

    old_project_ids = {str(item.get("project_id")) for item in old_data}
    new_projects = []

    # 4. ส่ง LINE ถ้ามีของใหม่
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

    # 5. เซฟข้อมูลใหม่
    if new_projects:
        all_projects = new_projects + old_data
        with open("data.json", "w", encoding="utf-8") as f:
            json.dump(all_projects, f, ensure_ascii=False, indent=2)
        print(f"อัปเดตไฟล์ data.json เรียบร้อย (เพิ่ม {len(new_projects)} รายการ)")
    else:
        # สร้างไฟล์เปล่าไว้ถ้ายังไม่มี จะได้ไม่เกิด Error สีแดงตอน Push
        if not os.path.exists("data.json"):
             with open("data.json", "w", encoding="utf-8") as f:
                 json.dump([], f)
        print("วันนี้ไม่มีโครงการใหม่ของอินทร์บุรี")

if __name__ == "__main__":
    main()

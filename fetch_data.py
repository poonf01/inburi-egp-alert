import os
import json
import requests
from datetime import datetime

# 1. ดึงกุญแจจากตู้เซฟ GitHub ที่เราตั้งไว้
LINE_TOKEN = os.environ.get("LINE_TOKEN")
API_KEY = os.environ.get("DATA_API_KEY")

# ตั้งค่า Headers สำหรับเรียก API และส่ง LINE
headers_api = {"api-key": API_KEY}
headers_line = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {LINE_TOKEN}"
}

# (ตัวอย่าง) URL ของ API ชุดข้อมูลจัดซื้อจัดจ้าง (สามารถปรับเปลี่ยนเป็น endpoint ที่ถูกต้องของปีปัจจุบันได้)
API_URL = "https://opend.data.go.th/get-ckan/datastore_search"
# ใส่ resource_id ของชุดข้อมูล e-GP ที่ต้องการ (เป็นตัวอย่าง)
PAYLOAD = {
    "resource_id": "c89b788a-3e8a-442d-b4ea-4700d3663a76", 
    "q": "อินทร์บุรี", # ค้นหาคำว่า อินทร์บุรี
    "limit": 50
}

def send_line_notify(project_name, budget, department):
    """ฟังก์ชันสำหรับส่งข้อความเข้า LINE OA"""
    url = "https://api.line.me/v2/bot/message/broadcast"
    message = f"📢 มีประกาศจัดซื้อจัดจ้างใหม่!\n\n🏢 หน่วยงาน: {department}\n📌 โครงการ: {project_name}\n💰 งบประมาณ: {budget} บาท\n\nดูรายละเอียดเพิ่มเติมได้ที่เว็บของเราครับ (by Alieninburi)"
    
    data = {
        "messages": [{"type": "text", "text": message}]
    }
    requests.post(url, headers=headers_line, json=data)

def main():
    print("กำลังดึงข้อมูลจาก Data.go.th...")
    try:
        response = requests.get(API_URL, headers=headers_api, params=PAYLOAD)
        response.raise_for_status()
        data = response.json()
        records = data.get("result", {}).get("records", [])
    except Exception as e:
        print(f"เกิดข้อผิดพลาดในการดึงข้อมูล: {e}")
        return

    # กรองเฉพาะจังหวัดสิงห์บุรี (เพื่อความชัวร์)
    inburi_projects = [r for r in records if "สิงห์บุรี" in str(r.get("prov_name", "")) or "อินทร์บุรี" in str(r)]

    # 2. อ่านข้อมูลเก่าจากไฟล์ data.json (ถ้ามี)
    old_data = []
    if os.path.exists("data.json"):
        with open("data.json", "r", encoding="utf-8") as f:
            try:
                old_data = json.load(f)
            except json.JSONDecodeError:
                old_data = []

    # หาโครงการเก่าที่เคยเซฟไว้แล้ว (เช็กจากรหัสโครงการ project_id)
    old_project_ids = {str(item.get("project_id")) for item in old_data}
    
    new_projects = []
    
    # 3. ตรวจสอบว่ามีโครงการใหม่หรือไม่
    for proj in inburi_projects:
        proj_id = str(proj.get("project_id"))
        if proj_id not in old_project_ids:
            new_projects.append(proj)
            # ส่ง LINE แจ้งเตือนโครงการใหม่ทีละรายการ
            send_line_notify(
                proj.get("project_name", "ไม่ระบุชื่อโครงการ"), 
                proj.get("sum_price_agree", "ไม่ระบุ"), 
                proj.get("dept_name", "ไม่ระบุหน่วยงาน")
            )
            print(f"ส่งแจ้งเตือนโครงการ: {proj.get('project_name')}")

    # 4. รวมข้อมูลใหม่กับข้อมูลเก่า และบันทึกทับไฟล์ data.json
    if new_projects:
        all_projects = new_projects + old_data
        with open("data.json", "w", encoding="utf-8") as f:
            json.dump(all_projects, f, ensure_ascii=False, indent=2)
        print(f"อัปเดตไฟล์ data.json เรียบร้อยแล้ว (เพิ่ม {len(new_projects)} รายการใหม่)")
    else:
        print("ไม่มีโครงการใหม่ในวันนี้")

if __name__ == "__main__":
    main()

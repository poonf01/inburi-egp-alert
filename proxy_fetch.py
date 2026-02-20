name: Update e-GP Data Inburi

on:
  schedule:
    - cron: '0 1 * * *'
  workflow_dispatch:

jobs:
  fetch-and-notify:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.10'

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install requests

      - name: Run Python script
        env:
          LINE_TOKEN: ${{ secrets.LINE_TOKEN }}
          DATA_API_KEY: ${{ secrets.DATA_API_KEY }}
          DATA_RESOURCE_ID: ${{ secrets.DATA_RESOURCE_ID }}
          PROXY_URL: ${{ secrets.PROXY_URL }}   # <-- จุดที่หายไป เพิ่มตรงนี้เพื่อให้ดึงผ่าน Proxy ได้
        run: python fetch_data.py

      - name: Commit and Push changes
        run: |
          git config --global user.name "github-actions[bot]"
          git config --global user.email "github-actions[bot]@users.noreply.github.com"
          if [ -f data.json ]; then
            git add data.json
            git commit -m "Auto-update data.json with new e-GP records" || echo "No changes to commit"
            git push
          else
            echo "ไม่มีไฟล์ data.json ให้เซฟ ข้ามไปเลย!"
          fi

"""
update_cb.py
從 https://cbas16889.pscnet.com.tw/marketInfo/expectedRelease 抓取「預計發行 / 即將掛牌」CB 資料
包含三階段：近期掛牌、申報生效、董事會決議
策略：攔截頁面發出的 XHR/fetch API 請求，直接取得 JSON
"""
import os
import json
import subprocess
import sys

def install_requirements():
    try:
        import playwright
        import pandas
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "playwright", "pandas"])
        subprocess.check_call([sys.executable, "-m", "playwright", "install", "chromium"])

install_requirements()

from playwright.sync_api import sync_playwright
import pandas as pd

def scrape_expected_cb():
    url = "https://cbas16889.pscnet.com.tw/marketInfo/expectedRelease"
    print(f"開啟瀏覽器攔截預計發行 CB API: {url}")

    apis = {}

    def handle_response(response):
        """攔截預計發行專區的三大 API 回應"""
        ct = response.headers.get("content-type", "")
        if "json" in ct and response.status == 200:
            for name in ["GetRecentlyListed", "GetRecentlyEffectively", "GetBoardAnnouncement"]:
                if name in response.url:
                    try:
                        res_json = response.json()
                        result = res_json.get("result", [])
                        if isinstance(result, list):
                            apis[name] = result
                            print(f"  [攔截成功] {name} ({len(result)} 筆)")
                    except Exception as e:
                        print(f"  [解析失敗] {name}: {e}")

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-blink-features=AutomationControlled"]
        )
        context = browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/125.0.0.0 Safari/537.36"
            ),
        )
        page = context.new_page()
        page.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
        )
        page.on("response", handle_response)

        print("載入頁面，等待 API 回應...")
        page.goto(url, timeout=30000, wait_until="networkidle")
        page.wait_for_timeout(3000)
        browser.close()

    if not apis:
        print("[ERR] 未攔截到任何預計發行 CB 的 API 回應。")
        return

    output_rows = []

    # 1. 近期掛牌 (已定價/即將掛牌上櫃)
    for item in apis.get("GetRecentlyListed", []):
        code = str(item.get("code", "")).strip()
        if not code or not any(c.isdigit() for c in code):
            continue
        output_rows.append({
            "股票代號": code,
            "CB名稱": str(item.get("cb_name", "")).strip(),
            "CB代號": str(item.get("cb_code", "")).strip(),
            "進度狀態": "近期掛牌",
            "主辦券商": str(item.get("host_broker", "")).strip(),
            "發行量(億)": str(item.get("circulation", "")).strip(),
            "轉換價格": str(item.get("conversion_price", "")).strip(),
            "掛牌日/進度日": str(item.get("listing_day", "")).strip(),
            "備註": str(item.get("remark", "")).strip()
        })

    # 2. 申報生效 (主管機關已核准生效)
    for item in apis.get("GetRecentlyEffectively", []):
        code = str(item.get("code", "")).strip()
        if not code or not any(c.isdigit() for c in code):
            continue
        effective_date = str(item.get("expected_effective_date", "")).strip() or str(item.get("announcement_day", "")).strip()
        output_rows.append({
            "股票代號": code,
            "CB名稱": str(item.get("cb_name", "")).strip(),
            "CB代號": str(item.get("cb_code", "")).strip(),
            "進度狀態": "申報生效",
            "主辦券商": str(item.get("host_broker", "")).strip(),
            "發行量(億)": str(item.get("circulation", "")).strip(),
            "轉換價格": str(item.get("tentative_premium_rate", "")).strip(),
            "掛牌日/進度日": effective_date,
            "備註": str(item.get("remark", "")).strip()
        })

    # 3. 董事會決議 (董事會通過發行)
    for item in apis.get("GetBoardAnnouncement", []):
        code = str(item.get("code", "")).strip()
        if not code or not any(c.isdigit() for c in code):
            continue
        output_rows.append({
            "股票代號": code,
            "CB名稱": str(item.get("cb_name", "")).strip(),
            "CB代號": str(item.get("cb_code", "")).strip(),
            "進度狀態": "董事會決議",
            "主辦券商": str(item.get("host_broker", "")).strip(),
            "發行量(億)": str(item.get("circulation", "")).strip(),
            "轉換價格": str(item.get("tentative_premium_rate", "")).strip(),
            "掛牌日/進度日": str(item.get("announcement_day", "")).strip(),
            "備註": str(item.get("remark", "")).strip()
        })

    if not output_rows:
        print("[WARN] 沒有抓取到有效資料。")
        return

    _save_and_push(output_rows)


def _save_and_push(output_rows):
    out_df = pd.DataFrame(output_rows,
                          columns=["股票代號", "CB名稱", "CB代號", "進度狀態",
                                   "主辦券商", "發行量(億)", "轉換價格",
                                   "掛牌日/進度日", "備註"])
    print(f"\n整理後共 {len(out_df)} 筆即將發行 CB，前 5 筆:")
    print(out_df.head(5).to_string())

    repo_dir = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(repo_dir, "近期發行CB.csv")
    out_df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"\n[OK] 已儲存至 {output_path}")

    print("正在推送到 GitHub...")
    try:
        subprocess.check_call(["git", "add", "近期發行CB.csv"], cwd=repo_dir)
        status = subprocess.check_output(
            ["git", "status", "--porcelain"], cwd=repo_dir
        ).decode("utf-8")
        if "近期發行CB.csv" in status:
            subprocess.check_call(
                ["git", "commit", "-m", "auto: update upcoming CB list (expected release / recently listed)"],
                cwd=repo_dir
            )
            subprocess.check_call(["git", "push"], cwd=repo_dir)
            print("[OK] 成功推送到 GitHub！")
        else:
            print("[OK] 資料沒有變動，無須推送。")
    except Exception as e:
        print(f"[ERR] 推送失敗: {e}")


if __name__ == "__main__":
    scrape_expected_cb()

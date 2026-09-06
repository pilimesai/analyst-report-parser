"""
update_cb.py
自動抓取兩大 CB 資料庫：
1. 預計發行專區 (近期掛牌、申報生效、董事會決議) -> 近期發行CB.csv
2. 已發行專區 (全市場所有已發行CB中，股價低於轉換價且尚未到期者) -> 目前股價低於CB轉換價.csv
"""
import os
import json
import datetime
import subprocess
import sys

def install_requirements():
    try:
        import playwright
        import pandas
        import requests
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "playwright", "pandas", "requests"])
        subprocess.check_call([sys.executable, "-m", "playwright", "install", "chromium"])

install_requirements()

from playwright.sync_api import sync_playwright
import pandas as pd
import requests
import urllib3
urllib3.disable_warnings()

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

        print("載入頁面，等待預計發行 API 回應...")
        page.goto(url, timeout=30000, wait_until="networkidle")
        page.wait_for_timeout(3000)
        browser.close()

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

    return output_rows


def scrape_issued_cb_below_conv():
    """從已發行專區 API 獲取全市場所有已發行 CB，並篩選股價低於轉換價且未過期之標的"""
    print("\n正在獲取全市場已發行 CB 名單 (GetIssuedCBSchedule)...")
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
        'Referer': 'https://cbas16889.pscnet.com.tw/marketInfo/issued',
        'Accept': 'application/json, text/plain, */*'
    }
    url = 'https://cbas16889.pscnet.com.tw/api/CbasQuote/GetIssuedCBSchedule'
    try:
        r = requests.get(url, headers=headers, verify=False, timeout=20)
        if r.status_code != 200:
            print(f"[ERR] 獲取已發行 CB 失敗，狀態碼: {r.status_code}")
            return []
        items = r.json().get('result', [])
        print(f"  [成功獲取] 全市場已發行 CB 共 {len(items)} 檔")
    except Exception as e:
        print(f"[ERR] 連線失敗: {e}")
        return []

    today = datetime.date.today()
    results = []

    for item in items:
        stock_code = str(item.get('convert_target_code', '')).strip()
        cb_name = str(item.get('underlying_bond', '')).strip()
        cb_code = str(item.get('bond_code', '')).strip()

        try:
            stock_price = float(item.get('underlying_stock_market_price') or 0)
            conv_price = float(item.get('conversion_price') or 0)
        except:
            continue

        expiry_str = str(item.get('expiry_date', '')).strip()
        # 檢查到期日是否已經過期
        if expiry_str:
            try:
                parts = expiry_str.replace('-', '/').split('/')
                if len(parts) == 3:
                    exp_date = datetime.date(int(parts[0]), int(parts[1]), int(parts[2]))
                    if exp_date < today:
                        continue  # 到期日已過期，略過
            except:
                pass

        # 只要目前股價 < CB轉換價
        if stock_price > 0 and conv_price > 0 and stock_price < conv_price:
            balance_ratio = str(item.get('balance_ratio', '')).strip()
            try:
                bal_num = float(balance_ratio.replace('%', '').strip())
            except:
                bal_num = 0.0

            # 若餘額低於 70% 就不採納 (餘額需 >= 70%)
            if bal_num < 70.0:
                continue

            diff_pct = round(((stock_price - conv_price) / conv_price) * 100, 2)
            circulation = str(item.get('circulation', '')).strip()

            results.append({
                '股票代號': stock_code,
                'CB名稱': cb_name,
                'CB代號': cb_code,
                '目前股價': stock_price,
                'CB轉換價': conv_price,
                '折價幅度': f"{diff_pct}%",
                '到期日': expiry_str,
                '餘額比例(%)': f"{balance_ratio}%" if balance_ratio else "",
                '發行規模(億)': circulation
            })

    # 依照折價幅度由深至淺排序（折價最多排在最前）
    results.sort(key=lambda x: float(x['折價幅度'].replace('%', '')))
    print(f"  [篩選完成] 目前股價低於轉換價且未過期之 CB 共 {len(results)} 檔")
    return results


def main():
    repo_dir = os.path.dirname(os.path.abspath(__file__))
    
    # 1. 抓取近期發行 CB
    expected_rows = scrape_expected_cb()
    if expected_rows:
        df_expected = pd.DataFrame(expected_rows,
                                   columns=["股票代號", "CB名稱", "CB代號", "進度狀態",
                                            "主辦券商", "發行量(億)", "轉換價格",
                                            "掛牌日/進度日", "備註"])
        out_path1 = os.path.join(repo_dir, "近期發行CB.csv")
        df_expected.to_csv(out_path1, index=False, encoding="utf-8-sig")
        print(f"[OK] 已儲存近期發行CB至 {out_path1}")

    # 2. 抓取目前股價低於轉換價之全市場 CB
    below_rows = scrape_issued_cb_below_conv()
    if below_rows:
        df_below = pd.DataFrame(below_rows,
                                columns=['股票代號', 'CB名稱', 'CB代號', '目前股價',
                                         'CB轉換價', '折價幅度', '到期日',
                                         '餘額比例(%)', '發行規模(億)'])
        out_path2 = os.path.join(repo_dir, "目前股價低於CB轉換價.csv")
        df_below.to_csv(out_path2, index=False, encoding="utf-8-sig")
        print(f"[OK] 已儲存目前股價低於CB轉換價至 {out_path2}")

    # 3. 推送到 GitHub
    print("\n正在推送到 GitHub...")
    try:
        subprocess.check_call(["git", "add", "近期發行CB.csv", "目前股價低於CB轉換價.csv"], cwd=repo_dir)
        status = subprocess.check_output(
            ["git", "status", "--porcelain"], cwd=repo_dir
        ).decode("utf-8")
        if "近期發行CB.csv" in status or "目前股價低於CB轉換價.csv" in status:
            subprocess.check_call(
                ["git", "commit", "-m", "auto: update upcoming CB list and CBs below conversion price"],
                cwd=repo_dir
            )
            subprocess.check_call(["git", "push"], cwd=repo_dir)
            print("[OK] 成功推送到 GitHub！")
        else:
            print("[OK] 資料沒有變動，無須推送。")
    except Exception as e:
        print(f"[ERR] 推送失敗: {e}")


if __name__ == "__main__":
    main()

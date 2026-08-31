"""
update_earnings_call.py
自動抓取兩週內法說會資料（公開資訊觀測站 t100sb02_1 法人說明會一覽表）
使用 Playwright 瀏覽器自動化處理 Vue 3 SPA 與新版彈出視窗查詢
欄位：代號, 公司, 法說日期
輸出：近期法說會.csv -> push 到 GitHub
"""
import os
import sys
import re
import datetime
import subprocess

def install_requirements():
    pkgs = []
    try:
        import playwright
    except ImportError:
        pkgs.append("playwright")
    try:
        import pandas
    except ImportError:
        pkgs.append("pandas")
    if pkgs:
        subprocess.check_call([sys.executable, "-m", "pip", "install"] + pkgs)
        subprocess.check_call([sys.executable, "-m", "playwright", "install", "chromium"])

install_requirements()

import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

def parse_roc_date_range(date_text, today, end_date):
    """
    解析民國年日期，例如 '115/08/31' 或 '115/08/11 ～ 115/08/12'
    若落在 today ~ end_date 範圍內則回傳西元年格式 'YYYY/MM/DD'，否則回傳 None。
    """
    dates = re.findall(r'(\d{2,3})/(\d{1,2})/(\d{1,2})', date_text)
    if not dates:
        return None
    
    for yr, m, d in dates:
        year = int(yr)
        if year < 1900:
            year += 1911
        try:
            dt = datetime.date(year, int(m), int(d))
            if today <= dt <= end_date:
                return dt.strftime('%Y/%m/%d')
        except ValueError:
            continue
    return None

def fetch_earnings_calls(window_days=14):
    """
    用 Playwright 打開 MOPS t100sb02_1 頁面，
    查詢未來 14 天內的法說會（涵蓋跨月、上市 sii 與上櫃 otc）。
    """
    today = datetime.date.today()
    end_date = today + datetime.timedelta(days=window_days)
    
    # 計算需要查詢的民國年月清單（如跨月則查詢當月與次月）
    months_to_query = []
    curr = datetime.date(today.year, today.month, 1)
    while curr <= end_date:
        months_to_query.append({
            'year': str(curr.year - 1911),
            'month': f"{curr.month:02d}"
        })
        if curr.month == 12:
            curr = datetime.date(curr.year + 1, 1, 1)
        else:
            curr = datetime.date(curr.year, curr.month + 1, 1)
            
    print(f"[法說會爬蟲] 查詢區間：{today} 至 {end_date} (共 {window_days} 天)")
    print(f"[法說會爬蟲] 查詢年月：{months_to_query}")
    
    all_results = []
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = ctx.new_page()
        
        url = "https://mops.twse.com.tw/mops/#/web/t100sb02_1"
        print(f"開啟 MOPS 法人說明會一覽表：{url}")
        page.goto(url, wait_until="networkidle", timeout=30000)
        page.wait_for_timeout(2000)
        
        for q in months_to_query:
            for market, market_name in [('sii', '上市'), ('otc', '上櫃')]:
                print(f"  查詢 {market_name} {q['year']}年{q['month']}月...")
                try:
                    # 填入市場別、民國年、月份
                    page.locator('#TYPEK').select_option(market)
                    page.locator('#year').fill("")
                    page.locator('#year').fill(q['year'])
                    page.locator('#month').select_option(q['month'])
                    page.wait_for_timeout(500)
                    
                    # 點擊查詢按鈕並攔截彈出視窗
                    with page.expect_popup(timeout=15000) as popup_info:
                        page.locator('#searchBtn').click()
                    popup = popup_info.value
                    popup.wait_for_load_state('networkidle')
                    popup.wait_for_timeout(1500)
                    
                    # 擷取彈出表格內容
                    js_extract = "rows => rows.map(r => Array.from(r.querySelectorAll('td, th')).map(c => c.textContent.trim()))"
                    trs = popup.eval_on_selector_all('table tr', js_extract)
                    popup.close()
                    
                    added = 0
                    for row in trs:
                        if len(row) < 3:
                            continue
                        code = row[0]
                        name = row[1]
                        date_str = row[2]
                        
                        # 股票代號格式檢查（4~6位純數字）
                        if not code.isdigit() or len(code) < 4:
                            continue
                            
                        matched_date = parse_roc_date_range(date_str, today, end_date)
                        if matched_date:
                            all_results.append({
                                '代號': code,
                                '公司': name if name else code,
                                '法說日期': matched_date
                            })
                            added += 1
                    print(f"    -> 符合 14 天內區間：{added} 筆 (該表總列數: {len(trs)})")
                except Exception as e:
                    print(f"    [WARN] {market_name} 查詢異常: {e}")
                    # 頁面異常時重新載入
                    try:
                        page.goto(url, wait_until="networkidle", timeout=30000)
                        page.wait_for_timeout(2000)
                    except:
                        pass
                    
        browser.close()
        
    # 去重
    seen = set()
    unique = []
    for item in all_results:
        key = (item['代號'], item['法說日期'])
        if key not in seen:
            seen.add(key)
            unique.append(item)
            
    # 依法說日期、代號排序
    unique.sort(key=lambda x: (x['法說日期'], x['代號']))
    print(f"\n[篩選完成] 兩週內法說會共 {len(unique)} 場")
    return unique

def main():
    repo_dir = os.path.dirname(os.path.abspath(__file__))
    rows = fetch_earnings_calls(14)
    out_path = os.path.join(repo_dir, "近期法說會.csv")

    if rows:
        df = pd.DataFrame(rows, columns=["代號", "公司", "法說日期"])
        df.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"[OK] 已成功儲存近期法說會至 {out_path}（共 {len(rows)} 筆）")
    else:
        pd.DataFrame(columns=["代號", "公司", "法說日期"]).to_csv(
            out_path, index=False, encoding="utf-8-sig"
        )
        print("[WARN] 無資料（目前兩週內無法說會），已寫入空 CSV")

    # 推送到 GitHub
    print("\n正在檢查 Git 變更並推送到 GitHub...")
    try:
        subprocess.check_call(["git", "add", "近期法說會.csv"], cwd=repo_dir)
        status = subprocess.check_output(
            ["git", "status", "--porcelain"], cwd=repo_dir
        ).decode("utf-8")
        if "近期法說會.csv" in status:
            subprocess.check_call(
                ["git", "commit", "-m", "auto: update upcoming earnings calls (2-week window)"],
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

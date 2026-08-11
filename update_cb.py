"""
update_cb.py
從 https://cbas16889.pscnet.com.tw/marketInfo/issued/ 抓取已發行 CB 資料（含轉換價格）
策略：攔截頁面發出的 XHR/fetch API 請求，直接取得 JSON，不依賴 DOM 解析
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

def scrape_issued_cb():
    url = "https://cbas16889.pscnet.com.tw/marketInfo/issued/"
    print(f"開啟瀏覽器攔截 API: {url}")

    captured_json = []

    def handle_response(response):
        """攔截所有 JSON 回應"""
        ct = response.headers.get("content-type", "")
        if "json" in ct and response.status == 200:
            try:
                data = response.json()
                # 只保留含陣列資料的回應（通常是清單 API）
                if isinstance(data, list) and len(data) > 5:
                    captured_json.append({"url": response.url, "data": data})
                    print(f"  [攔截] {response.url}  ({len(data)} 筆)")
                elif isinstance(data, dict):
                    # 找 data/list/items 等常見 key
                    for key in ("data", "list", "items", "rows", "result", "records"):
                        val = data.get(key)
                        if isinstance(val, list) and len(val) > 5:
                            captured_json.append({"url": response.url, "data": val})
                            print(f"  [攔截] {response.url} .{key}  ({len(val)} 筆)")
                            break
            except Exception:
                pass

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
        # 額外等一下確保非同步請求都完成
        page.wait_for_timeout(3000)
        browser.close()

    if not captured_json:
        print("[ERR] 未攔截到任何 JSON API 回應。")
        print("嘗試改用 DOM 解析作為備援...")
        return scrape_via_dom(url)

    # 找最多筆數的回應作為主資料
    best = max(captured_json, key=lambda x: len(x["data"]))
    records = best["data"]
    print(f"\n使用來源: {best['url']}  ({len(records)} 筆)")

    # 印出第一筆看 key 名
    if records:
        print("第一筆欄位:", list(records[0].keys()) if isinstance(records[0], dict) else records[0])

    # 整理輸出（依實際 API 欄位名 GetIssuedCBSchedule）
    output_rows = []
    for rec in records:
        if not isinstance(rec, dict):
            continue

        stock_code  = str(rec.get("convert_target_code", "")).strip()
        bond_name   = str(rec.get("underlying_bond", "")).strip()
        bond_code   = str(rec.get("bond_code", "")).strip()
        conv_price  = str(rec.get("conversion_price", "")).strip()
        issue_date  = str(rec.get("issue_date", "")).strip()
        balance_pct = str(rec.get("balance_ratio", "")).strip()
        # 額外有用欄位
        expiry_date    = str(rec.get("expiry_date", "")).strip()
        balance_sheets = str(rec.get("circulating_balance", "")).strip()

        if not stock_code or not any(c.isdigit() for c in stock_code):
            continue

        output_rows.append([stock_code, bond_name, bond_code,
                             conv_price, issue_date, expiry_date,
                             balance_sheets, balance_pct])

    if not output_rows:
        print("[WARN] 自動欄位對應失敗，印出第一筆原始 JSON 供參考:")
        print(json.dumps(records[0], ensure_ascii=False, indent=2))
        return

    _save_and_push(output_rows)


def scrape_via_dom(url):
    """備援：直接從 DOM 的 table 抓資料，並用固定欄位位置對應"""
    print("備援 DOM 解析...")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True,
                                    args=["--no-sandbox"])
        page = browser.new_page(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/125.0.0.0 Safari/537.36"
        )
        page.goto(url, timeout=30000, wait_until="networkidle")
        page.wait_for_timeout(5000)

        result = page.evaluate("""() => {
            const tbl = document.querySelector('table');
            if (!tbl) return null;

            // 讀所有標頭列（可能有多列）
            const headerRows = [];
            tbl.querySelectorAll('thead tr').forEach(tr => {
                const cells = [];
                tr.querySelectorAll('th').forEach(th => {
                    cells.push({ text: th.innerText.trim(), span: parseInt(th.colSpan || 1) });
                });
                headerRows.push(cells);
            });

            // 讀資料
            const rows = [];
            tbl.querySelectorAll('tbody tr').forEach(tr => {
                const cells = [];
                tr.querySelectorAll('td').forEach(td => cells.push(td.innerText.trim()));
                if (cells.length > 0) rows.push(cells);
            });

            return { headerRows, rows };
        }""")
        browser.close()

    if not result or not result["rows"]:
        print("[ERR] DOM 備援也失敗。")
        return

    rows = result["rows"]
    header_rows = result["headerRows"]
    print(f"DOM 取得 {len(rows)} 列，標頭列數: {len(header_rows)}")

    # 展開標頭列（處理 colspan）
    flat_headers = []
    for hr in header_rows:
        for cell in hr:
            for _ in range(cell["span"]):
                flat_headers.append(cell["text"])
    print(f"展開後標頭 ({len(flat_headers)}): {flat_headers[:20]}")

    # 找各欄位的索引
    def find_col(keywords):
        for i, h in enumerate(flat_headers):
            for kw in keywords:
                if kw in h:
                    return i
        return None

    idx_name     = find_col(["標的債券", "CB名稱", "債券名"])
    idx_bond     = find_col(["債券代號"])
    idx_stock    = find_col(["轉換標的代碼", "標的代碼"])
    idx_price    = find_col(["轉換價格", "轉換價"])
    idx_date     = find_col(["發行日期", "掛牌"])
    idx_balance  = find_col(["餘額比例"])

    print(f"欄位索引: 名稱={idx_name}, 債券代號={idx_bond}, 股票代號={idx_stock}, "
          f"轉換價格={idx_price}, 發行日={idx_date}, 餘額比例={idx_balance}")

    output_rows = []
    for row in rows:
        n = len(row)
        def get(idx):
            return row[idx].strip() if idx is not None and idx < n else ""

        stock_code = get(idx_stock)
        if not stock_code or not any(c.isdigit() for c in stock_code):
            continue
        output_rows.append([
            stock_code, get(idx_name), get(idx_bond),
            get(idx_price), get(idx_date), get(idx_balance)
        ])

    if not output_rows:
        print("[ERR] 未取得有效資料。")
        return

    _save_and_push(output_rows)


def _save_and_push(output_rows):
    out_df = pd.DataFrame(output_rows,
                          columns=["股票代號", "CB名稱", "債券代號", "轉換價格",
                                   "發行日期", "到期日", "流通餘額(張)", "餘額比例"])
    print(f"\n整理後共 {len(out_df)} 筆，前 5 筆:")
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
                ["git", "commit", "-m", "auto: update issued CB list with conversion price"],
                cwd=repo_dir
            )
            subprocess.check_call(["git", "push"], cwd=repo_dir)
            print("[OK] 成功推送到 GitHub！")
        else:
            print("[OK] 資料沒有變動，無須推送。")
    except Exception as e:
        print(f"[ERR] 推送失敗: {e}")


if __name__ == "__main__":
    scrape_issued_cb()

"""
update_quant.py
全自動台股量化選股引擎 (量化嚴選標的後端更新排程)
整合籌碼面(法人/大戶)、技術指標(日周KD/爆量)、基本面(營收/毛利/合約負債)、事件面(法說會/CB/質設/主動型ETF)綜合評分。

輸出：
  quant_results.json - 供網頁端 (電腦與手機) 秒級直接載入與觀看
"""

import os
import sys
import json
import csv
import time
import datetime
import urllib.request
import subprocess
import shutil
from zoneinfo import ZoneInfo
import pandas as pd
import numpy as np

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

TZ_TW = ZoneInfo('Asia/Taipei')
REPO_DIR = os.path.dirname(os.path.abspath(__file__))

def load_json(filename, default=None):
    path = os.path.join(REPO_DIR, filename)
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"Warning: Failed to load {filename}: {e}")
    return default if default is not None else {}

def load_csv_codes(filename):
    path = os.path.join(REPO_DIR, filename)
    codes = set()
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8-sig", errors="ignore") as f:
                reader = csv.reader(f)
                for row in reader:
                    for cell in row[:3]:
                        for token in str(cell).split():
                            t = token.strip()
                            if len(t) == 4 and t.isdigit():
                                codes.add(t)
        except Exception as e:
            print(f"Warning: Failed to load {filename}: {e}")
    return codes

def fetch_institutional_investors(trade_date_str=None):
    """
    抓取 TWSE 與 TPEx 最新一日的三大法人買賣超資料
    回傳: dict[stock_id] -> {'foreign': float, 'trust': float, 'dealer': float, 'total': float}
    """
    chips_map = {}
    today = datetime.datetime.now(TZ_TW).date()
    
    # 1. TWSE T86
    print("[Quant] 抓取 TWSE 三大法人買賣超行情 (T86)...")
    for offset in range(5):
        d = today - datetime.timedelta(days=offset)
        if d.weekday() >= 5:
            continue
        d_str = d.strftime('%Y%m%d')
        url = f"https://www.twse.com.tw/rwd/zh/fund/T86?date={d_str}&selectType=ALLBUT0999&response=json"
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'})
            with urllib.request.urlopen(req, timeout=15) as resp:
                jd = json.loads(resp.read().decode('utf-8'))
            if jd.get('stat') == 'OK' and jd.get('data'):
                for r in jd['data']:
                    code = str(r[0]).strip()
                    if len(code) == 4 and code.isdigit():
                        def parse_num(v):
                            return float(str(v).replace(',', '').strip() or 0)
                        f_net = parse_num(r[4])
                        t_net = parse_num(r[10])
                        d_net = parse_num(r[11])
                        tot_net = parse_num(r[18]) if len(r) > 18 else (f_net + t_net + d_net)
                        chips_map[code] = {
                            'foreign': f_net,
                            'trust': t_net,
                            'dealer': d_net,
                            'total': tot_net
                        }
                print(f"[Quant] TWSE 三大法人成功抓取: {len(chips_map)} 檔 (日期: {d_str})")
                break
        except Exception as e:
            print(f"[Quant] TWSE T86 抓取失敗 ({d_str}): {e}")

    # 2. TPEx 3itrade
    print("[Quant] 抓取 TPEx 三大法人買賣超行情...")
    curl_bin = shutil.which("curl") or shutil.which("curl.exe") or "curl"
    for offset in range(5):
        d = today - datetime.timedelta(days=offset)
        if d.weekday() >= 5:
            continue
        roc_str = f"{d.year - 1911}/{d.month:02d}/{d.day:02d}"
        url = f"https://www.tpex.org.tw/web/stock/3insti/daily_trades/3itrade_hedge_result.php?l=zh-tw&d={roc_str}&se=EW&t=D&_=1"
        try:
            res = subprocess.run(
                [curl_bin, '-s', '--http1.1', url, '-H', 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64)'],
                capture_output=True,
                timeout=25
            )
            if res.returncode == 0 and res.stdout:
                jd = json.loads(res.stdout.decode('utf-8', errors='ignore'))
                rows = jd.get('aaData') or jd.get('tables', [{}])[0].get('data', [])
                if rows:
                    count = 0
                    for r in rows:
                        code = str(r[0]).strip()
                        if len(code) == 4 and code.isdigit():
                            def parse_num(v):
                                return float(str(v).replace(',', '').strip() or 0)
                            f_net = parse_num(r[7]) if len(r) > 7 else 0
                            t_net = parse_num(r[10]) if len(r) > 10 else 0
                            d_net = parse_num(r[13]) if len(r) > 13 else 0
                            tot_net = parse_num(r[14]) if len(r) > 14 else (f_net + t_net + d_net)
                            chips_map[code] = {
                                'foreign': f_net,
                                'trust': t_net,
                                'dealer': d_net,
                                'total': tot_net
                            }
                            count += 1
                    print(f"[Quant] TPEx 三大法人成功抓取: {count} 檔 (民國日期: {roc_str})")
                    break
        except Exception as e:
            print(f"[Quant] TPEx 3itrade 抓取失敗 ({roc_str}): {e}")

    return chips_map

def calculate_kd(df, window=9):
    low_min = df['Low'].rolling(window=window).min()
    high_max = df['High'].rolling(window=window).max()
    denom = high_max - low_min
    denom = denom.replace(0, np.nan)
    rsv = (df['Close'] - low_min) / denom * 100
    rsv = rsv.fillna(50)
    k = rsv.ewm(com=2, adjust=False).mean()
    d = k.ewm(com=2, adjust=False).mean()
    return k, d

def main():
    start_time = time.time()
    now_dt = datetime.datetime.now(TZ_TW)
    trade_date = now_dt.strftime('%Y-%m-%d')

    print(f"==================================================")
    print(f"🚀 開始執行全市場量化選股排程 (update_quant.py)")
    print(f"目前時間: {now_dt.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"==================================================")

    # 1. 讀取現有市場資料庫
    stock_names = load_json("stock_names.json", {})
    top_turnover = load_json("top_turnover.json", {})
    active_etf = load_json("active_etf_holdings.json", {})
    volume_spike = load_json("volume_spike.json", {})
    sector_strength = load_json("sector_strength.json", {})
    contract_liabilities = load_json("contract_liabilities.json", {})
    stock_pledge = load_json("stock_pledge.json", {})
    stock_capital = load_json("stock_capital.json", {})

    cb_issued_codes = load_csv_codes("近期發行CB.csv")
    cb_below_codes = load_csv_codes("目前股價低於CB轉換價.csv")
    earnings_call_codes = load_csv_codes("近期法說會.csv")

    # 2. 建立股票所屬市場與多方族群對照表
    stock_market_map = {}
    bullish_sectors_map = {}
    for sec in sector_strength.get("sectors", []):
        sec_name = sec.get("industry", "")
        is_bullish = sec.get("signal") == "多方"
        for st in sec.get("stocks", []):
            sc = str(st.get("code", "")).strip()
            sm = st.get("market", "")
            if sc:
                if sm:
                    stock_market_map[sc] = sm
                if is_bullish:
                    if sc not in bullish_sectors_map:
                        bullish_sectors_map[sc] = []
                    if sec_name not in bullish_sectors_map[sc]:
                        bullish_sectors_map[sc].append(sec_name)

    # 3. 彙整大股東質設防守名單
    pledge_map = {}
    for p in stock_pledge.get("stocks", []):
        sc = str(p.get("code", "")).strip()
        if sc:
            pledge_map[sc] = p

    # 4. 彙整主動型 ETF 獨家持股 (僅 1 檔 ETF 且權重 > 1%)
    EXCLUDED_FUNDS = ['00985A', '00986A', '00987A', '00988A']
    exclusive_etf_codes = set()
    for s in active_etf.get("stocks", []):
        sc = str(s.get("symbol", "")).strip()
        funds = [f for f in s.get("funds", []) if f.get("fundSymbol") not in EXCLUDED_FUNDS and float(f.get("ratio", 0) or 0) > 1.0]
        if len(funds) == 1:
            exclusive_etf_codes.add(sc)

    # 5. 彙整 TOP 30 成交值標的
    top_turnover_codes = set()
    for r in top_turnover.get("twse", []) + top_turnover.get("tpex", []):
        sc = str(r.get("code", "")).strip()
        if sc.isdigit():
            top_turnover_codes.add(sc)

    # 6. 彙整成交量異常放大 (爆量紅K) 標的
    volume_spike_codes = set()
    for r in volume_spike.get("stocks", []):
        sc = str(r.get("code", "")).strip()
        if sc.isdigit():
            volume_spike_codes.add(sc)

    # 7. 彙整合約負債突破標的
    contract_liabilities_codes = set()
    for r in contract_liabilities.get("stocks", []):
        sc = str(r.get("code", "")).strip()
        if sc.isdigit():
            contract_liabilities_codes.add(sc)

    # 8. 收集所有候選個股
    candidate_stocks = set()
    candidate_stocks.update(top_turnover_codes)
    candidate_stocks.update(exclusive_etf_codes)
    candidate_stocks.update(volume_spike_codes)
    candidate_stocks.update(contract_liabilities_codes)
    candidate_stocks.update(earnings_call_codes)
    candidate_stocks.update(cb_issued_codes)
    candidate_stocks.update(cb_below_codes)

    # 質設股票若在成本線 20% 以內亦納入候選池
    for sc, p_info in pledge_map.items():
        diff_pct = p_info.get("diff_pct")
        if diff_pct is not None and abs(float(diff_pct)) <= 20:
            candidate_stocks.add(sc)

    # 清除非 4 碼純數字之代號
    candidate_stocks = sorted([c for c in candidate_stocks if len(c) == 4 and c.isdigit()])
    print(f"[Quant] 彙整出候選個股總數: {len(candidate_stocks)} 檔")

    # 9. 抓取法人籌碼資料
    chips_data = fetch_institutional_investors(trade_date)

    # 10. 抓取技術面資料 (KD / 均量)
    # 使用單一正確市場後綴，避免發送過多無效代碼
    hist_bulk = pd.DataFrame()
    import yfinance as yf
    tickers = []
    for c in candidate_stocks:
        market = stock_market_map.get(c, 'twse')
        suffix = '.TWO' if market == 'tpex' else '.TW'
        tickers.append(f"{c}{suffix}")

    print(f"[Quant] 嘗試抓取技術面報價 (共 {len(tickers)} 檔代碼)...")
    try:
        # 分批抓取以避免被 Yahoo Finance 限流
        CHUNK_SIZE = 50
        hist_frames = []
        for i in range(0, min(len(tickers), 250), CHUNK_SIZE):
            chunk = tickers[i:i+CHUNK_SIZE]
            try:
                sub_df = yf.download(chunk, period="6mo", threads=True, progress=False)
                if not sub_df.empty:
                    hist_frames.append(sub_df)
            except Exception as e:
                print(f"[Quant] yf chunk 下載提示: {e}")
            time.sleep(0.3)
        if hist_frames:
            hist_bulk = pd.concat(hist_frames, axis=1)
            print(f"[Quant] 成功下載 {len(hist_frames)} 批次技術報價。")
    except Exception as e:
        print(f"[Quant] yfinance 暫時無法連線: {e}，將使用籌碼與現有市場指標完整評估。")

    scores = {}
    matches = {}
    ranked_list = []

    for stock_id in candidate_stocks:
        stock_name = stock_names.get(stock_id, "")
        matched_criteria = []

        # --- A. 技術指標與成交量 ---
        df_stock = None
        market = stock_market_map.get(stock_id, 'twse')
        suffix = '.TWO' if market == 'tpex' else '.TW'
        tk = f"{stock_id}{suffix}"

        if not hist_bulk.empty and hasattr(hist_bulk.columns, 'levels'):
            try:
                df_candidate = hist_bulk.xs(tk, axis=1, level=1).dropna(how='all')
                if not df_candidate.empty and len(df_candidate) >= 15:
                    df_stock = df_candidate.copy()
            except Exception:
                pass

        if df_stock is not None and len(df_stock) >= 15:
            try:
                # 1. 日 KD 黃金交叉
                k_series, d_series = calculate_kd(df_stock, window=9)
                if len(k_series) >= 2:
                    today_k, today_d = k_series.iloc[-1], d_series.iloc[-1]
                    yest_k, yest_d = k_series.iloc[-2], d_series.iloc[-2]
                    if today_k > today_d and yest_k < yest_d:
                        matched_criteria.append("日KD黃金交叉")

                # 2. 周 KD 黃金交叉
                weekly_df = df_stock.resample('W').agg({
                    'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'
                }).dropna()
                if len(weekly_df) >= 10:
                    wk_series, wd_series = calculate_kd(weekly_df, window=9)
                    if len(wk_series) >= 2:
                        wt_k, wt_d = wk_series.iloc[-1], wd_series.iloc[-1]
                        wy_k, wy_d = wk_series.iloc[-2], wd_series.iloc[-2]
                        if wt_k > wt_d and wy_k < wy_d:
                            matched_criteria.append("周KD黃金交叉")

                # 3. 成交量條件 (量大於十週均量且大於三倍十日均量)
                if len(df_stock) > 10 and len(weekly_df) > 10:
                    vol_10d_avg = df_stock['Volume'].rolling(window=10).mean().iloc[-2]
                    vol_10w_avg = weekly_df['Volume'].rolling(window=10).mean().iloc[-2]
                    vol_10w_daily = vol_10w_avg / 5.0
                    today_vol = df_stock['Volume'].iloc[-1]
                    if today_vol == 0 and len(df_stock) > 1:
                        today_vol = df_stock['Volume'].iloc[-2]
                    if vol_10d_avg > 0 and vol_10w_daily > 0:
                        if today_vol > vol_10w_daily and today_vol > (3 * vol_10d_avg):
                            matched_criteria.append("成交量>十週平均量且>3倍十日均量")
            except Exception as e:
                pass

        # --- B. 法人籌碼條件 ---
        if stock_id in chips_data:
            c = chips_data[stock_id]
            if c['foreign'] > 0 and c['trust'] > 0 and c['dealer'] > 0:
                matched_criteria.append("三大法人同買")
            elif c['trust'] > 0 and c['foreign'] > 0:
                matched_criteria.append("外資投信聯袂買超")
            elif c['trust'] > 0:
                matched_criteria.append("投信持續加碼買超")

        # --- C. 爆量紅K標的 ---
        if stock_id in volume_spike_codes:
            matched_criteria.append("成交量異常放大 (爆量紅K)")

        # --- D. 事件與題材清單比對 ---
        if stock_id in top_turnover_codes:
            matched_criteria.append("入選今日成交值TOP30")

        if stock_id in exclusive_etf_codes:
            matched_criteria.append("入選主動型ETF獨家持股")

        if stock_id in contract_liabilities_codes:
            matched_criteria.append("入選合約負債")

        if stock_id in earnings_call_codes:
            matched_criteria.append("入選2周內會有法說會")

        if stock_id in cb_issued_codes:
            matched_criteria.append("入選要發行CB")

        if stock_id in cb_below_codes:
            matched_criteria.append("若已有發行的CB,且股價低於轉換價,並且轉換比例<10%")

        # --- E. 所在族群為多方族群 ---
        if stock_id in bullish_sectors_map:
            sec_names = " / ".join(bullish_sectors_map[stock_id])
            matched_criteria.append(f"所在族群為多方 ({sec_names})")

        # --- F. 大股東質設成本防守 ---
        if stock_id in pledge_map:
            p_info = pledge_map[stock_id]
            diff_pct = p_info.get("diff_pct")
            pledge_price = float(p_info.get("pledge_price") or 0)
            if diff_pct is not None and pledge_price > 0 and abs(float(diff_pct)) <= 20:
                diff_val = float(diff_pct)
                sign_str = "+" if diff_val >= 0 else ""
                matched_criteria.append(f"大股東質設成本防守 (現價在成本20%以內, 距成本{sign_str}{diff_val:.1f}%)")

        # --- G. 股本評估 (<20億且有籌碼或題材) ---
        cap_val = stock_capital.get(stock_id)
        if cap_val is not None:
            try:
                cap_yi = float(cap_val) / 1e8 if float(cap_val) > 10000 else float(cap_val)
                if 0 < cap_yi < 20 and len(matched_criteria) >= 2:
                    matched_criteria.append(f"小型成長股 (股本{cap_yi:.1f}億)")
            except Exception:
                pass

        # --- 加權總分計算 ---
        score = len(matched_criteria)
        if any("法說會" in m for m in matched_criteria):
            score += 1
        if any("低於轉換價" in m for m in matched_criteria):
            score += 1
        if any("大股東質設成本防守" in m for m in matched_criteria):
            score += 1
        if any("主動型ETF獨家持股" in m for m in matched_criteria):
            score += 1
        if any("今日成交值TOP30" in m for m in matched_criteria):
            score += 1

        scores[stock_id] = score
        matches[stock_id] = matched_criteria

        if score > 0:
            ranked_list.append({
                "stockId": stock_id,
                "name": stock_name,
                "score": score,
                "matches": matched_criteria
            })

    # 排序
    ranked_list.sort(key=lambda x: x["score"], reverse=True)
    for rank, item in enumerate(ranked_list, 1):
        item["rank"] = rank

    max_score = ranked_list[0]["score"] if ranked_list else 0
    passed_count = len(ranked_list)

    print(f"--------------------------------------------------")
    print(f"📊 量化選股運算結果摘要：")
    print(f"  - 評估候選個股數: {len(candidate_stocks)}")
    print(f"  - 符合條件嚴選標的數: {passed_count}")
    print(f"  - 最高分: {max_score} 分")
    if ranked_list:
        print(f"  - 第一名標的: {ranked_list[0]['stockId']} {ranked_list[0]['name']} ({ranked_list[0]['score']} 分)")
        for top_item in ranked_list[:5]:
            print(f"    #{top_item['rank']} {top_item['stockId']} {top_item['name']}: {top_item['score']}分 -> {top_item['matches']}")
    print(f"--------------------------------------------------")

    # 輸出 quant_results.json
    output_payload = {
        "updateTime": now_dt.strftime('%Y-%m-%d %H:%M:%S'),
        "tradeDate": trade_date,
        "totalCandidates": len(candidate_stocks),
        "passedCount": passed_count,
        "maxScore": max_score,
        "scores": scores,
        "matches": matches,
        "rankedList": ranked_list
    }

    out_path = os.path.join(REPO_DIR, "quant_results.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output_payload, f, ensure_ascii=False, indent=2)

    # 同步至 Google Sheet
    sync_quant_to_google_sheet(output_payload)

    elapsed = time.time() - start_time
    print(f"✅ 成功產出 {out_path} (耗時 {elapsed:.2f} 秒)！")

def sync_quant_to_google_sheet(output_payload):
    gas_url = "https://script.google.com/macros/s/AKfycbzm7ZXZyAe_8XhARcC8VKw2DDsWtaW_OdrANnA87lTQo2ozlM-2F4XsFFOXIC1HynqM/exec"
    trade_date = output_payload.get('tradeDate', '')
    update_time = output_payload.get('updateTime', '')
    ranked = output_payload.get('rankedList', [])
    if not ranked:
        return

    headers = ['排名', '股票代號', '股票名稱', '綜合評分', '符合條件數', '符合條件明細', '資料交易日', '更新時間']
    rows = [headers]
    for item in ranked:
        matches_str = ' | '.join(item.get('matches', []))
        rows.append([
            item.get('rank', ''),
            item.get('stockId', ''),
            item.get('name', ''),
            item.get('score', 0),
            len(item.get('matches', [])),
            matches_str,
            str(trade_date),
            str(update_time)
        ])

    try:
        print("☁️ 正在同步量化嚴選結果至 Google Sheet (分頁：量化嚴選)...")
        req_body = json.dumps({
            'action': 'save_csv',
            'sheetName': '量化嚴選',
            'data': rows
        }).encode('utf-8')
        req = urllib.request.Request(
            gas_url,
            data=req_body,
            headers={'Content-Type': 'text/plain; charset=utf-8', 'User-Agent': 'Mozilla/5.0'}
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            res = json.loads(resp.read().decode('utf-8'))
            if res.get('status') == 'success':
                print(f"🎉 成功同步 {len(ranked)} 檔量化嚴選標的至 Google Sheet 分頁【量化嚴選】！")
            else:
                print(f"⚠️ Google Sheet 同步回傳訊息: {res}")
    except Exception as e:
        print(f"⚠️ Google Sheet 同步遭遇例外 (不影響本機結果產出): {e}")

if __name__ == "__main__":
    main()


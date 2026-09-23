"""
update_volume_spike.py
每日自動篩選台灣上市（TWSE）與上櫃（TPEx）中「成交量異常變大」的個股

篩選條件（嚴格符合 5 大條件）：
  1. [日] 成交量大於 2000 張 (>= 2,000,000 股)
  2. [日] 成交金額(億)大於 0.5 億 (>= 50,000,000 元)
  3. [週] 成交量大於近 10 週平均 (今日量 > 10週均量換算日均量)
  4. [日] 成交量大於 10 日均量的 3 倍 (今日量 > 3 * 10日均量)
  5. [日] 收盤價大於開盤價 (實體紅K棒: Close > Open)

輸出檔案：
  volume_spike.json
"""
import sys
import os
import json
import datetime
from zoneinfo import ZoneInfo
import urllib.request
import subprocess
import shutil
import time

TZ_TW = ZoneInfo('Asia/Taipei')

if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

REPO_DIR = os.path.dirname(os.path.abspath(__file__))

def load_stock_names():
    sn_path = os.path.join(REPO_DIR, 'stock_names.json')
    if os.path.exists(sn_path):
        try:
            with open(sn_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f'Warning: load stock_names.json failed: {e}')
    return {}

def parse_int(v):
    if v is None:
        return 0
    s = str(v).replace(',', '').replace(' ', '').strip()
    if s == '' or s == '-' or s == '--':
        return 0
    try:
        return int(float(s))
    except ValueError:
        return 0

def parse_float(v):
    if v is None:
        return 0.0
    s = str(v).replace(',', '').replace(' ', '').strip()
    if s == '' or s == '-' or s == '--':
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0

def fetch_twse_candidates(stock_names):
    """
    抓取 TWSE 全市場當日行情並初篩：
    四碼普通股、量 >= 2000張、金額 >= 0.5億、收盤 > 開盤
    優先由 TWSE 官網 MI_INDEX 抓取當日即時盤後行情（解決 OpenAPI 跨日同步延遲）
    """
    print('Fetching TWSE quotes for candidates...')
    today = datetime.datetime.now(TZ_TW).date()
    for delta in range(5):
        d = today - datetime.timedelta(days=delta)
        if d.weekday() >= 5:
            continue  # 略過週末
        d_str = d.strftime('%Y%m%d')
        url = f'https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date={d_str}&type=ALLBUT0999&response=json'
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)', 'Accept': 'application/json, */*'}
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=20) as resp:
                jd = json.loads(resp.read().decode('utf-8', errors='ignore'))
            if jd.get('stat') == 'OK':
                tables = [tbl for tbl in jd.get('tables', []) if len(tbl.get('data', [])) > 500]
                if tables:
                    rows = tables[0].get('data', [])
                    trade_date = d.strftime('%Y-%m-%d')
                    candidates = {}
                    for r in rows:
                        if len(r) > 10:
                            code = str(r[0]).strip()
                            if not (len(code) == 4 and code.isdigit()):
                                continue
                            vol = parse_int(r[2])
                            val = parse_int(r[4])
                            op = parse_float(r[5])
                            cp = parse_float(r[8])
                            trans = parse_int(r[3])
                            chg_sign = -1 if '-' in str(r[9]) else (1 if '+' in str(r[9]) else 0)
                            chg = parse_float(r[10]) * (chg_sign if chg_sign != 0 else 1)
                            # 條件 1: 量 >= 2000 張 (2,000,000 股)
                            # 條件 2: 金額 >= 0.5 億 (50,000,000 元)
                            # 條件 5: 收盤 > 開盤 (實體紅K)
                            if vol >= 2000000 and val >= 50000000 and cp > op:
                                candidates[f"{code}.TW"] = {
                                    'market': 'twse',
                                    'code': code,
                                    'name': stock_names.get(code, str(r[1]).strip()),
                                    'volume_shares': vol,
                                    'volume_lots': vol // 1000,
                                    'trade_value': val,
                                    'trade_value_yi': round(val / 1e8, 2),
                                    'open_price': op,
                                    'close_price': cp,
                                    'change': chg,
                                    'transaction': trans,
                                    'date': trade_date
                                }
                    print(f'TWSE initial pre-filter candidates via MI_INDEX: {len(candidates)} (trade_date: {trade_date})')
                    return candidates, trade_date
        except Exception as ex:
            print(f'TWSE MI_INDEX {d_str} error: {ex}')

    # Fallback 回 OpenAPI
    print('Fallback to TWSE STOCK_DAY_ALL...')
    url = 'https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL'
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)', 'Accept': 'application/json'}
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=25) as resp:
        data = json.loads(resp.read().decode('utf-8'))

    candidates = {}
    trade_date = ''
    for d in data:
        code = str(d.get('Code', '')).strip()
        # 僅選四碼普通股
        if not (len(code) == 4 and code.isdigit()):
            continue
        vol = parse_int(d.get('TradeVolume', 0))
        val = parse_int(d.get('TradeValue', 0))
        op = parse_float(d.get('OpeningPrice', 0))
        cp = parse_float(d.get('ClosingPrice', 0))
        chg = parse_float(d.get('Change', 0))
        trans = parse_int(d.get('Transaction', 0))

        if not trade_date:
            d_str = str(d.get('Date', '')).strip()
            if len(d_str) == 7:
                trade_date = f"{int(d_str[:3]) + 1911}-{d_str[3:5]}-{d_str[5:7]}"

        # 條件 1: 量 >= 2000 張 (2,000,000 股)
        # 條件 2: 金額 >= 0.5 億 (50,000,000 元)
        # 條件 5: 收盤 > 開盤 (實體紅K)
        if vol >= 2000000 and val >= 50000000 and cp > op:
            candidates[f"{code}.TW"] = {
                'market': 'twse',
                'code': code,
                'name': stock_names.get(code, str(d.get('Name', '')).strip()),
                'volume_shares': vol,
                'volume_lots': vol // 1000,
                'trade_value': val,
                'trade_value_yi': round(val / 1e8, 2),
                'open_price': op,
                'close_price': cp,
                'change': chg,
                'transaction': trans,
                'date': trade_date
            }

    print(f'TWSE initial pre-filter candidates via OpenAPI: {len(candidates)} (trade_date: {trade_date})')
    return candidates, trade_date

def fetch_tpex_candidates(stock_names):
    """
    抓取 TPEx 上櫃全市場當日行情並初篩：
    四碼普通股、量 >= 2000張、金額 >= 0.5億、收盤 > 開盤
    """
    print('Fetching TPEx OTC quotes...')
    today = datetime.datetime.now(TZ_TW).date()
    candidates = {}
    trade_date = ''

    for delta in range(5):
        d = today - datetime.timedelta(days=delta)
        if d.weekday() >= 5:
            continue  # 略過週末
        roc = f"{d.year - 1911}/{d.month:02d}/{d.day:02d}"
        url = f"https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php?l=zh-tw&d={roc}&se=AL&_=1"
        raw_bytes = None

        # 優先使用 curl (處理 Windows/Linux 環境下 TLS Handshake 與大封包)
        curl_bin = shutil.which("curl") or shutil.which("curl.exe") or "curl"
        try:
            res = subprocess.run(
                [curl_bin, '-s', '--http1.1', url, '-H', 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64)'],
                capture_output=True,
                timeout=35
            )
            if res.returncode == 0 and res.stdout:
                raw_bytes = res.stdout
        except Exception as ex:
            print(f"TPEx curl error for {d}: {ex}")

        # 備援嘗試 urllib
        if not raw_bytes:
            try:
                req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)', 'Accept': 'application/json, */*'})
                with urllib.request.urlopen(req, timeout=30) as resp:
                    raw_bytes = resp.read()
            except Exception as ex:
                print(f"TPEx urllib error for {d}: {ex}")

        if raw_bytes:
            try:
                jd = json.loads(raw_bytes.decode('utf-8', errors='ignore'))
                tables = jd.get('tables', [])
                if tables and tables[0].get('data'):
                    rows = tables[0]['data']
                    trade_date = d.strftime('%Y-%m-%d')
                    for r in rows:
                        code = str(r[0]).strip()
                        if not (len(code) == 4 and code.isdigit()):
                            continue
                        cp = parse_float(r[2])
                        chg = parse_float(r[3])
                        op = parse_float(r[4])
                        vol = parse_int(r[7])
                        val = parse_int(r[8])
                        trans = parse_int(r[9]) if len(r) > 9 else 0

                        # 條件 1: 量 >= 2000 張
                        # 條件 2: 金額 >= 0.5 億
                        # 條件 5: 收盤 > 開盤
                        if vol >= 2000000 and val >= 50000000 and cp > op:
                            candidates[f"{code}.TWO"] = {
                                'market': 'tpex',
                                'code': code,
                                'name': stock_names.get(code, str(r[1]).strip()),
                                'volume_shares': vol,
                                'volume_lots': vol // 1000,
                                'trade_value': val,
                                'trade_value_yi': round(val / 1e8, 2),
                                'open_price': op,
                                'close_price': cp,
                                'change': chg,
                                'transaction': trans,
                                'date': trade_date
                            }
                    print(f'TPEx initial pre-filter candidates: {len(candidates)} (trade_date: {trade_date})')
                    break
            except Exception as e:
                print(f'TPEx parse error: {e}')

    return candidates, trade_date

def evaluate_volume_spikes(all_candidates):
    """
    透過 yfinance 批次並行計算歷史 10 日均量與 10 週均量，
    嚴格驗證條件 3 與條件 4。
    """
    import yfinance as yf
    import pandas as pd

    tickers = list(all_candidates.keys())
    if not tickers:
        return []

    print(f'Starting batch download for {len(tickers)} candidate stocks via yfinance...')
    t0 = time.time()
    try:
        df = yf.download(tickers, period='4mo', progress=False, group_by='ticker', threads=True)
    except Exception as e:
        print(f'yfinance download error: {e}')
        return []
    print(f'Download completed in {time.time() - t0:.2f}s')

    matched = []
    for ticker in tickers:
        try:
            sub = df[ticker].dropna(subset=['Close', 'Volume']) if ticker in df else None
            if sub is None or len(sub) < 15:
                continue

            # 聚合為週K
            weekly = sub.resample('W').agg({
                'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'
            }).dropna()

            if len(weekly) < 11:
                continue

            # 基準計算（排除今日，避免今日爆量自我稀釋）
            vol_10d_avg = sub['Volume'].rolling(window=10).mean().iloc[-2]
            vol_10w_avg = weekly['Volume'].rolling(window=10).mean().iloc[-2]

            today_vol = sub['Volume'].iloc[-1]
            if today_vol == 0 and len(sub) > 1:
                today_vol = sub['Volume'].iloc[-2]

            # 10週均量換算成單日基準（一週5個交易日）
            vol_10w_avg_daily = vol_10w_avg / 5

            # 條件 3: 今日成交量 > 10週平均單日成交量
            cond_3 = today_vol > vol_10w_avg_daily
            # 條件 4: 今日成交量 > 10日均量的 3 倍
            cond_4 = today_vol > (3 * vol_10d_avg)

            if cond_3 and cond_4 and vol_10d_avg > 0:
                info = dict(all_candidates[ticker])
                info['today_vol_lots'] = int(today_vol // 1000)
                info['ma10_vol_lots'] = int(vol_10d_avg // 1000)
                info['ma10w_vol_lots'] = int(vol_10w_avg_daily // 1000)
                info['vol_multiple'] = round(float(today_vol / vol_10d_avg), 2)
                info['vol_week_multiple'] = round(float(today_vol / vol_10w_avg_daily), 2) if vol_10w_avg_daily > 0 else 0
                matched.append(info)
        except Exception as ex:
            # 個別標的例外不中斷整體運算
            pass

    # 依「10日均量倍數」由大到小排序（最強勢爆量排前面）
    matched.sort(key=lambda x: x['vol_multiple'], reverse=True)
    for rank, item in enumerate(matched, 1):
        item['rank'] = rank

    print(f'Volume Spike Evaluation Done! Matched stocks: {len(matched)}')
    return matched

def main():
    stock_names = load_stock_names()

    twse_cands, twse_date = fetch_twse_candidates(stock_names)
    tpex_cands, tpex_date = fetch_tpex_candidates(stock_names)

    all_candidates = {**twse_cands, **tpex_cands}
    trade_date = twse_date or tpex_date or datetime.datetime.now(TZ_TW).strftime('%Y-%m-%d')

    matched_stocks = evaluate_volume_spikes(all_candidates)

    now_iso = datetime.datetime.now(TZ_TW).isoformat()
    out_data = {
        'updateTime': now_iso,
        'tradeDate': trade_date,
        'totalCount': len(matched_stocks),
        'conditions': [
            '[日] 成交量大於 2000 張 (>= 2000 張)',
            '[日] 成交金額大於 0.5 億 (>= 5000 萬元)',
            '[週] 成交量大於近 10 週平均 (單日基準)',
            '[日] 成交量大於 10 日均量的 3 倍 (>= 3x)',
            '[日] 收盤價大於開盤價 (實體紅K棒)'
        ],
        'twse': [s for s in matched_stocks if s['market'] == 'twse'],
        'tpex': [s for s in matched_stocks if s['market'] == 'tpex'],
        'stocks': matched_stocks
    }

    out_path = os.path.join(REPO_DIR, 'volume_spike.json')
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(out_data, f, ensure_ascii=False, indent=2)
    print(f'Successfully saved {len(matched_stocks)} volume spike stocks to {out_path}')

    # 若帶有 --push 參數，自動 commit & push 到 GitHub
    if '--push' in sys.argv:
        try:
            subprocess.run(['git', 'add', 'volume_spike.json'], cwd=REPO_DIR, check=True)
            commit_msg = f'auto: update volume spike stocks ({trade_date})'
            subprocess.run(['git', 'commit', '-m', commit_msg], cwd=REPO_DIR, check=True)
            subprocess.run(['git', 'push', 'origin', 'main'], cwd=REPO_DIR, check=True)
            print('Successfully pushed volume_spike.json to GitHub!')
        except Exception as e:
            print(f'Git push error: {e}')

if __name__ == '__main__':
    main()

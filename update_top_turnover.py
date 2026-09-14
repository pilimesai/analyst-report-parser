"""
update_top_turnover.py
每日自動抓取台灣上市（TWSE）與上櫃（TPEx）成交值前30名股票
資料來源：
  - 上市：https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL
  - 上櫃：https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php
輸出：top_turnover.json -> push 到 GitHub
"""
import sys
import os
import json
import datetime
import urllib.request
import subprocess

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
            print(f'Warning: {e}')
    return {}

def parse_trade_value(v):
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

def fetch_twse_top30(stock_names):
    print('Fetching TWSE quotes...')
    today = datetime.date.today()
    # 優先從 TWSE 官網 MI_INDEX 抓取最新行情（即時盤後，無 OpenAPI 跨日延遲問題）
    for delta in range(5):
        d = today - datetime.timedelta(days=delta)
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
                    valid = []
                    for r in rows:
                        if len(r) > 10:
                            code = str(r[0]).strip()
                            name = stock_names.get(code) or str(r[1]).strip()
                            val = parse_trade_value(r[4])
                            if val > 0:
                                chg_sign = -1 if '-' in str(r[9]) else (1 if '+' in str(r[9]) else 0)
                                chg_val = parse_float(r[10]) * (chg_sign if chg_sign != 0 else 1)
                                valid.append({
                                    'market': 'twse',
                                    'code': code,
                                    'name': name,
                                    'trade_value': val,
                                    'trade_volume': parse_trade_value(r[2]),
                                    'transaction': parse_trade_value(r[3]),
                                    'close_price': str(r[8]).replace(',', '').strip(),
                                    'change': f"{chg_val:+.2f}" if chg_val != 0 else "0.00",
                                    'date': d.strftime('%Y-%m-%d')
                                })
                    if valid:
                        valid.sort(key=lambda x: x['trade_value'], reverse=True)
                        top30 = valid[:30]
                        for rank, item in enumerate(top30, 1):
                            item['rank'] = rank
                        print(f'TWSE Top30 done via MI_INDEX ({d.strftime("%Y-%m-%d")}): {top30[0]["code"]} {top30[0]["name"]} value={top30[0]["trade_value"]:,}')
                        return top30
        except Exception as ex:
            print(f'TWSE MI_INDEX {d_str} error: {ex}')

    # 若 MI_INDEX 未能取得，Fallback 回 OpenAPI
    print('Fallback to TWSE STOCK_DAY_ALL...')
    url = 'https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL'
    headers = {'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json'}
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=20) as resp:
        data = json.loads(resp.read().decode('utf-8'))
    valid = [d for d in data if parse_trade_value(d.get('TradeValue', 0)) > 0]
    sorted_data = sorted(valid, key=lambda x: parse_trade_value(x.get('TradeValue', 0)), reverse=True)
    top30 = sorted_data[:30]
    result = []
    for rank, item in enumerate(top30, 1):
        code = str(item.get('Code', '')).strip()
        name = stock_names.get(code) or str(item.get('Name', '')).strip()
        trade_value = parse_trade_value(item.get('TradeValue', 0))
        date_str = str(item.get('Date', '')).strip()
        if len(date_str) == 7:
            date_str = f'{int(date_str[:3]) + 1911}-{date_str[3:5]}-{date_str[5:7]}'
        result.append({'rank': rank, 'market': 'twse', 'code': code, 'name': name,
            'trade_value': trade_value,
            'trade_volume': parse_trade_value(item.get('TradeVolume', 0)),
            'transaction': parse_trade_value(item.get('Transaction', 0)),
            'close_price': str(item.get('ClosingPrice', '')).strip(),
            'change': str(item.get('Change', '')).strip(),
            'date': date_str})
    print(f'TWSE Top30 done via OpenAPI: {result[0]["code"]} {result[0]["name"]} value={result[0]["trade_value"]:,}')
    return result

def fetch_tpex_top30(stock_names):
    print('Fetching TPEx OTC quotes...')
    today = datetime.date.today()
    data = None
    for delta in range(5):
        d = today - datetime.timedelta(days=delta)
        roc = f'{d.year - 1911}/{d.month:02d}/{d.day:02d}'
        url = f'https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php?l=zh-tw&d={roc.replace("/", "%2F")}&se=AL&_=1'
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36', 'Accept': 'application/json, */*'}
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=20) as resp:
                raw = resp.read()
                jd = json.loads(raw.decode('utf-8', errors='ignore'))
                tables = jd.get('tables', [])
                if tables:
                    rows = tables[0].get('data', [])
                    if rows:
                        data = {'rows': rows, 'date': d.strftime('%Y-%m-%d')}
                        print(f'TPEx: got {len(rows)} rows for {d}')
                        break
        except Exception as e:
            print(f'TPEx {d}: {e}')
    if not data:
        print('TPEx: no data')
        return []
    rows = data['rows']
    valid = [(str(r[0]).strip(), r, parse_trade_value(r[8] if len(r) > 8 else 0))
             for r in rows if len(r) > 8 and parse_trade_value(r[8] if len(r) > 8 else 0) > 0]
    valid.sort(key=lambda x: x[2], reverse=True)
    top30 = valid[:30]
    result = []
    for rank, (code, row, tv) in enumerate(top30, 1):
        name = stock_names.get(code) or ''
        result.append({'rank': rank, 'market': 'tpex', 'code': code, 'name': name,
            'trade_value': tv,
            'trade_volume': parse_trade_value(row[7] if len(row) > 7 else 0),
            'transaction': parse_trade_value(row[9] if len(row) > 9 else 0),
            'close_price': str(row[2] if len(row) > 2 else '').strip(),
            'change': str(row[3] if len(row) > 3 else '').strip(),
            'date': data['date']})
    print(f'TPEx Top30 done: {result[0]["code"]} {result[0]["name"]} value={result[0]["trade_value"]:,}')
    return result

def main():
    stock_names = load_stock_names()
    print(f'Loaded {len(stock_names)} stock names')
    twse = []
    tpex = []
    errors = []
    try:
        twse = fetch_twse_top30(stock_names)
    except Exception as e:
        print(f'TWSE error: {e}')
        errors.append(f'TWSE: {e}')
    try:
        tpex = fetch_tpex_top30(stock_names)
    except Exception as e:
        print(f'TPEx error: {e}')
        errors.append(f'TPEx: {e}')
    trade_date = (twse[0]['date'] if twse else tpex[0]['date'] if tpex else datetime.date.today().strftime('%Y-%m-%d'))
    output = {'updateTime': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'tradeDate': trade_date, 'twse': twse, 'tpex': tpex, 'errors': errors}
    out_path = os.path.join(REPO_DIR, 'top_turnover.json')
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f'Saved to {out_path}: TWSE={len(twse)}, TPEx={len(tpex)}')
    try:
        subprocess.check_call(['git', 'add', 'top_turnover.json'], cwd=REPO_DIR)
        status = subprocess.check_output(['git', 'status', '--porcelain'], cwd=REPO_DIR).decode('utf-8')
        if 'top_turnover.json' in status:
            subprocess.check_call(['git', 'commit', '-m', f'auto: update top turnover stocks ({trade_date})'], cwd=REPO_DIR)
            subprocess.check_call(['git', 'push'], cwd=REPO_DIR)
            print('Pushed to GitHub')
        else:
            print('No changes to push')
    except Exception as e:
        print(f'Git push error: {e}')

if __name__ == '__main__':
    main()

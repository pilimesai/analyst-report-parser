"""
update_call_warrants.py
全市場認購權證（Call Warrants）成交金額彙總至標的個股排行
資料來源：
  - 上市：TWSE OpenAPI (t187ap37_L 基本資料 + t187ap42_L 每日成交)
  - 上櫃：TPEx OpenAPI (mopsfin_t187ap42_O 每日成交)
輸出：call_warrants.json -> push 到 GitHub
"""
import sys
import os
import re
import json
import datetime
import subprocess
import shutil
import collections
from zoneinfo import ZoneInfo
import requests

if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

TZ_TW = ZoneInfo('Asia/Taipei')
REPO_DIR = os.path.dirname(os.path.abspath(__file__))

BROKERS = [
    "元大", "凱基", "統一", "永豐", "富邦", "群益", "國泰", "兆豐", "台新", "華南",
    "元富", "玉山", "國票", "康和", "第一", "合庫", "致和", "日盛", "宏遠", "大昌"
]
BROKER_PATTERN = re.compile(r'(' + '|'.join(BROKERS) + r')')

def load_stock_names():
    sn_path = os.path.join(REPO_DIR, 'stock_names.json')
    if os.path.exists(sn_path):
        try:
            with open(sn_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f'Warning loading stock_names.json: {e}')
    return {}

def load_cached_quotes():
    """從 top_turnover.json 獲取最新現貨收盤價與漲跌資訊（作為輔助顯示）"""
    quotes = {}
    tt_path = os.path.join(REPO_DIR, 'top_turnover.json')
    if os.path.exists(tt_path):
        try:
            with open(tt_path, 'r', encoding='utf-8') as f:
                d = json.load(f)
                for item in d.get('twse', []) + d.get('tpex', []):
                    code = str(item.get('code', '')).strip()
                    if code:
                        quotes[code] = {
                            'close_price': item.get('close_price', ''),
                            'change': item.get('change', ''),
                            'stock_trade_value': item.get('trade_value', 0)
                        }
        except Exception:
            pass
    return quotes

def fetch_twse_warrants(session):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    print("Fetching TWSE warrant basic info (t187ap37_L)...")
    r_basic = session.get('https://openapi.twse.com.tw/v1/opendata/t187ap37_L', headers=headers, timeout=30)
    basic_data = r_basic.json()

    twse_meta = {}
    for item in basic_data:
        w_code = str(item.get('權證代號', '')).strip()
        w_type = str(item.get('權證類型', '')).strip()
        underlying = str(item.get('標的證券/指數', '')).strip()
        w_name = str(item.get('權證簡稱', '')).strip()
        strike = str(item.get('最新履約價格(元)/履約指數', '')).strip()
        if w_code:
            twse_meta[w_code] = {
                'type': w_type,
                'underlying': underlying,
                'name': w_name,
                'strike': strike,
                'market': '上市'
            }

    print("Fetching TWSE warrant trades (t187ap42_L)...")
    r_trades = session.get('https://openapi.twse.com.tw/v1/opendata/t187ap42_L', headers=headers, timeout=30)
    trades_data = r_trades.json()

    return twse_meta, trades_data

def fetch_tpex_warrants(session):
    print("Fetching TPEx warrant trades (mopsfin_t187ap42_O)...")
    url = 'https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap42_O'
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}

    # 優先使用 curl --http1.1 避免 Windows 下 TPEx SSL 連線重置
    curl_bin = shutil.which("curl") or shutil.which("curl.exe") or "curl"
    try:
        res = subprocess.run(
            [curl_bin, '-s', '--http1.1', url, '-H', 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64)'],
            capture_output=True,
            timeout=35
        )
        if res.returncode == 0 and res.stdout:
            data = json.loads(res.stdout.decode('utf-8', errors='ignore'))
            if isinstance(data, list):
                return data
    except Exception as e:
        print(f"TPEx curl warning: {e}")

    # Fallback requests
    try:
        r = session.get(url, headers=headers, timeout=30)
        return r.json()
    except Exception as e:
        print(f"TPEx requests warning: {e}")
        return []

def extract_tpex_underlying(w_name):
    m = BROKER_PATTERN.search(w_name)
    if m:
        underlying = w_name[:m.start()].strip()
        is_call = '購' in w_name[m.end():] or '牛' in w_name[m.end():]
        return underlying, is_call
    return None, False

def parse_val(v):
    if v is None:
        return 0.0
    s = str(v).replace(',', '').strip()
    try:
        return float(s)
    except ValueError:
        return 0.0

def parse_int(v):
    if v is None:
        return 0
    s = str(v).replace(',', '').strip()
    try:
        return int(float(s))
    except ValueError:
        return 0

def format_roc_date(date_str):
    if not date_str:
        return datetime.datetime.now(TZ_TW).strftime('%Y-%m-%d')
    s = str(date_str).strip()
    if len(s) == 7:
        try:
            return f"{int(s[:3]) + 1911}-{s[3:5]}-{s[5:7]}"
        except ValueError:
            pass
    return s

def main():
    push_to_git = '--push' in sys.argv
    print(f"=== [update_call_warrants.py] 開始統計全市場認購權證買盤 ({datetime.datetime.now(TZ_TW).strftime('%Y-%m-%d %H:%M:%S')}) ===")

    stock_names = load_stock_names()
    name_to_code = {}
    for code, name in stock_names.items():
        c_name = name.strip()
        name_to_code[c_name] = code
        base_name = c_name.replace('*', '').replace('-KY', '').replace('KY', '')
        if base_name and base_name not in name_to_code:
            name_to_code[base_name] = code

    cached_quotes = load_cached_quotes()
    session = requests.Session()

    try:
        twse_meta, twse_trades = fetch_twse_warrants(session)
    except Exception as e:
        print(f"Error fetching TWSE warrants: {e}")
        twse_meta, twse_trades = {}, []

    try:
        tpex_trades = fetch_tpex_warrants(session)
    except Exception as e:
        print(f"Error fetching TPEx warrants: {e}")
        tpex_trades = []

    underlying_stats = collections.defaultdict(lambda: {
        'code': '',
        'name': '',
        'market': '',
        'total_call_value': 0.0,
        'total_call_volume': 0,
        'call_warrant_count': 0,
        'warrants': []
    })

    trade_date = None

    # 1. 處理上市成交明細
    for item in twse_trades:
        w_code = str(item.get('權證代號', '')).strip()
        trade_date = item.get('交易日期', item.get('日期', trade_date))
        val = parse_val(item.get('成交金額', item.get('金額', 0)))
        vol = parse_int(item.get('成交張數', item.get('成交量', 0)))
        if val <= 0:
            continue

        meta = twse_meta.get(w_code)
        if not meta or '認購' not in meta['type']:
            continue

        und_name = meta['underlying']
        und_code = name_to_code.get(und_name) or name_to_code.get(und_name.replace('*', '')) or ''

        st = underlying_stats[und_name]
        st['name'] = und_name
        st['code'] = und_code
        st['market'] = '上市'
        st['total_call_value'] += val
        st['total_call_volume'] += vol
        st['call_warrant_count'] += 1
        st['warrants'].append({
            'code': w_code,
            'name': meta['name'],
            'value': val,
            'volume': vol,
            'strike': meta['strike']
        })

    # 2. 處理上櫃成交明細
    for item in tpex_trades:
        w_code = str(item.get('權證代號', '')).strip()
        w_name = str(item.get('權證名稱', '')).strip()
        trade_date = item.get('交易日期', item.get('Date', trade_date))
        val = parse_val(item.get('成交金額', item.get('金額', 0)))
        vol = parse_int(item.get('成交數量', item.get('數量', 0)))
        if val <= 0:
            continue

        und_name, is_call = extract_tpex_underlying(w_name)
        if not is_call or not und_name:
            continue

        und_code = name_to_code.get(und_name) or name_to_code.get(und_name.replace('*', '')) or ''
        st = underlying_stats[und_name]
        st['name'] = und_name
        if not st['code']:
            st['code'] = und_code
            st['market'] = '上櫃'
        st['total_call_value'] += val
        st['total_call_volume'] += vol
        st['call_warrant_count'] += 1
        st['warrants'].append({
            'code': w_code,
            'name': w_name,
            'value': val,
            'volume': vol,
            'strike': ''
        })

    formatted_trade_date = format_roc_date(trade_date)

    # 針對每檔標的股票，先找出其「單一權證最大成交金額」
    all_processed_stocks = []
    for st in underlying_stats.values():
        top_w = sorted(st['warrants'], key=lambda w: w['value'], reverse=True)
        max_w = top_w[0] if top_w else None
        max_single_val = max_w['value'] if max_w else 0.0

        all_processed_stocks.append({
            'code': st['code'],
            'name': st['name'],
            'market': st['market'],
            'total_call_value': round(st['total_call_value']),
            'total_call_value_yi': round(st['total_call_value'] / 1e8, 2),
            'total_call_volume': st['total_call_volume'],
            'call_warrant_count': st['call_warrant_count'],
            'max_single_warrant_value': round(max_single_val),
            'max_single_warrant_wan': round(max_single_val / 1e4, 1),
            'max_single_warrant_name': max_w['name'] if max_w else '',
            'max_single_warrant_code': max_w['code'] if max_w else '',
            'max_single_warrant_strike': max_w['strike'] if max_w else '',
            'top_warrants': [
                {
                    'code': w['code'],
                    'name': w['name'],
                    'value': round(w['value']),
                    'volume': w['volume'],
                    'strike': w['strike']
                } for w in top_w[:5]
            ]
        })

    # 依照【單一權證最大成交金額】由大到小排序（符合用戶核心需求：主力重押單一權證優先）
    sorted_stocks = sorted(all_processed_stocks, key=lambda x: x['max_single_warrant_value'], reverse=True)

    total_market_call_val = sum(s['total_call_value'] for s in sorted_stocks)
    total_market_call_vol = sum(s['total_call_volume'] for s in sorted_stocks)
    total_market_warrants = sum(s['call_warrant_count'] for s in sorted_stocks)

    result_stocks = []
    for rank, st in enumerate(sorted_stocks, 1):
        code = st['code']
        quote = cached_quotes.get(code, {})
        st['rank'] = rank
        st['close_price'] = quote.get('close_price', '')
        st['change'] = quote.get('change', '')
        st['stock_trade_value'] = quote.get('stock_trade_value', 0)
        result_stocks.append(st)

    payload = {
        'updateTime': datetime.datetime.now(TZ_TW).strftime('%Y-%m-%d %H:%M:%S'),
        'tradeDate': formatted_trade_date,
        'totalStocks': len(result_stocks),
        'totalCallValue': round(total_market_call_val),
        'totalCallValueYi': round(total_market_call_val / 1e8, 2),
        'totalCallVolume': total_market_call_vol,
        'totalWarrants': total_market_warrants,
        'stocks': result_stocks
    }

    out_path = os.path.join(REPO_DIR, 'call_warrants.json')
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"\n[OK] 成功儲存 {out_path}！")
    print(f"交易日: {formatted_trade_date}")
    print(f"全市場認購權證總成交金額: {total_market_call_val/1e8:.2f} 億元，共 {len(result_stocks)} 檔標的個股，{total_market_warrants:,} 檔認購權證")
    print(f"\n前 10 大認購權證重押標的：")
    for s in result_stocks[:10]:
        print(f"  #{s['rank']} {s['code']} {s['name']} ({s['market']}): {s['total_call_value_yi']} 億元 ({s['call_warrant_count']} 檔權證)")

    if push_to_git:
        try:
            print("\n正在自動 Commit & Push 到 GitHub...")
            subprocess.run(['git', 'add', 'call_warrants.json'], cwd=REPO_DIR, check=True)
            status = subprocess.check_output(['git', 'status', '--porcelain'], cwd=REPO_DIR).decode('utf-8')
            if 'call_warrants.json' in status:
                subprocess.run(['git', 'commit', '-m', f"Auto-update: call_warrants.json ({formatted_trade_date})"], cwd=REPO_DIR, check=True)
                subprocess.run(['git', 'push'], cwd=REPO_DIR, check=True)
                print("成功 Push 至 GitHub！")
            else:
                print("檔案無變更，略過 commit & push。")
        except Exception as e:
            print(f"Git 操作失敗: {e}")

if __name__ == '__main__':
    main()

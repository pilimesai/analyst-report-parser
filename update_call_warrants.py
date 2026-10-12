"""
update_call_warrants.py
全市場認購權證（Call Warrants）成交金額彙總至標的個股排行
資料來源（優先使用盤後即時端點，OpenAPI 次日凌晨才更新，僅作為備援）：
  - 上市：TWSE MI_INDEX type=0999 (當日認購權證收盤行情，含標的代號)
          備援 TWSE OpenAPI (t187ap37_L 基本資料 + t187ap42_L 每日成交)
  - 上櫃：TPEx stk_wn1430_result.php se=AL (當日上櫃全部證券含權證)
          備援 TPEx OpenAPI (mopsfin_t187ap42_O 每日成交)
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
import urllib.request

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

UA_HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)', 'Accept': 'application/json, */*'}

def recent_weekdays(n=6):
    today = datetime.datetime.now(TZ_TW).date()
    days = []
    delta = 0
    while len(days) < n and delta < 14:
        d = today - datetime.timedelta(days=delta)
        if d.weekday() < 5:
            days.append(d)
        delta += 1
    return days

def _field_idx(fields, *names):
    clean = [re.sub(r'<[^>]+>', '', str(f)).strip() for f in fields]
    for n in names:
        if n in clean:
            return clean.index(n)
    return -1

def fetch_twse_warrants_mi_index(session, target_date=None):
    """TWSE 官網 MI_INDEX type=0999 (認購權證，不含牛證)：盤後約 15:00 即可取得當日資料"""
    dates = [target_date] if target_date else recent_weekdays()
    for d in dates:
        d_str = d.strftime('%Y%m%d')
        url = f'https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date={d_str}&type=0999&response=json'
        try:
            print(f"Fetching TWSE call warrants via MI_INDEX ({d_str})...")
            jd = session.get(url, headers=UA_HEADERS, timeout=40).json()
        except Exception as e:
            print(f"TWSE MI_INDEX {d_str} error: {e}")
            continue
        if jd.get('stat') != 'OK':
            continue
        for tbl in jd.get('tables', []):
            fields = tbl.get('fields') or []
            rows = tbl.get('data') or []
            i_und_code = _field_idx(fields, '標的代號')
            if i_und_code < 0 or not rows:
                continue
            i_code = _field_idx(fields, '證券代號')
            i_name = _field_idx(fields, '證券名稱')
            i_vol = _field_idx(fields, '成交股數')
            i_val = _field_idx(fields, '成交金額')
            i_und_name = _field_idx(fields, '標的名稱')
            trades = []
            for r in rows:
                trades.append({
                    'code': str(r[i_code]).strip(),
                    'name': str(r[i_name]).strip(),
                    'value': parse_val(r[i_val]),
                    'volume': parse_int(r[i_vol]),
                    'underlying_code': str(r[i_und_code]).strip(),
                    'underlying_name': str(r[i_und_name]).strip() if i_und_name >= 0 else '',
                })
            print(f"TWSE MI_INDEX: {len(trades)} 檔認購權證 ({d_str})")
            return d, trades
    return None, []

def fetch_tpex_warrants_wn1430(target_date):
    """TPEx 官網上櫃收盤行情 se=AL (含權證)：盤後即可取得當日資料，GitHub Actions 可正常連線"""
    roc = f'{target_date.year - 1911}/{target_date.month:02d}/{target_date.day:02d}'
    url = f'https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php?l=zh-tw&d={roc}&se=AL&_=1'
    print(f"Fetching TPEx warrants via stk_wn1430 ({roc})...")
    raw = None
    curl_bin = shutil.which("curl") or shutil.which("curl.exe") or "curl"
    try:
        res = subprocess.run([curl_bin, '-s', '--http1.1', url, '-H', f"User-Agent: {UA_HEADERS['User-Agent']}"],
                             capture_output=True, timeout=60)
        if res.returncode == 0 and res.stdout:
            raw = res.stdout.decode('utf-8', errors='ignore')
    except Exception as e:
        print(f"TPEx wn1430 curl warning: {e}")
    if not raw:
        try:
            raw = requests.get(url, headers=UA_HEADERS, timeout=60).text
        except Exception as e:
            print(f"TPEx wn1430 requests warning: {e}")
            return []
    try:
        jd = json.loads(raw)
    except Exception as e:
        print(f"TPEx wn1430 JSON parse error: {e}")
        return []
    tables = jd.get('tables') or []
    if not tables:
        return []
    fields = tables[0].get('fields') or []
    rows = tables[0].get('data') or []
    i_code = _field_idx(fields, '代號')
    i_name = _field_idx(fields, '名稱')
    i_vol = _field_idx(fields, '成交股數')
    i_val = _field_idx(fields, '成交金額(元)', '成交金額')
    if min(i_code, i_name, i_vol, i_val) < 0:
        print(f"TPEx wn1430 欄位格式異常: {fields}")
        return []
    trades = []
    for r in rows:
        trades.append({
            'code': str(r[i_code]).strip(),
            'name': str(r[i_name]).strip(),
            'value': parse_val(r[i_val]),
            'volume': parse_int(r[i_vol]),
        })
    print(f"TPEx wn1430: {len(trades)} 筆上櫃證券 ({roc})")
    return trades

def fetch_twse_warrant_meta(session):
    """TWSE 權證基本資料（履約價、到期日），僅作為輔助欄位"""
    print("Fetching TWSE warrant basic info (t187ap37_L)...")
    r_basic = session.get('https://openapi.twse.com.tw/v1/opendata/t187ap37_L', headers=UA_HEADERS, timeout=40)
    basic_data = r_basic.json()

    twse_meta = {}
    for item in basic_data:
        w_code = str(item.get('權證代號', '')).strip()
        w_type = str(item.get('權證類型', '')).strip()
        underlying = str(item.get('標的證券/指數', '')).strip()
        w_name = str(item.get('權證簡稱', '')).strip()
        strike = str(item.get('最新履約價格(元)/履約指數', '')).strip()
        expiry_raw = str(item.get('履約截止日', item.get('到期日', ''))).strip()
        expiry = format_roc_date(expiry_raw) if expiry_raw else parse_warrant_expiry_month(w_name)
        if w_code:
            twse_meta[w_code] = {
                'type': w_type,
                'underlying': underlying,
                'name': w_name,
                'strike': strike,
                'expiry': expiry,
                'market': '上市'
            }
    return twse_meta

def fetch_twse_warrants(session):
    """備援：TWSE OpenAPI (每日成交 t187ap42_L 通常於次日凌晨才更新)"""
    twse_meta = fetch_twse_warrant_meta(session)
    print("Fetching TWSE warrant trades (t187ap42_L)...")
    r_trades = session.get('https://openapi.twse.com.tw/v1/opendata/t187ap42_L', headers=UA_HEADERS, timeout=30)
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

MONTH_MAP = {'1':'01','2':'02','3':'03','4':'04','5':'05','6':'06','7':'07','8':'08','9':'09','A':'10','B':'11','C':'12','D':'12'}

def parse_warrant_expiry_month(w_name):
    """從台灣權證簡稱解析到期年月（例如 聯發科元大64購06 -> 2027/04）"""
    if not w_name:
        return ''
    m = re.search(r'([0-9])([1-9A-Da-d])(?:購|售|牛|熊)', w_name)
    if not m:
        return ''
    y_digit = int(m.group(1))
    m_char = m.group(2).upper()
    mm = MONTH_MAP.get(m_char, '01')
    roc_year = 110 + y_digit
    if roc_year < 114:
        roc_year += 10
    ad_year = roc_year + 1911
    return f"{ad_year}/{mm}"

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

    # ---- 優先：盤後即時端點 (TWSE MI_INDEX + TPEx stk_wn1430) ----
    twse_meta, twse_trades, tpex_trades = {}, [], []
    twse_norm, tpex_norm = [], []
    live_date = None
    try:
        live_date, twse_norm = fetch_twse_warrants_mi_index(session)
    except Exception as e:
        print(f"Error fetching TWSE MI_INDEX warrants: {e}")
    if live_date:
        try:
            tpex_norm = fetch_tpex_warrants_wn1430(live_date)
        except Exception as e:
            print(f"Error fetching TPEx wn1430 warrants: {e}")
        try:
            twse_meta = fetch_twse_warrant_meta(session)
        except Exception as e:
            print(f"Warning fetching TWSE warrant meta (略過履約價/到期日): {e}")

    # ---- 備援：OpenAPI (次日凌晨才會有前一交易日資料) ----
    if not twse_norm:
        try:
            twse_meta, twse_trades = fetch_twse_warrants(session)
        except Exception as e:
            print(f"Error fetching TWSE warrants: {e}")
            twse_meta, twse_trades = {}, []
    if not tpex_norm:
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

    trade_date = f"{live_date.year - 1911:03d}{live_date.month:02d}{live_date.day:02d}" if live_date else None

    # 0a. 上市（MI_INDEX 即時資料，已含標的代號/名稱）
    for w in twse_norm:
        if w['value'] <= 0:
            continue
        meta = twse_meta.get(w['code'], {})
        und_code = w.get('underlying_code', '')
        und_name = w.get('underlying_name') or stock_names.get(und_code) or meta.get('underlying', '')
        if not und_name:
            continue
        st = underlying_stats[und_name]
        st['name'] = und_name
        st['code'] = und_code or st['code']
        st['market'] = '上市'
        st['total_call_value'] += w['value']
        st['total_call_volume'] += w['volume']
        st['call_warrant_count'] += 1
        st['warrants'].append({
            'code': w['code'],
            'name': w['name'],
            'value': w['value'],
            'volume': w['volume'],
            'strike': meta.get('strike', ''),
            'expiry': meta.get('expiry') or parse_warrant_expiry_month(w['name'])
        })

    # 0b. 上櫃（stk_wn1430 即時資料，依權證名稱解析標的與認購）
    for w in tpex_norm:
        if w['value'] <= 0:
            continue
        und_name, is_call = extract_tpex_underlying(w['name'])
        if not is_call or not und_name:
            continue
        und_code = name_to_code.get(und_name) or name_to_code.get(und_name.replace('*', '')) or ''
        st = underlying_stats[und_name]
        st['name'] = und_name
        if not st['code']:
            st['code'] = und_code
            st['market'] = '上櫃'
        st['total_call_value'] += w['value']
        st['total_call_volume'] += w['volume']
        st['call_warrant_count'] += 1
        st['warrants'].append({
            'code': w['code'],
            'name': w['name'],
            'value': w['value'],
            'volume': w['volume'],
            'strike': '',
            'expiry': parse_warrant_expiry_month(w['name'])
        })

    # 1. 處理上市成交明細 (OpenAPI 備援)
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
            'strike': meta['strike'],
            'expiry': meta.get('expiry') or parse_warrant_expiry_month(meta['name'])
        })

    # 2. 處理上櫃成交明細 (OpenAPI 備援)
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
            'strike': '',
            'expiry': parse_warrant_expiry_month(w_name)
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
            'max_single_warrant_expiry': (max_w.get('expiry') or parse_warrant_expiry_month(max_w['name'])) if max_w else '',
            'top_warrants': [
                {
                    'code': w['code'],
                    'name': w['name'],
                    'value': round(w['value']),
                    'volume': w['volume'],
                    'strike': w['strike'],
                    'expiry': w.get('expiry') or parse_warrant_expiry_month(w['name'])
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

    if len(result_stocks) == 0:
        print("\n[WARNING] TWSE/TPEx OpenAPI 回傳 0 筆權證成交明細 (可能為假日或非開盤時間)。")
        print("為了保護數據完整性，系統保留原有 call_warrants.json 與 Google Sheet 資料，不進行覆蓋。")
        return

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

    # 同步至 Google Sheet 分頁【認購權證買盤】
    sync_warrants_to_google_sheet(payload)

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

def sync_warrants_to_google_sheet(output_payload):
    gas_url = "https://script.google.com/macros/s/AKfycbzm7ZXZyAe_8XhARcC8VKw2DDsWtaW_OdrANnA87lTQo2ozlM-2F4XsFFOXIC1HynqM/exec"
    trade_date = output_payload.get('tradeDate', '')
    update_time = output_payload.get('updateTime', '')
    stocks = output_payload.get('stocks', [])
    if not stocks:
        return

    headers = [
        '排名', '代號', '名稱', '市場', '買超最大權證名稱', '買超最大金額(萬)',
        '到期日', '認購總金額(億)', '認購總張數', '現貨收盤價', '現貨漲跌', '資料交易日', '更新時間'
    ]
    rows = [headers]
    for s in stocks[:100]:
        rows.append([
            s.get('rank', ''),
            s.get('code', ''),
            s.get('name', ''),
            s.get('market', ''),
            s.get('max_single_warrant_name', ''),
            s.get('max_single_warrant_wan', 0),
            s.get('max_single_warrant_expiry', ''),
            s.get('total_call_value_yi', 0),
            s.get('total_call_volume', 0),
            s.get('close_price', ''),
            s.get('change', ''),
            str(trade_date),
            str(update_time)
        ])

    try:
        print("☁️ 正在同步認購權證買盤至 Google Sheet (分頁：認購權證買盤)...")
        req_body = json.dumps({
            'action': 'save_csv',
            'sheetName': '認購權證買盤',
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
                print(f"🎉 成功同步 {len(rows)-1} 檔認購權證買盤至 Google Sheet 分頁【認購權證買盤】！")
            else:
                print(f"⚠️ Google Sheet 同步回傳訊息: {res}")
    except Exception as e:
        print(f"⚠️ Google Sheet 同步遭遇例外 (不影響本機結果產出): {e}")

if __name__ == '__main__':
    main()

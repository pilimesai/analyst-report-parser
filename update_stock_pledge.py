#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
update_stock_pledge.py
掃描全市場（上市 + 上櫃）董監事與大股東持股設質與解質異動：
1. 透過 TWSE / TPEx 開放資料取得目前有質設之公司清單
2. 透過公開資訊觀測站（MOPS）內部人設質解質公告查詢各個大股東的最新變動記錄
3. 狀態機判定：
   - 最新一筆為「設質（設質股數 > 0 且累計 > 0）」：標記為有效質設，錨定質設日收盤價為「大股東質設價」
   - 最新一筆為「解質」或累計歸零：該大股東資金撤回，即刻自名單中「剔除」，直到未來有新質設申報
4. 比對最新現價，計算相對於質設價的溢跌幅（護盤防守 / 破底警戒）
5. 產出 stock_pledge.json
"""

import os
import sys
import json
import time
import datetime
import subprocess
import tempfile
import urllib.request
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from bs4 import BeautifulSoup

# 強制輸出立即刷新，避免 Windows 背景任務緩衝
sys.stdout.reconfigure(line_buffering=True)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

CACHE_FILE = 'pledge_price_cache.json'

def load_price_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def save_price_cache(cache):
    try:
        with open(CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Warning: Failed to save price cache: {e}")

def get_market_quotes():
    """抓取全市場上市與上櫃當前收盤行情"""
    quotes = {}
    today = datetime.date.today()
    trade_date = None

    print("抓取 TWSE 上市當前收盤行情 (優先 MI_INDEX)...")
    for delta in range(5):
        d = today - datetime.timedelta(days=delta)
        if d.weekday() >= 5:
            continue
        d_str = d.strftime("%Y%m%d")
        url = f"https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date={d_str}&type=ALLBUT0999&response=json"
        try:
            req = urllib.request.Request(url, headers={'User-Agent': HEADERS['User-Agent'], 'Accept': 'application/json'})
            with urllib.request.urlopen(req, timeout=20) as resp:
                jd = json.loads(resp.read().decode('utf-8', errors='ignore'))
            if jd.get("stat") == "OK":
                tables = [tbl for tbl in jd.get("tables", []) if len(tbl.get("data", [])) > 500]
                if tables:
                    for r in tables[0].get("data", []):
                        code = str(r[0]).strip()
                        name = str(r[1]).strip()
                        if len(code) == 4 and code.isdigit():
                            cp_str = str(r[8]).replace(",", "").strip()
                            chg_sign = -1 if "-" in str(r[9]) else (1 if "+" in str(r[9]) else 0)
                            chg_val = str(r[10]).replace(",", "").strip()
                            try:
                                close = float(cp_str)
                                chg = float(chg_val) * chg_sign if chg_sign != 0 else 0.0
                                quotes[code] = {
                                    'code': code,
                                    'name': name,
                                    'close': close,
                                    'change': chg,
                                    'market': 'twse'
                                }
                            except ValueError:
                                pass
                    trade_date = d.strftime("%Y-%m-%d")
                    print(f"TWSE quotes fetched via MI_INDEX: {len(quotes)} stocks ({trade_date})")
                    break
        except Exception as ex:
            print(f"TWSE MI_INDEX {d_str} error: {ex}")

    if not quotes:
        print("Fallback to TWSE OpenAPI STOCK_DAY_ALL...")
        try:
            r = requests.get('https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL', headers=HEADERS, timeout=12)
            if r.status_code == 200:
                for item in r.json():
                    code = item.get('Code') or item.get('證券代號')
                    name = item.get('Name') or item.get('證券名稱')
                    close_str = item.get('ClosingPrice') or item.get('收盤價')
                    chg_str = item.get('Change') or item.get('漲跌價差')
                    try:
                        close = float(str(close_str).replace(',', ''))
                    except (ValueError, TypeError):
                        close = None
                    try:
                        chg = float(str(chg_str).replace(',', ''))
                    except (ValueError, TypeError):
                        chg = 0.0
                    if code and close is not None:
                        quotes[code] = {
                            'code': code,
                            'name': name,
                            'close': close,
                            'change': chg,
                            'market': 'twse'
                        }
        except Exception as e:
            print(f"TWSE quotes fetch failed: {e}")

    print("抓取 TPEx 上櫃當前收盤行情...")
    for delta in range(5):
        d = today - datetime.timedelta(days=delta)
        if d.weekday() >= 5:
            continue
        roc = f"{d.year - 1911}/{d.month:02d}/{d.day:02d}"
        url = f"https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php?l=zh-tw&d={roc}&se=AL&_=1"
        raw_bytes = None

        curl_bin = shutil.which("curl") or shutil.which("curl.exe") or "curl"
        try:
            res = subprocess.run(
                [curl_bin, '-s', '--http1.1', url, '-H', f'User-Agent: {HEADERS["User-Agent"]}'],
                capture_output=True,
                timeout=35
            )
            if res.returncode == 0 and res.stdout:
                raw_bytes = res.stdout
        except Exception as ex:
            print(f"TPEx curl error for {d}: {ex}")

        if not raw_bytes:
            try:
                req = urllib.request.Request(url, headers={'User-Agent': HEADERS['User-Agent'], 'Accept': 'application/json, */*'})
                with urllib.request.urlopen(req, timeout=30) as resp:
                    raw_bytes = resp.read()
            except Exception as ex:
                print(f"TPEx urllib error for {d}: {ex}")

        if raw_bytes:
            try:
                jd = json.loads(raw_bytes.decode('utf-8', errors='ignore'))
                tables = jd.get('tables', [])
                rows = tables[0].get('data', []) if tables else jd.get('aaData', [])
                if rows:
                    for r in rows:
                        code = str(r[0]).strip()
                        name = str(r[1]).strip()
                        close_str = str(r[2]).strip()
                        chg_str = str(r[3]).strip()
                        try:
                            close = float(close_str.replace(',', ''))
                        except ValueError:
                            close = None
                        try:
                            chg = float(chg_str.replace(',', ''))
                        except ValueError:
                            chg = 0.0
                        if code and close is not None and code not in quotes:
                            quotes[code] = {
                                'code': code,
                                'name': name,
                                'close': close,
                                'change': chg,
                                'market': 'tpex'
                            }
                    if not trade_date:
                        trade_date = d.strftime("%Y-%m-%d")
                    print(f"TPEx quotes fetched: got rows for {d}")
                    break
            except Exception:
                pass

    print(f"全市場行情已抓取完成，共 {len(quotes)} 檔個股。")
    return quotes, (trade_date or today.strftime("%Y-%m-%d"))

def get_pledged_candidates(quotes=None):
    """從 TWSE/TPEx 開放資料取得目前有內部人質押之公司代號清單，並合併持久候選名單"""
    candidate_stocks = {}
    print("從 TWSE OpenAPI 取得上市內部人持股設質清單 (t187ap11_L)...")
    try:
        r = requests.get('https://openapi.twse.com.tw/v1/opendata/t187ap11_L', headers=HEADERS, timeout=15)
        if r.status_code == 200:
            for d in r.json():
                code = d.get('公司代號')
                name = d.get('公司名稱')
                pledge_str = d.get('設質股數')
                try:
                    shares = int(str(pledge_str).replace(',', ''))
                except (ValueError, TypeError):
                    shares = 0
                if code and shares > 0:
                    if code not in candidate_stocks:
                        candidate_stocks[code] = {'code': code, 'name': name, 'market': 'twse', 'total_pledged': 0}
                    candidate_stocks[code]['total_pledged'] += shares
    except Exception as e:
        print(f"TWSE pledge candidates fetch error: {e}")

    print("從 TWSE OpenAPI 取得上櫃內部人持股設質清單 (t187ap11_P)...")
    try:
        r = requests.get('https://openapi.twse.com.tw/v1/opendata/t187ap11_P', headers=HEADERS, timeout=15)
        if r.status_code == 200:
            for d in r.json():
                code = d.get('公司代號')
                name = d.get('公司名稱')
                pledge_str = d.get('設質股數')
                try:
                    shares = int(str(pledge_str).replace(',', ''))
                except (ValueError, TypeError):
                    shares = 0
                if code and shares > 0:
                    if code not in candidate_stocks:
                        candidate_stocks[code] = {'code': code, 'name': name, 'market': 'tpex', 'total_pledged': 0}
                    candidate_stocks[code]['total_pledged'] += shares
    except Exception as e:
        print(f"TPEx pledge candidates fetch error: {e}")

    # 合併持久化候選名單快取 (包含 6830 汎銓 等當月最新設質個股，避免因月報時滯漏掉)
    CACHE_CANDIDATES_FILE = 'pledge_candidates_cache.json'
    persistent_candidates = set(['6830'])
    if os.path.exists(CACHE_CANDIDATES_FILE):
        try:
            with open(CACHE_CANDIDATES_FILE, 'r', encoding='utf-8') as f:
                persistent_candidates.update(json.load(f))
        except Exception:
            pass

    if quotes:
        for code in persistent_candidates:
            if code not in candidate_stocks and code in quotes:
                q = quotes[code]
                candidate_stocks[code] = {
                    'code': code,
                    'name': q['name'],
                    'market': q.get('market', 'twse'),
                    'total_pledged': 0
                }

    try:
        with open(CACHE_CANDIDATES_FILE, 'w', encoding='utf-8') as f:
            json.dump(sorted(list(set(list(candidate_stocks.keys()) + list(persistent_candidates)))), f, ensure_ascii=False, indent=2)
    except Exception:
        pass

    print(f"共發現 {len(candidate_stocks)} 家公司列入董監事/大股東持股設質追蹤。")
    return candidate_stocks

def fetch_mops_stock_pledge_history(code):
    """查詢個別公司在公開資訊觀測站的設質解質公告完整日誌 (優先查今年，無異動再查前一年)"""
    url = "https://mopsov.twse.com.tw/mops/web/ajax_STAMAK03_1"
    current_roc_year = datetime.date.today().year - 1911

    def query_year(yr):
        ev_list = []
        data = {
            "encodeURIComponent": "1",
            "step": "1",
            "firstin": "1",
            "off": "1",
            "co_id": code,
            "year": yr
        }
        try:
            r = requests.post(url, data=data, headers=HEADERS, timeout=6)
            if r.status_code == 200 and len(r.text) > 1000:
                soup = BeautifulSoup(r.text, 'html.parser')
                tables = soup.find_all('table')
                if tables:
                    rows = tables[0].find_all('tr')
                    for row in rows[1:]:
                        tds = [td.get_text(strip=True) for td in row.find_all(['th', 'td'])]
                        if len(tds) >= 9:
                            try:
                                pledge_shares = int(tds[5].replace(',', ''))
                            except ValueError:
                                pledge_shares = 0
                            try:
                                unpledge_shares = int(tds[6].replace(',', ''))
                            except ValueError:
                                unpledge_shares = 0
                            try:
                                cum_shares = int(tds[7].replace(',', ''))
                            except ValueError:
                                cum_shares = 0
                            ev_list.append({
                                'identity': tds[2],
                                'name': tds[3],
                                'event_date': tds[4],
                                'pledge_shares': pledge_shares,
                                'unpledge_shares': unpledge_shares,
                                'cum_shares': cum_shares,
                                'creditor': tds[8],
                                'report_date': tds[10] if len(tds) > 10 else tds[4]
                            })
        except Exception:
            pass
        return ev_list

    # 優先查當年度 (115)，若有資料即為最新
    events = query_year(str(current_roc_year))
    if not events:
        events = query_year(str(current_roc_year - 1))

    # 去重並以日期由舊到新排序，確保狀態機 latest_ev = ev_list[-1] 取到真正最新事件
    seen = set()
    unique_evts = []
    for e in events:
        k = (e['name'], e['identity'], e['event_date'], e['pledge_shares'], e['unpledge_shares'], e['cum_shares'])
        if k not in seen:
            seen.add(k)
            unique_evts.append(e)

    def parse_key(e):
        try:
            p = [int(x) for x in e['event_date'].split('/')]
            return (p[0], p[1], p[2])
        except Exception:
            return (0, 0, 0)
    unique_evts.sort(key=parse_key, reverse=False)
    return unique_evts

def get_closing_price_for_date(code, roc_date, price_cache, current_price=None):
    """
    透過 TWSE/TPEx STOCK_DAY 查詢某特定日期的收盤價
    roc_date: 如 "115/09/21"
    """
    cache_key = f"{code}_{roc_date}"
    if cache_key in price_cache:
        return price_cache[cache_key]

    parts = roc_date.split('/')
    if len(parts) != 3:
        return current_price

    try:
        roc_year = int(parts[0])
        ad_year = roc_year + 1911
        month = int(parts[1])
        day = int(parts[2])
    except ValueError:
        return current_price

    current_roc_year = datetime.date.today().year - 1911
    # 僅對近 2 年以內之最新設質進行線上精準對齊，歷史久遠者使用現價或既有快取
    if roc_year < (current_roc_year - 2):
        return current_price

    date_str = f"{ad_year}{month:02d}01"
    url = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date={date_str}&stockNo={code}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=5)
        if r.status_code == 200:
            j = r.json()
            data = j.get('data', [])
            target_price = None
            for row in data:
                r_date = row[0].strip()
                try:
                    c_price = float(row[6].replace(',', ''))
                    # 一併快取該月份所有交易日價格，極速複用
                    price_cache[f"{code}_{r_date}"] = c_price
                except ValueError:
                    continue
                if r_date <= roc_date:
                    target_price = c_price
                if r_date == roc_date:
                    target_price = c_price
                    break
            if target_price is not None:
                return target_price
    except Exception:
        pass

    return current_price

def analyze_stock_pledges(candidate_stocks, quotes):
    """
    核心狀態機：
    對每個候選個股查詢歷史異動，依大股東個別分析最新狀態：
    - 若大股東最新為設質且累計質設>0 -> 納入名單
    - 若大股東最新為解質或累計歸零 -> 剔除
    """
    price_cache = load_price_cache()
    active_stocks = []
    excluded_stocks = []

    print(f"正在向 MOPS 查詢質設解質歷史異動（候選股共 {len(candidate_stocks)} 檔）...")
    stock_codes = list(candidate_stocks.keys())

    events_map = {}
    done_cnt = 0
    with ThreadPoolExecutor(max_workers=16) as executor:
        future_to_code = {executor.submit(fetch_mops_stock_pledge_history, code): code for code in stock_codes}
        for future in as_completed(future_to_code):
            code = future_to_code[future]
            done_cnt += 1
            if done_cnt % 50 == 0 or done_cnt == len(stock_codes):
                print(f"MOPS 查詢進度: {done_cnt}/{len(stock_codes)} (已獲取 {len(events_map)} 檔質設事件)...")
            try:
                evts = future.result()
                if evts:
                    events_map[code] = evts
            except Exception:
                pass

    print(f"成功取得 {len(events_map)} 檔個股之完整質押異動記錄。")

    raw_active = []
    for code, events in events_map.items():
        q = quotes.get(code)
        if not q or not q.get('close'):
            continue

        cand = candidate_stocks.get(code, {})
        stock_name = q.get('name') or cand.get('name') or code
        market = q.get('market', 'twse')
        current_close = q.get('close')
        current_change = q.get('change', 0.0)

        # 依股東姓名與身份分組
        shareholder_events = {}
        for ev in events:
            sh_key = f"{ev['name']}_{ev['identity']}"
            if sh_key not in shareholder_events:
                shareholder_events[sh_key] = []
            shareholder_events[sh_key].append(ev)

        # 檢視大股東之最新動態
        for sh_key, ev_list in shareholder_events.items():
            latest_ev = ev_list[-1]

            is_active_pledge = (latest_ev['pledge_shares'] > 0 and latest_ev['cum_shares'] > 0)
            is_unpledged = (latest_ev['unpledge_shares'] > 0 or latest_ev['cum_shares'] == 0)

            clean_name = latest_ev['name']
            clean_title = latest_ev['identity'].split('0')[-1].replace('本人', '').replace('代表', '')
            if not clean_title:
                clean_title = latest_ev['identity']

            if is_active_pledge:
                raw_active.append({
                    'code': code,
                    'name': stock_name,
                    'market': market,
                    'current_price': current_close,
                    'current_change': current_change,
                    'shareholder_name': clean_name,
                    'shareholder_title': clean_title,
                    'pledge_date': latest_ev['event_date'],
                    'pledge_shares': latest_ev['pledge_shares'],
                    'pledge_shares_lots': round(latest_ev['pledge_shares'] / 1000, 1),
                    'cum_shares': latest_ev['cum_shares'],
                    'cum_shares_lots': round(latest_ev['cum_shares'] / 1000, 1),
                    'creditor': latest_ev['creditor'] or '金融機構'
                })
            elif is_unpledged:
                excluded_stocks.append({
                    'code': code,
                    'name': stock_name,
                    'market': market,
                    'shareholder_name': clean_name,
                    'shareholder_title': clean_title,
                    'unpledge_date': latest_ev['event_date'],
                    'unpledge_shares': latest_ev['unpledge_shares'],
                    'unpledge_shares_lots': round(latest_ev['unpledge_shares'] / 1000, 1),
                    'cum_shares': latest_ev['cum_shares'],
                    'reason': '同一大股東最新異動為解質，依規定剔除於名單之外'
                })

    print(f"發現 {len(raw_active)} 筆有效質設紀錄，正在對齊質設日收盤價...")

    # 批次查詢收盤價（優先查詢近 2 年最新設質；久遠設質直接以現價作為防守基準，避免大量外部請求壅塞）
    current_roc_year = datetime.date.today().year - 1911
    needed_prices = set(
        (item['code'], item['pledge_date'])
        for item in raw_active
        if item['pledge_date'] and (int(item['pledge_date'].split('/')[0]) >= current_roc_year - 2) and f"{item['code']}_{item['pledge_date']}" not in price_cache
    )
    print(f"需查詢最新質設收盤價：{len(needed_prices)} 筆 (已有快取 {len(price_cache)} 筆)")

    # 限制並發數避免被 TWSE 限速
    with ThreadPoolExecutor(max_workers=8) as executor:
        future_map = {
            executor.submit(get_closing_price_for_date, c, p_date, price_cache, quotes.get(c, {}).get('close')): (c, p_date)
            for (c, p_date) in needed_prices
        }
        for f in as_completed(future_map):
            c, p_date = future_map[f]
            try:
                p = f.result()
                if p:
                    price_cache[f"{c}_{p_date}"] = p
            except Exception:
                pass

    save_price_cache(price_cache)

    # 封裝 active_stocks 並計算相對於質設價的溢跌幅
    for item in raw_active:
        code = item['code']
        pledge_date = item['pledge_date']
        current_close = item['current_price']
        pledge_price = price_cache.get(f"{code}_{pledge_date}") or current_close

        diff_pct = round(((current_close - pledge_price) / pledge_price) * 100, 2) if pledge_price > 0 else 0.0
        status_label = "above_pledge" if diff_pct >= 0 else "below_pledge"

        item['pledge_price'] = pledge_price
        item['diff_pct'] = diff_pct
        item['status'] = status_label
        active_stocks.append(item)

    def parse_roc_date_key(d_str):
        try:
            parts = str(d_str).split('/')
            if len(parts) == 3:
                return (int(parts[0]), int(parts[1]), int(parts[2]))
        except Exception:
            pass
        return (0, 0, 0)

    # 排序：優先以最新質設日期（正確解析民國年）、累計質押張數排序
    active_stocks.sort(key=lambda x: (parse_roc_date_key(x.get('pledge_date', '')), x.get('cum_shares', 0)), reverse=True)

    # 每檔個股保留最新一筆主要質設
    unique_active = []
    seen = set()
    for s in active_stocks:
        if s['code'] not in seen:
            seen.add(s['code'])
            unique_active.append(s)

    # 依使用者需求：超過三年前質設的就不列入
    today = datetime.date.today()
    cutoff_date = datetime.date(today.year - 3, today.month, today.day)

    def is_within_3_years(d_str):
        try:
            parts = [int(p) for p in str(d_str).split('/')]
            if len(parts) == 3:
                ad_date = datetime.date(parts[0] + 1911, parts[1], parts[2])
                return ad_date >= cutoff_date
        except Exception:
            pass
        return False

    filtered_active = [s for s in unique_active if is_within_3_years(s.get('pledge_date', ''))]

    # 排除股依最新解質日降序排序
    excluded_stocks.sort(key=lambda x: parse_roc_date_key(x.get('unpledge_date', '')), reverse=True)

    return filtered_active, excluded_stocks

def main():
    start_time = time.time()
    print("=" * 60)
    print("啟動大股東股票質設追蹤掃描器")
    print("=" * 60)

    quotes, detected_trade_date = get_market_quotes()
    candidates = get_pledged_candidates(quotes)
    active_stocks, excluded_stocks = analyze_stock_pledges(candidates, quotes)

    total_active = len(active_stocks)
    above_count = sum(1 for s in active_stocks if s['diff_pct'] >= 0)
    above_ratio = round((above_count / total_active * 100), 1) if total_active > 0 else 0.0

    top_gain = max(active_stocks, key=lambda x: x['diff_pct']) if active_stocks else None
    top_loss = min(active_stocks, key=lambda x: x['diff_pct']) if active_stocks else None

    today_str = datetime.date.today().isoformat()
    now_str = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8))).isoformat()
    trade_date = detected_trade_date or today_str

    output_data = {
        'updateTime': now_str,
        'tradeDate': trade_date,
        'summary': {
            'total_active_stocks': total_active,
            'above_pledge_count': above_count,
            'below_pledge_count': total_active - above_count,
            'above_pledge_ratio': above_ratio,
            'top_gain_stock': {
                'code': top_gain['code'],
                'name': top_gain['name'],
                'diff_pct': top_gain['diff_pct'],
                'pledge_price': top_gain['pledge_price'],
                'current_price': top_gain['current_price']
            } if top_gain else None,
            'top_loss_stock': {
                'code': top_loss['code'],
                'name': top_loss['name'],
                'diff_pct': top_loss['diff_pct'],
                'pledge_price': top_loss['pledge_price'],
                'current_price': top_loss['current_price']
            } if top_loss else None
        },
        'stocks': active_stocks,
        'excluded_stocks': excluded_stocks[:60]
    }

    output_path = 'stock_pledge.json'
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)

    elapsed = round(time.time() - start_time, 1)
    print(f"\n[OK] 掃描完成！耗時 {elapsed} 秒。")
    print(f"交易日期：{trade_date}")
    print(f"有效質設標的：{total_active} 檔（現價高於質設價比例：{above_ratio}%）")
    if top_gain:
        print(f"最大溢價標的：{top_gain['code']} {top_gain['name']} (+{top_gain['diff_pct']}%)")
    if top_loss:
        print(f"最大跌破標的：{top_loss['code']} {top_loss['name']} ({top_loss['diff_pct']}%)")
    print(f"解質剔除標的：{len(excluded_stocks)} 筆")
    print(f"產出檔案：{output_path}")

    if '--push' in sys.argv:
        try:
            subprocess.run(['git', 'add', 'stock_pledge.json'], check=True)
            subprocess.run(['git', 'commit', '-m', f'auto: update stock pledge ({trade_date})'], check=True)
            subprocess.run(['git', 'push', 'origin', 'main'], check=True)
            print('Successfully pushed stock_pledge.json to GitHub!')
        except Exception as e:
            print(f'Git push error: {e}')

if __name__ == '__main__':
    main()

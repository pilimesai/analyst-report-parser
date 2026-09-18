"""
update_sector_strength.py
每日掃描所有上市（TWSE）與上櫃（TPEx）股票
依官方產業別分組，計算各族群中股價站上月線（MA20）的比例

多方族群：>= 50% 個股 Close > MA20
空方族群：< 50% 個股 Close > MA20

輸出：sector_strength.json
"""
import sys
import os
import json
import re
import datetime
from zoneinfo import ZoneInfo
import urllib.request
import subprocess
import time

TZ_TW = ZoneInfo("Asia/Taipei")

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

REPO_DIR = os.path.dirname(os.path.abspath(__file__))

BULLISH_THRESHOLD = 0.5  # 多方門檻：>= 50% 個股站上月線
MIN_SECTOR_STOCKS = 3    # 最小有效樣本：< 3 支則標記為 insufficient

# TWSE 官方產業別代碼 → 中文名稱對照表（經台灣證券交易所官方編碼核對）
TWSE_INDUSTRY_CODE_MAP = {
    "01": "水泥工業",
    "02": "食品工業",
    "03": "塑膠工業",
    "04": "紡織纖維",
    "05": "電機機械",
    "06": "電器電纜",
    "08": "玻璃陶瓷",
    "09": "造紙工業",
    "10": "鋼鐵工業",
    "11": "橡膠工業",
    "12": "汽車工業",
    "14": "建材營造業",
    "15": "航運業",
    "16": "觀光餐旅業",
    "17": "金融保險業",
    "18": "貿易百貨業",
    "20": "其他業",
    "21": "化學工業",
    "22": "生技醫療業",
    "23": "油電燃氣業",
    "24": "半導體業",
    "25": "電腦及週邊設備業",
    "26": "光電業",
    "27": "通信網路業",
    "28": "電子零組件業",
    "29": "電子通路業",
    "30": "資訊服務業",
    "31": "其他電子業",
    "32": "文化創意業",
    "33": "農業科技業",
    "35": "綠能環保",
    "36": "數位雲端業",
    "37": "運動休閒業",
    "38": "居家生活業",
    "91": "存託憑證",
}

# 市場熱門題材 / 細分子族群（311 檔市場核心題材代表股，經台股代碼與營業項目逐一核對）
THEME_SECTORS = {
    "CCL（銅箔基板）": ["2383", "6274", "6213", "6672", "8358", "1815"],
    "IC 封測": ["3711", "2449", "6239", "3264", "6257", "8150", "3265", "8110", "2441", "3374", "6525", "8131", "6147", "2369", "6223", "6515", "6683", "6510", "3289", "3587", "6271"],
    "散熱模組": ["3017", "3324", "3653", "3483", "6230", "2421", "3338", "6591", "8996", "6124", "6831", "3071", "6275", "2486", "3013"],
    "PCB（印刷電路板）": ["2368", "3037", "3189", "8046", "2313", "3044", "5469", "6191", "6153", "2367", "4958", "2355", "5475", "8155", "3715", "6141", "5439", "6269", "4927", "2316", "6278", "8213", "2402"],
    "被動元件": ["2327", "2492", "3026", "2478", "3624", "6834", "6449", "6173", "8043", "6224", "2428", "2472", "3090", "2457", "3236", "3357", "6155", "8042", "3207", "2375", "6284", "6127", "5328", "6204", "6175", "6174", "2431", "3432", "8431", "3042", "8182", "3221"],
    "CPO / 矽光子": ["6442", "3450", "3363", "4979", "3163", "4977", "3081", "6451", "3234", "4908", "6530", "3491", "8086", "2455", "2345", "3167", "6285"],
    "IC 設計": ["2454", "2379", "3034", "3035", "3661", "3443", "3529", "6415", "6531", "6533", "4966", "5274", "3227", "3014", "2458", "4919", "6462", "8016", "5269", "8299", "5351", "3545", "6138", "6243", "4968", "6732"],
    "CoWoS / 設備檢測": ["3131", "3583", "3680", "6187", "6640", "5443", "2467", "6830", "3587", "6937", "3413", "6788", "6223", "6515", "6510", "6271", "3289", "6667", "6196", "5536", "2404", "8028", "1560", "6683", "6217", "3167", "3374"],
    "伺服器 / AI ODM": ["2382", "2317", "3231", "6669", "2356", "2376", "2357", "3706", "2377", "4938", "2324", "3515"],
    "軸承 / 折疊機": ["6805", "3548", "3376", "3013", "1582", "5215", "4976"],
    "連接器 / 線材": ["3023", "3533", "6715", "6197", "3665", "2392", "3605", "3003", "3526", "3217", "5457", "6290", "8103", "6205", "3501", "6217", "2462", "6220", "3092", "3305"],
    "記憶體 / 模組": ["2408", "2344", "2337", "3260", "4967", "2451", "8271", "3006", "6485", "8299", "5351"],
    "重電 / 綠能儲能": ["1519", "1513", "1503", "1514", "1609", "1605", "6869", "6873", "3708", "9958", "6806", "1504", "6443", "6477", "3576"],
    "工具機 / 機器人概念": ["2049", "4583", "4576", "2359", "4526", "1504", "4563", "8374", "2464", "6215", "6125", "1597", "4566", "4540"],
    "光學鏡頭": ["3008", "3406", "3019", "3362", "3504", "3441", "6517", "4976", "6209"],
    "車用零組件": ["1319", "2201", "2207", "2231", "1522", "1524", "1536", "6279", "3552", "2497", "4551", "1533", "6605"],
    "航運（貨櫃/散裝/航空）": ["2603", "2609", "2615", "2606", "2605", "2637", "2618", "2610", "2612", "5608"],
    "晶圓代工 / 第三代半導體": ["2330", "2303", "5347", "6770", "3707", "3016", "6488", "5483", "3105"],
    "生技醫療核心": ["6472", "6446", "1795", "6491", "6782", "3218", "6919", "4174", "4743", "4128", "1760", "6589"],
    "AI 機殼 / 電源供應器": ["2308", "2301", "6282", "3078", "6412", "8210", "3013", "6117", "3693", "3706", "2465", "2457"],
}

def normalize_industry(code_or_name: str) -> str:
    """
    將 TWSE/TPEx 產業別代碼（如 "14"）轉為中文名稱（"半導體業"）。
    若已是中文名稱則直接回傳。
    """
    s = code_or_name.strip()
    # 若全為數字（代碼），查對照表
    if s.isdigit() or (len(s) == 2 and s[0].isdigit()):
        return TWSE_INDUSTRY_CODE_MAP.get(s, f"產業{s}")
    return s


def load_stock_names():
    sn_path = os.path.join(REPO_DIR, "stock_names.json")
    if os.path.exists(sn_path):
        try:
            with open(sn_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"Warning: load stock_names.json failed: {e}")
    return {}


def fetch_twse_industry_map(stock_names):
    """
    抓取 TWSE 上市公司 <-> 產業別對照表
    來源：TWSE OpenAPI t187ap03_L（JSON，含公司代號、公司簡稱、產業別）
    """
    print("Fetching TWSE industry map from OpenAPI t187ap03_L...")
    url = "https://openapi.twse.com.tw/v1/opendata/t187ap03_L"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)", "Accept": "application/json"}
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=25) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        result = {}
        for item in data:
            code = str(item.get("公司代號", "")).strip()
            industry = str(item.get("產業別", "")).strip()
            name = str(item.get("公司簡稱", "")).strip()
            name = stock_names.get(code, name)
            if code and industry and len(code) == 4 and code.isdigit():
                result[code] = {"name": name, "industry": normalize_industry(industry), "market": "twse"}
        print(f"TWSE: fetched {len(result)} stocks with industry classification")
        return result
    except Exception as e:
        print(f"TWSE industry fetch error: {e}")
        return {}


TPEX_INDUSTRY_CODE_MAP = {
    "02": "食品工業", "03": "塑膠工業", "04": "紡織纖維", "05": "電機機械", "06": "電器電纜",
    "10": "鋼鐵工業", "14": "建材營造業", "15": "航運業", "16": "觀光餐旅業", "17": "金融保險業",
    "20": "其他業", "21": "化學工業", "22": "生技醫療", "23": "油電燃氣業", "24": "半導體業",
    "25": "電腦及週邊設備業", "26": "光電業", "27": "通信網路業", "28": "電子零組件業", "29": "電子通路業",
    "30": "資訊服務業", "31": "其他電子業", "32": "文化創意業", "33": "農業科技業", "35": "綠能環保",
    "36": "數位雲端業", "37": "運動休閒業", "38": "居家生活業"
}


def fetch_tpex_industry_map(stock_names):
    """
    抓取 TPEx 上櫃公司 <-> 產業別對照表
    來源：TPEx OpenAPI mopsfin_t187ap03_O (SecuritiesCompanyCode + SecuritiesIndustryCode)
    """
    print("Fetching TPEx industry map from OpenAPI mopsfin_t187ap03_O...")
    url = "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)", "Accept": "application/json"}
    result = {}
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=25) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        for item in data:
            code = str(item.get("SecuritiesCompanyCode", "")).strip()
            ind_code = str(item.get("SecuritiesIndustryCode", "")).strip()
            if len(code) == 4 and code.isdigit():
                ind_name = TPEX_INDUSTRY_CODE_MAP.get(ind_code, "其他業")
                display_name = stock_names.get(code, code)
                result[code] = {
                    "name": display_name,
                    "industry": ind_name,
                    "market": "tpex"
                }
        print(f"TPEx: fetched {len(result)} stocks with industry classification")
        return result
    except Exception as e:
        print(f"TPEx OpenAPI fetch error: {e}")
        return {}


def build_industry_map(stock_names):
    """合併 TWSE + TPEx 產業對照表，TWSE 優先"""
    twse_map = fetch_twse_industry_map(stock_names)
    tpex_map = fetch_tpex_industry_map(stock_names)

    combined = {}
    combined.update(tpex_map)
    combined.update(twse_map)

    filtered = {
        code: info for code, info in combined.items()
        if info["industry"] and len(info["industry"]) >= 2
    }

    print(f"Combined industry map: {len(filtered)} stocks (TWSE={len(twse_map)}, TPEx={len(tpex_map)})")
    return filtered


def fetch_official_today_closes():
    """
    從 TWSE MI_INDEX 與 TPEx stk_wn1430 抓取全市場最新官方收盤價。
    直接解決 yfinance 當日盤後資料延遲（日K棒 Close 呈現 NaN）導致抓到昨日價格的問題。
    """
    today = datetime.datetime.now(TZ_TW).date()
    closes = {}
    twse_date = None
    tpex_date = None

    print("Fetching TWSE official closing quotes (MI_INDEX)...")
    for delta in range(5):
        d = today - datetime.timedelta(days=delta)
        d_str = d.strftime("%Y%m%d")
        url = f"https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date={d_str}&type=ALLBUT0999&response=json"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)", "Accept": "application/json"}
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=12) as resp:
                jd = json.loads(resp.read().decode("utf-8", errors="ignore"))
            if jd.get("stat") == "OK":
                tables = [tbl for tbl in jd.get("tables", []) if len(tbl.get("data", [])) > 500]
                if tables:
                    for r in tables[0].get("data", []):
                        code = str(r[0]).strip()
                        if len(code) == 4 and code.isdigit():
                            cp_str = str(r[8]).replace(",", "").strip()
                            try:
                                closes[code] = float(cp_str)
                            except ValueError:
                                pass
                    twse_date = d.strftime("%Y-%m-%d")
                    print(f"TWSE official closes: {len(closes)} stocks ({twse_date})")
                    break
        except Exception:
            pass

    print("Fetching TPEx official closing quotes (stk_wn1430)...")
    for delta in range(5):
        d = today - datetime.timedelta(days=delta)
        roc = f"{d.year - 1911}/{d.month:02d}/{d.day:02d}"
        url = f"https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php?l=zh-tw&d={roc.replace('/', '%2F')}&se=AL&_=1"
        try:
            res = subprocess.run(["curl.exe", "-s", "--http1.1", url, "-H", "User-Agent: Mozilla/5.0"], capture_output=True, timeout=15)
            jd = json.loads(res.stdout.decode("utf-8", errors="ignore"))
            tables = jd.get("tables", [])
            if tables and tables[0].get("data"):
                cnt = 0
                for r in tables[0]["data"]:
                    code = str(r[0]).strip()
                    if len(code) == 4 and code.isdigit():
                        cp_str = str(r[2]).replace(",", "").strip()
                        try:
                            closes[code] = float(cp_str)
                            cnt += 1
                        except ValueError:
                            pass
                tpex_date = d.strftime("%Y-%m-%d")
                print(f"TPEx official closes: {cnt} stocks ({tpex_date})")
                break
        except Exception:
            pass

    official_trade_date = twse_date or tpex_date
    return closes, official_trade_date


def compute_ma20_signals(industry_map, official_closes=None, official_trade_date=None):
    """
    批次下載所有股票 2 個月歷史，並結合官方最新收盤價計算 MA20
    回傳 (signals, detected_trade_date)
    """
    import yfinance as yf
    import pandas as pd

    ticker_to_code = {}
    tickers = []
    for code, info in industry_map.items():
        suffix = ".TW" if info["market"] == "twse" else ".TWO"
        ticker = f"{code}{suffix}"
        ticker_to_code[ticker] = code
        tickers.append(ticker)

    if not tickers:
        return {}, None

    print(f"Downloading price history for {len(tickers)} stocks via yfinance...")
    t0 = time.time()
    try:
        df = yf.download(
            tickers,
            period="2mo",
            progress=False,
            group_by="ticker",
            threads=True,
            auto_adjust=True
        )
    except Exception as e:
        print(f"yfinance batch download error: {e}")
        return {}, None
    print(f"Download completed in {time.time() - t0:.1f}s")

    detected_trade_date = official_trade_date
    if not detected_trade_date and df is not None and not df.empty:
        try:
            detected_trade_date = df.index[-1].strftime("%Y-%m-%d")
        except Exception:
            pass

    signals = {}
    for ticker, code in ticker_to_code.items():
        try:
            if len(tickers) == 1:
                sub = df
            else:
                if ticker not in df.columns.get_level_values(0):
                    continue
                sub = df[ticker]

            if sub is None or sub.empty:
                continue

            close = sub["Close"].dropna()

            # 結合官方最新收盤價（解決 yfinance 當日歷史日K尚未寫入或為 NaN 的問題）
            if official_trade_date and official_closes and code in official_closes:
                today_close = official_closes[code]
                if len(close) > 0:
                    last_date_str = str(close.index[-1])[:10]
                    if last_date_str < official_trade_date:
                        # yfinance 尚未包含當日數據，補上今日官方收盤價
                        close = pd.concat([close, pd.Series([today_close], index=[pd.Timestamp(official_trade_date)])])
                    elif last_date_str == official_trade_date:
                        close.iloc[-1] = today_close
                else:
                    close = pd.Series([today_close], index=[pd.Timestamp(official_trade_date)])

            if len(close) < 20:
                continue

            ma20 = float(close.iloc[-20:].mean())
            latest_close = float(close.iloc[-1])

            signals[code] = {
                "above_ma20": latest_close > ma20,
                "close": round(latest_close, 2),
                "ma20": round(ma20, 2)
            }
        except Exception:
            continue

    print(f"MA20 signals computed for {len(signals)} stocks, latest trade date: {detected_trade_date}")
    return signals, detected_trade_date


def aggregate_sectors(industry_map, signals, stock_names):
    """
    聚合族群多空比例：
    1. 熱門題材次族群（category="theme"），如 CCL、封測、散熱、PCB、被動元件等
    2. 官方產業大類（category="official"），如 半導體業、電子零組件業 等
    """
    def evaluate_group(name, stocks, category="official"):
        above_list = []
        below_list = []
        no_data_list = []

        for s in stocks:
            code = s["code"]
            sig = signals.get(code)
            entry = {
                "code": code,
                "name": s.get("name", stock_names.get(code, code)),
                "market": s.get("market", "twse"),
            }
            if sig is None:
                entry["above_ma20"] = None
                entry["close"] = None
                entry["ma20"] = None
                no_data_list.append(entry)
            else:
                entry["above_ma20"] = sig["above_ma20"]
                entry["close"] = sig["close"]
                entry["ma20"] = sig["ma20"]
                if sig["above_ma20"]:
                    above_list.append(entry)
                else:
                    below_list.append(entry)

        valid_count = len(above_list) + len(below_list)
        above_count = len(above_list)
        total_count = len(stocks)

        if valid_count < MIN_SECTOR_STOCKS:
            signal = "insufficient"
            ratio = None
        else:
            ratio = round(above_count / valid_count, 4)
            signal = "多方" if ratio >= BULLISH_THRESHOLD else "空方"

        all_stocks = (
            sorted(above_list, key=lambda x: x["close"] or 0, reverse=True) +
            sorted(below_list, key=lambda x: x["close"] or 0, reverse=True) +
            no_data_list
        )

        return {
            "industry": name,
            "category": category,
            "signal": signal,
            "ratio": ratio,
            "above_count": above_count,
            "valid_count": valid_count,
            "total": total_count,
            "stocks": all_stocks
        }

    sectors = []

    # 1. 計算題材次族群 (Theme Sub-sectors)
    for theme_name, codes in THEME_SECTORS.items():
        theme_stocks = []
        for c in codes:
            info = industry_map.get(c, {})
            theme_stocks.append({
                "code": c,
                "name": info.get("name", stock_names.get(c, c)),
                "market": info.get("market", "twse")
            })
        item = evaluate_group(theme_name, theme_stocks, category="theme")
        sectors.append(item)

    # 2. 計算官方產業大類 (Official Sectors)
    sector_stocks = {}
    for code, info in industry_map.items():
        industry = info["industry"]
        if industry not in sector_stocks:
            sector_stocks[industry] = []
        sector_stocks[industry].append({
            "code": code,
            "name": info["name"],
            "market": info["market"]
        })

    for industry, stocks in sector_stocks.items():
        item = evaluate_group(industry, stocks, category="official")
        sectors.append(item)

    # 排序：優先題材族群（多方在前，按 ratio 降序），接續官方大類（多方在前，按 ratio 降序）
    def sort_key(s):
        cat_order = 0 if s.get("category") == "theme" else 1
        sig_order = 0 if s["signal"] == "多方" else (1 if s["signal"] == "空方" else 2)
        return (cat_order, sig_order, -(s["ratio"] or 0))

    sectors.sort(key=sort_key)
    return sectors


def get_trade_date():
    """Fallback: 從 volume_spike.json 讀最新交易日，或回傳台灣今日"""
    vs_path = os.path.join(REPO_DIR, "volume_spike.json")
    if os.path.exists(vs_path):
        try:
            with open(vs_path, "r", encoding="utf-8") as f:
                vs = json.load(f)
            td = vs.get("tradeDate", "")
            if td:
                return td
        except Exception:
            pass
    return datetime.datetime.now(TZ_TW).strftime("%Y-%m-%d")


def main():
    print("=== Sector Strength Scanner ===")
    stock_names = load_stock_names()
    print(f"Loaded {len(stock_names)} stock names")

    # Step 1: 建立產業對照表
    industry_map = build_industry_map(stock_names)
    if not industry_map:
        print("ERROR: No industry map data, aborting.")
        sys.exit(1)

    # Step 2: 抓取 TWSE/TPEx 當日官方最終收盤價，確保最新交易日不落後
    official_closes, official_trade_date = fetch_official_today_closes()

    # Step 3: 批次計算 MA20 信號
    signals, detected_trade_date = compute_ma20_signals(industry_map, official_closes, official_trade_date)
    if not signals:
        print("ERROR: No MA20 signals computed, aborting.")
        sys.exit(1)

    # Step 3: 按族群聚合 (題材次族群 + 官方產業大類)
    sectors = aggregate_sectors(industry_map, signals, stock_names)

    # Step 4: 統計
    bullish = [s for s in sectors if s["signal"] == "多方"]
    bearish = [s for s in sectors if s["signal"] == "空方"]
    insufficient = [s for s in sectors if s["signal"] == "insufficient"]

    # 交易日優先採用從歷史行情中實際提取之最新交易日（如 2026-09-18）
    trade_date = detected_trade_date or get_trade_date()
    now_iso = datetime.datetime.now(TZ_TW).isoformat()

    out_data = {
        "updateTime": now_iso,
        "tradeDate": trade_date,
        "threshold": BULLISH_THRESHOLD,
        "minSectorStocks": MIN_SECTOR_STOCKS,
        "totalSectors": len(sectors),
        "bullishSectors": len(bullish),
        "bearishSectors": len(bearish),
        "insufficientSectors": len(insufficient),
        "totalStocksScanned": len(signals),
        "totalStocksAboveMA20": sum(1 for v in signals.values() if v["above_ma20"]),
        "sectors": sectors
    }

    out_path = os.path.join(REPO_DIR, "sector_strength.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out_data, f, ensure_ascii=False, indent=2)

    print(f"\n=== Results ===")
    print(f"Trade date     : {trade_date}")
    print(f"Total sectors  : {len(sectors)}  (多方={len(bullish)}, 空方={len(bearish)}, 不足={len(insufficient)})")
    print(f"Stocks scanned : {len(signals)}  (站上月線={out_data['totalStocksAboveMA20']})")
    if bullish:
        print(f"\nTop 5 強勢族群:")
        for s in bullish[:5]:
            print(f"  [{s['industry']}] {s['ratio']:.0%} ({s['above_count']}/{s['valid_count']})")
    if bearish:
        print(f"\nBottom 5 弱勢族群:")
        for s in list(reversed(bearish))[:5]:
            print(f"  [{s['industry']}] {s['ratio']:.0%} ({s['above_count']}/{s['valid_count']})")
    print(f"\nSaved to {out_path}")


if __name__ == "__main__":
    main()

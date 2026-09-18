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
import time

TZ_TW = ZoneInfo("Asia/Taipei")

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

REPO_DIR = os.path.dirname(os.path.abspath(__file__))

BULLISH_THRESHOLD = 0.5  # 多方門檻：>= 50% 個股站上月線
MIN_SECTOR_STOCKS = 3    # 最小有效樣本：< 3 支則標記為 insufficient

# TWSE 官方產業別代碼 → 中文名稱對照表
# 來源：TWSE 產業別編碼（t187ap03_L 的 產業別 欄位為數字代碼）
TWSE_INDUSTRY_CODE_MAP = {
    "01": "水泥工業",
    "02": "食品工業",
    "03": "塑膠工業",
    "04": "紡織纖維",
    "05": "電機機械",
    "06": "電器電纜",
    "07": "化學工業",
    "08": "生技醫療",
    "09": "玻璃陶瓷",
    "10": "造紙工業",
    "11": "鋼鐵工業",
    "12": "橡膠工業",
    "13": "汽車工業",
    "14": "半導體業",
    "15": "電腦及週邊設備業",
    "16": "光電業",
    "17": "通信網路業",
    "18": "電子零組件業",
    "19": "電子通路業",
    "20": "資訊服務業",
    "21": "其他電子業",
    "22": "建材營造業",
    "23": "航運業",
    "24": "觀光餐旅業",
    "25": "金融保險業",
    "26": "貿易百貨業",
    "27": "油電燃氣業",
    "28": "綠能環保",
    "29": "油電燃氣業",
    "30": "其他業",
    "31": "文化創意業",
    "32": "農業科技業",
    "33": "電子商務業",
    "34": "觀光及百貨業",
    "35": "運動休閒業",
    "36": "居家生活業",
    "37": "數位雲端業",
    "38": "航運業",
    "39": "其他業",
    # TPEx 常見代碼
    "91": "其他業（上櫃）",
    "92": "電子業（上櫃）",
    "93": "化工業（上櫃）",
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


def fetch_tpex_industry_map(stock_names):
    """
    抓取 TPEx 上櫃公司 <-> 產業別對照表
    來源：ISIN 公開資訊頁（strMode=4 = 上櫃），解析 HTML table
    表格結構：產業別標題列 -> 各股 "代號　名稱" 資料列
    """
    print("Fetching TPEx industry map from ISIN page (strMode=4)...")
    url = "https://isin.twse.com.tw/isin/C_public.jsp?strMode=4"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "text/html,application/xhtml+xml",
    }
    result = {}
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=25) as resp:
            raw = resp.read()
        html = raw.decode("big5", errors="replace")

        rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.DOTALL | re.IGNORECASE)
        current_industry = ""

        for row in rows:
            cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.DOTALL | re.IGNORECASE)
            cells = [re.sub(r"<[^>]+>", "", c).strip() for c in cells]
            cells = [c for c in cells if c]

            if not cells:
                continue

            # 產業別標題列：只有 1 個 cell，含漢字，不含純數字開頭
            if len(cells) <= 2 and re.search(r"[\u4e00-\u9fff]", cells[0]) and not cells[0][:4].isdigit():
                candidate = cells[0]
                if len(candidate) <= 20:
                    current_industry = candidate
                continue

            # 資料列：第一個 cell 為 "XXXX　公司名" 格式
            first = cells[0]
            parts = re.split(r"\u3000|\s{2,}", first, maxsplit=1)
            if len(parts) == 2:
                code = parts[0].strip()
                name = parts[1].strip()
            elif len(first) >= 4 and first[:4].isdigit():
                code = first[:4]
                name = first[4:].strip()
            else:
                continue

            if len(code) == 4 and code.isdigit() and current_industry:
                display_name = stock_names.get(code, name)
                result[code] = {
                    "name": display_name,
                    "industry": current_industry,
                    "market": "tpex"
                }

        print(f"TPEx: fetched {len(result)} stocks with industry classification")
        return result
    except Exception as e:
        print(f"TPEx ISIN fetch error: {e}")
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


def compute_ma20_signals(industry_map):
    """
    批次下載所有股票 2 個月歷史，計算 MA20
    回傳 {code: {above_ma20: bool, close: float, ma20: float}}
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
        return {}

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
        return {}
    print(f"Download completed in {time.time() - t0:.1f}s")

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

    print(f"MA20 signals computed for {len(signals)} stocks")
    return signals


def aggregate_sectors(industry_map, signals):
    """依族群聚合，計算多空比例"""
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

    sectors = []
    for industry, stocks in sector_stocks.items():
        above_list = []
        below_list = []
        no_data_list = []

        for s in stocks:
            code = s["code"]
            sig = signals.get(code)
            entry = {
                "code": code,
                "name": s["name"],
                "market": s["market"],
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

        # 族群內排序：站上月線 -> 未站上 -> 無資料；各組依收盤降序
        all_stocks = (
            sorted(above_list, key=lambda x: x["close"] or 0, reverse=True) +
            sorted(below_list, key=lambda x: x["close"] or 0, reverse=True) +
            no_data_list
        )

        sectors.append({
            "industry": industry,
            "signal": signal,
            "ratio": ratio,
            "above_count": above_count,
            "valid_count": valid_count,
            "total": total_count,
            "stocks": all_stocks
        })

    # 排序：多方（ratio 降序）-> 空方（ratio 降序）-> insufficient
    def sort_key(s):
        if s["signal"] == "多方":
            return (0, -(s["ratio"] or 0))
        elif s["signal"] == "空方":
            return (1, -(s["ratio"] or 0))
        else:
            return (2, 0)

    sectors.sort(key=sort_key)
    return sectors


def get_trade_date():
    """從 volume_spike.json 讀最新交易日，fallback 為台灣今日"""
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

    # Step 2: 批次計算 MA20 信號
    signals = compute_ma20_signals(industry_map)
    if not signals:
        print("ERROR: No MA20 signals computed, aborting.")
        sys.exit(1)

    # Step 3: 按族群聚合
    sectors = aggregate_sectors(industry_map, signals)

    # Step 4: 統計
    bullish = [s for s in sectors if s["signal"] == "多方"]
    bearish = [s for s in sectors if s["signal"] == "空方"]
    insufficient = [s for s in sectors if s["signal"] == "insufficient"]

    trade_date = get_trade_date()
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

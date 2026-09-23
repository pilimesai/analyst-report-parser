"""
update_sector_capital_flow.py
全市場「資金族群流向」每日自動運算引擎

功能：
1. 抓取 TWSE (MI_INDEX) 與 TPEx (stk_wn1430) 最近 2 個交易日（T0 今日 vs T1 昨日）全市場行情。
2. 計算上市櫃官方產業別（30+類）與熱門題材次族群（20+類）之：
   - 族群成交金額（億元）
   - 全市場資金佔比（%）
   - 資金流向增減變化（今日佔比 - 昨日佔比，百分點 pp）
   - 成交額成長率（%）
   - 族群平均漲跌幅（%）
   - 漲/平/跌家數比
   - 主力量價訊號（吸金領漲、爆量滯跌、量縮抗跌、資金流出）
   - 族群前 3 大吸金指標股
3. 輸出：sector_capital_flow.json
"""

import os
import sys
import json
import time
import datetime
from zoneinfo import ZoneInfo
import urllib.request
import subprocess
import shutil

TZ_TW = ZoneInfo("Asia/Taipei")

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

REPO_DIR = os.path.dirname(os.path.abspath(__file__))

# TWSE 官方產業代碼對照表
TWSE_INDUSTRY_CODE_MAP = {
    "01": "水泥工業", "02": "食品工業", "03": "塑膠工業", "04": "紡織纖維",
    "05": "電機機械", "06": "電器電纜", "08": "玻璃陶瓷", "09": "造紙工業",
    "10": "鋼鐵工業", "11": "橡膠工業", "12": "汽車工業", "14": "建材營造業",
    "15": "航運業", "16": "觀光餐旅業", "17": "金融保險業", "18": "貿易百貨業",
    "20": "其他業", "21": "化學工業", "22": "生技醫療業", "23": "油電燃氣業",
    "24": "半導體業", "25": "電腦及週邊設備業", "26": "光電業", "27": "通信網路業",
    "28": "電子零組件業", "29": "電子通路業", "30": "資訊服務業", "31": "其他電子業",
    "32": "文化創意業", "33": "農業科技業", "35": "綠能環保", "36": "數位雲端業",
    "37": "運動休閒業", "38": "居家生活業", "91": "存託憑證",
}

TPEX_INDUSTRY_CODE_MAP = {
    "02": "食品工業", "03": "塑膠工業", "04": "紡織纖維", "05": "電機機械", "06": "電器電纜",
    "10": "鋼鐵工業", "14": "建材營造業", "15": "航運業", "16": "觀光餐旅業", "17": "金融保險業",
    "20": "其他業", "21": "化學工業", "22": "生技醫療業", "23": "油電燃氣業", "24": "半導體業",
    "25": "電腦及週邊設備業", "26": "光電業", "27": "通信網路業", "28": "電子零組件業", "29": "電子通路業",
    "30": "資訊服務業", "31": "其他電子業", "32": "文化創意業", "33": "農業科技業", "35": "綠能環保",
    "36": "數位雲端業", "37": "運動休閒業", "38": "居家生活業"
}

# 市場主流核心題材次族群（經營業項目嚴格核對）
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


def load_stock_names():
    sn_path = os.path.join(REPO_DIR, "stock_names.json")
    if os.path.exists(sn_path):
        try:
            with open(sn_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def build_industry_map(stock_names):
    """取得 TWSE 與 TPEx 全市場官方產業別分類"""
    ind_map = {}

    # TWSE
    url_twse = "https://openapi.twse.com.tw/v1/opendata/t187ap03_L"
    try:
        req = urllib.request.Request(url_twse, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        for item in data:
            code = str(item.get("公司代號", "")).strip()
            raw_ind = str(item.get("產業別", "")).strip()
            if len(code) == 4 and code.isdigit():
                ind_name = TWSE_INDUSTRY_CODE_MAP.get(raw_ind, raw_ind)
                ind_map[code] = ind_name
    except Exception as e:
        print(f"Warning: TWSE industry fetch failed: {e}")

    # TPEx
    url_tpex = "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O"
    try:
        curl_bin = shutil.which("curl") or shutil.which("curl.exe") or "curl"
        raw_text = None
        try:
            res = subprocess.run([curl_bin, "-s", "--http1.1", url_tpex, "-H", "User-Agent: Mozilla/5.0"], capture_output=True, timeout=20)
            if res.returncode == 0 and res.stdout:
                raw_text = res.stdout.decode("utf-8", errors="ignore")
        except Exception:
            pass

        if not raw_text:
            req = urllib.request.Request(url_tpex, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=15) as resp:
                raw_text = resp.read().decode("utf-8", errors="ignore")

        if raw_text:
            data = json.loads(raw_text)
            for item in data:
                code = str(item.get("SecuritiesCompanyCode", "")).strip()
                raw_ind = str(item.get("SecuritiesIndustryCode", "")).strip()
                if len(code) == 4 and code.isdigit():
                    ind_name = TPEX_INDUSTRY_CODE_MAP.get(raw_ind, "其他業")
                    ind_map[code] = ind_name
    except Exception as e:
        print(f"Warning: TPEx industry fetch failed: {e}")

    print(f"Loaded industry mapping for {len(ind_map)} stocks")
    return ind_map


def fetch_twse_quotes_two_days(stock_names):
    """抓取 TWSE 最近 2 個交易日的成交行情"""
    today = datetime.datetime.now(TZ_TW).date()
    found_days = []

    for delta in range(10):
        d = today - datetime.timedelta(days=delta)
        if d.weekday() >= 5:
            continue  # 略過週末
        d_str = d.strftime("%Y%m%d")
        url = f"https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date={d_str}&type=ALLBUT0999&response=json"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=20) as resp:
                jd = json.loads(resp.read().decode("utf-8", errors="ignore"))
            if jd.get("stat") == "OK":
                tbl = [t for t in jd.get("tables", []) if len(t.get("data", [])) > 500]
                if tbl:
                    day_dict = {}
                    tot_val = 0.0
                    for row in tbl[0]["data"]:
                        code = str(row[0]).strip()
                        if len(code) == 4 and code.isdigit():
                            try:
                                val = float(str(row[4]).replace(",", "").strip())
                                cp = float(str(row[8]).replace(",", "").strip())
                                chg_raw = str(row[10]).replace(",", "").strip()
                                chg = float(chg_raw) if chg_raw not in ["--", "", "0.00"] else 0.0
                                sign = str(row[9]).strip()
                                if "-" in sign:
                                    chg = -chg
                                name = stock_names.get(code, str(row[1]).strip())
                                day_dict[code] = {
                                    "market": "twse",
                                    "name": name,
                                    "trade_val": val,
                                    "close": cp,
                                    "change": round(chg, 2)
                                }
                                tot_val += val
                            except ValueError:
                                pass
                    fmt_date = f"{d.year}-{d.month:02d}-{d.day:02d}"
                    found_days.append({"date": fmt_date, "stocks": day_dict, "total_val": tot_val})
                    print(f"TWSE fetched {len(day_dict)} stocks for {fmt_date} (Total: {tot_val/1e8:.1f}億)")
                    if len(found_days) == 2:
                        break
        except Exception as ex:
            print(f"TWSE MI_INDEX {d_str} error: {ex}")

    return found_days


def fetch_tpex_quotes_two_days(stock_names):
    """抓取 TPEx 最近 2 個交易日的成交行情"""
    today = datetime.datetime.now(TZ_TW).date()
    found_days = []
    curl_bin = shutil.which("curl") or shutil.which("curl.exe") or "curl"

    for delta in range(10):
        d = today - datetime.timedelta(days=delta)
        if d.weekday() >= 5:
            continue  # 略過週末
        roc = f"{d.year - 1911}/{d.month:02d}/{d.day:02d}"
        url = f"https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php?l=zh-tw&d={roc}&se=AL&_=1"
        raw_text = None
        try:
            res = subprocess.run(
                [curl_bin, "-s", "--http1.1", url, "-H", "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64)"],
                capture_output=True, timeout=35
            )
            if res.returncode == 0 and res.stdout:
                raw_text = res.stdout.decode("utf-8", errors="ignore")
        except Exception as ex:
            print(f"TPEx curl error for {d}: {ex}")

        if not raw_text:
            try:
                headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)", "Accept": "application/json, */*"}
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    raw_text = resp.read().decode("utf-8", errors="ignore")
            except Exception as e:
                print(f"TPEx urllib error for {d}: {e}")

        if raw_text:
            try:
                jd = json.loads(raw_text)
                tbl = jd.get("tables", [])
                if tbl and tbl[0].get("data") and len(tbl[0]["data"]) > 500:
                    day_dict = {}
                    tot_val = 0.0
                    for row in tbl[0]["data"]:
                        code = str(row[0]).strip()
                        if len(code) == 4 and code.isdigit():
                            try:
                                val = float(str(row[8]).replace(",", "").strip())
                                cp = float(str(row[2]).replace(",", "").strip())
                                chg_raw = str(row[3]).replace(",", "").strip()
                                chg = float(chg_raw) if chg_raw not in ["--", "", "0.00"] else 0.0
                                name = stock_names.get(code, str(row[1]).strip())
                                day_dict[code] = {
                                    "market": "tpex",
                                    "name": name,
                                    "trade_val": val,
                                    "close": cp,
                                    "change": round(chg, 2)
                                }
                                tot_val += val
                            except ValueError:
                                pass
                    fmt_date = f"{d.year}-{d.month:02d}-{d.day:02d}"
                    found_days.append({"date": fmt_date, "stocks": day_dict, "total_val": tot_val})
                    print(f"TPEx fetched {len(day_dict)} stocks for {fmt_date} (Total: {tot_val/1e8:.1f}億)")
                    if len(found_days) == 2:
                        break
            except Exception as ex:
                print(f"TPEx stk_wn1430 parse error for {d}: {ex}")

    return found_days


def calculate_sector_flow(sector_name, sector_type, stock_codes, quotes_t0, quotes_t1, tot_mkt_t0, tot_mkt_t1):
    """
    計算單一族群在 T0 vs T1 的各項資金指標
    """
    t0_val = 0.0
    t1_val = 0.0
    advances = 0
    declines = 0
    unchanged = 0
    pct_changes = []
    active_stocks = []

    for code in stock_codes:
        s0 = quotes_t0.get(code)
        s1 = quotes_t1.get(code)

        if s0:
            val0 = s0["trade_val"]
            t0_val += val0
            cp0 = s0["close"]
            chg0 = s0["change"]
            prev_close = cp0 - chg0
            pct = (chg0 / prev_close * 100.0) if prev_close > 0 else 0.0
            pct_changes.append(pct)

            if chg0 > 0:
                advances += 1
            elif chg0 < 0:
                declines += 1
            else:
                unchanged += 1

            active_stocks.append({
                "code": code,
                "name": s0["name"],
                "market": s0["market"],
                "close": cp0,
                "change": chg0,
                "pct_change": round(pct, 2),
                "trade_val_yi": round(val0 / 1e8, 2)
            })

        if s1:
            t1_val += s1["trade_val"]

    # 資金佔比與流向增減
    share0 = (t0_val / tot_mkt_t0 * 100.0) if tot_mkt_t0 > 0 else 0.0
    share1 = (t1_val / tot_mkt_t1 * 100.0) if tot_mkt_t1 > 0 else 0.0
    flow_pp = share0 - share1  # 佔比百分點變動

    # 成交金額增幅
    growth_pct = ((t0_val - t1_val) / t1_val * 100.0) if t1_val > 0 else 0.0
    avg_pct_chg = (sum(pct_changes) / len(pct_changes)) if pct_changes else 0.0

    # 量價訊號判定
    # 🔥 主力吸金領漲 (flow >= +0.3% & avg_pct > 0)
    # ⚠️ 爆量滯跌出貨 (flow >= +0.3% & avg_pct <= 0)
    # ❄️ 資金流出回檔 (flow <= -0.3% & avg_pct < 0)
    # 💎 量縮抗跌整理 (flow <= -0.3% & avg_pct >= 0)
    # ⚖️ 資金平穩
    if flow_pp >= 0.3:
        if avg_pct_chg > 0:
            signal = "🔥 主力吸金領漲"
            signal_type = "bullish_inflow"
        else:
            signal = "⚠️ 爆量滯跌出貨"
            signal_type = "bearish_heavy"
    elif flow_pp <= -0.3:
        if avg_pct_chg < 0:
            signal = "❄️ 資金流出回檔"
            signal_type = "bearish_outflow"
        else:
            signal = "💎 量縮抗跌整理"
            signal_type = "bullish_resist"
    else:
        signal = "⚖️ 資金平穩"
        signal_type = "neutral"

    # 按成交金額由高到低排序，取前 3 大主力指標股
    active_stocks.sort(key=lambda s: s["trade_val_yi"], reverse=True)
    top3_stocks = active_stocks[:3]

    # 計算前三大指標股佔該族群成交金額的集中度
    top3_val = sum(s["trade_val_yi"] for s in top3_stocks)
    sector_yi = round(t0_val / 1e8, 2)
    top3_concentration = round((top3_val / sector_yi * 100.0), 1) if sector_yi > 0 else 0.0

    return {
        "sector_name": sector_name,
        "sector_type": sector_type,  # "theme" or "official"
        "stock_count": len(stock_codes),
        "active_count": len(active_stocks),
        "trade_val_yi": sector_yi,
        "prev_trade_val_yi": round(t1_val / 1e8, 2),
        "val_diff_yi": round((t0_val - t1_val) / 1e8, 2),
        "growth_pct": round(growth_pct, 2),
        "share0_pct": round(share0, 2),
        "share1_pct": round(share1, 2),
        "flow_pp": round(flow_pp, 2),
        "avg_pct_chg": round(avg_pct_chg, 2),
        "advances": advances,
        "declines": declines,
        "unchanged": unchanged,
        "signal": signal,
        "signal_type": signal_type,
        "top3_concentration": top3_concentration,
        "top_stocks": top3_stocks
    }


def main():
    print("=== Taiwan Stock Sector Capital Flow Scanner ===")
    t_start = time.time()

    stock_names = load_stock_names()
    ind_map = build_industry_map(stock_names)

    twse_days = fetch_twse_quotes_two_days(stock_names)
    tpex_days = fetch_tpex_quotes_two_days(stock_names)

    if len(twse_days) < 2 or len(tpex_days) < 2:
        print("Error: Could not obtain 2 complete trading days from TWSE/TPEx")
        return

    # 確保 TWSE 與 TPEx 採用完全相同的交易日
    twse_by_date = {d["date"]: d for d in twse_days}
    tpex_by_date = {d["date"]: d for d in tpex_days}
    common_dates = [d["date"] for d in twse_days if d["date"] in tpex_by_date]

    if len(common_dates) >= 2:
        t0_date = common_dates[0]
        t1_date = common_dates[1]
        t0_twse = twse_by_date[t0_date]
        t0_tpex = tpex_by_date[t0_date]
        t1_twse = twse_by_date[t1_date]
        t1_tpex = tpex_by_date[t1_date]
    else:
        print(f"Warning: Exact date alignment fallback: TWSE dates={[d['date'] for d in twse_days]}, TPEx dates={[d['date'] for d in tpex_days]}")
        t0_date = twse_days[0]["date"]
        t1_date = twse_days[1]["date"]
        t0_twse = twse_days[0]
        t0_tpex = tpex_days[0]
        t1_twse = twse_days[1]
        t1_tpex = tpex_days[1]

    quotes_t0 = {**t0_twse["stocks"], **t0_tpex["stocks"]}
    quotes_t1 = {**t1_twse["stocks"], **t1_tpex["stocks"]}

    tot_mkt_t0 = t0_twse["total_val"] + t0_tpex["total_val"]
    tot_mkt_t1 = t1_twse["total_val"] + t1_tpex["total_val"]

    print(f"\n--- Market Summary ---")
    print(f"T0 (Today): {t0_date} | Total Turnover: {tot_mkt_t0/1e8:.1f}億 ({len(quotes_t0)} stocks)")
    print(f"T1 (Yest) : {t1_date} | Total Turnover: {tot_mkt_t1/1e8:.1f}億 ({len(quotes_t1)} stocks)")
    mkt_diff_yi = round((tot_mkt_t0 - tot_mkt_t1) / 1e8, 1)
    mkt_growth_pct = round(((tot_mkt_t0 - tot_mkt_t1) / tot_mkt_t1 * 100.0), 2) if tot_mkt_t1 > 0 else 0.0

    # 1. 計算官方產業別 (Official Industries)
    official_stocks_by_ind = {}
    for code, ind in ind_map.items():
        if ind not in official_stocks_by_ind:
            official_stocks_by_ind[ind] = []
        official_stocks_by_ind[ind].append(code)

    official_results = []
    for ind, codes in official_stocks_by_ind.items():
        res = calculate_sector_flow(ind, "official", codes, quotes_t0, quotes_t1, tot_mkt_t0, tot_mkt_t1)
        if res["trade_val_yi"] > 0:
            official_results.append(res)

    # 2. 計算熱門題材次族群 (Theme Subsectors)
    theme_results = []
    for theme, codes in THEME_SECTORS.items():
        res = calculate_sector_flow(theme, "theme", codes, quotes_t0, quotes_t1, tot_mkt_t0, tot_mkt_t1)
        if res["trade_val_yi"] > 0:
            theme_results.append(res)

    # 依資金流向 (flow_pp) 排序
    official_results.sort(key=lambda s: s["flow_pp"], reverse=True)
    theme_results.sort(key=lambda s: s["flow_pp"], reverse=True)

    # 全族群合併清單（亦可按 flow_pp 或 成交金額排序）
    all_sectors = sorted(theme_results + official_results, key=lambda s: s["flow_pp"], reverse=True)

    # 摘要指標
    top_inflow_theme = [s for s in theme_results if s["flow_pp"] > 0][:3]
    top_outflow_theme = [s for s in sorted(theme_results, key=lambda s: s["flow_pp"]) if s["flow_pp"] < 0][:3]
    top_inflow_official = [s for s in official_results if s["flow_pp"] > 0][:3]
    top_outflow_official = [s for s in sorted(official_results, key=lambda s: s["flow_pp"]) if s["flow_pp"] < 0][:3]

    output_data = {
        "updateTime": datetime.datetime.now(TZ_TW).isoformat(),
        "tradeDate": t0_date,
        "prevTradeDate": t1_date,
        "market": {
            "t0_total_yi": round(tot_mkt_t0 / 1e8, 2),
            "t1_total_yi": round(tot_mkt_t1 / 1e8, 2),
            "diff_yi": mkt_diff_yi,
            "growth_pct": mkt_growth_pct,
            "stock_count_t0": len(quotes_t0),
            "stock_count_t1": len(quotes_t1)
        },
        "summary": {
            "top_inflow_theme": top_inflow_theme,
            "top_outflow_theme": top_outflow_theme,
            "top_inflow_official": top_inflow_official,
            "top_outflow_official": top_outflow_official,
            "most_bullish_sector": sorted(all_sectors, key=lambda s: s["avg_pct_chg"], reverse=True)[0] if all_sectors else None
        },
        "themes": theme_results,
        "official": official_results,
        "all_sectors": all_sectors
    }

    out_file = os.path.join(REPO_DIR, "sector_capital_flow.json")
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)

    elapsed = time.time() - t_start
    print(f"\n=== Completed in {elapsed:.1f}s ===")
    print(f"Total Official Sectors: {len(official_results)}")
    print(f"Total Theme Sectors   : {len(theme_results)}")
    print(f"Top Theme Inflow      : {[s['sector_name'] + ' (+' + str(s['flow_pp']) + '%)' for s in top_inflow_theme]}")
    print(f"Top Theme Outflow     : {[s['sector_name'] + ' (' + str(s['flow_pp']) + '%)' for s in top_outflow_theme]}")
    print(f"Saved to: {out_file}")

    if '--push' in sys.argv:
        try:
            subprocess.run(['git', 'add', 'sector_capital_flow.json'], cwd=REPO_DIR, check=True)
            subprocess.run(['git', 'commit', '-m', f'auto: update sector capital flow ({t0_date})'], cwd=REPO_DIR, check=True)
            subprocess.run(['git', 'push', 'origin', 'main'], cwd=REPO_DIR, check=True)
            print('Successfully pushed sector_capital_flow.json to GitHub!')
        except Exception as e:
            print(f'Git push error: {e}')


if __name__ == "__main__":
    main()

"""
update_contract_liabilities.py
每日自動篩選台股「合約負債強勢爆發」個股

篩選條件（嚴格依據 XQ 5 大規則）：
  1. [日] 股本(億)小於 20 億 (< 2,000,000,000 元)
  2. [季] 合約負債大於近 4 季平均 (Q0 > Avg(Q0, Q1, Q2, Q3))
  3. [季] 合約負債創 4 季新高 (Q0 > Max(Q1, Q2, Q3))
  4. [季] 近 1 季合約負債平均成長大於 50 % (季增率 QoQ >= 50% 或 相對4季均值增幅 >= 50%)
  5. [日] 成交金額(元)大於 0.5 億元 (>= 50,000,000 元)

輸出檔案：
  contract_liabilities.json
"""

import os
import sys
import json
import time
import datetime
from zoneinfo import ZoneInfo
import urllib.request
import subprocess
from concurrent.futures import ThreadPoolExecutor

TZ_TW = ZoneInfo("Asia/Taipei")

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

REPO_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_FILE = os.path.join(REPO_DIR, "contract_liabilities_cache.json")


def load_stock_names():
    sn_path = os.path.join(REPO_DIR, "stock_names.json")
    if os.path.exists(sn_path):
        try:
            with open(sn_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"Warning: load stock_names.json failed: {e}")
    return {}


def load_stock_capital():
    sc_path = os.path.join(REPO_DIR, "stock_capital.json")
    if os.path.exists(sc_path):
        try:
            with open(sc_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"Warning: load stock_capital.json failed: {e}")
    return {}


def load_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_cache(cache):
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Warning: save cache failed: {e}")


def fetch_twse_daily_turnover(stock_names):
    """
    抓取 TWSE 全市場當日成交行情 (MI_INDEX)
    """
    print("Fetching TWSE daily quotes (MI_INDEX)...")
    today = datetime.datetime.now(TZ_TW).date()
    turnovers = {}
    trade_date = None

    for delta in range(5):
        d = today - datetime.timedelta(days=delta)
        d_str = d.strftime("%Y%m%d")
        url = f"https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date={d_str}&type=ALLBUT0999&response=json"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Accept": "application/json"
        }
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=12) as resp:
                jd = json.loads(resp.read().decode("utf-8", errors="ignore"))
            if jd.get("stat") == "OK":
                tables = [tbl for tbl in jd.get("tables", []) if len(tbl.get("data", [])) > 500]
                if tables:
                    for r in tables[0]["data"]:
                        code = str(r[0]).strip()
                        if len(code) == 4 and code.isdigit():
                            val_str = str(r[4]).replace(",", "").strip()
                            cp_str = str(r[8]).replace(",", "").strip()
                            chg_sign = -1 if "-" in str(r[9]) else (1 if "+" in str(r[9]) else 0)
                            chg_val = str(r[10]).replace(",", "").strip()
                            try:
                                val = float(val_str)
                                cp = float(cp_str)
                                chg = float(chg_val) * chg_sign if chg_sign != 0 else 0.0
                                turnovers[code] = {
                                    "market": "twse",
                                    "code": code,
                                    "name": stock_names.get(code, str(r[1]).strip()),
                                    "trade_val": val,
                                    "close": cp,
                                    "change": round(chg, 2)
                                }
                            except ValueError:
                                pass
                    trade_date = d.strftime("%Y-%m-%d")
                    print(f"TWSE turnover fetched: {len(turnovers)} stocks ({trade_date})")
                    break
        except Exception:
            pass

    return turnovers, trade_date


def fetch_tpex_daily_turnover(stock_names):
    """
    抓取 TPEx 全市場當日成交行情 (stk_wn1430)
    """
    print("Fetching TPEx daily quotes (stk_wn1430)...")
    today = datetime.datetime.now(TZ_TW).date()
    turnovers = {}
    trade_date = None

    for delta in range(5):
        d = today - datetime.timedelta(days=delta)
        roc = f"{d.year - 1911}/{d.month:02d}/{d.day:02d}"
        url = f"https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php?l=zh-tw&d={roc.replace('/', '%2F')}&se=AL&_=1"
        try:
            res = subprocess.run(
                ["curl.exe", "-s", "--http1.1", url, "-H", "User-Agent: Mozilla/5.0"],
                capture_output=True,
                timeout=15
            )
            jd = json.loads(res.stdout.decode("utf-8", errors="ignore"))
            tables = jd.get("tables", [])
            if tables and tables[0].get("data"):
                for r in tables[0]["data"]:
                    code = str(r[0]).strip()
                    if len(code) == 4 and code.isdigit():
                        val_str = str(r[8]).replace(",", "").strip()
                        cp_str = str(r[2]).replace(",", "").strip()
                        chg_str = str(r[3]).replace(",", "").strip()
                        try:
                            val = float(val_str)
                            cp = float(cp_str)
                            chg = float(chg_str) if chg_str else 0.0
                            turnovers[code] = {
                                "market": "tpex",
                                "code": code,
                                "name": stock_names.get(code, str(r[1]).strip()),
                                "trade_val": val,
                                "close": cp,
                                "change": round(chg, 2)
                            }
                        except ValueError:
                            pass
                trade_date = d.strftime("%Y-%m-%d")
                print(f"TPEx turnover fetched: {len(turnovers)} stocks ({trade_date})")
                break
        except Exception:
            pass

    return turnovers, trade_date


def fetch_stock_contract_liabilities(code, cache):
    """
    從 FinMind 取得該個股近 6 季合約負債 (CurrentContractLiabilities)
    若快取中已有近期資料則優先沿用，減少網路請求
    """
    cached = cache.get(code)
    if cached and isinstance(cached, list) and len(cached) >= 4:
        return cached

    token = os.environ.get("FINMIND_API_TOKEN", "").strip()
    token_param = f"&token={token}" if token else ""
    url = f"https://api.finmindtrade.com/api/v4/data?dataset=TaiwanStockBalanceSheet&data_id={code}&start_date=2024-01-01{token_param}"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=10) as resp:
            d = json.loads(resp.read().decode("utf-8"))
        records = [r for r in d.get("data", []) if r.get("type") == "CurrentContractLiabilities"]
        if not records:
            return None
        records.sort(key=lambda x: x["date"])
        # 保留最近 6 季
        saved = [{"date": r["date"], "value": float(r["value"])} for r in records[-6:]]
        cache[code] = saved
        return saved
    except Exception:
        return None


def main():
    print("=== Taiwan Stock Contract Liabilities Scanner ===")
    t0 = time.time()

    stock_names = load_stock_names()
    stock_capital = load_stock_capital()
    cache = load_cache()
    print(f"Loaded {len(stock_names)} stock names, {len(stock_capital)} stock capitals, {len(cache)} cached reports")

    # Step 1: 取得全市場當日成交行情
    twse_to, twse_date = fetch_twse_daily_turnover(stock_names)
    tpex_to, tpex_date = fetch_tpex_daily_turnover(stock_names)
    all_turnover = {**twse_to, **tpex_to}
    trade_date = twse_date or tpex_date or datetime.datetime.now(TZ_TW).strftime("%Y-%m-%d")
    print(f"Total stocks with market turnover: {len(all_turnover)} (Trade Date: {trade_date})")

    # Step 2: 依條件 1 & 條件 5 初篩
    # 條件 1: 股本小於 20 億 (< 2,000,000,000)
    # 條件 5: 成交金額大於 0.5 億 (>= 50,000,000)
    candidates = []
    for code, info in all_turnover.items():
        cap = stock_capital.get(code, 0)
        if 0 < cap < 2000000000 and info["trade_val"] >= 50000000:
            candidates.append((code, cap, info))

    print(f"Candidates meeting Condition 1 (Capital < 20億) & Condition 5 (Turnover >= 0.5億): {len(candidates)}")

    # Step 3: 多線程查詢合約負債並判定條件 2, 3, 4
    matched_results = []

    def evaluate_candidate(item):
        code, cap, info = item
        history = fetch_stock_contract_liabilities(code, cache)
        if not history or len(history) < 4:
            return None

        # 取最近 4 季
        recent_4 = history[-4:]
        q_vals = [r["value"] for r in recent_4]
        q_dates = [r["date"] for r in recent_4]

        q0 = q_vals[-1]  # 最新一季合約負債
        q1 = q_vals[-2]  # 前一季
        avg4 = sum(q_vals) / 4.0
        max_prev3 = max(q_vals[:-1])

        # 條件 2: 合約負債大於近 4 季平均
        cond2 = q0 > avg4

        # 條件 3: 合約負債創 4 季新高
        cond3 = q0 > max_prev3

        # 條件 4: 近 1 季合約負債平均成長大於 50%
        # (依 XQ 規範：最新季相對於前季成長 >= 50%，或最新季相對 4 季平均成長 >= 50%)
        qoq_growth = (q0 - q1) / q1 if q1 > 0 else 0
        avg_growth = (q0 - avg4) / avg4 if avg4 > 0 else 0
        cond4 = (qoq_growth >= 0.5) or (avg_growth >= 0.5)

        if cond2 and cond3 and cond4:
            return {
                "code": code,
                "name": info["name"],
                "market": info["market"],
                "close": info["close"],
                "change": info["change"],
                "capital_yi": round(cap / 1e8, 2),
                "trade_val_yi": round(info["trade_val"] / 1e8, 2),
                "latest_report_date": q_dates[-1],
                "latest_contract_liab_yi": round(q0 / 1e8, 2),
                "prev_contract_liab_yi": round(q1 / 1e8, 2),
                "avg4_contract_liab_yi": round(avg4 / 1e8, 2),
                "qoq_growth_pct": round(qoq_growth * 100, 1),
                "avg_growth_pct": round(avg_growth * 100, 1),
                "history_4q": [
                    {"date": q_dates[i], "value_yi": round(q_vals[i] / 1e8, 2)}
                    for i in range(4)
                ]
            }
        return None

    with ThreadPoolExecutor(max_workers=10) as executor:
        for res in executor.map(evaluate_candidate, candidates):
            if res:
                matched_results.append(res)

    # 儲存快取
    save_cache(cache)

    # 排序：優先按當日成交金額由大至小排序
    matched_results.sort(key=lambda x: x["trade_val_yi"], reverse=True)

    # 計算統計指標
    avg_qoq = (
        sum(r["qoq_growth_pct"] for r in matched_results) / len(matched_results)
        if matched_results
        else 0
    )
    max_liab_stock = (
        max(matched_results, key=lambda x: x["latest_contract_liab_yi"])
        if matched_results
        else None
    )

    out_data = {
        "updateTime": datetime.datetime.now(TZ_TW).isoformat(),
        "tradeDate": trade_date,
        "totalScanned": len(candidates),
        "totalMatched": len(matched_results),
        "criteria": [
            "[日] 股本(億)小於 20 億",
            "[季] 合約負債大於近 4 季平均",
            "[季] 合約負債創 4 季新高",
            "[季] 近 1 季合約負債平均成長大於 50 %",
            "[日] 成交金額(元)大於 0.5 億元"
        ],
        "summary": {
            "avg_qoq_growth_pct": round(avg_qoq, 1),
            "max_contract_liab_code": max_liab_stock["code"] if max_liab_stock else "",
            "max_contract_liab_name": max_liab_stock["name"] if max_liab_stock else "",
            "max_contract_liab_yi": max_liab_stock["latest_contract_liab_yi"] if max_liab_stock else 0
        },
        "stocks": matched_results
    }

    out_path = os.path.join(REPO_DIR, "contract_liabilities.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out_data, f, ensure_ascii=False, indent=2)

    elapsed = time.time() - t0
    print(f"\n=== Completed in {elapsed:.1f}s ===")
    print(f"Trade Date: {trade_date}")
    print(f"Matched Stocks: {len(matched_results)} / {len(candidates)} candidates")
    print(f"Saved to: {out_path}")


if __name__ == "__main__":
    main()

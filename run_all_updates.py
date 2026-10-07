"""
run_all_updates.py
一鍵排程/手動執行所有台股盤後資料更新腳本，並輸出狀態與交易日摘要報告。
支援參數：
  --push : 執行完成後自動 commit & push 至 GitHub
"""
import os
import sys
import time
import json
import subprocess
import datetime
from zoneinfo import ZoneInfo

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

TZ_TW = ZoneInfo('Asia/Taipei')
REPO_DIR = os.path.dirname(os.path.abspath(__file__))

SCRIPTS = [
    {
        "name": "今日成交值排行",
        "file": "update_top_turnover.py",
        "json": "top_turnover.json",
        "desc": "TWSE + TPEx 成交值前30名"
    },
    {
        "name": "股票股本資訊",
        "file": "update_stock_capital.py",
        "json": "stock_capital.json",
        "desc": "上市櫃股本資料庫"
    },
    {
        "name": "族群強弱指標 (MA20)",
        "file": "update_sector_strength.py",
        "json": "sector_strength.json",
        "desc": "全產業族群站上月線多空比例"
    },
    {
        "name": "資金族群流向",
        "file": "update_sector_capital_flow.py",
        "json": "sector_capital_flow.json",
        "desc": "20大題材與官方族群資金佔比消長"
    },
    {
        "name": "合約負債突破",
        "file": "update_contract_liabilities.py",
        "json": "contract_liabilities.json",
        "desc": "XQ合約負債新高突破標的"
    },
    {
        "name": "成交量異常放大",
        "file": "update_volume_spike.py",
        "json": "volume_spike.json",
        "desc": "量大於10日均量3倍爆量紅K標的"
    },
    {
        "name": "大股東股票質設",
        "file": "update_stock_pledge.py",
        "json": "stock_pledge.json",
        "desc": "董監質設成本線與破質設監控"
    },
    {
        "name": "主動型 ETF 篩選",
        "file": "update_active_etf.py",
        "json": "active_etf_holdings.json",
        "desc": "Goal Star 主動型 ETF 持股"
    },
    {
        "name": "兩週內近期法說會",
        "file": "update_earnings_call.py",
        "json": "近期法說會.csv",
        "desc": "公開資訊觀測站法說會名單"
    },
    {
        "name": "可轉換公司債 (CB)",
        "file": "update_cb.py",
        "json": "近期發行CB.csv",
        "desc": "近期發行與低於轉換價 CB 名單"
    },
    {
        "name": "量化嚴選標的",
        "file": "update_quant.py",
        "json": "quant_results.json",
        "desc": "多維度量化評分與嚴選股票清單"
    },
    {
        "name": "認購權證買盤排行",
        "file": "update_call_warrants.py",
        "json": "call_warrants.json",
        "desc": "全市場認購權證成交金額彙總至個股排行"
    }
]

def get_json_info(json_name):
    path = os.path.join(REPO_DIR, json_name)
    if not os.path.exists(path):
        return "N/A", "不存在"
    try:
        if json_name.endswith('.csv'):
            with open(path, "r", encoding="utf-8-sig") as f:
                lines = [l for l in f if l.strip()]
            return f"{max(0, len(lines)-1)} 筆", "CSV"
        with open(path, "r", encoding="utf-8") as f:
            d = json.load(f)
        trade_date = d.get("tradeDate") or d.get("date") or "N/A"
        update_time = d.get("updateTime") or "N/A"
        return trade_date, update_time
    except Exception:
        return "解析失敗", "N/A"

from concurrent.futures import ThreadPoolExecutor, as_completed

def run_single_script(item, full_scan=False):
    s_name = item["name"]
    s_file = item["file"]
    script_path = os.path.join(REPO_DIR, s_file)

    if not os.path.exists(script_path):
        print(f"⚠️ 找不到腳本: {script_path}")
        return item, (s_name, s_file, "找不到檔案", 0, "N/A")

    cmd = [sys.executable, s_file]
    if s_file == "update_stock_pledge.py" and full_scan:
        cmd.append("--full-scan")

    t0 = time.time()
    try:
        proc = subprocess.run(
            cmd,
            cwd=REPO_DIR,
            capture_output=True,
            encoding='utf-8',
            errors='replace'
        )
        elapsed = round(time.time() - t0, 1)
        if proc.returncode == 0:
            t_date, _ = get_json_info(item["json"])
            print(f"✅ [{s_name}] 完成 ({elapsed}s, 交易日: {t_date})")
            return item, (s_name, s_file, "✅ 成功", elapsed, t_date)
        else:
            print(f"❌ [{s_name}] 失敗 (代碼 {proc.returncode}, {elapsed}s)")
            if proc.stderr:
                print(f"   錯誤訊息: {proc.stderr[-300:]}")
            return item, (s_name, s_file, f"❌ 失敗 (代碼 {proc.returncode})", elapsed, "N/A")
    except Exception as e:
        elapsed = round(time.time() - t0, 1)
        print(f"❌ [{s_name}] 發生例外: {e}")
        return item, (s_name, s_file, f"❌ 例外 ({e})", elapsed, "N/A")

def main():
    full_scan = ('--full-scan' in sys.argv or '--full' in sys.argv)
    mode_text = "【含全市場質押深度巡檢】" if full_scan else ""
    print("=" * 70)
    print(f"🚀 台股盤後資料庫一鍵整合更新 ({datetime.datetime.now(TZ_TW).strftime('%Y-%m-%d %H:%M:%S')}){mode_text}")
    print("=" * 70)

    total_start = time.time()
    results_map = {}

    print(f"⚡ 啟動並行加速引擎 (10 Workers 全並行) 同時爬取 10 大模組...")
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(run_single_script, item, full_scan): item for item in SCRIPTS}
        for future in as_completed(futures):
            item, res = future.result()
            results_map[item["file"]] = res

    results = [results_map.get(item["file"], (item["name"], item["file"], "未執行", 0, "N/A")) for item in SCRIPTS]
    total_elapsed = round(time.time() - total_start, 1)

    print("\n" + "=" * 70)
    print(f"📊 執行摘要報告 (總耗時: {total_elapsed} 秒)")
    print("=" * 70)
    print(f"{'任務名稱':<22} {'狀態':<12} {'耗時':<8} {'產出交易日':<12} {'產出檔案'}")
    print("-" * 70)
    for res, item in zip(results, SCRIPTS):
        name, file, status, elapsed, t_date = res
        print(f"{name:<22} {status:<10} {str(elapsed)+'s':<8} {t_date:<12} {item['json']}")
    print("=" * 70)

    if "--push" in sys.argv:
        print("\n📤 準備推送所有產出資料至 GitHub...")
        try:
            files_to_add = [item["json"] for item in SCRIPTS if os.path.exists(os.path.join(REPO_DIR, item["json"]))]
            for extra in ["目前股價低於CB轉換價.csv", "pledge_events_cache.json", "pledge_candidates_cache.json"]:
                if os.path.exists(os.path.join(REPO_DIR, extra)) and extra not in files_to_add:
                    files_to_add.append(extra)
            subprocess.run(["git", "add"] + files_to_add, cwd=REPO_DIR, check=True)
            status_out = subprocess.check_output(["git", "status", "--porcelain"], cwd=REPO_DIR).decode("utf-8")
            if any(f in status_out for f in files_to_add):
                today_str = datetime.datetime.now(TZ_TW).strftime('%Y-%m-%d')
                subprocess.run(["git", "commit", "-m", f"chore: auto-update all market data ({today_str})"], cwd=REPO_DIR, check=True)
                subprocess.run(["git", "push", "origin", "main"], cwd=REPO_DIR, check=True)
                print("🎉 所有最新資料已成功推送至 GitHub！")
            else:
                print("ℹ️ 所有 JSON 均無實質變更，無需推送。")
        except Exception as e:
            print(f"❌ Git push 失敗: {e}")

if __name__ == "__main__":
    main()

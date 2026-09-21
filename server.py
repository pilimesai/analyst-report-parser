"""
server.py
台股量化分析平台 - 本地即時後端伺服器
提供靜態網頁託管與即時爬蟲 API 橋樑。
當前端網頁點擊「一鍵抓取」或「重新載入」時，由本伺服器現場執行 Python 爬蟲並回傳最新盤後資料。
"""
import os
import sys

if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

import json
import time
import subprocess
import webbrowser
from http.server import HTTPServer, SimpleHTTPRequestHandler
import datetime
from zoneinfo import ZoneInfo

TZ_TW = ZoneInfo('Asia/Taipei')
PORT = 8000
REPO_DIR = os.path.dirname(os.path.abspath(__file__))

TASK_SCRIPT_MAP = {
    "top_turnover": {
        "script": "update_top_turnover.py",
        "json": "top_turnover.json",
        "name": "今日成交值排行"
    },
    "sector_strength": {
        "script": "update_sector_strength.py",
        "json": "sector_strength.json",
        "name": "族群強弱指標"
    },
    "sector_capital_flow": {
        "script": "update_sector_capital_flow.py",
        "json": "sector_capital_flow.json",
        "name": "資金族群流向"
    },
    "contract_liabilities": {
        "script": "update_contract_liabilities.py",
        "json": "contract_liabilities.json",
        "name": "合約負債突破"
    },
    "volume_spike": {
        "script": "update_volume_spike.py",
        "json": "volume_spike.json",
        "name": "成交量異常名單"
    },
    "stock_pledge": {
        "script": "update_stock_pledge.py",
        "json": "stock_pledge.json",
        "name": "大股東股票質設"
    },
    "stock_capital": {
        "script": "update_stock_capital.py",
        "json": "stock_capital.json",
        "name": "上市櫃股本資料"
    },
    "active_etf": {
        "script": "update_active_etf.py",
        "json": "active_etf_holdings.json",
        "name": "主動型 ETF 篩選"
    }
}

class LiveAnalysisHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=REPO_DIR, **kwargs)

    def end_headers(self):
        # 允許跨網域與避免快取
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type, *')
        self.send_header('Cache-Control', 'no-cache, no-store, must-revalidate')
        super().end_headers()

    def do_OPTIONS(self):
        self.send_response(200)
        self.end_headers()

    def do_GET(self):
        if self.path == '/api/status':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.end_headers()

            files_status = {}
            for task_id, meta in TASK_SCRIPT_MAP.items():
                p = os.path.join(REPO_DIR, meta["json"])
                trade_date = "未生成"
                update_time = "未生成"
                if os.path.exists(p):
                    try:
                        with open(p, 'r', encoding='utf-8') as f:
                            jd = json.load(f)
                        trade_date = jd.get("tradeDate") or jd.get("date") or "未知"
                        update_time = jd.get("updateTime") or "未知"
                    except Exception:
                        pass
                files_status[task_id] = {
                    "name": meta["name"],
                    "json": meta["json"],
                    "tradeDate": trade_date,
                    "updateTime": update_time
                }

            resp = {
                "status": "online",
                "mode": "live_crawler",
                "currentTime": datetime.datetime.now(TZ_TW).strftime('%Y-%m-%d %H:%M:%S'),
                "files": files_status
            }
            self.wfile.write(json.dumps(resp, ensure_ascii=False).encode('utf-8'))
            return

        super().do_GET()

    def do_POST(self):
        if self.path == '/api/run-task':
            content_len = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(content_len) if content_len > 0 else b'{}'
            try:
                payload = json.loads(body.decode('utf-8'))
            except Exception:
                payload = {}

            task = payload.get("task", "")
            print(f"\n[API 觸發] 收到前端即時抓取請求: task = {task}")

            if task == "all":
                # 依序執行所有爬蟲
                results = []
                t_all_start = time.time()
                for task_key in ["top_turnover", "stock_capital", "sector_strength", "sector_capital_flow", "contract_liabilities", "volume_spike", "stock_pledge"]:
                    res = self._execute_task(task_key)
                    results.append(res)
                total_elapsed = round(time.time() - t_all_start, 1)

                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.end_headers()
                resp = {
                    "success": True,
                    "task": "all",
                    "totalElapsed": total_elapsed,
                    "results": results
                }
                self.wfile.write(json.dumps(resp, ensure_ascii=False).encode('utf-8'))
                return

            elif task in TASK_SCRIPT_MAP:
                res = self._execute_task(task)
                self.send_response(200 if res["success"] else 500)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.end_headers()
                self.wfile.write(json.dumps(res, ensure_ascii=False).encode('utf-8'))
                return
            else:
                self.send_response(400)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.end_headers()
                self.wfile.write(json.dumps({"success": False, "error": f"Unknown task: {task}"}).encode('utf-8'))
                return

        self.send_response(404)
        self.end_headers()

    def _execute_task(self, task_key):
        meta = TASK_SCRIPT_MAP.get(task_key)
        if not meta:
            return {"task": task_key, "success": False, "error": "Not found"}

        script = meta["script"]
        script_path = os.path.join(REPO_DIR, script)
        json_path = os.path.join(REPO_DIR, meta["json"])

        print(f"  ▶ 開始現場執行: {meta['name']} ({script})...")
        t0 = time.time()
        try:
            proc = subprocess.run(
                [sys.executable, script],
                cwd=REPO_DIR,
                capture_output=True,
                text=True,
                encoding='utf-8',
                errors='ignore'
            )
            elapsed = round(time.time() - t0, 1)
            if proc.returncode == 0:
                print(f"  ✅ 現場爬取成功: {meta['name']} (耗時 {elapsed}s)")
                # 讀取產出的 JSON
                json_data = None
                trade_date = None
                if os.path.exists(json_path):
                    try:
                        with open(json_path, 'r', encoding='utf-8') as f:
                            json_data = json.load(f)
                        trade_date = json_data.get("tradeDate") or json_data.get("date")
                    except Exception:
                        pass
                return {
                    "task": task_key,
                    "name": meta["name"],
                    "success": True,
                    "elapsed": elapsed,
                    "tradeDate": trade_date,
                    "data": json_data
                }
            else:
                print(f"  ❌ 爬取失敗: {meta['name']} (代碼 {proc.returncode}):\n{proc.stderr[:300]}")
                return {
                    "task": task_key,
                    "name": meta["name"],
                    "success": False,
                    "elapsed": elapsed,
                    "error": proc.stderr[:300] or proc.stdout[:300]
                }
        except Exception as ex:
            elapsed = round(time.time() - t0, 1)
            print(f"  ❌ 爬取例外: {meta['name']}: {ex}")
            return {
                "task": task_key,
                "name": meta["name"],
                "success": False,
                "elapsed": elapsed,
                "error": str(ex)
            }


def get_lan_ip():
    try:
        import socket
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return '127.0.0.1'

def main():
    server_address = ('0.0.0.0', PORT)
    try:
        httpd = HTTPServer(server_address, LiveAnalysisHandler)
    except OSError:
        print(f"⚠️ 埠號 {PORT} 已被佔用，嘗試切換至 8080...")
        server_address = ('0.0.0.0', 8080)
        httpd = HTTPServer(server_address, LiveAnalysisHandler)

    active_port = server_address[1]
    local_url = f"http://127.0.0.1:{active_port}/index.html"
    lan_ip = get_lan_ip()
    mobile_url = f"http://{lan_ip}:{active_port}/index.html"

    print("=" * 70)
    print("🚀 台股量化研報分析平台 - 本地即時爬蟲伺服器 (Live Server)")
    print("=" * 70)
    print(f"💻 電腦瀏覽位址: {local_url}")
    print(f"📱 手機瀏覽位址: {mobile_url} (連同個 Wi-Fi 即可用手機現場抓取！)")
    print("⚡ 模式: 【現場即時爬取】")
    print("   當您在網頁點擊「一鍵抓取所有資料」或各分頁「重新載入」時，")
    print("   本伺服器將直接在現場調用 Python 爬蟲，實時抓取當天最新資料！")
    print("=" * 70)
    print("💡 正在為您自動打開瀏覽器...")
    print("   (如需結束伺服器，請在此視窗按下 Ctrl + C)")
    print("=" * 70)

    try:
        webbrowser.open(url)
    except Exception:
        pass

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n🛑 伺服器已停止。")
        httpd.server_close()

if __name__ == '__main__':
    main()

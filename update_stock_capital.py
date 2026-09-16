"""
update_stock_capital.py
自動抓取全台股（上市 TWSE + 上櫃 TPEx）最新實收資本額
資料來源：
  - 上市：TWSE OpenAPI (t187ap03_L 上市公司基本資料)
  - 上櫃：TPEx OpenAPI (mopsfin_t187ap03_O 上櫃公司基本資料)
輸出：
  - stock_capital.json -> 供前端網頁秒查各股股本，篩選股本<20億
"""
import sys
import os
import json
import urllib.request

if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

REPO_DIR = os.path.dirname(os.path.abspath(__file__))
OUT_PATH = os.path.join(REPO_DIR, 'stock_capital.json')

def fetch_capitals():
    capitals = {}

    # 1. 上市公司基本資料
    print("Fetching TWSE capitals...")
    try:
        url = 'https://openapi.twse.com.tw/v1/opendata/t187ap03_L'
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode('utf-8'))
            for item in data:
                code = str(item.get('公司代號', '')).strip()
                cap = str(item.get('實收資本額', '')).strip().replace(',', '')
                if code and cap:
                    try:
                        capitals[code] = float(cap)
                    except ValueError:
                        pass
        print(f"TWSE loaded: {len(capitals)} records")
    except Exception as e:
        print(f"TWSE capital fetch failed: {e}")

    # 2. 上櫃公司基本資料
    print("Fetching TPEx capitals...")
    try:
        url = 'https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O'
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode('utf-8'))
            tpex_count = 0
            for item in data:
                code = str(item.get('SecuritiesCompanyCode', '')).strip()
                cap = str(item.get('Paidin.Capital.NTDollars', '')).strip().replace(',', '')
                if code and cap:
                    try:
                        capitals[code] = float(cap)
                        tpex_count += 1
                    except ValueError:
                        pass
        print(f"TPEx loaded: {tpex_count} records")
    except Exception as e:
        print(f"TPEx capital fetch failed: {e}")

    if capitals:
        with open(OUT_PATH, 'w', encoding='utf-8') as f:
            json.dump(capitals, f, ensure_ascii=False, indent=2)
        print(f"✅ Successfully updated {OUT_PATH} with {len(capitals)} stocks!")
    else:
        print("⚠️ No capital data fetched, keeping existing file.")

if __name__ == '__main__':
    fetch_capitals()

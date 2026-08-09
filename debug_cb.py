import requests
from bs4 import BeautifulSoup
import sys
import warnings
warnings.filterwarnings('ignore')
sys.stdout.reconfigure(encoding='utf-8')

# 神秘金字塔 - 董監持股
url = "https://norway.twsthr.info/StockBoardTop.aspx"
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

print("=== 董監持股排行 ===")
resp = requests.get(url, headers=headers, timeout=15, verify=False)
resp.encoding = 'utf-8'
print(f"Status: {resp.status_code}, Length: {len(resp.text)}")

soup = BeautifulSoup(resp.text, 'html.parser')
tables = soup.find_all('table')
print(f"Tables: {len(tables)}")

for i, t in enumerate(tables):
    text = t.get_text()[:150]
    if any(k in text for k in ['董監', '持股', '增加', '減少', '變動', '股票']):
        print(f"\n=== Table {i} ===")
        rows = t.find_all('tr')
        for j, r in enumerate(rows[:12]):
            cells = [c.get_text(strip=True) for c in r.find_all(['td', 'th'])]
            print(f"  Row {j}: {cells[:12]}")

# 也試個股頁面
print("\n\n=== 個股董監持股 (6725) ===")
url2 = "https://norway.twsthr.info/StockBoardTop.aspx?stock=6725"
resp2 = requests.get(url2, headers=headers, timeout=15, verify=False)
resp2.encoding = 'utf-8'
print(f"Status: {resp2.status_code}, Length: {len(resp2.text)}")

soup2 = BeautifulSoup(resp2.text, 'html.parser')
tables2 = soup2.find_all('table')
print(f"Tables: {len(tables2)}")

for i, t in enumerate(tables2):
    rows = t.find_all('tr')
    if len(rows) > 2:
        print(f"\n--- Table {i} ({len(rows)} rows) ---")
        for j, r in enumerate(rows[:8]):
            cells = [c.get_text(strip=True) for c in r.find_all(['td', 'th'])]
            if cells and any(c for c in cells if c):
                print(f"  Row {j}: {cells[:15]}")

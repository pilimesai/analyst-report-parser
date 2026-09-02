"""
update_active_etf.py
自動抓取 Goal Star 主動型 ETF 資料 (https://goal-star.com/summary/fund-weights)
排除 00984A，彙整各標的之持有 ETF 檔數與權重，產出 active_etf_holdings.json
"""
import urllib.request
import json
import os
import sys
import datetime
import subprocess

if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

def fetch_goal_star_data():
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json',
        'Referer': 'https://goal-star.com/summary/fund-weights'
    }
    
    print("📡 正在連線 Goal Star 取得主動型 ETF 清單...")
    req = urllib.request.Request('https://goal-star.com/api/funds', headers=headers)
    with urllib.request.urlopen(req, timeout=15) as resp:
        funds_data = json.loads(resp.read().decode('utf-8'))
        
    active_funds = funds_data.get('active', {}).get('items', [])
    # 嚴格排除 00984A, 00983A, 00989A
    EXCLUDED_ETFS = {'00984A', '00983A', '00989A'}
    filtered_funds = [f for f in active_funds if f.get('symbol', '').upper() not in EXCLUDED_ETFS]
    print(f"✅ 取得 {len(filtered_funds)} 檔主動型 ETF（已嚴格排除 00984A、00983A、00989A）")
    
    stock_map = {}
    for idx, fund in enumerate(filtered_funds, 1):
        sym = fund.get('symbol')
        name = fund.get('name', '')
        print(f"[{idx}/{len(filtered_funds)}] 抓取持股明細：{sym} {name}...")
        try:
            fund_url = f'https://goal-star.com/api/funds/{sym}/shares'
            f_req = urllib.request.Request(fund_url, headers=headers)
            with urllib.request.urlopen(f_req, timeout=15) as f_resp:
                shares_data = json.loads(f_resp.read().decode('utf-8'))
                items = shares_data.get('items', [])
                for item in items:
                    sid = str(item.get('stock_symbol', '')).strip()
                    sname = str(item.get('stock_name', '')).strip()
                    industry = str(item.get('industry', '一般產業')).strip()
                    ratio = float(item.get('ratio', 0) or 0)
                    
                    if not sid:
                        continue
                    if sid not in stock_map:
                        stock_map[sid] = {
                            'symbol': sid,
                            'name': sname,
                            'industry': industry,
                            'funds': [],
                            'totalRatio': 0.0,
                            'maxRatio': 0.0
                        }
                    
                    stock_map[sid]['funds'].append({
                        'fundSymbol': sym,
                        'fundName': name,
                        'ratio': round(ratio, 2)
                    })
                    stock_map[sid]['totalRatio'] += ratio
                    if ratio > stock_map[sid]['maxRatio']:
                        stock_map[sid]['maxRatio'] = ratio
        except Exception as e:
            print(f"⚠️ 抓取 {sym} 失敗: {e}")
            
    # 格式化與排序
    stocks_list = list(stock_map.values())
    for s in stocks_list:
        s['funds'].sort(key=lambda f: f['ratio'], reverse=True)
        s['totalRatio'] = round(s['totalRatio'], 2)
        s['maxRatio'] = round(s['maxRatio'], 2)
        
    # 最少 ETF 持有數優先，次依最高單一權重排序
    stocks_list.sort(key=lambda s: (len(s['funds']), -s['maxRatio']))
    
    output_payload = {
        'updateTime': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'totalFunds': len(filtered_funds),
        'funds': filtered_funds,
        'stocks': stocks_list
    }
    
    json_path = os.path.join(os.path.dirname(__file__), 'active_etf_holdings.json')
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(output_payload, f, ensure_ascii=False, indent=2)
        
    print(f"🎉 成功輸出 {len(stocks_list)} 檔持股至 {json_path}")
    print(f"⭐ 僅 1 檔獨家持有: {len([s for s in stocks_list if len(s['funds']) == 1])} 檔")
    print(f"   2 檔共同持有: {len([s for s in stocks_list if len(s['funds']) == 2])} 檔")
    return output_payload

if __name__ == '__main__':
    fetch_goal_star_data()

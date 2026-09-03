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
    # 嚴格排除 00984A, 00983A, 00989A, 00988A, 00990A, 00997A, 00986A, 00999A
    EXCLUDED_ETFS = {'00984A', '00983A', '00989A', '00988A', '00990A', '00997A', '00986A', '00999A'}
    filtered_funds = [f for f in active_funds if f.get('symbol', '').upper() not in EXCLUDED_ETFS]
    print(f"✅ 取得 {len(filtered_funds)} 檔主動型 ETF（已嚴格排除 00984A、00983A、00989A、00988A、00990A、00997A、00986A、00999A）")
    
    # 讀取台股中文名稱資料庫
    stock_names_map = {}
    sn_path = os.path.join(os.path.dirname(__file__), 'stock_names.json')
    if os.path.exists(sn_path):
        try:
            with open(sn_path, 'r', encoding='utf-8') as sn_f:
                stock_names_map = json.load(sn_f)
        except Exception as e:
            print(f"⚠️ 讀取 stock_names.json 失敗: {e}")

    # 產業英翻中權威對照表
    INDUSTRY_MAP = {
        'Airlines': '航空業',
        'Apparel Manufacturing': '成衣製造',
        'Auto Parts': '汽車零組件',
        'Banks - Regional': '銀行業',
        'Banks - Diversified': '綜合銀行',
        'Building Materials': '建材營造',
        'Chemicals': '化學工業',
        'Communication Equipment': '通信網路',
        'Computer Hardware': '電腦及週邊設備',
        'Consumer Electronics': '消費性電子',
        'Credit Services': '融資租賃服務',
        'Department Stores': '百貨零售',
        'Drug Manufacturers - Specialty & Generic': '製藥與生技',
        'Electrical Equipment & Parts': '電機機械',
        'Electronic Components': '電子零組件',
        'Electronic Gaming & Multimedia': '遊戲與多媒體',
        'Electronics & Computer Distribution': '電子通路',
        'Engineering & Construction': '營造工程',
        'Financial Conglomerates': '金融控股',
        'Furnishings, Fixtures & Appliances': '居家生活',
        'Grocery Stores': '食品量販 / 超市',
        'Household & Personal Products': '家庭與個人用品',
        'Information Technology Services': '資訊服務',
        'Insurance - Life': '人壽保險',
        'Insurance - Property & Casualty': '產物保險',
        'Insurance - Diversified': '綜合保險',
        'Marine Shipping': '航運業',
        'Metal Fabrication': '金屬加工',
        'Packaged Foods': '食品工業',
        'Packaging & Containers': '包裝材料',
        'Pollution & Treatment Controls': '綠能環保',
        'Real Estate - Development': '建材營造',
        'Real Estate - Services': '不動產服務',
        'Restaurants': '觀光餐飲',
        'Scientific & Technical Instruments': '精密儀器',
        'Semiconductor Equipment & Materials': '半導體設備與材料',
        'Semiconductors': '半導體業',
        'Specialty Chemicals': '特用化學',
        'Specialty Industrial Machinery': '特殊工業機械',
        'Specialty Retail': '特種零售',
        'Steel': '鋼鐵工業',
        'Telecom Services': '電信服務',
        'Textile Manufacturing': '紡織纖維',
        'Tools & Accessories': '工具五金',
        'Biotechnology': '生物科技',
        'Medical Devices': '醫療器材',
        'Medical Instruments & Supplies': '醫療用品',
        'Healthcare Plans': '醫療保健',
        'Software - Application': '應用軟體',
        'Software - Infrastructure': '基礎架構軟體',
        'Internet Content & Information': '網路資訊',
        'Solar': '太陽能',
        'Auto Manufacturers': '汽車製造',
        'Aerospace & Defense': '航太國防',
        'Asset Management': '資產管理',
        'Capital Markets': '資本市場',
        'Oil & Gas Integrated': '石油與天然氣',
        'Oil & Gas Refining & Marketing': '油品煉製',
        'Utilities - Regulated Electric': '公用事業',
        'Utilities - Renewable': '再生能源'
    }

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
                    # 優先採用中文標準公司名稱
                    sname = stock_names_map.get(sid) or sname
                    raw_ind = str(item.get('industry', '一般產業')).strip()
                    industry = INDUSTRY_MAP.get(raw_ind, raw_ind)
                    ratio = float(item.get('ratio', 0) or 0)
                    
                    if not sid:
                        continue
                    # 權重要超過 1% 才留下 (排除 <= 1.0% 之零星碎股)
                    if ratio <= 1.0:
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

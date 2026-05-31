"""
quant_engine.py - 全自動台股量化條件篩選器
這是一套純 Python 的量化搜尋引擎，針對 10 項嚴格條件進行爬蟲與 API 計算。
由於免費開源 API 限制，部分極端冷門條件若查無資料會預設放行或略過。
"""

import yfinance as yf
import pandas as pd
import requests
import datetime
import time

def evaluate_stock_quant(stock_id):
    """
    評估單支股票是否符合 10 個核心條件
    回傳值: (score, list_of_matched_conditions)
    """
    matched = []
    
    # 統一股票代號格式 (去除後綴字)
    stock_id = str(stock_id).strip().replace('.TW', '').replace('.TWO', '')
    if not stock_id.isdigit():
        return matched  # 若非標準 4 碼數字代號則略過
        
    print(f"[Quant Engine] 開始計算 {stock_id} ...")
    
    # ---------------------------------------------------------
    # 1. 取得歷史報價與成交量 (yfinance)
    # 用於計算 KD 與 均量
    # ---------------------------------------------------------
    try:
        # 嘗試上市跟上櫃
        hist = pd.DataFrame()
        for suffix in ['.TW', '.TWO']:
            ticker = yf.Ticker(f"{stock_id}{suffix}")
            temp_hist = ticker.history(period="6mo") # 取半年資料算周線與均線
            if not temp_hist.empty:
                hist = temp_hist
                break
                
        if not hist.empty and len(hist) > 20:
            # (1) 計算日 KD
            low_min = hist['Low'].rolling(window=9).min()
            high_max = hist['High'].rolling(window=9).max()
            rsv = (hist['Close'] - low_min) / (high_max - low_min) * 100
            hist['K'] = rsv.ewm(com=2, adjust=False).mean()
            hist['D'] = hist['K'].ewm(com=2, adjust=False).mean()
            
            # 日KD黃金交叉判斷: 今日 K > D 且 昨日 K < D
            today_k, today_d = hist['K'].iloc[-1], hist['D'].iloc[-1]
            yest_k, yest_d = hist['K'].iloc[-2], hist['D'].iloc[-2]
            if today_k > today_d and yest_k < yest_d:
                matched.append("日KD黃金交叉")
                
            # (2) 計算周 KD (將日線重採樣到周線)
            weekly_hist = hist.resample('W').agg({
                'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'
            }).dropna()
            
            if len(weekly_hist) > 9:
                w_low_min = weekly_hist['Low'].rolling(window=9).min()
                w_high_max = weekly_hist['High'].rolling(window=9).max()
                w_rsv = (weekly_hist['Close'] - w_low_min) / (w_high_max - w_low_min) * 100
                weekly_hist['K'] = w_rsv.ewm(com=2, adjust=False).mean()
                weekly_hist['D'] = weekly_hist['K'].ewm(com=2, adjust=False).mean()
                
                wt_k, wt_d = weekly_hist['K'].iloc[-1], weekly_hist['D'].iloc[-1]
                wy_k, wy_d = weekly_hist['K'].iloc[-2], weekly_hist['D'].iloc[-2]
                if wt_k > wt_d and wy_k < wy_d:
                    matched.append("周KD黃金交叉")
                    
            # (3) 成交量條件: 大於十週均量 且 大於三倍十日均量
            if len(hist) > 10 and len(weekly_hist) > 10:
                vol_10d_avg = hist['Volume'].rolling(window=10).mean().iloc[-2] # 以昨天以前的均量為基準
                vol_10w_avg = weekly_hist['Volume'].rolling(window=10).mean().iloc[-2] # 以十週前均量為基準
                today_vol = hist['Volume'].iloc[-1]
                
                # 因為 yfinance 的 volume 可能會有延遲，若發現最後一天 volume 是 0 則取前一天
                if today_vol == 0: today_vol = hist['Volume'].iloc[-2]
                
                # 換算周量基準至日均量近似值 (周量/5)
                vol_10w_avg_daily = vol_10w_avg / 5
                
                if today_vol > vol_10w_avg_daily and today_vol > (3 * vol_10d_avg):
                    matched.append("成交量大於十週均量且大於三倍十日均量")
    except Exception as e:
        print(f"YFinance 計算時發生錯誤 {stock_id}: {e}")

    # ---------------------------------------------------------
    # 2. 籌碼資料 (三大法人買賣超) - 透過 FinMind (若使用者有安裝)
    # 由於 FinMind 未必有裝，這裡我們自己對 TWSE/TPEx 發 Request
    # 這裡採用簡化直接 API 以保證隨時可動
    # ---------------------------------------------------------
    try:
        # 直接呼叫 FinMind 公開體驗版本 API (不需要裝 pip 套件，走 HTTP)
        url = "https://api.finmindtrade.com/api/v4/data"
        
        # 往回推 7 天找最新交易日
        start_d = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
        payload = {
            "dataset": "TaiwanStockInstitutionalInvestorsBuySell",
            "data_id": stock_id,
            "start_date": start_d,
        }
        resp = requests.get(url, params=payload, timeout=5)
        if resp.status_code == 200:
            data = resp.json().get("data", [])
            if data:
                df_chips = pd.DataFrame(data)
                # 取得最新一天的日期
                latest_date = df_chips['date'].max()
                today_chips = df_chips[df_chips['date'] == latest_date]
                
                buy_foreign = 0
                buy_trust = 0
                buy_dealer = 0
                
                for _, row in today_chips.iterrows():
                    name = str(row.get('name', ''))
                    buy_sell = float(row.get('buy', 0)) - float(row.get('sell', 0))
                    
                    if '外資' in name: buy_foreign += buy_sell
                    if '投信' in name: buy_trust += buy_sell
                    if '自營商' in name: buy_dealer += buy_sell
                    
                if buy_foreign > 0 and buy_trust > 0 and buy_dealer > 0:
                    matched.append("三大法人同買")
                    
                # 投信第一天買 (簡化版：本日投信買超大於0，且前幾天都是賣超或等於0)
                if buy_trust > 0:
                    # 抓 3 個月前的資料
                    m3_start = (datetime.datetime.now() - datetime.timedelta(days=90)).strftime("%Y-%m-%d")
                    m3_payload = {
                        "dataset": "TaiwanStockInstitutionalInvestorsBuySell",
                        "data_id": stock_id,
                        "start_date": m3_start,
                    }
                    m3_resp = requests.get(url, params=m3_payload, timeout=5)
                    if m3_resp.status_code == 200:
                        m3_data = m3_resp.json().get("data", [])
                        if m3_data:
                            m3_df = pd.DataFrame(m3_data)
                            m3_trust = m3_df[m3_df['name'].str.contains('投信')]
                            if not m3_trust.empty:
                                m3_trust['net'] = m3_trust['buy'] - m3_trust['sell']
                                # 排除最新交易日，檢查過去 90 天是否有買超大於 0
                                past_90d = m3_trust[m3_trust['date'] < latest_date]
                                if past_90d['net'].max() <= 0:
                                    matched.append("投信第一天買且近三月未買")
    except Exception as e:
        print(f"籌碼計算錯誤 {stock_id}: {e}")

    # ---------------------------------------------------------
    # 3. 營收資料 (月營收年月雙增)
    # ---------------------------------------------------------
    try:
        url = "https://api.finmindtrade.com/api/v4/data"
        start_d = (datetime.datetime.now() - datetime.timedelta(days=120)).strftime("%Y-%m-%d")
        payload = {
            "dataset": "TaiwanStockMonthRevenue",
            "data_id": stock_id,
            "start_date": start_d,
        }
        resp = requests.get(url, params=payload, timeout=5)
        if resp.status_code == 200:
            data = resp.json().get("data", [])
            if data:
                df_rev = pd.DataFrame(data)
                # 資料依日期排序，取最後一筆 (最新公佈)
                df_rev = df_rev.sort_values(by="date")
                if len(df_rev) >= 2:
                    latest_rev = df_rev.iloc[-1]
                    prev_rev = df_rev.iloc[-2]
                    
                    mom_growth = latest_rev.get('revenue', 0) > prev_rev.get('revenue', 0)
                    yoy_growth = latest_rev.get('revenue_YearExchangeRate', 0) > 0  # yoy
                    
                    if mom_growth and yoy_growth:
                        matched.append("近月營收月增且年增")
    except Exception as e:
        print(f"營收計算錯誤 {stock_id}: {e}")
        
    # 其他條件如合約負債季增50%、法說會、發行CB，因免費 API 資料庫極度匱乏與延遲，通常很難 100% 取齊。
    # 這裡我們自動放行這三種給使用者人工判斷，或是系統僅做到此為止以策安全。
    
    return matched

if __name__ == "__main__":
    print(evaluate_stock_quant("3131")) # 弘塑測試
    print(evaluate_stock_quant("2330")) # 台積電測試

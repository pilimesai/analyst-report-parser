import streamlit as st
import requests
import datetime

def evaluate_stock_quant(stock_id, tdcc_df=None, tdcc_prev_df=None, conference_stocks=None, cb_stocks=None, cb_issued_data=None):
    """純 Python 量化條件篩選器 - 透過 FinMind API + yfinance + TDCC + 法說會 + CB 即時計算"""
    matched = []
    stock_id = str(stock_id).strip().replace('.TW', '').replace('.TWO', '')
    if not stock_id.isdigit():
        return matched

    # 1. 取得歷史報價 (yfinance) → 計算 KD 與均量
    try:
        hist = pd.DataFrame()
        for suffix in ['.TW', '.TWO']:
            ticker = yf.Ticker(f"{stock_id}{suffix}")
            temp_hist = ticker.history(period="6mo")
            if not temp_hist.empty:
                hist = temp_hist
                break

        if not hist.empty and len(hist) > 20:
            # 日 KD
            low_min = hist['Low'].rolling(window=9).min()
            high_max = hist['High'].rolling(window=9).max()
            rsv = (hist['Close'] - low_min) / (high_max - low_min) * 100
            hist['K'] = rsv.ewm(com=2, adjust=False).mean()
            hist['D'] = hist['K'].ewm(com=2, adjust=False).mean()
            today_k, today_d = hist['K'].iloc[-1], hist['D'].iloc[-1]
            yest_k, yest_d = hist['K'].iloc[-2], hist['D'].iloc[-2]
            if today_k > today_d and yest_k < yest_d:
                matched.append("日KD黃金交叉")

            # 周 KD
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

            # 成交量條件 (需全部滿足)
            if len(hist) > 10 and len(weekly_hist) > 10:
                today_vol = hist['Volume'].iloc[-1]
                today_close = hist['Close'].iloc[-1]
                today_open = hist['Open'].iloc[-1]
                if today_vol == 0:
                    today_vol = hist['Volume'].iloc[-2]
                    today_close = hist['Close'].iloc[-2]
                    today_open = hist['Open'].iloc[-2]
                
                today_vol_lots = today_vol / 1000  # 張 = 股 / 1000
                turnover = today_close * today_vol  # 成交金額(元)
                vol_10d_avg = hist['Volume'].rolling(window=10).mean().iloc[-2]
                vol_10w_avg = weekly_hist['Volume'].rolling(window=10).mean().iloc[-2]
                vol_10w_avg_daily = vol_10w_avg / 5
                
                cond_1 = today_vol_lots > 2000          # 成交量 > 2000 張
                cond_2 = turnover > 5e8                  # 成交金額 > 0.5 億
                cond_3 = today_vol > vol_10w_avg_daily   # 量 > 近10週平均
                cond_4 = today_vol > (3 * vol_10d_avg)   # 量 > 10日均量3倍
                cond_5 = today_close > today_open         # 收盤 > 開盤(紅K)
                
                if cond_1 and cond_2 and cond_3 and cond_4 and cond_5:
                    matched.append(f"爆量紅K({today_vol_lots:.0f}張,{turnover/1e8:.1f}億)")
    except Exception as e:
        print(f"YFinance 計算錯誤 {stock_id}: {e}")

    # 2. 籌碼資料 (FinMind HTTP API)
    try:
        url = "https://api.finmindtrade.com/api/v4/data"
        start_d = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
        payload = {"dataset": "TaiwanStockInstitutionalInvestorsBuySell", "data_id": stock_id, "start_date": start_d}
        resp = requests.get(url, params=payload, timeout=10)
        if resp.status_code == 200:
            data = resp.json().get("data", [])
            if data:
                df_chips = pd.DataFrame(data)
                latest_date = df_chips['date'].max()
                today_chips = df_chips[df_chips['date'] == latest_date]
                buy_foreign, buy_trust, buy_dealer = 0, 0, 0
                for _, row in today_chips.iterrows():
                    name = str(row.get('name', ''))
                    net = float(row.get('buy', 0)) - float(row.get('sell', 0))
                    if '外資' in name: buy_foreign += net
                    if '投信' in name: buy_trust += net
                    if '自營商' in name: buy_dealer += net
                if buy_foreign > 0 and buy_trust > 0 and buy_dealer > 0:
                    matched.append("三大法人同買")
                if buy_trust > 0:
                    m3_start = (datetime.datetime.now() - datetime.timedelta(days=90)).strftime("%Y-%m-%d")
                    m3_payload = {"dataset": "TaiwanStockInstitutionalInvestorsBuySell", "data_id": stock_id, "start_date": m3_start}
                    m3_resp = requests.get(url, params=m3_payload, timeout=10)
                    if m3_resp.status_code == 200:
                        m3_data = m3_resp.json().get("data", [])
                        if m3_data:
                            m3_df = pd.DataFrame(m3_data)
                            m3_trust = m3_df[m3_df['name'].str.contains('投信')]
                            if not m3_trust.empty:
                                m3_trust = m3_trust.copy()
                                m3_trust['net'] = m3_trust['buy'] - m3_trust['sell']
                                past_90d = m3_trust[m3_trust['date'] < latest_date]
                                if not past_90d.empty and past_90d['net'].max() <= 0:
                                    matched.append("投信第一天買且近三月未買")
    except Exception as e:
        print(f"籌碼計算錯誤 {stock_id}: {e}")

    # 3. 營收資料
    try:
        url = "https://api.finmindtrade.com/api/v4/data"
        # 抓 14 個月資料，確保能比較去年同月
        start_d = (datetime.datetime.now() - datetime.timedelta(days=400)).strftime("%Y-%m-%d")
        payload = {"dataset": "TaiwanStockMonthRevenue", "data_id": stock_id, "start_date": start_d}
        resp = requests.get(url, params=payload, timeout=10)
        if resp.status_code == 200:
            data = resp.json().get("data", [])
            if data:
                df_rev = pd.DataFrame(data).sort_values(by="date")
                if len(df_rev) >= 2:
                    latest_rev = df_rev.iloc[-1]
                    prev_rev = df_rev.iloc[-2]
                    latest_revenue = float(latest_rev.get('revenue', 0))
                    prev_revenue = float(prev_rev.get('revenue', 0))
                    
                    # MoM 月增: 最新月營收 > 上個月營收
                    mom_growth = latest_revenue > prev_revenue
                    
                    # YoY 年增: 手動找去年同月份的營收來比較
                    latest_month = int(latest_rev.get('revenue_month', 0))
                    latest_year = int(latest_rev.get('revenue_year', 0))
                    yoy_growth = False
                    
                    # 在歷史資料中尋找去年同月
                    same_month_last_year = df_rev[
                        (df_rev['revenue_month'].astype(int) == latest_month) & 
                        (df_rev['revenue_year'].astype(int) == latest_year - 1)
                    ]
                    if not same_month_last_year.empty:
                        last_year_revenue = float(same_month_last_year.iloc[-1].get('revenue', 0))
                        if last_year_revenue > 0:
                            yoy_growth = latest_revenue > last_year_revenue
                    
                    if mom_growth and yoy_growth:
                        matched.append("近月營收月增且年增")
    except Exception as e:
        print(f"營收計算錯誤 {stock_id}: {e}")

    # 4. 大戶持股比例成長 (TDCC 集保庫存分級)
    try:
        if tdcc_df is not None and not tdcc_df.empty:
            df_filtered = tdcc_df[tdcc_df['證券代號'].astype(str) == stock_id]
            if not df_filtered.empty:
                levels = pd.to_numeric(df_filtered['持股分級'], errors='coerce')
                curr_data = df_filtered[levels >= 12]
                curr_pct = pd.to_numeric(curr_data['占集保庫存數比例%'], errors='coerce').sum()
                
                # 只有當我們有上週資料且當下百分大於0時才做比較
                if curr_pct > 0 and tdcc_prev_df is not None and not tdcc_prev_df.empty:
                    prev_filtered = tdcc_prev_df[tdcc_prev_df['證券代號'].astype(str) == stock_id]
                    if not prev_filtered.empty:
                        p_levels = pd.to_numeric(prev_filtered['持股分級'], errors='coerce')
                        prev_data = prev_filtered[p_levels >= 12]
                        prev_pct = pd.to_numeric(prev_data['占集保庫存數比例%'], errors='coerce').sum()
                        
                        if curr_pct > prev_pct and (curr_pct - prev_pct) >= 0.1:
                            matched.append(f"大戶持股增加({prev_pct:.1f}%→{curr_pct:.1f}%)")
    except Exception as e:
        print(f"TDCC持股計算錯誤 {stock_id}: {e}")

    # 5. 法說會 (使用者上傳的 Excel 比對)
    try:
        if conference_stocks and stock_id in conference_stocks:
            conf_date = conference_stocks[stock_id]
            today = datetime.datetime.now().date()
            delta = (conf_date - today).days
            if 0 <= delta <= 14:
                matched.append(f"兩周內有法說會({conf_date.strftime('%m/%d')})")
    except Exception as e:
        print(f"法說會計算錯誤 {stock_id}: {e}")

    # 6. CB 可轉債 (使用者輸入的近期發行 CB 股票清單)
    try:
        if cb_stocks and stock_id in cb_stocks:
            matched.append("近期將發行CB")
    except Exception as e:
        print(f"CB計算錯誤 {stock_id}: {e}")

    # 7. CB 轉換條件：已發行CB、股價低於轉換價、轉換比例<10%
    try:
        if cb_issued_data and stock_id in cb_issued_data:
            cb_info = cb_issued_data[stock_id]
            conv_price = cb_info.get('conv_price', 0)
            balance_pct = cb_info.get('balance_pct', 0)  # 餘額比例
            conversion_pct = 100 - balance_pct  # 已轉換比例
            
            if conv_price > 0 and conversion_pct < 10:
                # 取得目前股價 (使用前面 yfinance 已抓的 hist)
                try:
                    current_price = 0
                    for suffix in ['.TW', '.TWO']:
                        ticker = yf.Ticker(f"{stock_id}{suffix}")
                        h = ticker.history(period="1d")
                        if not h.empty:
                            current_price = h['Close'].iloc[-1]
                            break
                    if current_price > 0 and current_price < conv_price:
                        matched.append(f"CB股價({current_price:.1f})<轉換價({conv_price})且轉換率{conversion_pct:.1f}%")
                except:
                    pass
    except Exception as e:
        print(f"CB轉換計算錯誤 {stock_id}: {e}")

    return matched

import pandas as pd

import json

import fitz  # PyMuPDF

from google import genai

from google.genai import types

import yfinance as yf

import re

import os

import ast

from dotenv import load_dotenv

import gspread

from google.oauth2.service_account import Credentials

load_dotenv()

st.set_page_config(page_title="券商報告分析器", page_icon="📈", layout="wide")

st.title("📈 券商報告自動分析器 (Gemini版)")

st.markdown("上傳各家券商的分析報告 (PDF 或 TXT)，AI 將自動為您萃取並整理出關鍵資訊。")

# Sidebar for API Key

st.sidebar.header("⚙️ 設定")

# Try to get API Key from environment or Streamlit Secrets

default_api_key = os.environ.get("GEMINI_API_KEY", "")

try:

    if not default_api_key and "GEMINI_API_KEY" in st.secrets:

        default_api_key = st.secrets["GEMINI_API_KEY"]

except Exception:

    pass

api_key = st.sidebar.text_input("輸入 Gemini API Key", value=default_api_key, type="password")

st.sidebar.markdown(

    "[取得 Gemini API Key](https://aistudio.google.com/app/apikey)"

)

# Data persistence

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

HISTORY_FILE = os.path.join(BASE_DIR, "history.json")

def get_gspread_client():

    if "gcp_service_account" in st.secrets:

        try:

            creds = Credentials.from_service_account_info(

                st.secrets["gcp_service_account"],

                scopes=[

                    "https://www.googleapis.com/auth/spreadsheets",

                    "https://www.googleapis.com/auth/drive"

                ]

            )

            return gspread.authorize(creds)

        except Exception as e:

            st.warning(f"Google 憑證錯誤: {e}")

    return None

def get_worksheet():

    client = get_gspread_client()

    if client and "sheets" in st.secrets and "url" in st.secrets["sheets"]:

        try:

            sheet = client.open_by_url(st.secrets["sheets"]["url"])

            return sheet.sheet1

        except Exception as e:

            st.error(f"無法開啟指定的 Google Sheet: {e}")

            return None

    return None

def load_history():

    ws = get_worksheet()

    if ws:

        try:

            records = ws.get_all_records()

            if records:
                st.toast(f"☁️ 成功從 Google Sheets (Sheet1) 載入 {len(records)} 筆紀錄！", icon="📂")
                return records
            
            # Sheet1 是空的，嘗試從 Sheet2（備份）還原
            try:
                ws2 = ws.spreadsheet.worksheet("備份")
                backup_records = ws2.get_all_records()
                if backup_records:
                    st.warning(f"⚠️ Sheet1 為空！已從 Sheet2（備份）還原 {len(backup_records)} 筆紀錄。")
                    return backup_records
            except:
                pass
            
            st.toast("ℹ️ Google Sheets 目前沒有資料。", icon="🆕")
            return []

        except Exception as e:

            st.error(f"❌ 從 Google Sheets 讀取失敗: {e}")

            return []

    

    # 這是沒有設定 GCP 金鑰時的「本機暫存版本」

    if os.path.exists(HISTORY_FILE):

        try:

            with open(HISTORY_FILE, "r", encoding="utf-8") as f:

                data = json.load(f)

                st.toast(f"✅ 成功從本地檔案載入 {len(data)} 筆紀錄！", icon="📂")

                return data

        except Exception as e:

            st.error(f"❌ 載入本地歷史紀錄失敗 (檔案毀損或格式錯誤): {e}")

            return []

    else:

        st.toast("ℹ️ 沒有找到歷史紀錄檔案，準備建立新紀錄。", icon="🆕")

    return []

def save_history(history):

    ws = get_worksheet()

    if ws:

        try:

            # ⚠️ 安全防護：拒絕寫入空白資料，防止誤清表格
            if not history:
                st.warning("⚠️ 偵測到空白資料，已中止寫入以保護現有記錄！")
                return

            df = pd.DataFrame(history)

            df = df.fillna("N/A")

            # 將所有列表轉換為以逗號分隔的字串，避免 Google Sheets 報錯 list_value {}

            for col in df.columns:

                df[col] = df[col].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)

            data = [df.columns.values.tolist()] + df.values.tolist()

            

            # Step 1: 寫入 Sheet1（主表）
            ws.clear()

            try:

                ws.update(values=data, range_name="A1")

            except TypeError:

                # 兼容舊版語法

                ws.update(data, "A1")

            st.toast(f"☁️ 已成功將 {len(history)} 筆紀錄同步至 Sheet1！", icon="💾")

            # Step 2: Sheet1 寫入成功，複製備份到 Sheet2
            try:
                spreadsheet = ws.spreadsheet
                try:
                    ws2 = spreadsheet.worksheet("備份")
                except:
                    ws2 = spreadsheet.add_worksheet(title="備份", rows=str(len(data) + 10), cols=str(len(data[0]) + 2))
                
                ws2.clear()
                try:
                    ws2.update(values=data, range_name="A1")
                except TypeError:
                    ws2.update(data, "A1")
                
                st.toast(f"🛡️ 已同步備份 {len(history)} 筆紀錄至 Sheet2（備份）！", icon="✅")
            except Exception as e2:
                st.warning(f"⚠️ Sheet1 寫入成功，但備份至 Sheet2 失敗: {e2}")

        except Exception as e:

            st.error(f"❌ 寫入 Google Sheets 失敗: {str(e)}")

    else:

        # 本機暫存版本

        try:

            with open(HISTORY_FILE, "w", encoding="utf-8") as f:

                json.dump(history, f, ensure_ascii=False, indent=2)

            st.toast(f"💾 已成功寫入實體檔案 (因未設定 Google 金鑰)！", icon="💾")

        except Exception as e:
            st.error(f"❌ 無法儲存歷史紀錄: {str(e)}")

def load_conf_dates():
    _conf_cache_path = os.path.join(BASE_DIR, "conf_dates.json")
    ws = get_worksheet()
    if ws:
        try:
            spreadsheet = ws.spreadsheet
            ws_conf = spreadsheet.worksheet("法說會")
            records = ws_conf.get_all_records()
            if records:
                conf_map = {str(r.get("代號", "")).strip(): str(r.get("日期", "")) for r in records if r.get("代號")}
                st.toast(f"☁️ 成功從 Google Sheets (法說會) 載入 {len(conf_map)} 筆紀錄！", icon="📅")
                return conf_map
        except Exception as e:
            if "WorksheetNotFound" not in str(type(e).__name__):
                st.warning(f"⚠️ 從 Google Sheets 讀取法說會資料失敗: {e}")

    # Fallback to local
    if os.path.exists(_conf_cache_path):
        try:
            with open(_conf_cache_path, 'r', encoding='utf-8') as _f:
                conf_map = json.load(_f)
                st.toast(f"✅ 成功從本地檔案載入 {len(conf_map)} 筆法說會紀錄！", icon="📅")
                return conf_map
        except Exception as e:
            st.error(f"❌ 載入本地法說會紀錄失敗: {e}")
    return {}

def save_conf_dates(conf_map):
    _conf_cache_path = os.path.join(BASE_DIR, "conf_dates.json")
    ws = get_worksheet()
    if ws:
        try:
            spreadsheet = ws.spreadsheet
            try:
                ws_conf = spreadsheet.worksheet("法說會")
            except:
                ws_conf = spreadsheet.add_worksheet(title="法說會", rows="1000", cols="5")
                st.toast("🆕 已建立新的 Google Sheet 分頁：法說會", icon="✨")
            
            data = [["代號", "日期"]] + [[k, v] for k, v in conf_map.items()]
            ws_conf.clear()
            try:
                ws_conf.update(values=data, range_name="A1")
            except TypeError:
                ws_conf.update(data, "A1")
            st.toast(f"☁️ 已成功將 {len(conf_map)} 筆法說會紀錄同步至 Google Sheets！", icon="💾")
        except Exception as e:
            st.error(f"❌ 寫入 Google Sheets (法說會) 失敗: {str(e)}")
            
    # Fallback/also save to local
    try:
        with open(_conf_cache_path, "w", encoding="utf-8") as f:
            json.dump(conf_map, f, ensure_ascii=False, indent=2)
    except Exception as e:
        st.error(f"❌ 無法儲存法說會紀錄至本地: {str(e)}")

def load_revenue_stocks():
    _rev_cache_path = os.path.join(BASE_DIR, "revenue_stocks.json")
    ws = get_worksheet()
    if ws:
        try:
            spreadsheet = ws.spreadsheet
            ws_rev = spreadsheet.worksheet("營收條件")
            records = ws_rev.get_all_records()
            if records:
                rev_list = [str(r.get("代號", "")).strip() for r in records if r.get("代號")]
                st.toast(f"☁️ 成功從 Google Sheets (營收條件) 載入 {len(rev_list)} 筆紀錄！", icon="💰")
                return rev_list
        except Exception as e:
            if "WorksheetNotFound" not in str(type(e).__name__):
                st.warning(f"⚠️ 從 Google Sheets 讀取營收條件資料失敗: {e}")

    # Fallback to local
    if os.path.exists(_rev_cache_path):
        try:
            with open(_rev_cache_path, 'r', encoding='utf-8') as _f:
                rev_list = json.load(_f)
                st.toast(f"✅ 成功從本地檔案載入 {len(rev_list)} 筆營收紀錄！", icon="💰")
                return rev_list
        except Exception as e:
            st.error(f"❌ 載入本地營收紀錄失敗: {e}")
    return []

def save_revenue_stocks(rev_list):
    _rev_cache_path = os.path.join(BASE_DIR, "revenue_stocks.json")
    ws = get_worksheet()
    if ws:
        try:
            spreadsheet = ws.spreadsheet
            try:
                ws_rev = spreadsheet.worksheet("營收條件")
            except:
                ws_rev = spreadsheet.add_worksheet(title="營收條件", rows="1000", cols="2")
                st.toast("🆕 已建立新的 Google Sheet 分頁：營收條件", icon="✨")
            
            data = [["代號"]] + [[k] for k in rev_list]
            ws_rev.clear()
            try:
                ws_rev.update(values=data, range_name="A1")
            except TypeError:
                ws_rev.update(data, "A1")
            st.toast(f"☁️ 已成功將 {len(rev_list)} 筆營收紀錄同步至 Google Sheets！", icon="💾")
        except Exception as e:
            st.error(f"❌ 寫入 Google Sheets (營收條件) 失敗: {str(e)}")
            
    # Fallback/also save to local
    try:
        with open(_rev_cache_path, "w", encoding="utf-8") as f:
            json.dump(rev_list, f, ensure_ascii=False, indent=2)
    except Exception as e:
        st.error(f"❌ 無法儲存營收紀錄至本地: {str(e)}")


# Initialize session history

if 'history' not in st.session_state:

    st.session_state.history = load_history()

# Main area

tab1, tab2 = st.tabs(["📂 檔案上傳", "📝 直接貼上文字"])

with tab1:

    uploaded_files = st.file_uploader(

        "上傳券商報告", 

        type=["pdf", "txt", "xlsx", "xls", "csv"], 

        accept_multiple_files=True

    )

with tab2:

    pasted_text = st.text_area("在這裡貼上報告的純文字內容", height=300, placeholder="將信件或網頁內容直接貼到這裡...")

    pasted_name = st.text_input("請為這份貼上的報告命名 (選填)", value="貼上文字_報告")

def extract_text(file):

    text = ""

    try:

        if file.name.endswith(".pdf"):

            doc = fitz.open("pdf", file.read())

            for page in doc:

                text += page.get_text("text") + "\n"

        elif file.name.endswith(".txt"):

            text = file.read().decode("utf-8")

        elif file.name.endswith((".xlsx", ".xls")):

            import pandas as pd

            df = pd.read_excel(file)

            text = "這是一份表格資料：\n" + df.to_csv(index=False)

        elif file.name.endswith(".csv"):

            import pandas as pd

            df = pd.read_csv(file)

            text = "這是一份表格資料：\n" + df.to_csv(index=False)

    except Exception as e:

        st.error(f"提取文字時發生錯誤 ({file.name}): {str(e)}")

    return text

# ── Rate Limiter for Free Tier (10 req/min) ─────────────────────────────────
_rate_limiter_state = {"count": 0, "window_start": None}
_FREE_TIER_RPM = 8  # 保守設 8，留緩衝（免費版上限 10/分鐘）

def _gemini_rate_limit_wait(placeholder=None):
    """若本分鐘已達 Free Tier 上限，等到下一分鐘再繼續。"""
    import time as _rl_time
    now = _rl_time.time()
    state = _rate_limiter_state
    if state["window_start"] is None or now - state["window_start"] >= 60:
        state["window_start"] = now
        state["count"] = 0
    state["count"] += 1
    if state["count"] > _FREE_TIER_RPM:
        wait_sec = 60 - (now - state["window_start"]) + 2
        if wait_sec > 0:
            if placeholder:
                for i in range(int(wait_sec), 0, -1):
                    placeholder.info(f"⏳ Free Tier 每分鐘限速保護，等候 {i} 秒後繼續...")
                    _rl_time.sleep(1)
                placeholder.empty()
            else:
                _rl_time.sleep(wait_sec)
        state["window_start"] = _rl_time.time()
        state["count"] = 1
# ─────────────────────────────────────────────────────────────────────────────

def parse_report_with_gemini(text, api_key, source_name="未知來源", _rate_placeholder=None):

    client = genai.Client(api_key=api_key)

    

    # 截斷過長報告，避免消耗過多 token（保留前 4000 字已足夠提取關鍵資訊）
    _MAX_TEXT_LEN = 4000
    if len(text) > _MAX_TEXT_LEN:
        text = text[:_MAX_TEXT_LEN] + "\n...[內容過長已截斷]"

    prompt = f"""從以下券商報告中提取下列欄位，以 JSON 回傳。

欄位說明（請嚴格遵守）：
- date: 報告日期，格式 YYYY-MM-DD。優先從來源名稱「{source_name}」中找數字序列（如 20240325→2024-03-25）；其次從內文找；找不到填"未知"。
- stock: 股票代號+名稱（如"2330 台積電"）
- brokerage: 券商名稱
- rating: 評等（買進/中立/賣出或英文原文）
- target_price: 目標價（純數字，如"150"）；無則填"N/A"
- eps: 券商預估EPS（純數字）；無則填"N/A"
- summary: 繁體中文，30字內說明核心看法（看多/看空理由）。若無實質分析填""
- daily_stock_selection: 若有明確標示為每日選股填"✅ 是"；否則填"N/A"
- matched_criteria: 陣列，只填以下出現的項目："投信第一天買且近三月未買"、"三大法人同買"、"日KD黃金交叉"、"周KD黃金交叉"、"成交量大於十週均量且大於三倍十日均量"、"合約負債季增50%且創四季新高"、"兩周內有法說會"、"近期將發行CB"、"近月營收月增且年增"、"大戶持股比例成長"；無則填[]

回傳格式(JSON only)：
{{"date":"","stock":"","brokerage":"","rating":"","target_price":"","eps":"","summary":"","daily_stock_selection":"","matched_criteria":[]}}

報告內容：
"""

    

    import time as _time
    _models_to_try = ['gemini-2.5-flash', 'gemini-2.0-flash', 'gemini-2.0-flash-lite']
    _max_wait_retries = 2  # 遇到全模型限速時最多等待重試幾次

    def _parse_retry_delay(err_str):
        """從錯誤訊息中解析建議等待秒數"""
        m = re.search(r"retryDelay['\"]?:\s*['\"]?(\d+)s", err_str)
        return int(m.group(1)) + 2 if m else 60  # 預設等 60 秒

    # 呼叫前先做 rate limit 檢查（Free Tier 保護）
    _gemini_rate_limit_wait(placeholder=_rate_placeholder)

    for _wait_attempt in range(_max_wait_retries + 1):
        _last_err = ""
        for _model_name in _models_to_try:
            try:
                response = client.models.generate_content(
                    model=_model_name,
                    contents=prompt + text,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                    ),
                )
                try:
                    return json.loads(response.text)
                except json.JSONDecodeError:
                    st.error("JSON 解析失敗，模型回傳的值可能不符預期。")
                    with st.expander("檢視原始回傳內容"):
                        st.write(response.text)
                    return None
            except Exception as e:
                err_str = str(e)
                _last_err = err_str
                if 'RESOURCE_EXHAUSTED' in err_str or '429' in err_str or '503' in err_str or 'UNAVAILABLE' in err_str or 'quota' in err_str.lower():
                    if _model_name != _models_to_try[-1]:
                        st.toast(f"⚠️ {_model_name} 經流限，切換備用模型...", icon="🔄")
                        continue
                    # 所有模型都被限速
                    if _wait_attempt < _max_wait_retries:
                        _wait_sec = _parse_retry_delay(err_str)
                        st.warning(f"⏳ 所有模型都被請求限速，等候 {_wait_sec} 秒後自動重試... (第 {_wait_attempt+1}/{_max_wait_retries} 次)")
                        _time.sleep(_wait_sec)
                        break  # 跳出模型循環，進入下一輪等待重試
                else:
                    st.error(f"呼叫 API 時發生錯誤 ({_model_name}): {err_str}")
                    return None
        else:
            continue  # 模型循環正常結束（此分支不會被執行）
        # 模型循環被 break 中斷，逐層 break 讓外層 for 知道
        pass
    st.error(f"所有模型皆被限速，已重試 {_max_wait_retries} 次仍無法完成請稍後再試。")
    return None

def get_latest_close_price(stock_id):

    match = re.search(r'\d{4}', str(stock_id))

    if match:

        code = match.group()

        # 嘗試先用上市 (.TW)，再用上櫃 (.TWO)

        for suffix in ['.TW', '.TWO']:

            try:

                ticker = yf.Ticker(f"{code}{suffix}")

                hist = ticker.history(period="1d")

                if not hist.empty:

                    return round(float(hist['Close'].iloc[-1]), 2)

            except:

                continue

    return None

def evaluate_stock_with_search(stock, api_key):

    client = genai.Client(api_key=api_key)

    

    # 使用 Chain of Thought (CoT) 的方式，讓 AI 有空間整理搜尋結果，大幅提高搜尋準確率

    prompt = f"""

    你是一位專業的台灣股市分析師。請務必使用 Google 搜尋，分別查詢台灣股市「{stock}」的最新即時資料。

    請一步一步搜尋並判斷這檔股票【今日】或【近期最新公佈資料】是否符合以下 10 項條件：

    

    1. "投信第一天買且近三月未買" (查詢近期投信買賣超)

    2. "三大法人同買" (查詢外資、投信、自營商是否同步買超)

    3. "日KD黃金交叉" (查詢技術線圖 KD 指標)

    4. "周KD黃金交叉" (查詢周線 KD)

    5. "成交量大於十週均量且大於三倍十日均量" (查詢近期成交量變化)

    6. "合約負債季增50%且創四季新高" (查詢近期財報或新聞)

    7. "兩周內有法說會" (查詢近期法說會日程)

    8. "近期將發行CB" (查詢可轉債發行新聞)

    9. "近月營收月增且年增" (查詢最新單月營收 YoY 與 MoM)

    10. "大戶持股比例成長" (查詢千張大戶持股比例)

    

    【極度重要警告：防範幻覺與舊資料】

    1. 網路搜尋極容易查到「舊新聞（例如上個月的法人買超新聞）」。你必須非常嚴格地確認資料的「日期」是不是【最近一個交易日】！如果查到的網頁沒有寫明是今天的籌碼，絕對不能當作符合。

    2. 對於「近三月未買」、「創四季新高」這種需要長期歷史數據比對的條件，除非你搜尋到明確的新聞標題或內文直接這樣寫，否則光看一天的數據不能判定符合，請一律當作「不符合」。

    3. 如果查不到明確的即時量化數字，或者有任何一絲不確定，寧可漏判，也【絕對不可】自行臆測或預設為符合。

    

    請先寫下你的搜尋過程與判斷（務必標出你參考的資料日期），最後請務必在回應的結尾放上一個 JSON 區塊，列出確切符合的官方標籤字串，格式如下：

    ```json

    {{

       "matched_criteria": ["符合的標籤一", "符合的標籤二"]

    }}

    ```

    """

    

    try:
        import time as _time2
        _models_s = ['gemini-2.5-flash', 'gemini-2.0-flash', 'gemini-2.0-flash-lite']
        _max_wait_s = 2

        def _parse_delay(es):
            m2 = re.search(r"retryDelay['\"]?:\s*['\"]?(\d+)s", es)
            return int(m2.group(1)) + 2 if m2 else 60

        response = None
        for _w_attempt in range(_max_wait_s + 1):
            for _mn in _models_s:
                try:
                    response = client.models.generate_content(
                        model=_mn,
                        contents=prompt,
                        config=types.GenerateContentConfig(
                            tools=[{"google_search": {}}],
                            temperature=0.1
                        ),
                    )
                    break
                except Exception as _e2:
                    _es2 = str(_e2)
                    if ('RESOURCE_EXHAUSTED' in _es2 or '429' in _es2 or '503' in _es2 or 'UNAVAILABLE' in _es2 or 'quota' in _es2.lower()):
                        if _mn != _models_s[-1]:
                            continue
                        if _w_attempt < _max_wait_s:
                            _ws = _parse_delay(_es2)
                            _time2.sleep(_ws)
                            break
                    raise
            else:
                break  # 模型循環正常完成（已 break）
        if response is None:
            return []

        

        text = response.text

        # 用 regex 擷取 markdown 中的 json 區塊

        json_match = re.search(r'```json\s*(\{.*?\})\s*```', text, re.DOTALL)

        if json_match:

            try:

                data = json.loads(json_match.group(1))

                return data.get("matched_criteria", [])

            except:

                pass

                

        # 作為備案，直接在全文中尋找雙引號內的標籤

        criteria = [

            "投信第一天買且近三月未買", "三大法人同買", "日KD黃金交叉", "周KD黃金交叉",

            "成交量大於十週均量且大於三倍十日均量", "合約負債季增50%且創四季新高",

            "兩周內有法說會", "近期將發行CB", "近月營收月增且年增", "大戶持股比例成長"

        ]

        results = set()

        for c in criteria:

            if c in text: # 如果 AI 的回覆內容中直接出現了這個字眼，我們就當作有符合

                results.add(c)

        return list(results)

        

    except Exception as e:

        print(f"評估 {stock} 時發生錯誤: {e}")

        return []

col1, col2 = st.columns([1, 1])

analyze_btn = col1.button("開始分析", type="primary", use_container_width=True)

# 使用 popover 加入確認對話框與「管理員密碼」權限機制

with col2.popover("🧹 清空歷史紀錄", use_container_width=True):

    st.warning("⚠️ 此為管理員專屬動作，清空後資料將**無法復原**。")

    admin_pwd = st.text_input("輸入管理員密碼：", type="password")

    

    if st.button("🔴 我確定，清空全部", use_container_width=True):

        # 取得系統設定的密碼 (預設為 "admin123")

        correct_pwd = os.environ.get("ADMIN_PASSWORD", "admin123")

        try:

            if "ADMIN_PASSWORD" in st.secrets:

                correct_pwd = st.secrets["ADMIN_PASSWORD"]

        except Exception:

            pass

            

        if admin_pwd == correct_pwd:

            st.session_state.history = []

            save_history(st.session_state.history)

            st.rerun()

        else:

            st.error("❌ 密碼錯誤，您無權限清除歷史紀錄！")

if analyze_btn:

    if not api_key:

        st.warning("⚠️ 請先在左側邊欄輸入你的 Gemini API Key！")

    elif not uploaded_files and not pasted_text.strip():

        st.warning("⚠️ 請先上傳至少一份報告或貼上報告內容！")

    else:

        results = []

        progress_bar = st.progress(0)

        status_text = st.empty()

        

        tasks_count = len(uploaded_files) if uploaded_files else 0

        if pasted_text.strip():

            tasks_count += 1

            

        current = 0

        

        if uploaded_files:

            for file in uploaded_files:

                current += 1

                status_text.text(f"正在分析 ({current}/{tasks_count}): {file.name} ...")

                

                # 1. 萃取文字

                text = extract_text(file)

                if not text.strip():

                    st.warning(f"檔案 {file.name} 內容為空或無法提取文字。")

                    continue

                

                # 2. 呼叫 Gemini 分析（函式內已截斷至 4000 字）

                parsed_data = parse_report_with_gemini(text, api_key, source_name=file.name, _rate_placeholder=status_text)

                # 多份報告之間加延遲（Free Tier: 10 req/min → 每份至少間隔 7 秒）
                import time as _loop_time
                if current < tasks_count:
                    for _cd in range(7, 0, -1):
                        status_text.info(f"⏱️ 限速保護：{_cd} 秒後繼續分析下一份報告...")
                        _loop_time.sleep(1)
                    status_text.empty()

                

                if parsed_data:

                    items = parsed_data if isinstance(parsed_data, list) else [parsed_data]

                    for item in items:

                        item['檔案名稱'] = file.name

                        close_price = get_latest_close_price(item.get('stock', ''))

                        item['最新收盤價'] = close_price if close_price else "N/A"

                        

                        try:

                            eps_val = float(str(item.get('eps', 'N/A')))

                            if close_price and eps_val != 0: # Avoid division by zero

                                pe = round(close_price / eps_val, 2)

                                item['目前本益比(PE)'] = pe

                                item['低於20倍PE?'] = "✅ 是" if pe < 20 else "❌ 否"

                            else:

                                item['目前本益比(PE)'] = "N/A"

                                item['低於20倍PE?'] = "N/A"

                        except ValueError:

                            item['目前本益比(PE)'] = "N/A"

                            item['低於20倍PE?'] = "N/A"

                            

                        results.append(item)

                    

                progress_bar.progress(current / tasks_count)

            

        if pasted_text.strip():

            current += 1

            status_text.text(f"正在分析 ({current}/{tasks_count}): {pasted_name} ...")

            

            parsed_data = parse_report_with_gemini(pasted_text, api_key, source_name=pasted_name)

            

            if parsed_data:

                items = parsed_data if isinstance(parsed_data, list) else [parsed_data]

                for item in items:

                    item['檔案名稱'] = pasted_name

                    close_price = get_latest_close_price(item.get('stock', ''))

                    item['最新收盤價'] = close_price if close_price else "N/A"

                    

                    try:

                        eps_val = float(str(item.get('eps', 'N/A')))

                        if close_price and eps_val != 0: # Avoid division by zero

                            pe = round(close_price / eps_val, 2)

                            item['目前本益比(PE)'] = pe

                            item['低於20倍PE?'] = "✅ 是" if pe < 20 else "❌ 否"

                        else:

                            item['目前本益比(PE)'] = "N/A"

                            item['低於20倍PE?'] = "N/A"

                    except ValueError:

                        item['目前本益比(PE)'] = "N/A"

                        item['低於20倍PE?'] = "N/A"

                        

                    results.append(item)

                

            progress_bar.progress(current / tasks_count)

            

        if results:

            st.session_state.history.extend(results)

            

            # --- 自動覆蓋舊報告機制 (同步採用超強正規化去重複) ---

            best_items = {}

            import re

            for item in st.session_state.history:

                b_name = str(item.get('brokerage', '')).strip()

                s_name = str(item.get('stock', '')).strip()

                n_b = re.sub(r'[ \(\)\-]', '', b_name).upper()

                for eng in ['KGI', 'SINOPAC', 'YUANTA', 'FUBON', 'CATHAY', 'CTBC', 'CAPITAL', 'MASTERLINK']:

                    n_b = n_b.replace(eng, '')

                for suffix in ["證券", "投顧", "控股", "金控", "金融", "金", "SECURITIES", "證", "公司", "股份有限公司", "期貨", "亞洲"]:

                    n_b = n_b.replace(suffix, "")

                n_b = n_b.strip()

                

                n_s = re.sub(r'[0-9\W_]', '', s_name).upper()

                key = (n_s, n_b)

                

                if key not in best_items:

                    best_items[key] = item

                else:

                    old_date = str(best_items[key].get('date', ''))

                    new_date = str(item.get('date', ''))

                    if new_date >= old_date:

                        old_summary = str(best_items[key].get('summary', '')).strip()

                        new_summary = str(item.get('summary', '')).strip()

                        null_vals = ['', 'N/A', '無', 'UNKNOWN', '未知', 'NONE', 'NAN']

                        if (new_summary.upper() in null_vals) and (old_summary.upper() not in null_vals):

                            item['summary'] = old_summary

                        old_rating = str(best_items[key].get('rating', '')).strip()

                        new_rating = str(item.get('rating', '')).strip()

                        if (new_rating.upper() in null_vals) and (old_rating.upper() not in null_vals):

                            item['rating'] = old_rating

                        old_daily = str(best_items[key].get('daily_stock_selection', '')).strip()

                        new_daily = str(item.get('daily_stock_selection', '')).strip()

                        if (new_daily.upper() in null_vals) and (old_daily.upper() not in null_vals):

                            item['daily_stock_selection'] = old_daily

                            

                        # 聯集 matched_criteria

                        old_mc = best_items[key].get('matched_criteria', [])

                        new_mc = item.get('matched_criteria', [])

                        if not isinstance(old_mc, list): old_mc = []

                        if not isinstance(new_mc, list): new_mc = []

                        item['matched_criteria'] = list(set(old_mc + new_mc))

                        

                        best_items[key] = item

            st.session_state.history = list(best_items.values())

            # -------------------------------------------------------

            

            status_text.text("🔄 正在為資料庫中的所有股票同步今日最新收盤價...")

            unique_stocks = list(set([str(item.get('stock', '')) for item in st.session_state.history if str(item.get('stock', '')).strip()]))

            

            stock_prices = {}

            for i, stock in enumerate(unique_stocks):

                price = get_latest_close_price(stock)

                if price:

                    stock_prices[stock] = price

                progress_bar.progress((i + 1) / max(1, len(unique_stocks)))

                

            for item in st.session_state.history:

                stock_key = str(item.get('stock', ''))

                if stock_key in stock_prices:

                    new_price = stock_prices[stock_key]

                    item['最新收盤價'] = new_price

                    try:

                        eps_val = float(str(item.get('eps', 'N/A')))

                        if new_price and eps_val != 0:

                            pe = round(float(new_price) / eps_val, 2)

                            item['目前本益比(PE)'] = pe

                            item['低於20倍PE?'] = "✅ 是" if pe < 20 else "❌ 否"

                    except:

                        pass

            

            status_text.text("✅ 分析成功，且所有歷史名單股價皆已自動更新！")

            save_history(st.session_state.history)

        else:

            status_text.text("✅ 分析完成，但無新擷取的資料。")

if st.session_state.history:

    df_raw = pd.DataFrame(st.session_state.history)

    

    # --- 採用功能強大的 Pandas 全域歷史去重複大掃除 ---

    if not df_raw.empty and 'stock' in df_raw.columns:

        # 建立標準化欄位以供分組去重複

        df_raw['norm_stock'] = df_raw['stock'].astype(str).str.extract(r'(\d{4})')[0].fillna(df_raw['stock'].astype(str).str.replace(r'[\W_]', '', regex=True).str.upper())

        df_raw['norm_broker'] = df_raw['brokerage'].astype(str).str.replace(r'[\W_]', '', regex=True).str.upper()

        

        for eng in ['KGI', 'SINOPAC', 'YUANTA', 'FUBON', 'CATHAY', 'CTBC', 'CAPITAL', 'MASTERLINK']:

            df_raw['norm_broker'] = df_raw['norm_broker'].str.replace(eng, '')

        for suffix in ["證券", "投顧", "控股", "金控", "金融", "金", "SECURITIES", "證", "公司", "股份有限公司", "期貨", "亞洲", "研究"]:

            df_raw['norm_broker'] = df_raw['norm_broker'].str.replace(suffix, '')

            

        # 處理日期排序，將「未知」替換成「0000」排在最前

        df_raw['sort_date'] = df_raw['date'].astype(str).str.replace('未知.*', '0000', regex=True)

        df_raw = df_raw.sort_values('sort_date', ascending=True)

        

        # 依序使用 ffill 將舊資料中的 summary 和 rating 往前填補給較新的缺失記錄

        null_vals = ['', 'N/A', 'N/a', '無', 'UNKNOWN', '未知', 'NONE', 'NAN']

        df_raw['summary'] = df_raw['summary'].replace(null_vals, pd.NA)

        df_raw['rating'] = df_raw['rating'].replace(null_vals, pd.NA)

        if 'daily_stock_selection' in df_raw.columns:

            df_raw['daily_stock_selection'] = df_raw['daily_stock_selection'].replace(null_vals, pd.NA)

            df_raw['daily_stock_selection'] = df_raw.groupby(['norm_stock', 'norm_broker'])['daily_stock_selection'].ffill()

        

        df_raw['summary'] = df_raw.groupby(['norm_stock', 'norm_broker'])['summary'].ffill()

        df_raw['rating'] = df_raw.groupby(['norm_stock', 'norm_broker'])['rating'].ffill()

        

        # 保留每個群組的「最後一筆」(時間最新)

        clean_df = df_raw.drop_duplicates(subset=['norm_stock', 'norm_broker'], keep='last')

        

        # 轉換回 dictionary 並儲存回 history

        clean_history = clean_df.drop(columns=['norm_stock', 'norm_broker', 'sort_date']).fillna('N/A').to_dict('records')

        

        if len(clean_history) != len(st.session_state.history):

            st.session_state.history = clean_history

            save_history(st.session_state.history)

            df_raw = pd.DataFrame(st.session_state.history) # 重新建立清好的資料以供顯示

        

    # ----------------------------------------------------

    st.divider()

    st.subheader("📊 歷次分析彙整結果 (依股票整合)")

    

    # 預先解析法說會 Excel（若已上傳），供歷史表格使用
    if 'conf_dates_map' not in st.session_state:
        st.session_state['conf_dates_map'] = load_conf_dates()

    if 'global_name_map' not in st.session_state:
        import os as _os, json as _json
        _cache_path = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "stock_names.json")
        _name_map = {}
        if _os.path.exists(_cache_path):
            try:
                with open(_cache_path, 'r', encoding='utf-8') as _f:
                    _name_map = _json.load(_f)
            except: pass
        
        # 如果本地檔案不見了或是空的（例如部署到 Streamlit Cloud 重啟後），則主動重新下載
        if not _name_map:
            try:
                _r1 = requests.get("https://openapi.twse.com.tw/v1/opendata/t187ap03_L", timeout=10, verify=False)
                if _r1.status_code == 200:
                    for d in _r1.json():
                        _name_map[str(d.get('公司代號', '')).strip()] = str(d.get('公司簡稱', '')).strip()
            except: pass
            
            try:
                _r2 = requests.get("https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O", timeout=10, verify=False)
                if _r2.status_code == 200:
                    for d in _r2.json():
                        code = str(d.get('SecuritiesCompanyCode', '')).strip()
                        name = str(d.get('CompanyAbbreviation', '')).strip()
                        if code and name:
                            _name_map[code] = name
            except: pass
            
            # 若下載成功，嘗試寫入快取
            if _name_map:
                try:
                    with open(_cache_path, 'w', encoding='utf-8') as _f:
                        _json.dump(_name_map, _f, ensure_ascii=False)
                except: pass

        st.session_state['global_name_map'] = _name_map

    # 進行整合邏輯

    consolidated = []

    

    if 'stock' in df_raw.columns:

        # 0. 整理全域股票名稱對照 (確保名稱一致性)
        name_map = st.session_state.get("global_name_map", {})
        
        def _get_standard_stock_name(s):
            s_str = str(s).strip()
            # 提取 4 位股票代碼
            m = re.search(r'(\d{4})', s_str)
            if m:
                code = m.group()
                # 優先用官方名稱，若無則提取原字串中的名稱部分
                official_name = name_map.get(code, "").strip()
                if not official_name:
                    # 嘗試從原字串提取中文/英文名稱（去除數字與符號）
                    official_name = re.sub(r'[\d\W_]', '', s_str).strip()
                return f"{code} {official_name}".strip()
            return s_str

        # 1. 正規化所有歷史紀錄中的股票名稱
        if not df_raw.empty:
            df_raw['stock'] = df_raw['stock'].apply(_get_standard_stock_name)

        # 2. 預先計算每檔股票的積分與條件，以供排序
        def parse_criteria_global(mc):
            if isinstance(mc, list): return mc
            if isinstance(mc, str):
                try:
                    parsed = ast.literal_eval(mc)
                    if isinstance(parsed, list): return parsed
                except:
                    if mc and mc not in ["N/A", "NaN", "無"]:
                        return [x.strip() for x in mc.split(',')]
            return []
            
        # [定義] 取得法說會日期
        def _get_conf_date(stock_str):
            conf_map = st.session_state.get('conf_dates_map', {})
            m = re.search(r'(\d{4})', str(stock_str))
            return conf_map.get(m.group(), "") if m else ""

        today = datetime.datetime.now().date()
        group_scores = {}
        group_criteria = {}
        history_stock_set = set() # 記錄所有已有報告的股號

        for stock, group in df_raw.groupby('stock', dropna=False):
            stock_clean = str(stock).strip()
            if not stock_clean:
                continue
            
            all_c = set()
            if 'matched_criteria' in group.columns:
                for mc in group['matched_criteria']:
                    all_c.update(parse_criteria_global(mc))
            
            # 計算法說會加分
            conf_date_str = _get_conf_date(stock_clean)
            if conf_date_str:
                try:
                    cdate = pd.to_datetime(conf_date_str).date()
                    delta = (cdate - today).days
                    if 0 <= delta <= 14:
                        all_c.add("兩周內有法說會")
                except:
                    pass
            
            # 計算營收條件加分 (來自手動上傳清單)
            rev_list = st.session_state.get('revenue_stocks', [])
            m_code = re.search(r'(\d{4})', stock_clean)
            if m_code and m_code.group() in rev_list:
                all_c.add("近月營收月增且年增")
            
            group_scores[stock_clean] = len(all_c)
            group_criteria[stock_clean] = all_c
            
            # 紀錄股號
            m = re.search(r'(\d{4})', stock_clean)
            if m: history_stock_set.add(m.group())

        # 3. 排序已有報告的股票
        sorted_stocks = sorted(group_scores.keys(), key=lambda x: group_scores[x], reverse=True)
        
        # 4. 併入「法說會尚無報告」的個股
        conf_map = st.session_state.get('conf_dates_map', {})
            
        for code, date_str in conf_map.items():
            # 字串比對股號，避免重複
            if str(code).strip() not in history_stock_set:
                # 檢查日期，若已過期則不列入
                try:
                    cdate = pd.to_datetime(date_str).date()
                    if cdate < today: continue
                except: pass
                
                # 使用相同的標準化邏輯建立區塊
                full_name = _get_standard_stock_name(code)
                if full_name and full_name not in group_scores:
                    sorted_stocks.append(full_name)
                    group_scores[full_name] = -1 
                    group_criteria[full_name] = set()

        # 5. 併入「營收符合尚無報告」的個股
        rev_list = st.session_state.get('revenue_stocks', [])
        for code in rev_list:
            if str(code).strip() not in history_stock_set:
                full_name = _get_standard_stock_name(code)
                if full_name and full_name not in group_scores:
                    sorted_stocks.append(full_name)
                    group_scores[full_name] = -1 
                    group_criteria[full_name] = set()

        

        # 2. 將同股票的資料 Group 起來並依序加入 consolidated

        for stock in sorted_stocks:
            group = df_raw[df_raw['stock'] == stock]
            if not str(stock).strip():
                continue
                
            # --- [新增過濾] 若法說會已過，且沒有任何券商觀點 (Summary 為空)，則予以移除/不顯示 ---
            conf_date_str = _get_conf_date(stock)
            has_no_summary = True
            placeholder_vals = ["N/A", "NAN", "NONE", "無", "未知", "UNKNOWN", ""]
            
            # 檢查該股是否真的完全沒有任何一筆有效的總結
            if not group.empty:
                for _, r in group.iterrows():
                    sum_val = str(r.get('summary', '')).strip().upper()
                    # 偵測是否為佔位符或自定義的「無內容」說明文字
                    is_placeholder = sum_val in placeholder_vals or any(p in sum_val for p in ["此報告僅提供", "未包含具體", "未提供分析", "無法從中提取", "無法生成", "表格資料"])
                    if not is_placeholder:
                        has_no_summary = False
                        break


            
            if conf_date_str:
                try:
                    cdate_obj = pd.to_datetime(conf_date_str).date()
                    if cdate_obj < today and has_no_summary:
                        continue # 符合已過時且無觀點，跳過此股票
                except: pass

            # --- [處理] 如果這檔股票完全沒有歷史報告資料 (來自法說會清單的缺失股) ---
            if group.empty:

                stock_code_match = re.search(r'\d{4}', str(stock))
                stock_code = stock_code_match.group() if stock_code_match else None
                close_price = get_latest_close_price(stock_code) if stock_code else "N/A"
                
                consolidated.append({
                    "股票名稱/代號": stock,
                    "最新收盤價": close_price,
                    "法說會日期": _get_conf_date(stock),
                    "發布日期": "尚無報告",
                    "券商名稱": "-",
                    "券商評等": "無",
                    "券商目標價": "N/A",
                    "券商預估EPS": "N/A",
                    "重點分析": "", # 依照使用者需求保持空白
                    "平均目標價": "N/A",
                    "平均預估EPS": "N/A",
                    "綜合本益比(PE)": "N/A",
                    "低於20倍PE?": "N/A"
                })
                continue


                

            # 取得最新收盤價 (由於同一股票理論上收盤價相同，取最後一個有效值)

            close_price = "N/A"

            valid_closes = group[group['最新收盤價'] != "N/A"]['最新收盤價']

            if not valid_closes.empty:

                close_price = valid_closes.iloc[-1]

            

            # 計算平均 EPS 與 目標價

            # 將 'N/A' 等轉為 NaN 後計算平均

            valid_eps = pd.to_numeric(group['eps'], errors='coerce').dropna()

            avg_eps = valid_eps.mean() if not valid_eps.empty else None

            

            # 目標價有時候包含貨幣符號，先做一點清理

            def clean_tp(x):

                try:

                    return float(re.sub(r'[^\d.]', '', str(x)))

                except:

                    return None

                    

            valid_tps = group['target_price'].apply(clean_tp).dropna()

            avg_tp = valid_tps.mean() if not valid_tps.empty else None

            

            # 重新計算「綜合本益比」 (基於平均EPS)

            pe_str = "N/A"

            pe_below_20 = "N/A"

            

            if close_price != "N/A" and avg_eps and avg_eps != 0:

                try:

                    pe = round(float(close_price) / float(avg_eps), 2)

                    pe_str = str(pe)

                    pe_below_20 = "✅ 是" if pe < 20 else "❌ 否"

                except:

                    pass

            

            # 使用我們預先計算好的積分

            all_criteria = group_criteria[stock]

            stock_score = group_scores[stock]

            stock_criteria_str = "、".join(all_criteria) if all_criteria else ""

                    

            # 展開各家券商評價紀錄為獨立列，只有第一列顯示共用的股票資訊

            is_first_row = True

            for _, row in group.iterrows():

                if is_first_row:

                    consolidated.append({

                        "股票名稱/代號": stock,

                        "最新收盤價": close_price,

                        "法說會日期": _get_conf_date(stock),

                        "發布日期": row.get('date', '未知日期'),

                        "券商名稱": row.get('brokerage', '未知券商'),

                        "券商評等": row.get('rating', '無'),

                        "券商目標價": row.get('target_price', 'N/A'),

                        "券商預估EPS": row.get('eps', 'N/A'),

                        "重點分析": row.get('summary', '') if str(row.get('summary', '')).strip().upper() not in ["N/A", "NAN", "NONE", "無", "未知", "UNKNOWN", ""] and not any(p in str(row.get('summary', '')) for p in ["此報告僅提供", "未包含具體", "未提供分析", "無法從中提取", "無法生成", "表格資料"]) else "",



                        "平均目標價": round(avg_tp, 2) if avg_tp else "N/A",

                        "平均預估EPS": round(avg_eps, 2) if avg_eps else "N/A",

                        "綜合本益比(PE)": pe_str,

                        "低於20倍PE?": pe_below_20

                    })

                    is_first_row = False

                else:

                    consolidated.append({

                        "股票名稱/代號": "",

                        "最新收盤價": "",

                        "法說會日期": "",

                        "發布日期": row.get('date', '未知日期'),

                        "券商名稱": row.get('brokerage', '未知券商'),

                        "券商評等": row.get('rating', '無'),

                        "券商目標價": row.get('target_price', 'N/A'),

                        "券商預估EPS": row.get('eps', 'N/A'),

                        "重點分析": row.get('summary', '') if str(row.get('summary', '')).strip().upper() not in ["N/A", "NAN", "NONE", "無", "未知", "UNKNOWN", ""] and not any(p in str(row.get('summary', '')) for p in ["此報告僅提供", "未包含具體", "未提供分析", "無法從中提取", "無法生成", "表格資料"]) else "",



                        "平均目標價": "",

                        "平均預估EPS": "",

                        "綜合本益比(PE)": "",

                        "低於20倍PE?": ""

                    })

            

        df_display = pd.DataFrame(consolidated)

        

        if not df_display.empty:

            # 找出所有有效日期中的「最新日期」

            valid_dates = [str(d) for d in df_display.get('發布日期', []) if str(d) != '未知日期' and str(d).strip() != '']

            latest_date = max(valid_dates) if valid_dates else None

            

            # --- 定義高亮函數 ---

            def highlight_latest_row(row):

                # 最新日期的整列加上淡淡的黃色背景

                if latest_date and str(row.get('發布日期')) == latest_date:

                    return ['background-color: rgba(255, 235, 59, 0.15)'] * len(row)

                return [''] * len(row)

                

            def highlight_strong_buy(s):

                # 強力買進標紅加粗

                return ['color: #ff4b4b; font-weight: bold' if isinstance(v, str) and '強力買進' in v else '' for v in s]

                

            # 依序疊加樣式

            styled_df = df_display.style.apply(highlight_latest_row, axis=1)

            if '券商評等' in df_display.columns:

                styled_df = styled_df.apply(highlight_strong_buy, subset=['券商評等'])

        else:

            styled_df = df_display

            

        # 設定選項切換手機端閱讀與電腦端閱讀
        use_mobile_view = st.toggle("📱 開啟手機閱讀模式 (表格自動換行以利閱讀全部重點分析)")

        if use_mobile_view:
            # 在手機閱讀模式下，使用 st.table 搭配 CSS 強制所有文字自動換行顯示
            st.markdown("""
                <style>
                .stTable td, .stTable th {
                    white-space: normal !important;
                    word-break: break-all;
                }
                </style>
            """, unsafe_allow_html=True)
            st.table(styled_df)
        else:
            # 電腦端預設模式：加入 column_config 拉大欄位寬度方便閱讀
            st.dataframe(styled_df, use_container_width=True, column_config={
                "重點分析": st.column_config.TextColumn("重點分析", width="large", help="重點分析內容")
            })

        

        # 建立 CSV 下載按鈕 (加上 BOM 以解決 Excel 中文亂碼)

        csv = df_display.to_csv(index=False).encode('utf-8-sig')

        st.download_button(

            label="📥 下載整合後 CSV 表格",

            data=csv,

            file_name="券商報告整合表.csv",

            mime="text/csv",

            use_container_width=True

        )

        

        # --- 獨立整理：尚無報告的法說會個股 (已整合於上方主表格，此處由主表格呈現) ---
        conf_map = st.session_state.get('conf_dates_map', {})
        if not conf_map:
            st.info("📅 目前無即將到來的法說會紀錄（上傳的資料可能為空或皆已過期）。")
        else:
            # 由於已將資料併入上方主表格，此處僅簡單顯示成功訊息或對齊資訊
            import re as _re
            history_codes = set()
            for s in df_raw['stock'].dropna():
                m = _re.search(r'\d{4}', str(s))
                if m: history_codes.add(m.group())
            missing_codes = [c for c in conf_map.keys() if c not in history_codes]
            if not missing_codes:
                st.success("🎉 太棒了！目前所有已排定的法說會個股，都已經有對應的券商報告！")


        # --- 法說會資料（獨立區塊，不依賴主表格） ---
        st.divider()
        st.subheader("📅 法說會追蹤")
        
        if 'conf_dates_map' not in st.session_state:
            st.session_state['conf_dates_map'] = load_conf_dates()
                
        conf_file = st.file_uploader("上傳法說會日期 Excel（需含『股票代號』與『法說會日期』欄位）", type=['xlsx', 'xls', 'csv'])

        if conf_file:
            try:
                conf_file.seek(0)
                if conf_file.name.endswith('.csv'):
                    try:
                        _conf_df = pd.read_csv(conf_file, encoding='utf-8-sig', dtype=str)
                    except:
                        conf_file.seek(0)
                        _conf_df = pd.read_csv(conf_file, encoding='cp950', dtype=str)
                else:
                    _conf_df = pd.read_excel(conf_file, dtype=str)
                
                import re as _re
                
                # 顯示 Excel 原始內容預覽
                with st.expander("📋 查看上傳的 Excel 原始內容"):
                    st.dataframe(_conf_df.head(5), use_container_width=True)
                
                # 智慧欄位偵測：分析實際值來判斷哪欄是代號、哪欄是日期
                _code_col = None
                _date_col = None
                
                for c in _conf_df.columns:
                    sample = _conf_df[c].dropna().head(5).astype(str).tolist()
                    if not sample:
                        continue
                    
                    # 測試是否為日期欄（包含 / 或 - 且可解析為日期）
                    date_count = sum(1 for v in sample if ('/' in v or '-' in v) and pd.notna(pd.to_datetime(v, errors='coerce')))
                    if date_count >= len(sample) * 0.5 and not _date_col:
                        _date_col = c
                        continue
                    
                    # 測試是否為股票代號欄（4位數字，且不是年份 2020~2030）
                    code_count = 0
                    for v in sample:
                        v_stripped = v.strip()
                        m = _re.search(r'^\d{4}$', v_stripped)
                        if m and not (2020 <= int(m.group()) <= 2030):
                            code_count += 1
                        elif _re.search(r'[\u4e00-\u9fff]', v_stripped) and _re.search(r'\d{4}', v_stripped):
                            code_count += 1
                    if code_count >= len(sample) * 0.5 and not _code_col:
                        _code_col = c
                        continue
                
                # 容錯：如果只偵測到其中一個，另一個取剩餘欄位
                all_cols = list(_conf_df.columns)
                if _code_col and not _date_col:
                    remaining = [c for c in all_cols if c != _code_col]
                    if remaining:
                        _date_col = remaining[0]
                elif _date_col and not _code_col:
                    remaining = [c for c in all_cols if c != _date_col]
                    if remaining:
                        _code_col = remaining[0]
                elif not _code_col and not _date_col:
                    for c in all_cols:
                        if any(k in str(c) for k in ['代號', '股票', '代碼', 'code']) and not _code_col:
                            _code_col = c
                        elif any(k in str(c) for k in ['日期', '法說', 'date', '時間']) and not _date_col:
                            _date_col = c
                    if not _code_col:
                        _code_col = all_cols[0]
                    if not _date_col and len(all_cols) > 1:
                        _date_col = [c for c in all_cols if c != _code_col][0]
                
                st.caption(f"📌 偵測結果：代號欄＝「{_code_col}」, 日期欄＝「{_date_col}」")

                if _code_col and _date_col:
                    today = datetime.datetime.now().date()
                    
                    # 預先偵測名稱欄位（必須含中文字，排除純數字欄）
                    name_col = None
                    for c in _conf_df.columns:
                        if any(k in str(c) for k in ['名稱', '公司', 'name', 'Name']) and c != _code_col and c != _date_col:
                            sample_vals = _conf_df[c].dropna().head(3).astype(str)
                            if any(_re.search(r'[\u4e00-\u9fff]', v) for v in sample_vals):
                                name_col = c
                                break
                    
                    # 一次性下載所有上市櫃公司名稱對照表（帶快取）
                    import json as _json
                    import os as _os
                    STOCK_NAMES_CACHE = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "stock_names.json")
                    _name_map = st.session_state.get("global_name_map", {}).copy()
                    
                    # 嘗試從 API 下載
                    try:
                        _r1 = requests.get("https://openapi.twse.com.tw/v1/opendata/t187ap03_L", timeout=10, verify=False)
                        if _r1.status_code == 200:
                            for d in _r1.json():
                                _name_map[str(d.get('公司代號', '')).strip()] = str(d.get('公司簡稱', '')).strip()
                    except Exception as _e:
                        print(f"TWSE名稱下載失敗: {_e}")
                    try:
                        _r2 = requests.get("https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O", timeout=10, verify=False)
                        if _r2.status_code == 200:
                            for d in _r2.json():
                                code = str(d.get('SecuritiesCompanyCode', '')).strip()
                                name = str(d.get('CompanyAbbreviation', '')).strip()
                                if code and name:
                                    _name_map[code] = name
                    except Exception as _e:
                        print(f"TPEx名稱下載失敗: {_e}")
                    
                    # 下載成功則存檔快取
                    if _name_map:
                        try:
                            with open(STOCK_NAMES_CACHE, 'w', encoding='utf-8') as _f:
                                _json.dump(_name_map, _f, ensure_ascii=False)
                            st.toast(f"✅ 已載入 {len(_name_map)} 間公司名稱！", icon="🏢")
                        except:
                            pass
                    else:
                        # API 失敗，嘗試讀取快取
                        if _os.path.exists(STOCK_NAMES_CACHE):
                            try:
                                with open(STOCK_NAMES_CACHE, 'r', encoding='utf-8') as _f:
                                    _name_map = _json.load(_f)
                                st.toast(f"📂 API 不可用，已從快取載入 {len(_name_map)} 間公司名稱", icon="🏢")
                            except:
                                pass
                        if not _name_map:
                            st.warning("⚠️ 無法下載公司名稱對照表，公司名稱可能為空")
                    
                    if _name_map:
                        st.session_state["global_name_map"] = _name_map
                    
                    display_rows = []
                    unparsed_rows = []
                    _name_map_rev = {v: k for k, v in _name_map.items()} if '_name_map' in locals() and _name_map else {}
                    
                    # 嘗試在整個檔案前 20 列中尋找是否有統一的「全局日期」
                    global_date_found = ""
                    for c in _conf_df.columns:
                        c_str = str(c).strip()
                        m = _re.search(r'(20\d{2})\s*[年/.-]\s*(\d{1,2})\s*[月/.-]\s*(\d{1,2})', c_str)
                        if m:
                            global_date_found = f"{m.group(1)}/{m.group(2)}/{m.group(3)}"
                            break
                        
                    if not global_date_found:
                        for c in _conf_df.columns:
                            for v in _conf_df[c].dropna().head(20).astype(str):
                                m = _re.search(r'(20\d{2})\s*[年/.-]\s*(\d{1,2})\s*[月/.-]\s*(\d{1,2})', v.strip())
                                if m:
                                    global_date_found = f"{m.group(1)}/{m.group(2)}/{m.group(3)}"
                                    break
                            if global_date_found: break
                    
                    all_rows = [dict(zip(_conf_df.columns, _conf_df.columns))] + [row.to_dict() for _, row in _conf_df.iterrows()]
                    for row_dict in all_rows:
                        stock_code = ""
                        raw_code_used = ""
                        # 從整列的所有欄位中尋找可能是股票代號的字串
                        for val in row_dict.values():
                            val_s = str(val).strip()
                            if not val_s or val_s.lower() == 'nan': continue
                            
                            # 測代號 (4位數字，排除像是年份 2026)
                            m = _re.search(r'(?<!\d)(\d{4})(?!\d)', val_s)
                            if m:
                                code_int = int(m.group(1))
                                if 1100 <= code_int <= 9999 and str(code_int)[:2] != '20':
                                    stock_code = m.group(1)
                                    raw_code_used = val_s
                                    break
                            
                            # 測名稱
                            if _name_map_rev:
                                mapped = _name_map_rev.get(val_s, "")
                                if mapped:
                                    stock_code = mapped
                                    raw_code_used = val_s
                                    break
                                # 模糊匹配（前提是該文字長度夠且包含中文，例如 "1307三芳"）
                                if len(val_s) >= 2 and _re.search(r'[\u4e00-\u9fff]', val_s):
                                    for name, cd in _name_map_rev.items():
                                        if val_s in name or name in val_s:
                                            stock_code = cd
                                            raw_code_used = val_s
                                            break
                            if stock_code: break
                        
                        raw_date_used = ""
                        d_parsed = pd.NaT
                        
                        # 從整列中掃描出日期字串（如果上面沒掃到，或者該列獨立有日期）
                        for val in row_dict.values():
                            val_s = str(val).strip()
                            if not val_s or val_s.lower() == 'nan' or val_s == raw_code_used: continue
                            
                            safe_date = val_s[:-2] if val_s.endswith('.0') else val_s
                            # 強制避開單純的 4 位數代碼被pandas誤判為年份
                            if _re.fullmatch(r'\d{4}', safe_date): continue
                            try:
                                test_d = pd.to_datetime(safe_date, errors='coerce')
                                if pd.notna(test_d):
                                    d_parsed = test_d
                                    raw_date_used = val_s
                                    break
                                # 容錯處理：無分隔符的純數字日期 (例如 20260331)
                                if pd.isna(test_d) and len(safe_date) == 8 and safe_date.isdigit():
                                    test_d = pd.to_datetime(safe_date, format='%Y%m%d', errors='coerce')
                                    if pd.notna(test_d):
                                        d_parsed = test_d
                                        raw_date_used = val_s
                                        break
                            except: pass
                        
                        # 當行內找不到獨立日期，如果檔案有全局日期，就自動套用
                        if pd.isna(d_parsed) and global_date_found:
                            d_parsed = pd.to_datetime(global_date_found, errors='coerce')
                            raw_date_used = f"全局日期 ({global_date_found})"
                            
                        # 如果連全區皆無，且當前列無效
                        if not stock_code:
                            # 忽略常規的 header row 或是完全無意義的行
                            dump_str = " | ".join([str(v) for v in row_dict.values() if str(v).lower() != 'nan' and 'unnamed' not in str(v).lower()][:3])
                            if dump_str and not _re.search(r'(代碼|日期|符合條件|策略|序號)', dump_str):
                                unparsed_rows.append({"原列內容 snippet": dump_str, "未抓取原因": "無法找到對應四位數證券代碼"})
                            continue
                            
                        if pd.isna(d_parsed):
                            unparsed_rows.append({"原內容(代號)": raw_code_used, "原內容(日期)": raw_date_used, "未抓取原因": "沒有偵測到有效日期"})
                            continue
                            
                        # 成功抓取！
                        if d_parsed.year == 1900: d_parsed = d_parsed.replace(year=today.year)
                        delta = (d_parsed.date() - today).days
                        
                        company = ""
                        # 優先從偵測到的公司名稱欄位取值
                        if name_col and name_col in row_dict:
                            cell_val = str(row_dict[name_col]).strip()
                            if cell_val and cell_val.lower() != 'nan':
                                company = cell_val
                        
                        if not company:
                            company = _name_map.get(stock_code, "")
                            
                        # 嘗試從原代號中提取名稱
                        if not company:
                            name_part = _re.sub(r'[\d\s\.]+', '', raw_code_used).strip()
                            if name_part and _re.search(r'[\u4e00-\u9fff]', name_part):
                                company = name_part
                                
                        if not company:
                            company = "未知"
                            
                        # 若有找到有效的名稱，則即時更新至字典中，以便後續與系統共用
                        if company != "未知" and stock_code:
                            _name_map[stock_code] = company
                            
                        status_text = "✅ 兩周內" if 0 <= delta <= 14 else ("⏳ 即將到來" if delta > 14 else "⏰ 已結束")
                        display_rows.append({
                            "股票代號": stock_code,
                            "公司名稱": company,
                            "法說會日期": d_parsed.strftime('%Y/%m/%d'),
                            "距今天數": f"{delta} 天",
                            "狀態": status_text
                        })
                    
                    if unparsed_rows:
                        st.warning(f"⚠️ 有 {len(unparsed_rows)} 筆上傳資料在此次判定中無法被系統自動辨識！", icon="⚠️")
                        with st.expander("❌ 展開查看無法辨識的項目明細與可能原因"):
                            st.dataframe(pd.DataFrame(unparsed_rows), use_container_width=True)

                    # 將法說會日期存入 session_state 供歷史表格使用
                    _conf_map = {}
                    
                    # 檔案解析完畢，更新全局公司名稱快取與實體檔案
                    st.session_state["global_name_map"] = _name_map
                    try:
                        with open(STOCK_NAMES_CACHE, 'w', encoding='utf-8') as _f:
                            _json.dump(_name_map, _f, ensure_ascii=False)
                    except: pass
                    
                    for dr in display_rows:
                        code = str(dr['股票代號']).strip()
                        if '✅' in dr['狀態'] or '⏳' in dr['狀態']:  # 只保留未過期的
                            _conf_map[code] = dr['法說會日期']
                    
                    # 如果是新資料，存入並觸發重新整理讓表格顯示
                    old_map = st.session_state.get('conf_dates_map', {})
                    if _conf_map != old_map:
                        st.session_state['conf_dates_map'] = _conf_map
                        save_conf_dates(_conf_map)
                        st.rerun()
                        
                conf_file.seek(0)
            except Exception as e:
                st.warning(f"⚠️ 法說會預覽解析失敗：{e}")
                
        # 無論剛剛有沒有上傳，只要 session 裡面有有效法說會資料，就印出表格
        curr_conf_map = st.session_state.get('conf_dates_map', {})
        if curr_conf_map:
            from datetime import datetime as dt
            today_date = dt.today().date()
            curr_name_map = st.session_state.get('global_name_map', {})
            rebuilt_rows = []
            
            for code, date_str in curr_conf_map.items():
                d = pd.to_datetime(date_str, errors='coerce')
                if pd.notna(d):
                    delta = (d.date() - today_date).days
                    if delta < 0: continue # 自動過濾已過期的資料
                    status_text = "✅ 兩周內" if 0 <= delta <= 14 else ("⏳ 即將到來" if delta > 14 else "⏰ 已結束")
                    
                    rebuilt_rows.append({
                        "股票代號": code,
                        "公司名稱": curr_name_map.get(code, "未知名稱"),
                        "法說會日期": d.strftime('%Y/%m/%d'),
                        "距今天數": f"{delta} 天",
                        "狀態": status_text
                    })
            
            if rebuilt_rows:
                st.markdown(f"##### 📅 法說會清單（共 {len(rebuilt_rows)} 筆）")
                conf_display_df = pd.DataFrame(rebuilt_rows).sort_values(by="法說會日期")
                
                def highlight_conf_row(row):
                    if '✅' in str(row['狀態']): return ['background-color: rgba(76, 175, 80, 0.15)'] * len(row)
                    elif '⏰' in str(row['狀態']): return ['color: #999'] * len(row)
                    return [''] * len(row)
                    
                st.dataframe(conf_display_df.style.apply(highlight_conf_row, axis=1), hide_index=True, use_container_width=True)
                
                # 給予清除快取的選項
                _, col_clear = st.columns([8, 2])
                with col_clear:
                    if st.button("🗑️ 清空歷史法說資料", use_container_width=True):
                        st.session_state['conf_dates_map'] = {}
                        save_conf_dates({})
                        st.rerun()

        # --- 營收與財報條件資料（獨立區塊） ---
        st.divider()
        st.subheader("💰 營收與財報選股名單")
        
        if 'revenue_stocks' not in st.session_state:
            st.session_state['revenue_stocks'] = load_revenue_stocks()
            
        rev_file = st.file_uploader("上傳符合營收條件的 Excel 或 CSV（系統將自動掃描內容中的股票代號）", type=['xlsx', 'xls', 'csv'], key="rev_uploader")
        
        if rev_file:
            try:
                rev_file.seek(0)
                if rev_file.name.endswith('.csv'):
                    try:
                        _rev_df = pd.read_csv(rev_file, encoding='utf-8-sig', dtype=str)
                    except:
                        rev_file.seek(0)
                        _rev_df = pd.read_csv(rev_file, encoding='cp950', dtype=str)
                else:
                    _rev_df = pd.read_excel(rev_file, dtype=str)
                
                import re as _re
                new_rev_stocks = set()
                # 簡單暴力掃描所有欄位抓取四位數台股代號
                for c in _rev_df.columns:
                    for v in _rev_df[c].dropna().astype(str):
                        v_str = v.strip()
                        m = _re.search(r'(?<!\d)(\d{4})(?!\d)', v_str)
                        if m:
                            code_int = int(m.group(1))
                            if 1100 <= code_int <= 9999 and str(code_int)[:2] != '20':
                                new_rev_stocks.add(m.group(1))
                
                if new_rev_stocks:
                    st.success(f"✅ 成功萃取出 {len(new_rev_stocks)} 檔符合營收條件的股票！")
                    curr_rev = set(st.session_state.get('revenue_stocks', []))
                    if curr_rev != new_rev_stocks:
                        st.session_state['revenue_stocks'] = list(new_rev_stocks)
                        save_revenue_stocks(list(new_rev_stocks))
                        st.rerun()
                else:
                    st.warning("⚠️ 檔案中未發現有效股票代號")
            except Exception as e:
                st.warning(f"⚠️ 營收清單解析失敗：{e}")

        curr_rev_list = st.session_state.get('revenue_stocks', [])
        if curr_rev_list:
            st.markdown(f"##### 💰 目前營收條件清單（共 {len(curr_rev_list)} 筆）")
            curr_name_map = st.session_state.get('global_name_map', {})
            rev_display_rows = [{"股票代號": code, "公司名稱": curr_name_map.get(code, "未知名稱")} for code in curr_rev_list]
            st.dataframe(pd.DataFrame(rev_display_rows), hide_index=True)
            
            _, col_clear_rev = st.columns([8, 2])
            with col_clear_rev:
                if st.button("🗑️ 清空營收條件資料", use_container_width=True):
                    st.session_state['revenue_stocks'] = []
                    save_revenue_stocks([])
                    st.rerun()

        # --- 股票搜尋功能 ---

        if not df_display.empty:

            st.divider()

            st.subheader("🔍 個股報告搜尋")

            search_query = st.text_input("輸入股票名稱或代號 (例如：台積電 或 2330)", placeholder="搜尋...")

            

            if search_query.strip():

                # 因為 df_display 畫面上為了美觀，將同股票後續列的名稱設為空字串，所以我們先暫時向下填滿回來做搜尋

                temp_df = df_display.copy()

                temp_df['股票名稱/代號'] = temp_df['股票名稱/代號'].replace('', float('NaN')).ffill()

                

                # 使用 contains 進行不分大小寫的關鍵字模糊搜尋

                mask = temp_df['股票名稱/代號'].astype(str).str.contains(search_query.strip(), case=False, na=False)

                filtered_df = df_display[mask]

                

                if not filtered_df.empty:

                    st.success(f"✅ 找到 {len(filtered_df)} 筆關於「{search_query}」的紀錄：")

                    

                    # 在搜尋結果中找出該次搜尋出現的最新的日期

                    valid_filtered_dates = [str(d) for d in filtered_df.get('發布日期', []) if str(d) != '未知日期' and str(d).strip() != '']

                    local_latest = max(valid_filtered_dates) if valid_filtered_dates else None

                    

                    def highlight_search_latest_row(row):

                        if local_latest and str(row.get('發布日期')) == local_latest:

                            return ['background-color: rgba(255, 235, 59, 0.15)'] * len(row)

                        return [''] * len(row)

                    

                    styled_filtered = filtered_df.style.apply(highlight_search_latest_row, axis=1)

                    

                    # 借用原本的 highlight_strong_buy

                    if '券商評等' in filtered_df.columns:

                        def highlight_strong_buy(s):

                            return ['color: #ff4b4b; font-weight: bold' if isinstance(v, str) and '強力買進' in v else '' for v in s]

                        styled_filtered = styled_filtered.apply(highlight_strong_buy, subset=['券商評等'])

                        

                    st.dataframe(styled_filtered, use_container_width=True)

                else:

                    st.warning(f"⚠️ 目前沒有找到關於「{search_query}」的報告。")

        

        # --- 自動每日選股評分功能 ---

        st.divider()

        st.subheader("🏆 每日選股評分 (自動計算)")

        st.markdown("針對上方表格中的股票，系統會自動根據擷取出來的符合條件，幫您加總積分找出最佳標的。")

        


        cb_default = "3290,2301,6603,4714,1727,3294,8476,3605,4123,8271,8111,3149,7738,8442,4764,1717,4503,5464,8462,5284,4749,1295,3680,4722,8467,2762,2109,6692,4760,6807,2466,8038,3581,8114,3576"
        cb_input = st.text_input("📋 （選填）近期將發行 CB 的股票代號（逗號分隔）", value=cb_default, help="資料來源：https://cbas16889.pscnet.com.tw/marketInfo/expectedRelease/")

        with st.expander("📊 （選填）已發行 CB 轉換資料（股票代號:轉換價:餘額比例%，每行一筆）"):
            cb_issued_default = """1101:35.2:100
1316:17.4:100
1560:284.6:60.36
1609:51.3:100
2455:162.9:100
2464:78.5:98.03
2486:125.2:100
2528:42.4:100
2530:30.2:95.09"""
            cb_issued_input = st.text_area("格式：股票代號:轉換價:餘額比例%", value=cb_issued_default, height=150, help="資料來源：https://cbas16889.pscnet.com.tw/marketInfo/issued/ — 餘額比例>90%表示轉換率<10%")

        daily_pick_btn = st.button("🚀 執行條件積分比對", type="primary", use_container_width=True)

        

        if daily_pick_btn:
            st.info("⚡ 系統正在啟動本機純 Python 量化運算引擎，透過 FinMind、YFinance 與 TDCC 集保結算所即時抓取最新資料！這可能需要 1~2 分鐘，請稍候...")
            
            try:
                progress_bar = st.progress(0)
                status = st.empty()
                
                # 取得畫面上所有不重複的股票代號
                all_raw_stocks = df_display['股票名稱/代號'].replace('', float('NaN')).ffill().dropna().unique()
                valid_stocks = [s for s in all_raw_stocks if str(s).strip() and str(s).upper() != 'NAN']
                
                # 同時將「尚無報告的法說會個股」一併納入量化分析清單
                _conf_map = st.session_state.get('conf_dates_map', {})
                if _conf_map:
                    import re as _re
                    _history_codes = set()
                    for s in df_raw['stock'].dropna():
                        m = _re.search(r'\d{4}', str(s))
                        if m: _history_codes.add(m.group())
                    _missing = [c for c in _conf_map.keys() if c not in _history_codes]
                    _global_names = st.session_state.get("global_name_map", {})
                    for c in _missing:
                        combined_name = f"{c} {_global_names.get(c, '')}".strip()
                        if not any(c in str(vs) for vs in valid_stocks):
                            valid_stocks.append(combined_name)
                
                # 預先下載 TDCC 集保結算所資料 (67K rows)，避免每檔重複下載
                import io as _io
                import os as _os
                tdcc_df = None
                tdcc_prev_df = None
                TDCC_CACHE_FILE = "tdcc_prev.csv"
                
                # 載入上週的 TDCC 快照 (用於比較大戶持股成長)
                status.text("📊 正在載入集保結算所歷史資料...")
                try:
                    if _os.path.exists(TDCC_CACHE_FILE):
                        tdcc_prev_df = pd.read_csv(TDCC_CACHE_FILE)
                        st.toast("📂 已載入上次集保快照作為對照！", icon="📊")
                except Exception as e:
                    print(f"TDCC 快照載入失敗: {e}")
                
                # 下載本週最新 TDCC 資料
                status.text("📊 正在下載集保結算所 (TDCC) 神秘金字塔最新資料...")
                try:
                    tdcc_resp = requests.get("https://smart.tdcc.com.tw/opendata/getOD.ashx?id=1-5",
                                            headers={"User-Agent": "Mozilla/5.0"}, timeout=15, verify=False)
                    if tdcc_resp.status_code == 200:
                        tdcc_content = tdcc_resp.content.decode('utf-8-sig')
                        tdcc_df = pd.read_csv(_io.StringIO(tdcc_content))
                        
                        # 檢查是否與上次不同日期（代表是新一週的資料）
                        curr_date = str(tdcc_df.iloc[0, 0]) if not tdcc_df.empty else ""
                        prev_date = str(tdcc_prev_df.iloc[0, 0]) if tdcc_prev_df is not None and not tdcc_prev_df.empty else ""
                        
                        if curr_date != prev_date:
                            # 新日期！把當前資料存為下次的「上週對照」
                            tdcc_df.to_csv(TDCC_CACHE_FILE, index=False, encoding='utf-8-sig')
                            st.toast(f"✅ 集保資料已更新 ({curr_date})，舊快照 ({prev_date or '無'}) 已保存供比較！", icon="🏛️")
                        else:
                            st.toast(f"✅ 集保資料日期相同 ({curr_date})，使用快取對照。", icon="🏛️")
                except Exception as e:
                    print(f"TDCC 下載失敗: {e}")
                
                # 載入畫面上已自動預解析的法說會日期
                conference_stocks = {}
                conf_map = st.session_state.get('conf_dates_map', {})
                if conf_map:
                    for sid, d_str in conf_map.items():
                        try:
                            d = pd.to_datetime(d_str, errors='coerce')
                            if pd.notna(d):
                                conference_stocks[sid] = d.date()
                        except:
                            pass
                    if conference_stocks:
                        st.toast(f"✅ 套用 {len(conference_stocks)} 檔法說會日期！", icon="📅")
                
                # 解析 CB 股票清單
                cb_stocks = set()
                if cb_input:
                    for code in cb_input.replace(' ', '').split(','):
                        code = code.strip()
                        code_match = re.search(r'\d{4}', code)
                        if code_match:
                            cb_stocks.add(code_match.group())
                
                # 解析已發行 CB 轉換資料
                cb_issued_data = {}
                try:
                    if cb_issued_input:
                        for line in cb_issued_input.strip().split('\n'):
                            parts = line.strip().split(':')
                            if len(parts) >= 3:
                                sid = parts[0].strip()
                                try:
                                    cb_issued_data[sid] = {
                                        'conv_price': float(parts[1].strip()),
                                        'balance_pct': float(parts[2].strip())
                                    }
                                except ValueError:
                                    pass
                except:
                    pass
                
                live_scores = {}
                live_matches = {}
                
                for i, s in enumerate(valid_stocks):
                    status.text(f"🔍 正在爬取並計算 {s} 的即時量化指標...")
                    
                    # 取出真正代號
                    stock_id_match = re.search(r'\d{4}', str(s))
                    if stock_id_match:
                        stock_id = stock_id_match.group()
                        
                        # 呼叫純 Python 的 quant_engine (傳入所有預載資料)
                        matched = evaluate_stock_quant(stock_id, tdcc_df=tdcc_df, tdcc_prev_df=tdcc_prev_df, conference_stocks=conference_stocks, cb_stocks=cb_stocks, cb_issued_data=cb_issued_data)
                        
                        # 條件 11: 目前股價低於 20 倍 PE（從報告表格讀取）
                        try:
                            stock_rows = df_display[df_display['股票名稱/代號'].astype(str).str.contains(stock_id, na=False)]
                            if not stock_rows.empty and '低於20倍PE?' in df_display.columns:
                                pe_vals = stock_rows['低於20倍PE?'].dropna().unique()
                                if any('✅' in str(v) for v in pe_vals):
                                    matched.append("目前股價低於20倍PE")
                        except Exception as e:
                            print(f"PE條件錯誤: {e}")
                        
                        # 條件 12: 券商給予正面評價（從報告表格讀取）
                        try:
                            if not stock_rows.empty and '券商評等' in df_display.columns:
                                ratings = stock_rows['券商評等'].dropna().unique()
                                positive_keywords = ['買進', '買入', '強力買進', 'Buy', 'Outperform', 'Overweight', 
                                                     '優於大盤', '調升', '增持', '推薦', 'Strong Buy', '加碼']
                                for rating in ratings:
                                    if any(kw in str(rating) for kw in positive_keywords):
                                        matched.append(f"券商正面評價({rating})")
                                        break
                        except Exception as e:
                            print(f"評等條件錯誤: {e}")
                        
                        live_scores[s] = len(matched)
                        live_matches[s] = matched
                    else:
                        live_scores[s] = 0
                        live_matches[s] = []
                        
                    progress_bar.progress((i + 1) / len(valid_stocks))
                    
                status.text("✅ Python 量化運算完成！")
                
                scored_auto = [(s, live_scores[s], live_matches[s]) for s in valid_stocks if live_scores[s] > 0]
                scored_auto.sort(key=lambda x: x[1], reverse=True)
                
                if not scored_auto:
                    st.warning("⚠️ 根據即時量化運算結果，目前表格中的股票都沒有符合條件。")
                else:
                    max_score = scored_auto[0][1]
                    champs_auto = [(s, sc, m) for s, sc, m in scored_auto if sc == max_score]
                    others_auto = [(s, sc, m) for s, sc, m in scored_auto if sc < max_score]
                    
                    st.success(f"🎉 最高分 **{max_score}** 分，以下為 Python 量化篩選後的嚴選標的：")
                    
                    cols_auto = st.columns(min(3, len(champs_auto)))
                    for idx, (t_stock, t_score, t_match) in enumerate(champs_auto):
                        c = cols_auto[idx % 3]
                        with c.container(border=True):
                            c.metric(label="🏆 股票代號/名稱", value=t_stock, delta=f"條件數 {t_score}", delta_color="normal")
                            c.markdown("**✅ 達成的純量化條件：**")
                            for m in t_match:
                                c.markdown(f"- {m}")
                            
                            df_h = pd.DataFrame(st.session_state.history)
                            if 'stock' in df_h.columns:
                                recent_sums = df_h[df_h['stock'] == t_stock]['summary'].dropna().unique()
                                valid_sums = [sv for sv in recent_sums if str(sv).strip() and str(sv) not in ['N/A', '無', 'NAN']]
                                if valid_sums:
                                    with c.expander("看近期報告摘要"):
                                        for sv in valid_sums:
                                            st.caption(f"▪️ {sv}")
                    
                    if others_auto:
                        st.write("")
                        with st.expander("👀 其他有符合部份條件的潛力股（按符合條件數排列）"):
                            for r_stock, r_score, r_match in others_auto:
                                st.markdown(f"**{r_stock}** — 符合 {r_score} 項：{' / '.join(r_match)}")
            except Exception as e:
                st.error(f"❌ 執行 Python 量化運算時發生錯誤：{str(e)}")
    else:

        st.info("尚無完整的股票資料可供分析。")


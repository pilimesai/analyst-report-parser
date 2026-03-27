import streamlit as st
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
            st.toast(f"☁️ 成功從 Google Sheets 載入 {len(records)} 筆紀錄！", icon="📂")
            return records
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
            ws.clear()
            if history:
                df = pd.DataFrame(history)
                df = df.fillna("N/A")
                # 將所有列表轉換為以逗號分隔的字串，避免 Google Sheets 報錯 list_value {}
                for col in df.columns:
                    df[col] = df[col].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
                data = [df.columns.values.tolist()] + df.values.tolist()
                
                # gspread v6 之後的語法是 worksheet.update(values=..., range_name=...)
                try:
                    ws.update(values=data, range_name="A1")
                except TypeError:
                    # 兼容舊版語法
                    ws.update(data, "A1")
            st.toast(f"☁️ 已成功將 {len(history)} 筆紀錄同步至 Google Sheets！", icon="💾")
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

def parse_report_with_gemini(text, api_key, source_name="未知來源"):
    client = genai.Client(api_key=api_key)
    
    prompt = f"""
    你是一位專業的金融分析師。請仔細閱讀以下券商分析報告，並精確地提取出以下資訊：
    1. 股票代號與名稱 (例如: 2330 台積電)
    2. 發布該報告的券商名稱 (Brokerage)
    3. 券商給予的評等 (Rating，例如: 買進、中立、買入、Buy、Outperform 等)
    4. 券商預估EPS (Estimated EPS)，請只輸出數字 (如 15.2)。如果在報告中沒有明確給出預估 EPS，請填 "N/A"。
    5. 目標價 (Target Price, TP)，請只輸出數字或貨幣字串，例如 1200 或 $1200。如果沒有給，請填 "N/A"。
    6. 重點分析內容 (Summary)，請用繁體中文，將這篇報告中最核心的看多/看空理由濃縮在 50-100 字以內。
    7. 報告發布日期 (Date)。格式強制轉換為 YYYY-MM-DD。
       👉 第一優先：請觀察這份報告的來源名稱「{source_name}」。如果有連續數字（例如 20231005 或 240325），請直接轉換為 2023-10-05 等。若只有月日（如 1005 或 10月5日），請自動補上今年年份（例如 2024-10-05）。
       👉 第二優先：若名稱中真的毫無日期線索，再從內文中尋找。
       👉 若窮盡一切方法仍找不出日期，才填入 "未知"。
    8. 每日選股 (Daily Stock Selection)。如果報告中有特別推薦為「每日選股」或類似的標的，請填寫相關內容（如「✅ 是」或短評），如果沒有提及，請填 "N/A"。
    9. 選股積分條件 (Stock Scoring Criteria)。請檢查報告中是否提及以下 10 項條件。只要報告中有明確提及、同義詞表達（例如「外資與投信同步買超」、「KD交叉向上」、「出量上漲」），請將其對應的「官方標籤字串」加入陣列中：
       - "投信第一天買且近三月未買" (或提及投信初升段買進、投信破冰首度買進等)
       - "三大法人同買" (或提及外資、投信、自營商聯手買超，法人齊買等)
       - "日KD黃金交叉" (或日KD交叉向上)
       - "周KD黃金交叉" (或周KD交叉向上)
       - "成交量大於十週均量且大於三倍十日均量" (或提及爆量起漲、放量突破等)
       - "合約負債季增50%且創四季新高"
       - "兩周內有法說會" (或近期將舉辦業績發表會、法說行情)
       - "近期將發行CB" (或將發行可轉債)
       - "近月營收月增且年增" (或營收雙增)
       - "大戶持股比例成長" (或千張大戶增加、籌碼集中大戶等)
       （注意：輸出時請一定要輸出上述雙引號內的官方標籤字串，例如 `"三大法人同買"`。如果報�col1, col2 = st.columns([1, 1])
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
            st.error("❌ 密碼错誤，您無權限清除歷史紀錄！")
date": "報告發布日期",
      "stock": "股票代號與名稱",
      "brokerage": "券商名稱",
      "rating": "評等",
      "target_price": "目標價",
      "eps": "券商預估EPS",
      "summary": "重點分析內容",
      "daily_stock_selection": "每日選股",
      "matched_criteria": ["符合的標籤一", "符合的標籤二"]
    }}
    
    以下是內容資料：
    """
    
    try:
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=[prompt, text],
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
        st.error(f"呼叫 API 時發生錯誤: {str(e)}")
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
                
                # 截斷過長的文字 (避免極端情況，雖然 Gemini 可以支援很長)
                text = text[:300000] 
                
                # 2. 呼叫 Gemini 分析
                parsed_data = parse_report_with_gemini(text, api_key, source_name=file.name)
                
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
            
            text_truncated = pasted_text[:300000]
            parsed_data = parse_report_with_gemini(text_truncated, api_key, source_name=pasted_name)
            
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
        df_raw['norm_stock'] = df_raw['stock'].astype(str).str.replace(r'[0-9\W_]', '', regex=True).str.upper()
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
    
    # 進行整合邏輯
    consolidated = []
    
    if 'stock' in df_raw.columns:
        # 1. 預先計算每檔股票的積分，以供排序 (最高分排前面)
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
            
        group_scores = {}
        group_criteria = {}
        
        for stock, group in df_raw.groupby('stock', dropna=False):
            if not str(stock).strip():
                group_scores[stock] = -1
                group_criteria[stock] = set()
                continue
            all_c = set()
            if 'matched_criteria' in group.columns:
                for mc in group['matched_criteria']:
                    all_c.update(parse_criteria_global(mc))
            group_scores[stock] = len(all_c)
            group_criteria[stock] = all_c
            
        # 依照分數由大到小排序股票
        sorted_stocks = sorted(group_scores.keys(), key=lambda x: group_scores[x], reverse=True)
        
        # 2. 將同股票的資料 Group 起來並依序加入 consolidated
        for stock in sorted_stocks:
            group = df_raw[df_raw['stock'] == stock]
            if not str(stock).strip():
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
                        "選股積分": stock_score,
                        "符合條件": stock_criteria_str,
                        "最新收盤價": close_price,
                        "發布日期": row.get('date', '未知日期'),
                        "券商名稱": row.get('brokerage', '未知券商'),
                        "每日選股": row.get('daily_stock_selection', 'N/A'),
                        "券商評等": row.get('rating', '無'),
                        "券商目標價": row.get('target_price', 'N/A'),
                        "券商預估EPS": row.get('eps', 'N/A'),
                        "重點分析": row.get('summary', ''),
                        "平均目標價": round(avg_tp, 2) if avg_tp else "N/A",
                        "平均預估EPS": round(avg_eps, 2) if avg_eps else "N/A",
                        "綜合本益比(PE)": pe_str,
                        "低於20倍PE?": pe_below_20
                    })
                    is_first_row = False
                else:
                    consolidated.append({
                        "股票名稱/代號": "",
                        "選股積分": "",
                        "符合條件": "",
                        "最新收盤價": "",
                        "發布日期": row.get('date', '未知日期'),
                        "券商名稱": row.get('brokerage', '未知券商'),
                        "每日選股": row.get('daily_stock_selection', 'N/A'),
                        "券商評等": row.get('rating', '無'),
                        "券商目標價": row.get('target_price', 'N/A'),
                        "券商預估EPS": row.get('eps', 'N/A'),
                        "重點分析": row.get('summary', ''),
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
            
        # 顯示 Dataframe，設定使用最大寬度，讓多行文字可以展開
        st.dataframe(styled_df, use_container_width=True)
        
        # 建立 CSV 下載按鈕 (加上 BOM 以解決 Excel 中文亂碼)
        csv = df_display.to_csv(index=False).encode('utf-8-sig')
        st.download_button(
            label="📥 下載整合後 CSV 表格",
            data=csv,
            file_name="券商報告整合表.csv",
            mime="text/csv",
            use_container_width=True
        )
        
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
        
        daily_pick_btn = st.button("🚀 執行條件積分比對", type="primary", use_container_width=True)
        
        if daily_pick_btn:
            scored_auto = [(s, group_scores[s], list(group_criteria[s])) for s in group_scores if group_scores[s] > 0]
            scored_auto.sort(key=lambda x: x[1], reverse=True)
            
            if not scored_auto:
                st.info("⚠️ 目前這份報告的分析結果中，沒有股票符合任何一項選股條件。")
            else:
                max_score = scored_auto[0][1]
                champs_auto = [(s, sc, m) for s, sc, m in scored_auto if sc == max_score]
                others_auto = [(s, sc, m) for s, sc, m in scored_auto if sc < max_score]
                
                st.success(f"🎉 最高分 **{max_score}** 分，以下為目前積分最高的嚴選標的：")
                
                cols_auto = st.columns(min(3, len(champs_auto)))
                for idx, (t_stock, t_score, t_match) in enumerate(champs_auto):
                    c = cols_auto[idx % 3]
                    with c.container(border=True):
                        c.metric(label="🏆 股票代號/名稱", value=t_stock, delta=f"總分 {t_score} 分", delta_color="normal")
                        c.markdown("**✅ 達成的條件包括：**")
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
                    with st.expander("👀 其他有符合條件的潛力股（按分數排列）"):
                        for r_stock, r_score, r_match in others_auto:
                            st.markdown(f"**{r_stock}** — {r_score} 分：{' / '.join(r_match)}")

    else:
        st.info("尚無完整的股票資料可供分析。")

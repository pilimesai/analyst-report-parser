import re

def add_search_evaluation():
    with open('d:\\vibe code\\report-analyzer\\app.py', 'rb') as f:
        content = f.read().decode('utf-8', errors='replace')
        
    # --- Part 1: We need to define the function evaluate_stock_with_search ---
    # We can inject it right before `col1, col2 = st.columns([1, 1])` around line 260
    target_split = "col1, col2 = st.columns([1, 1])"
    
    if target_split not in content:
        print("Could not find the injection point for evaluate_stock_with_search")
        return
        
    search_func = """def evaluate_stock_with_search(stock, api_key):
    client = genai.Client(api_key=api_key)
    prompt = f\"\"\"
    你是一位專業的台灣股市分析師。請使用 Google 搜尋，查詢台灣股市「{stock}」的最新財務、籌碼與技術面即時資料。
    請判斷這檔股票【今日】或【近期最新公佈資料】是否確切符合以下 10 項條件。
    如果符合，請將該條件的「標籤字串」放入 matched_criteria 陣列中。如果不確定、查無資料或不符合，請不要放入。
    
    條件清單：
    1. "投信第一天買且近三月未買" (近期投信剛開始買超)
    2. "三大法人同買" (外資、投信、自營商同步買超)
    3. "日KD黃金交叉"
    4. "周KD黃金交叉"
    5. "成交量大於十週均量且大於三倍十日均量" (近期爆量)
    6. "合約負債季增50%且創四季新高" (近期財報合約負債大增)
    7. "兩周內有法說會" (近期即將舉辦法說會)
    8. "近期將發行CB" (近期有發行可轉債計畫)
    9. "近月營收月增且年增" (最新公布的單月營收呈現年月雙增)
    10. "大戶持股比例成長" (近期千張大戶持股比例增加)
    
    請務必以 JSON 格式回應格式如下：
    {{
       "matched_criteria": ["符合的標籤一", "符合的標籤二"]
    }}
    \"\"\"
    
    try:
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt,
            config=types.GenerateContentConfig(
                tools=[{"google_search": {}}],
                response_mime_type="application/json",
                temperature=0.2
            ),
        )
        try:
            data = json.loads(response.text)
            return data.get("matched_criteria", [])
        except:
            return []
    except Exception as e:
        return []

"""
    content = content.replace(target_split, search_func + target_split)
    
    # --- Part 2: Modify the button logic to use the search function ---
    old_logic_start = "if daily_pick_btn:"
    old_logic_end = "st.write(\"\")\n                    with st.expander(\"👀 其他有符合條件的潛力股（按分數排列）\"):\n                        for r_stock, r_score, r_match in others_auto:\n                            st.markdown(f\"**{r_stock}** — {r_score} 分：{' / '.join(r_match)}\")"
    
    
    new_logic = """if daily_pick_btn:
            if not api_key:
                st.warning("⚠️ 請先在左側邊欄輸入你的 Gemini API Key，才能呼叫 AI 上網搜尋！")
            else:
                st.info("🌐 系統正在啟動 AI 網路搜尋引擎，為您逐一上網即時查詢各檔股票的籌碼與財報條件，這可能需要一到三分鐘，請耐心等候...")
                
                search_progress = st.progress(0)
                search_status = st.empty()
                
                # 取得畫面上所有不重複的股票
                all_raw_stocks = df_display['股票名稱/代號'].replace('', float('NaN')).ffill().dropna().unique()
                valid_stocks = [s for s in all_raw_stocks if str(s).strip() and str(s).upper() != 'NAN']
                
                live_scores = {}
                live_matches = {}
                
                for i, s in enumerate(valid_stocks):
                    search_status.text(f"🔍 正在上網蒐集 {s} 的籌碼與財務資料...")
                    
                    # 呼叫具有 Google Search 權限的 Gemini 進行即時查詢
                    matched = evaluate_stock_with_search(s, api_key)
                    if not isinstance(matched, list):
                        matched = []
                        
                    live_scores[s] = len(matched)
                    live_matches[s] = matched
                    
                    search_progress.progress((i + 1) / len(valid_stocks))
                    
                search_status.text("✅ 即時連網搜尋與條件比對完成！")
                
                scored_auto = [(s, live_scores[s], live_matches[s]) for s in valid_stocks if live_scores[s] > 0]
                scored_auto.sort(key=lambda x: x[1], reverse=True)
                
                if not scored_auto:
                    st.warning("⚠️ 根據 AI 剛剛的最新網路搜尋結果，目前表格中的股票都沒有符合任何選股條件。")
                else:
                    max_score = scored_auto[0][1]
                    champs_auto = [(s, sc, m) for s, sc, m in scored_auto if sc == max_score]
                    others_auto = [(s, sc, m) for s, sc, m in scored_auto if sc < max_score]
                    
                    st.success(f"🎉 最高分 **{max_score}** 分，以下為 AI 上網查證後的嚴選標的：")
                    
                    cols_auto = st.columns(min(3, len(champs_auto)))
                    for idx, (t_stock, t_score, t_match) in enumerate(champs_auto):
                        c = cols_auto[idx % 3]
                        with c.container(border=True):
                            c.metric(label="🏆 股票代號/名稱", value=t_stock, delta=f"總分 {t_score} 分", delta_color="normal")
                            c.markdown("**✅ 網路上查證達成的條件包括：**")
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
                                st.markdown(f"**{r_stock}** — {r_score} 分：{' / '.join(r_match)}")"""
                                
    # Use re to replace the old block
    pattern = re.compile(r'if daily_pick_btn:.*?st\.write\(""\)\s*with st\.expander\("👀 其他有符合條件的潛力股（按分數排列）"\):\s*for r_stock, r_score, r_match in others_auto:\s*st\.markdown\(f"\*\*{r_stock}\*\* — {r_score} 分：\{\' / \'\.join\(r_match\)\}"\)', re.DOTALL)
    
    new_content, count = pattern.subn(new_logic, content)
    
    if count == 0:
        print("Warning: Could not find the old block to replace. Debug info:")
        idx = content.find("if daily_pick_btn:")
        if idx != -1:
            print(repr(content[idx:idx+200]))
    else:
        with open('d:\\vibe code\\report-analyzer\\app.py', 'w', encoding='utf-8') as f:
            f.write(new_content)
        print("Successfully implemented AI Web Search Stock Scoring!")

add_search_evaluation()

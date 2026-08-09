import re

def fix_search_function():
    with open('d:\\vibe code\\report-analyzer\\app.py', 'rb') as f:
        content = f.read().decode('utf-8', errors='replace')
        
    start_func = "def evaluate_stock_with_search(stock, api_key):"
    
    # We will find the entire evaluate_stock_with_search body by searching until `col1, col2 = st.columns([1, 1])`
    end_func = "col1, col2 = st.columns([1, 1])"
    
    if start_func not in content or end_func not in content:
        print("Could not find the function boundaries.")
        return
        
    start_idx = content.find(start_func)
    end_idx = content.find(end_func)
    
    new_func = """def evaluate_stock_with_search(stock, api_key):
    client = genai.Client(api_key=api_key)
    
    # 使用 Chain of Thought (CoT) 的方式，讓 AI 有空間整理搜尋結果，大幅提高搜尋準確率
    prompt = f\"\"\"
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
    
    請先寫下你的搜尋過程與判斷，最後請務必在回應的結尾放上一個 JSON 區塊，列出確切符合的官方標籤字串，格式如下：
    ```json
    {{
       "matched_criteria": ["符合的標籤一", "符合的標籤二"]
    }}
    ```
    \"\"\"
    
    try:
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt,
            config=types.GenerateContentConfig(
                tools=[{"google_search": {}}],
                temperature=0.1
            ),
        )
        
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

"""
    
    content = content[:start_idx] + new_func + content[end_idx:]
    with open('d:\\vibe code\\report-analyzer\\app.py', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Successfully rewritten evaluate_stock_with_search to use CoT reasoning!")

fix_search_function()

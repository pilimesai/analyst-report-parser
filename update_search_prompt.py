import re

def update_search_prompt_strict():
    with open('d:\\vibe code\\report-analyzer\\app.py', 'rb') as f:
        content = f.read().decode('utf-8', errors='replace')
        
    old_target = """請先寫下你的搜尋過程與判斷，最後請務必在回應的結尾放上一個 JSON 區塊"""
    
    new_target = """【極度重要警告：防範幻覺與舊資料】
    1. 網路搜尋極容易查到「舊新聞（例如上個月的法人買超新聞）」。你必須非常嚴格地確認資料的「日期」是不是【最近一個交易日】！如果查到的網頁沒有寫明是今天的籌碼，絕對不能當作符合。
    2. 對於「近三月未買」、「創四季新高」這種需要長期歷史數據比對的條件，除非你搜尋到明確的新聞標題或內文直接這樣寫，否則光看一天的數據不能判定符合，請一律當作「不符合」。
    3. 如果查不到明確的即時量化數字，或者有任何一絲不確定，寧可漏判，也【絕對不可】自行臆測或預設為符合。
    
    請先寫下你的搜尋過程與判斷（務必標出你參考的資料日期），最後請務必在回應的結尾放上一個 JSON 區塊"""
    
    if old_target in content:
        content = content.replace(old_target, new_target)
        with open('d:\\vibe code\\report-analyzer\\app.py', 'w', encoding='utf-8') as f:
            f.write(content)
        print("Updated search prompt successfully.")
    else:
        print("Could not find old text to replace.")

update_search_prompt_strict()

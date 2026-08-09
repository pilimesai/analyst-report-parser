import re

def implement_xq_cloud():
    with open('d:\\vibe code\\report-analyzer\\app.py', 'rb') as f:
        content = f.read().decode('utf-8', errors='replace')
        
    # --- Modify the UI to include a file_uploader ---
    ui_find = "daily_pick_btn = st.button(\"🚀 執行條件積分比對\", use_container_width=True)"
    ui_replace = """xq_file = st.file_uploader("📥 請上傳您從 XQ 匯出的盤後精準選股 CSV 檔案 (xq_picks.csv)", type=['csv'])
        daily_pick_btn = st.button("🚀 執行條件積分比對", use_container_width=True)"""
        
    if ui_find in content:
        content = content.replace(ui_find, ui_replace)
    else:
        print("Could not find the button creation line")
        
    # --- Modify the file reading logic ---
    logic_start = "xq_csv_path = os.path.join(BASE_DIR, \"xq_picks.csv\")"
    logic_end = "except:\n                        xq_df = pd.read_csv(xq_csv_path, encoding='cp950', dtype=str)"
    
    logic_replace = """if not xq_file:
                st.error("❌ 找不到 XQ 匯出的檔案！這是因為您的 App 部署在雲端。請將 XQ 的選股結果 CSV 先【上傳到上方的拖曳框】，然後再點擊按鈕。")
                st.info("💡 CSV 欄位格式建議：必須包含 `股票代號` 欄位。對於各項條件可以建立獨立的欄位，若該檔股票符合，請填入 任何文字 或 1，系統會抓取該欄位名稱當作達成的條件！")
            else:
                st.info("📊 系統正在讀取您上傳的 XQ 全球贏家精準盤後籌碼與技術面清單...")
                
                try:
                    # 讀取使用者上傳的 CSV，嘗試多種編碼
                    xq_file.seek(0)
                    try:
                        xq_df = pd.read_csv(xq_file, encoding='utf-8-sig', dtype=str)
                    except:
                        xq_file.seek(0)
                        xq_df = pd.read_csv(xq_file, encoding='cp950', dtype=str)"""
                        
    # Replace the chunk
    pattern = re.compile(re.escape(logic_start) + r".*?" + re.escape("except:\n                        xq_df = pd.read_csv(xq_csv_path, encoding='cp950', dtype=str)"), re.DOTALL)
    
    new_content, count = pattern.subn(logic_replace, content)
    
    if count == 0:
        print("Warning: Could not replace the file reading chunk.")
    else:
        with open('d:\\vibe code\\report-analyzer\\app.py', 'w', encoding='utf-8') as f:
            f.write(new_content)
        print("Successfully adapted XQ integration for Cloud environments via File Uploader!")

implement_xq_cloud()

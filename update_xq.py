import re

def implement_xq_csv():
    with open('d:\\vibe code\\report-analyzer\\app.py', 'rb') as f:
        content = f.read().decode('utf-8', errors='replace')
        
    # We will replace the entire "if daily_pick_btn:" block up to the end of that block.
    # The start is `if daily_pick_btn:`
    # The end is `st.markdown(f"**{r_stock}** — {r_score} 分：{' / '.join(r_match)}")`
    
    start_str = "if daily_pick_btn:"
    end_str = "st.markdown(f\"**{r_stock}** — {r_score} 分：{' / '.join(r_match)}\")"
    
    start_idx = content.find(start_str)
    end_idx = content.find(end_str)
    
    if start_idx == -1 or end_idx == -1:
        print("Could not find boundaries for replacement.")
        return
        
    end_idx += len(end_str)
    
    new_logic = """if daily_pick_btn:
            xq_csv_path = os.path.join(BASE_DIR, "xq_picks.csv")
            
            if not os.path.exists(xq_csv_path):
                st.error("❌ 找不到 XQ 匯出的檔案！請確認您已將 XQ 的選股結果匯出並儲存為本程式資料夾下的 `xq_picks.csv`。")
                st.info("💡 檔案格式建議：包含欄位 `股票代號`。對於各項條件可以建立獨立的欄位，若該檔股票符合該條件，請填入 1、Y、True 或是任何文字，系統會抓取該欄位名稱當作達成的條件！")
            else:
                st.info("📊 系統正在讀取 XQ 全球贏家的精準盤後籌碼與技術面選股清單...")
                
                try:
                    # 讀取 XQ 輸出的 CSV，嘗試多種編碼以防 XQ 預設為 Big5/CP950
                    try:
                        xq_df = pd.read_csv(xq_csv_path, encoding='utf-8-sig', dtype=str)
                    except:
                        xq_df = pd.read_csv(xq_csv_path, encoding='cp950', dtype=str)
                    
                    # 嘗試標準化股票代號欄位名稱
                    stock_col = None
                    for col in xq_df.columns:
                        if '股票代號' in col or '代號' in col or 'Symbol' in col or 'stock' in col.lower():
                            stock_col = col
                            break
                            
                    if not stock_col:
                        st.error("❌ XQ 的 CSV 檔案內找不到代表股票代號的欄位！請確認有一個欄位名稱叫做「股票代號」。")
                    else:
                        xq_df[stock_col] = xq_df[stock_col].astype(str).str.replace(r'[^0-9]', '', regex=True)
                        
                        # 取得畫面上所有不重複的股票代號
                        all_raw_stocks = df_display['股票名稱/代號'].replace('', float('NaN')).ffill().dropna().unique()
                        valid_stocks = [s for s in all_raw_stocks if str(s).strip() and str(s).upper() != 'NAN']
                        
                        live_scores = {}
                        live_matches = {}
                        
                        # 處理 CSV 來找配對
                        for s in valid_stocks:
                            # Parse out numeric stock ID
                            stock_id_match = re.search(r'\d{4}', str(s))
                            if not stock_id_match:
                                live_scores[s] = 0
                                live_matches[s] = []
                                continue
                                
                            stock_id = stock_id_match.group()
                            
                            # XQ 表格裡有沒有這檔
                            matched_row = xq_df[xq_df[stock_col] == stock_id]
                            if matched_row.empty:
                                live_scores[s] = 0
                                live_matches[s] = []
                            else:
                                matched_row = matched_row.iloc[0]
                                conditions_met = []
                                # 將所有「非股票代號」的欄位當成條件欄位來檢查
                                for col in xq_df.columns:
                                    if col != stock_col:
                                        val = str(matched_row[col]).strip().upper()
                                        if val not in ['', 'NAN', '0', 'N/A', 'FALSE', 'N']:
                                            conditions_met.append(col)
                                            
                                live_scores[s] = len(conditions_met)
                                live_matches[s] = conditions_met
                                
                        st.text("✅ XQ 精準籌碼與技術條件比對完成！")
                        
                        scored_auto = [(s, live_scores[s], live_matches[s]) for s in valid_stocks if live_scores[s] > 0]
                        scored_auto.sort(key=lambda x: x[1], reverse=True)
                        
                        if not scored_auto:
                            st.warning("⚠️ 根據 XQ 匯出的精準清單，目前券商報告表格中的股票都沒有符合您設定的 XQ 條件。")
                        else:
                            max_score = scored_auto[0][1]
                            champs_auto = [(s, sc, m) for s, sc, m in scored_auto if sc == max_score]
                            others_auto = [(s, sc, m) for s, sc, m in scored_auto if sc < max_score]
                            
                            st.success(f"🎉 最高分 **{max_score}** 分，以下為 XQ 系統精準比對後的嚴選標的：")
                            
                            cols_auto = st.columns(min(3, len(champs_auto)))
                            for idx, (t_stock, t_score, t_match) in enumerate(champs_auto):
                                c = cols_auto[idx % 3]
                                with c.container(border=True):
                                    c.metric(label="🏆 股票代號/名稱", value=t_stock, delta=f"條件數 {t_score}", delta_color="normal")
                                    c.markdown("**✅ XQ 條件達成：**")
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
                    st.error(f"❌ 讀取 XQ 檔案時發生錯誤：{str(e)}")"""
                    
    content = content[:start_idx] + new_logic + content[end_idx:]
    with open('d:\\vibe code\\report-analyzer\\app.py', 'w', encoding='utf-8') as f:
        f.write(content)
        
    print("Successfully replaced AI search with XQ CSV integration!")

implement_xq_csv()

import re

def swap_to_quant_engine():
    with open('d:\\vibe code\\report-analyzer\\app.py', 'rb') as f:
        content = f.read().decode('utf-8', errors='replace')
        
    start_str = "if daily_pick_btn:"
    end_str = "st.markdown(f\"**{r_stock}** — 符合 {r_score} 項：{' / '.join(r_match)}\")"
    
    start_idx = content.find(start_str)
    end_idx = content.find(end_str)
    if start_idx == -1 or end_idx == -1:
        print("Failed to find boundaries in app.py to swap back.")
        return
        
    end_idx += len(end_str)
    
    # Needs to import the new module at the top if not there
    if "from quant_engine import evaluate_stock_quant" not in content:
        content = content.replace("import yfinance as yf\n", "import yfinance as yf\nfrom quant_engine import evaluate_stock_quant\n")
    
    new_logic = """if daily_pick_btn:
            st.info("⚡ 系統正在啟動本機純 Python 量化運算引擎，透過 FinMind 與 YFinance 即時抓取最新資料！這可能需要 1~2 分鐘，請稍候...")
            
            try:
                progress_bar = st.progress(0)
                status = st.empty()
                
                # 取得畫面上所有不重複的股票代號
                all_raw_stocks = df_display['股票名稱/代號'].replace('', float('NaN')).ffill().dropna().unique()
                valid_stocks = [s for s in all_raw_stocks if str(s).strip() and str(s).upper() != 'NAN']
                
                live_scores = {}
                live_matches = {}
                
                for i, s in enumerate(valid_stocks):
                    status.text(f"🔍 正在爬取並計算 {s} 的即時量化指標...")
                    
                    # 取出真正代號
                    stock_id_match = re.search(r'\d{4}', str(s))
                    if stock_id_match:
                        stock_id = stock_id_match.group()
                        
                        # 呼叫純 Python 的 quant_engine 進行計算
                        matched = evaluate_stock_quant(stock_id)
                        
                        # 因為純 Python API 較難準確抓取以下稀有資料，我們直接放行或標記(也可以選擇忽略)
                        # 目前 `quant_engine` 僅先實作主要的 KD、營收與三大法人
                        missing_conditions = ["合約負債季增50%且創四季新高", "兩周內有法說會", "近期將發行CB", "大戶持股比例成長"]
                        
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
                st.error(f"❌ 執行 Python 量化運算時發生錯誤：{str(e)}")"""
                
    content = content[:start_idx] + new_logic + content[end_idx:]
    with open('d:\\vibe code\\report-analyzer\\app.py', 'w', encoding='utf-8') as f:
        f.write(content)
        
    print("Successfully migrated app.py to the pure Python quant engine.")

swap_to_quant_engine()

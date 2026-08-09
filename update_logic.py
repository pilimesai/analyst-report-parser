import re

def update_manual_to_auto():
    with open('d:\\vibe code\\report-analyzer\\app.py', 'rb') as f:
        content = f.read().decode('utf-8', errors='replace')
        
    start_str = "        # --- 每日選股評分功能 ---"
    
    # We want to replace everything from start_str up to the end of the if-block.
    # The end of the if-block is `    else:\n        st.info("尚無完整的股票資料可供分析。")`
    end_str = "    else:\n        st.info(\"尚無完整的股票資料可供分析。\")"
    
    start_idx = content.find(start_str)
    end_idx = content.find(end_str)
    
    if start_idx == -1 or end_idx == -1:
        print(f"Could not find boundaries. start_idx={start_idx}, end_idx={end_idx}")
        return
        
    new_logic = """        # --- 自動每日選股評分功能 ---
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
                            valid_sums = [sv for sv in recent_sums if str(sv).strip() and str(sv) not in ['N/A', '無']]
                            if valid_sums:
                                with c.expander("看近期報告摘要"):
                                    for sv in valid_sums:
                                        st.caption(f"▪️ {sv}")
                
                if others_auto:
                    st.write("")
                    with st.expander("👀 其他有符合條件的潛力股（按分數排列）"):
                        for r_stock, r_score, r_match in others_auto:
                            st.markdown(f"**{r_stock}** — {r_score} 分：{' / '.join(r_match)}")

"""

    content = content[:start_idx] + new_logic + content[end_idx:]
    
    with open('d:\\vibe code\\report-analyzer\\app.py', 'w', encoding='utf-8') as f:
        f.write(content)
        
    print("Successfully replaced manual stock scoring with automated logic.")
    
update_manual_to_auto()

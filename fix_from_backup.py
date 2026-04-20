content = open('tmp.txt', 'r', encoding='utf-8').read()

# ── Patch 1: parse_report_with_gemini ──────────────────────────────────────
old1 = (
    '    try:\n\n'
    '        response = client.models.generate_content(\n\n'
    "            model='gemini-2.5-flash',\n\n"
    '            contents=[prompt, text],\n\n'
    '            config=types.GenerateContentConfig(\n\n'
    '                response_mime_type="application/json",\n\n'
    '            ),\n\n'
    '        )\n\n'
    '        try:\n\n'
    '            return json.loads(response.text)\n\n'
    '        except json.JSONDecodeError:\n\n'
    '            st.error("JSON 解析失敗，模型回傳的值可能不符預期。")\n\n'
    '            with st.expander("檢視原始回傳內容"):\n\n'
    '                st.write(response.text)\n\n'
    '            return None\n\n'
    '    except Exception as e:\n\n'
    '        st.error(f"呼叫 API 時發生錯誤: {str(e)}")\n\n'
    '        return None'
)

new1 = (
    "    _models_to_try = ['gemini-2.5-flash', 'gemini-2.0-flash']\n"
    '    for _model_name in _models_to_try:\n'
    '        try:\n'
    '            response = client.models.generate_content(\n'
    '                model=_model_name,\n'
    '                contents=[prompt, text],\n'
    '                config=types.GenerateContentConfig(\n'
    '                    response_mime_type="application/json",\n'
    '                ),\n'
    '            )\n'
    '            try:\n'
    '                return json.loads(response.text)\n'
    '            except json.JSONDecodeError:\n'
    '                st.error("JSON 解析失敗，模型回傳的值可能不符預期。")\n'
    '                with st.expander("檢視原始回傳內容"):\n'
    '                    st.write(response.text)\n'
    '                return None\n'
    '        except Exception as e:\n'
    '            err_str = str(e)\n'
    "            if 'RESOURCE_EXHAUSTED' in err_str or '429' in err_str or 'quota' in err_str.lower():\n"
    '                if _model_name != _models_to_try[-1]:\n'
    '                    st.toast(f"⚠️ {_model_name} 額度耗盡，自動切換至備用模型重試...", icon="🔄")\n'
    '                    continue\n'
    '            st.error(f"呼叫 API 時發生錯誤 ({_model_name}): {err_str}")\n'
    '            return None\n'
    '    return None'
)

if old1 in content:
    content = content.replace(old1, new1, 1)
    print('✅ Patch 1 (parse_report_with_gemini): 成功')
else:
    print('❌ Patch 1: 找不到目標字串，嘗試簡單替換...')
    # 簡單替換 model name
    content = content.replace("model='gemini-2.5-flash',\n\n            contents=[prompt, text],", 
                               "model='gemini-2.0-flash',\n\n            contents=[prompt, text],", 1)
    print('  → 已改用 gemini-2.0-flash 作為備案')

# ── Patch 2: evaluate_stock_with_search ────────────────────────────────────
old2 = (
    '    try:\n\n'
    '        response = client.models.generate_content(\n\n'
    "            model='gemini-2.5-flash',\n\n"
    '            contents=prompt,\n\n'
    '            config=types.GenerateContentConfig(\n\n'
    '                tools=[{"google_search": {}}],\n\n'
    '                temperature=0.1\n\n'
    '            ),\n\n'
    '        )'
)

new2 = (
    '    try:\n'
    "        _models_s = ['gemini-2.5-flash', 'gemini-2.0-flash']\n"
    '        response = None\n'
    '        for _mn in _models_s:\n'
    '            try:\n'
    '                response = client.models.generate_content(\n'
    '                    model=_mn,\n'
    '                    contents=prompt,\n'
    '                    config=types.GenerateContentConfig(\n'
    '                        tools=[{"google_search": {}}],\n'
    '                        temperature=0.1\n'
    '                    ),\n'
    '                )\n'
    '                break\n'
    '            except Exception as _e2:\n'
    '                _es2 = str(_e2)\n'
    "                if ('RESOURCE_EXHAUSTED' in _es2 or '429' in _es2 or 'quota' in _es2.lower()) and _mn != _models_s[-1]:\n"
    '                    continue\n'
    '                raise\n'
    '        if response is None:\n'
    '            return []'
)

if old2 in content:
    content = content.replace(old2, new2, 1)
    print('✅ Patch 2 (evaluate_stock_with_search): 成功')
else:
    print('❌ Patch 2: 找不到目標字串，改用備案...')
    content = content.replace("model='gemini-2.5-flash',\n\n            contents=prompt,",
                               "model='gemini-2.0-flash',\n\n            contents=prompt,", 1)
    print('  → 已改用 gemini-2.0-flash 作為備案')

open('app.py', 'w', encoding='utf-8').write(content)
print(f'✅ app.py 寫入完成，總行數: {content.count(chr(10))}')

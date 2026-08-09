import re

def clean_app():
    with open('d:\\vibe code\\report-analyzer\\app.py', 'r', encoding='utf-8') as f:
        content = f.read()
        
    # 1. Remove excessive blank lines (more than 2 consecutive newlines)
    content = re.sub(r'\n{3,}', '\n\n', content)
    
    # 2. Fix the duplicate except block from XQ error
    # We are looking for:
    # except Exception as e:
    #     st.error(f"❌ 執行 Python 量化運算時發生錯誤：{str(e)}")
    # except Exception as e:
    #     st.error(f"❌ 讀取 XQ 檔案時發生錯誤：{str(e)}")
    
    # Using regex to remove any trailing except blocks about XQ
    pattern = re.compile(
        r'(except Exception as e:\s*st\.error\(f"❌ 執行 Python 量化運算時發生錯誤：\{str\(e\)\}"\))\s*except Exception as e:\s*st\.error\(f"❌ 讀取 XQ 檔案時發生錯誤：\{str\(e\)\}"\)',
        re.DOTALL
    )
    
    content = pattern.sub(r'\1', content)
    
    with open('d:\\vibe code\\report-analyzer\\app.py', 'w', encoding='utf-8') as f:
        f.write(content)
        
    print("Cleaned excessive blank lines and fixed syntax!")

clean_app()

import re

def fix_prompt():
    with open('d:\\vibe code\\report-analyzer\\app.py', 'rb') as f:
        content = f.read().decode('utf-8', errors='replace')
        
    pattern = re.compile(r"如果報col1, col2 = st\.columns.+?清除歷史紀錄！\"\)\n*date\"", re.DOTALL)
    replacement_prompt = '''如果報告沒有明確提到上述條件，請回傳空陣列 []。\n\n    請以 JSON 格式回應，格式如下：\n    {{\n      "date"'''
    
    match = pattern.search(content)
    if match:
        content = content[:match.start()] + replacement_prompt + content[match.end()-6:]
        print("Successfully fixed prompt string")
        with open('d:\\vibe code\\report-analyzer\\app.py', 'w', encoding='utf-8') as f:
            f.write(content)
    else:
        print("Warning: Broken prompt string still not found.")
        print("Let's print the relevant substring to debug.")
        idx = content.find("如果報col1")
        if idx != -1:
            print("Found prefix at:", idx)
            print(repr(content[idx:idx+200]))
            idx2 = content.find("歷史紀錄！")
            if idx2 != -1:
                print("Found suffix at:", idx2)
                print(repr(content[idx2:idx2+50]))

fix_prompt()

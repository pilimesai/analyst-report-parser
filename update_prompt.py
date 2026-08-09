import re

def fix_prompt_advanced():
    with open('d:\\vibe code\\report-analyzer\\app.py', 'rb') as f:
        content = f.read().decode('utf-8', errors='replace')
        
    pattern = re.compile(
        r'(例如\s*`"三大法人同買"`。).*?(date"\s*:\s*"報告發布日期")', 
        re.DOTALL
    )
    
    replacement = r'\1如果報告沒有明確提到上述條件，請回傳空陣列 []。）\n\n    請以 JSON 格式回應格式如下：\n    {{\n      "\2'
    
    new_content, count = pattern.subn(replacement, content)
    
    if count > 0:
        with open('d:\\vibe code\\report-analyzer\\app.py', 'w', encoding='utf-8') as f:
            f.write(new_content)
        print("Successfully fixed the prompt garbage string!")
    else:
        print("Could not find the target string to replace.")
        print("Here is a snippet of app.py to help debug:")
        idx = content.find('三大法人同買')
        if idx != -1:
            print(repr(content[idx:idx+150]))
        
fix_prompt_advanced()

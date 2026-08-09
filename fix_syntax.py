def fix():
    with open('d:\\vibe code\\report-analyzer\\app.py', 'r', encoding='utf-8') as f:
        lines = f.readlines()
        
    new_lines = []
    skip = False
    for i, line in enumerate(lines):
        if "讀取 XQ 檔案時發生錯誤" in line:
            # We skip this line and the line before it if it is an except
            if len(new_lines) > 0 and "except Exception as e:" in new_lines[-1]:
                new_lines.pop()
            continue
        new_lines.append(line)
        
    with open('d:\\vibe code\\report-analyzer\\app.py', 'w', encoding='utf-8') as f:
        f.writelines(new_lines)
    print("Fixed syntax error")

fix()

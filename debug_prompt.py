with open('d:\\vibe code\\report-analyzer\\app.py', 'rb') as f:
    text = f.read().decode('utf-8', errors='replace')
    
out = []
idx = text.find('如果報')
out.append(f"Index of 如果報: {idx}")
if idx != -1:
    out.append(repr(text[idx:idx+150]))
    
with open('d:\\vibe code\\report-analyzer\\debug.txt', 'w', encoding='utf-8') as f:
    f.write('\n'.join(out))

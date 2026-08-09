import os
import sys
import json

os.chdir(r"d:\vibe code\report-analyzer")

import app

print("Testing app module loaded.")
print("HISTORY_FILE:", app.HISTORY_FILE)
print("load_history():", app.load_history())

test_data = [{"stock": "2330", "eps": 5.0}]
app.save_history(test_data)
if os.path.exists(app.HISTORY_FILE):
    with open(app.HISTORY_FILE, "r", encoding="utf-8") as f:
        print("Content of history.json:", f.read())
else:
    print("history.json NOT FOUND!")

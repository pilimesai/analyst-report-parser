"""
大戶持股資料解析模組 (major_holder_parser.py)
功能：
1. 支援上傳/輸入 CSV、Excel 或是文字內容。
2. 自動處理各種編碼 (UTF-8, UTF-8-SIG, CP950/Big5)。
3. 自動跳過前綴標題與日期行，定位表格標頭。
4. 自動提取：股票代碼、公司名稱（遇亂碼自動由 stock_names.json 補齊）、大戶持股比例 (%)、收盤價、成交量等。
5. 支援輸出為 Excel (.xlsx)、SQLite 資料庫或 JSON 快取。
"""

import os
import re
import io
import json
import sqlite3
import argparse
import pandas as pd
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STOCK_NAMES_PATH = os.path.join(BASE_DIR, "stock_names.json")

def load_stock_names_map():
    """載入本地股票代碼與公司名稱對照表"""
    if os.path.exists(STOCK_NAMES_PATH):
        try:
            with open(STOCK_NAMES_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def is_garbled(text):
    """判斷文字是否為亂碼、空值或含有替換字符"""
    if not isinstance(text, str):
        return True
    t = text.strip()
    if not t or t.lower() in ["nan", "none", "null", ""]:
        return True
    if "\ufffd" in t or "?" in t:
        return True
    # 檢查是否有正常的繁簡中文
    has_chinese = bool(re.search(r"[\u4e00-\u9fff]", t))
    if not has_chinese and not t.replace(".", "").isalnum():
        return True
    return False

def parse_major_holder_content(content_or_file):
    """
    解析大戶資料，傳入可以是：
    - 檔案路徑 (str)
    - 檔案物件 (io.BytesIO, UploadedFile)
    - 純文字字串 (str)

    回傳：(DataFrame, data_date_str)
    """
    raw_lines = []
    data_date = None

    # 1. 取得行資料 (處理編碼)
    if isinstance(content_or_file, str):
        if os.path.exists(content_or_file):
            for enc in ["utf-8-sig", "cp950", "big5", "utf-8", "gb18030"]:
                try:
                    with open(content_or_file, "r", encoding=enc) as f:
                        raw_lines = f.readlines()
                    break
                except UnicodeDecodeError:
                    continue
        else:
            raw_lines = content_or_file.strip().splitlines()
    elif hasattr(content_or_file, "read"):
        content_bytes = content_or_file.read()
        if hasattr(content_or_file, "seek"):
            content_or_file.seek(0)
        for enc in ["utf-8-sig", "cp950", "big5", "utf-8", "gb18030"]:
            try:
                text = content_bytes.decode(enc)
                raw_lines = text.splitlines()
                break
            except UnicodeDecodeError:
                continue

    if not raw_lines:
        return pd.DataFrame(), None

    # 2. 擷取資料日期 (例如 "資料日期：2026年 10月 30日")
    for line in raw_lines[:10]:
        m_date = re.search(r"(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日", line)
        if m_date:
            y, m, d = m_date.groups()
            data_date = f"{y}-{int(m):02d}-{int(d):02d}"
            break
        m_date2 = re.search(r"(\d{4})[/-](\d{1,2})[/-](\d{1,2})", line)
        if m_date2:
            y, m, d = m_date2.groups()
            data_date = f"{y}-{int(m):02d}-{int(d):02d}"
            break

    # 3. 尋找表格標頭行 (Header)
    header_idx = -1
    for i, line in enumerate(raw_lines[:15]):
        cols = [c.strip().strip('"\'') for c in line.split(",") if c.strip()]
        if len(cols) < 2:
            continue
        line_clean = line.replace(" ", "").replace("\t", "")
        if any(k in line_clean for k in ["代碼", "代號", "Code", "Symbol", "stock"]):
            header_idx = i
            break
        if ("序號" in line_clean or "No" in line_clean) and any(k in line_clean for k in ["商品", "名稱", "大戶"]):
            header_idx = i
            break
        if i + 1 < len(raw_lines) and re.search(r'\d{4}\.TW', raw_lines[i + 1]):
            header_idx = i
            break

    if header_idx == -1:
        for i, line in enumerate(raw_lines):
            if line.count(",") >= 3:
                header_idx = i
                break
        if header_idx == -1:
            header_idx = 0

    csv_data = "\n".join(raw_lines[header_idx:])
    try:
        df = pd.read_csv(io.StringIO(csv_data), skipinitialspace=True, dtype=str)
    except Exception:
        lines = [l.strip() for l in raw_lines[header_idx:] if l.strip()]
        header = [c.strip().strip('"\'') for c in lines[0].split(",")]
        rows = []
        for l in lines[1:]:
            parts = [p.strip().strip('"\'') for p in l.split(",")]
            if len(parts) >= len(header):
                rows.append(parts[:len(header)])
        df = pd.DataFrame(rows, columns=header)

    if df.empty:
        return pd.DataFrame(), data_date

    # 清理欄位名稱
    df.columns = [str(c).strip().strip('"\'') for c in df.columns]

    # 4. 定位關鍵欄位
    code_col = None
    name_col = None
    major_holder_col = None
    price_col = None
    change_col = None
    volume_col = None

    for col in df.columns:
        c_clean = col.replace(" ", "").replace("\t", "")
        if any(k in c_clean for k in ["代碼", "代號", "Code", "Symbol"]):
            code_col = col
        elif any(k in c_clean for k in ["商品", "公司", "名稱", "股票名稱", "Name"]):
            name_col = col
        elif any(k in c_clean for k in ["大戶持股", "大戶比例", "大戶持股比例", "大戶"]):
            major_holder_col = col
        elif any(k in c_clean for k in ["收盤", "成交價", "現價", "Close"]):
            price_col = col
        elif any(k in c_clean for k in ["漲跌%", "漲幅", "漲跌幅", "幅度"]):
            change_col = col
        elif any(k in c_clean for k in ["總量", "成交量", "成交張數", "Volume"]):
            volume_col = col

    # 如果欄位亂碼導致找不到，依據第一列資料內容規則推估
    sample_row = df.iloc[0] if len(df) > 0 else None
    if sample_row is not None:
        for idx, col in enumerate(df.columns):
            val = str(sample_row[col]).strip().strip('"\'')
            if not code_col and re.search(r'^\d{4}(\.TW|\.TWO)?$', val):
                code_col = col
            elif not name_col and idx == 2:
                name_col = col
            elif not major_holder_col and (idx == 6 or ("%" in val and idx >= 4)):
                major_holder_col = col
            elif not price_col and idx == 3:
                price_col = col
            elif not change_col and idx == 4:
                change_col = col
            elif not volume_col and idx == 5:
                volume_col = col

    # 5. 格式化提取標準欄位
    stock_names_map = load_stock_names_map()
    records = []

    for _, row in df.iterrows():
        # 提取股票代號
        code_val = str(row[code_col]).strip().strip('"\'') if code_col else ""
        m_code = re.search(r'(?<!\d)(\d{4})(?!\d)', code_val)
        if not m_code:
            continue
        stock_id = m_code.group(1)

        # 提取公司名稱 (若原欄位為亂碼或空值，由對照表自動補齊)
        raw_name = str(row[name_col]).strip().strip('"\'') if name_col else ""
        if is_garbled(raw_name) or not raw_name:
            company_name = stock_names_map.get(stock_id, raw_name if (raw_name and not is_garbled(raw_name)) else stock_id)
        else:
            company_name = raw_name

        # 提取大戶持股比例
        raw_mh = str(row[major_holder_col]).strip().strip('"\'%') if major_holder_col else ""
        try:
            major_holder_pct = float(raw_mh)
        except ValueError:
            major_holder_pct = None

        # 提取收盤價
        close_price = None
        if price_col:
            try:
                close_price = float(str(row[price_col]).strip().strip('"\'').replace(",", ""))
            except ValueError:
                pass

        # 提取漲跌%
        change_pct = None
        if change_col:
            try:
                change_pct = float(str(row[change_col]).strip().strip('"\'%'))
            except ValueError:
                pass

        # 提取總量
        volume = None
        if volume_col:
            try:
                volume = int(float(str(row[volume_col]).strip().strip('"\'').replace(",", "")))
            except ValueError:
                pass

        records.append({
            "股票代號": stock_id,
            "公司名稱": company_name,
            "大戶持股比例(%)": major_holder_pct,
            "收盤價": close_price,
            "漲跌幅(%)": change_pct,
            "成交量": volume,
            "資料日期": data_date or datetime.today().strftime("%Y-%m-%d")
        })

    result_df = pd.DataFrame(records)
    return result_df, data_date


def export_to_excel(df, output_path="大戶持股分析.xlsx"):
    """將解析後的 DataFrame 輸出為格式化美觀的 Excel 檔案"""
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="大戶持股清單")
        ws = writer.sheets["大戶持股清單"]
        # 設定欄寬
        for col in ws.columns:
            max_len = max(len(str(cell.value or '')) for cell in col)
            col_letter = col[0].column_letter
            ws.column_dimensions[col_letter].width = max(max_len + 4, 14)
    return output_path


def save_to_database(df, db_path="major_holders.db", table_name="major_holders"):
    """將大戶資料儲存至 SQLite 資料庫"""
    if df.empty:
        return 0
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            stock_id TEXT,
            company_name TEXT,
            major_holder_pct REAL,
            close_price REAL,
            change_pct REAL,
            volume INTEGER,
            data_date TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (stock_id, data_date)
        )
    """)
    inserted = 0
    for _, r in df.iterrows():
        cursor.execute(f"""
            INSERT OR REPLACE INTO {table_name} 
            (stock_id, company_name, major_holder_pct, close_price, change_pct, volume, data_date)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            str(r["股票代號"]),
            str(r["公司名稱"]),
            r["大戶持股比例(%)"],
            r["收盤價"],
            r["漲跌幅(%)"],
            r["成交量"],
            str(r["資料日期"])
        ))
        inserted += 1
    conn.commit()
    conn.close()
    return inserted


def main():
    parser = argparse.ArgumentParser(description="大戶持股資料解析工具")
    parser.add_argument("input_file", help="輸入的 CSV 檔案路徑")
    parser.add_argument("--excel", "-e", help="輸出 Excel 檔案路徑", default=None)
    parser.add_argument("--db", "-d", help="儲存至 SQLite 資料庫路徑", default="major_holders.db")
    args = parser.parse_args()

    if not os.path.exists(args.input_file):
        print(f"錯誤：找不到檔案 {args.input_file}")
        return

    df, date_str = parse_major_holder_content(args.input_file)
    print(f"=== 解析完成！共 {len(df)} 筆資料（資料日期：{date_str or '未指定'}）===")
    print(df.to_string(index=False))

    if args.excel:
        export_to_excel(df, args.excel)
        print(f"✅ Excel 匯出完成：{args.excel}")

    if args.db:
        cnt = save_to_database(df, args.db)
        print(f"✅ 資料庫儲存完成：已寫入/更新 {cnt} 筆至 {args.db}")


if __name__ == "__main__":
    main()

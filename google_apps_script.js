/**
 * Google Apps Script 橋接器程式碼
 * 
 * 部署步驟：
 * 1. 在您的 Google 試算表視窗中，點選上方選單的「擴充功能」 -> 「Apps Script」。
 * 2. 清空原本的預設程式碼，將此檔案的所有內容貼上。
 * 3. 點選左上角的「儲存」圖示 (磁碟片)。
 * 4. 點選右上角的「部署」按鈕 -> 「新增部署」。
 * 5. 在選取類型中選擇「網頁應用程式 (Web App)」。
 * 6. 設定：
 *    - 說明：填入任意文字（例如：分析器同步）
 *    - 誰有權限存取：選擇「所有人 (Anyone)」（這非常重要，才能讓網頁能直接寫入數據！）。
 * 7. 點選「部署」後，會要求您授權權限，請點選「授予存取權」並完成 Google 帳號授權。
 * 8. 部署成功後，複製畫面上的「網頁應用程式 URL」並貼回分析器網頁側邊欄的「Google Apps Script 寫入網址」中即可！
 */

function doPost(e) {
  try {
    var payload = JSON.parse(e.postData.contents);
    var action = payload.action || "append_reports";
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    
    if (action === "save_settings") {
      // 儲存設定邏輯
      var settingsSheet = ss.getSheetByName("Settings");
      if (!settingsSheet) {
        settingsSheet = ss.insertSheet("Settings");
      }
      settingsSheet.clear(); // 清空舊設定
      settingsSheet.appendRow(["Key", "Value"]);
      
      var settingsData = payload.data || {};
      for (var key in settingsData) {
        settingsSheet.appendRow([key, settingsData[key]]);
      }
      
      return ContentService.createTextOutput(JSON.stringify({
        status: "success", 
        message: "設定已成功同步至雲端！"
      }))
      .setMimeType(ContentService.MimeType.JSON)
      .setHeader("Access-Control-Allow-Origin", "*");
      
    } else {
      // 原本的新增報告邏輯 (action === "append_reports")
      var sheet = ss.getSheetByName("Reports") || ss.getActiveSheet();
      
      // 如果工作表全空，自動初始化欄位標頭
      if (sheet.getLastRow() === 0) {
        sheet.appendRow([
          "date", 
          "stock", 
          "brokerage", 
          "rating", 
          "target_price", 
          "eps", 
          "summary", 
          "daily_stock_selection", 
          "matched_criteria"
        ]);
      }
      
      var data = payload.data || payload; // 相容舊版陣列
      var records = Array.isArray(data) ? data : [data];
      
      for (var i = 0; i < records.length; i++) {
        var r = records[i];
        var matchedCriteriaStr = Array.isArray(r.matched_criteria) ? r.matched_criteria.join(', ') : (r.matched_criteria || '');
        
        sheet.appendRow([
          r.date || '',
          r.stock || '',
          r.brokerage || '',
          r.rating || '',
          r.target_price || '',
          r.eps || '',
          r.summary || '',
          r.daily_stock_selection || '無',
          matchedCriteriaStr
        ]);
      }
      
      return ContentService.createTextOutput(JSON.stringify({
        status: "success", 
        message: "雲端 Sheets 資料同步成功，共寫入 " + records.length + " 筆！"
      }))
      .setMimeType(ContentService.MimeType.JSON)
      .setHeader("Access-Control-Allow-Origin", "*");
    }
    
  } catch (err) {
    return ContentService.createTextOutput(JSON.stringify({
      status: "error", 
      message: "寫入失敗: " + err.toString()
    }))
    .setMimeType(ContentService.MimeType.JSON)
    .setHeader("Access-Control-Allow-Origin", "*");
  }
}

// 處理 GET 請求，回傳設定資料
function doGet(e) {
  try {
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    var settingsSheet = ss.getSheetByName("Settings");
    var settings = {};
    
    if (settingsSheet) {
      var data = settingsSheet.getDataRange().getValues();
      // 第一行是標頭 (Key, Value)
      for (var i = 1; i < data.length; i++) {
        settings[data[i][0]] = data[i][1];
      }
    }
    
    return ContentService.createTextOutput(JSON.stringify({
      status: "success",
      data: settings
    }))
    .setMimeType(ContentService.MimeType.JSON)
    .setHeader("Access-Control-Allow-Origin", "*");
    
  } catch (err) {
    return ContentService.createTextOutput(JSON.stringify({
      status: "error", 
      message: "讀取失敗: " + err.toString()
    }))
    .setMimeType(ContentService.MimeType.JSON)
    .setHeader("Access-Control-Allow-Origin", "*");
  }
}

// 處理瀏覽器 CORS 預檢 OPTIONS 請求
function doOptions(e) {
  return ContentService.createTextOutput("")
    .setMimeType(ContentService.MimeType.TEXT)
    .setHeader("Access-Control-Allow-Origin", "*")
    .setHeader("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
    .setHeader("Access-Control-Allow-Headers", "Content-Type");
}

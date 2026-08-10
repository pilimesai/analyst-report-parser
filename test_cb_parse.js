const fs = require('fs');

const csvText = `CB名稱,CB代號,代號,TCRI/擔保,發行量(億),主辦券商,賣回條件,年期,轉換溢價率,轉換價,轉換價值,掛牌日
昶昕一,84381,8438,TCRI6/無擔,3,台新證,YTP(3)=(0%),3年,102.02%,90.8,73.35,2026/08/05
雙鍵二,47642,4764,TCRI5/土地銀,5,凱基證,YTP(3)=(0%),3年,102.03%,336.8,68.88,2026/08/05
聯電一,23031,2303,TCRI3/無擔,120,宏遠證,YTP(3)=(0%),5年,111.97%,146,84.25,2026/08/07`;

function testParse() {
    const lines = csvText.split(/\r?\n/);
    let csvRows = [];
    for (let i = 0; i < lines.length; i++) {
        if (!lines[i].trim()) continue;
        const cols = lines[i].split(',').map(c => c.trim().replace(/["']/g, ''));
        csvRows.push(cols);
    }

    const sheetName = '近期發行CB';
    let headerIdx = 0;
    let codeColIdx = -1;
    let nameColIdx = -1;
    let dateColIdx = -1;
    let priceColIdx = -1;

    for (let i = 0; i < Math.min(5, csvRows.length); i++) {
        const row = csvRows[i];
        
        let cIdx = row.findIndex(h => typeof h === 'string' && (h === '代號' || h === '股號' || h === '股票代號' || h === '代碼'));
        if (cIdx === -1) {
            cIdx = row.findIndex(h => typeof h === 'string' && (h.includes('代號') || h.includes('代碼') || h.includes('股號') || h.includes('股票')));
        }

        if (cIdx !== -1) {
            headerIdx = i;
            codeColIdx = cIdx;
            
            nameColIdx = row.findIndex(h => typeof h === 'string' && (h === '名稱' || h === '公司名稱' || h === '股名' || h === '簡稱'));
            if (nameColIdx === -1) {
                nameColIdx = row.findIndex(h => typeof h === 'string' && (h.includes('名稱') || h.includes('公司') || h.includes('股名') || h.includes('簡稱') || h.includes('商品')));
            }

            if (sheetName === '法說會' || sheetName === '近期發行CB') {
                dateColIdx = row.findIndex(h => typeof h === 'string' && (h.includes('日期') || h.includes('時間') || h.includes('掛牌')));
            }
            if (sheetName === '近期發行CB') {
                priceColIdx = row.findIndex(h => typeof h === 'string' && (h.includes('定價') || h.includes('轉換價') || h.includes('價格') || h.includes('發行價')));
            }
            break;
        }
    }

    const newCsvRows = [];
    for (let i = headerIdx; i < csvRows.length; i++) {
        const row = csvRows[i];
        if (i === headerIdx) {
            if (sheetName === '近期發行CB') {
                newCsvRows.push(['股號', '名稱', '轉換價', '掛牌日']);
            }
            continue;
        }
        
        const cCode = row[codeColIdx] || '';
        const cName = row[nameColIdx] || '';
        if (!cCode) continue;

        if (sheetName === '近期發行CB') {
            const cPrice = priceColIdx !== -1 ? (row[priceColIdx] || '') : '';
            const cDate = dateColIdx !== -1 ? (row[dateColIdx] || '') : '';
            newCsvRows.push([cCode, cName, cPrice, cDate]);
        }
    }
    
    console.table(newCsvRows);
}

testParse();

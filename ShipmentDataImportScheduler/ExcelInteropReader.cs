using System.Data;
using ExcelDataReader;

/// <summary>
/// 提供以 ExcelDataReader 讀取 Excel 檔案的輔助方法（非 COM）
/// </summary>
/// <remarks>
/// 使用 ExcelDataReader 可在沒有安裝 Microsoft Excel 的環境中解析 .xls/.xlsx/.xlsm 檔案。
/// 方法會回傳與原本 Excel Interop 相容的 object[,] 結構，保持呼叫端相容性。
/// </remarks>
public static class ExcelInteropReader
{
    #region 靜態建構子
    // 只註冊一次 Encoding provider，避免每次讀檔時重複註冊造成的開銷
    static ExcelInteropReader()
    {
        System.Text.Encoding.RegisterProvider(System.Text.CodePagesEncodingProvider.Instance); // 註冊編碼提供者，支援 ExcelDataReader 解析各種編碼格式
    }
    #endregion

    #region 主要方法 - 讀取 Excel UsedRange
    /// <summary>
    /// 以唯讀模式開啟指定的 Excel 檔案，讀取工作表的 UsedRange 並回傳底層的 Value2 二維陣列。
    /// </summary>
    /// <param name="filePath">要開啟的 Excel 檔案路徑。</param>
    /// <param name="sheetName">可選的工作表名稱；若為 <c>null</c> 或空字串，則讀取第一個工作表。</param>
    /// <param name="waitMsAfterCalc">在呼叫 <c>Calculate()</c> 後等待的毫秒數，預設為 500 毫秒，以便完成計算。</param>
    /// <returns>
    /// 回傳一個以 1 為起始索引的二維 <c>object[,]</c> 陣列，對應到 Excel 的儲存格值；
    /// 若 UsedRange 僅包含單一值，會回傳長度為 [2,2] 的陣列，且該值位於索引 [1,1]。
    /// </returns>
    /// <exception cref="InvalidOperationException">當找不到指定的工作表或 UsedRange 為空時拋出。</exception>
    /// <example>
    /// <code language="csharp">
    /// var arr = ExcelInteropReader.ReadUsedRangeValue2_ReadOnly(@"C:\\sheet.xlsx", "Sheet1");
    /// </code>
    /// </example>
    public static object?[,] ReadUsedRangeValue2_ReadOnly(string filePath, string? sheetName = null, int waitMsAfterCalc = 500)
    {
        if (!File.Exists(filePath)) throw new FileNotFoundException("Excel file not found", filePath);

        // 以 ReadOnly 開啟檔案流，允許其他程序同時開啟（避免鎖定）
        using var stream = File.Open(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        using var reader = ExcelReaderFactory.CreateReader(stream);

        var conf = new ExcelDataSetConfiguration
        {
            ConfigureDataTable = _ => new ExcelDataTableConfiguration { UseHeaderRow = false }
        };

        var dataSet = reader.AsDataSet(conf);

        DataTable table = string.IsNullOrWhiteSpace(sheetName)
            ? (dataSet.Tables.Count == 0 ? throw new InvalidOperationException("找不到工作表") : dataSet.Tables[0])
            : (dataSet.Tables.Contains(sheetName) ? dataSet.Tables[sheetName] ?? throw new InvalidOperationException("找不到工作表") : throw new InvalidOperationException("找不到工作表"));

        int rowCount = table.Rows.Count;
        int colCount = table.Columns.Count;

        // 若整張表完全空（無列無欄），維持舊約定回傳至少 2x2 的陣列
        if (rowCount == 0 && colCount == 0)
        {
            var single = new object?[2, 2];
            single[1, 1] = null;
            return single;
        }

        // ExcelInterop 使用 1-based indexing
        var result = new object?[Math.Max(1, rowCount) + 1, Math.Max(1, colCount) + 1];

        // 快取常用變數以減少屬性存取
        for (int r = 0; r < rowCount; r++)
        {
            var dr = table.Rows[r];
            for (int c = 0; c < colCount; c++)
            {
                var v = dr[c];
                result[r + 1, c + 1] = v == DBNull.Value ? null : v;
            }
        }

        return result;
    }
    #endregion

    #region 主要方法 - 轉換二維陣列為 DataTable
    /// <summary>
    /// 將 Excel Interop 所回傳的二維物件陣列轉換為 <see cref="DataTable"/>。
    /// </summary>
    /// <param name="arr">以 1 為基底索引的二維物件陣列，通常來自 Excel Range.Value2。</param>
    /// <param name="firstRowIsHeader">若為 <c>true</c>，會將陣列第一列視為欄位名稱。</param>
    /// <param name="startRowInclusive">要開始匯入的列（包含此列），以陣列的索引為準。</param>
    /// <param name="endRowInclusive">要結束匯入的列（包含此列），以陣列的索引為準。</param>
    /// <returns>轉換後的 <see cref="DataTable"/>，會略過全為空值的列。</returns>
    /// <remarks>
    /// 注意：Excel Interop 傳回的陣列以 1 為起始索引；本方法會依此方式處理索引。
    /// 空字串會被視為 <see cref="DBNull.Value"/>。若需要不同行為，請在呼叫前處理陣列。
    /// </remarks>
    public static DataTable ConvertObjectArrayToDataTable(object?[,] arr, bool firstRowIsHeader, int startRowInclusive, int endRowInclusive)
    {
        if (arr is null) throw new ArgumentNullException(nameof(arr)); // 檢查輸入陣列是否為 null

        // The incoming array is expected to be 1-based (Excel interop style),
        // i.e. valid indices range from 1..(GetLength(dim)-1).
        // Compute the max valid row/col indexes accordingly.
        int maxRowIndex = Math.Max(0, arr.GetLength(0) - 1); // 計算最大列索引
        int maxColIndex = Math.Max(0, arr.GetLength(1) - 1); // 計算最大欄索引

        var dt = new DataTable();

        if (maxColIndex == 0) return dt; // 沒有欄直接回傳空 DataTable

        if (firstRowIsHeader)
        {
            // 使用 HashSet 快速檢查重複，避免頻繁呼叫 dt.Columns.Contains
            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            for (int c = 1; c <= maxColIndex; c++)
            {
                var hdr = arr[1, c]?.ToString()?.Trim();
                if (string.IsNullOrEmpty(hdr)) hdr = $"C{c}";
                var colName = hdr!;
                int dup = 1;
                while (!seen.Add(colName)) colName = $"{hdr}_{dup++}";
                dt.Columns.Add(colName, typeof(object));
            }
        }
        else
        {
            for (int c = 1; c <= maxColIndex; c++) dt.Columns.Add($"F{c}", typeof(object));
        }

        int s = firstRowIsHeader ? Math.Max(startRowInclusive, 2) : startRowInclusive;
        int end = Math.Min(endRowInclusive, maxRowIndex);

        if (s > end) return dt; // 沒有可處理的列

        // 以陣列暫存每列的欄位值，最後使用 ItemArray 一次性賦值，減少 DataRow 的多次索引
        for (int r = s; r <= end; r++)
        {
            var values = new object[maxColIndex];
            bool allNull = true;
            for (int c = 1; c <= maxColIndex; c++)
            {
                var v = arr[r, c];
                if (v is null)
                {
                    values[c - 1] = DBNull.Value;
                }
                else if (v is string sval)
                {
                    sval = sval.Trim();
                    if (sval.Length == 0) values[c - 1] = DBNull.Value;
                    else { values[c - 1] = sval; allNull = false; }
                }
                else
                {
                    values[c - 1] = v;
                    allNull = false;
                }
            }

            if (!allNull)
            {
                var dr = dt.NewRow();
                dr.ItemArray = values;
                dt.Rows.Add(dr);
            }
        }

        return dt;
    }
    #endregion
}

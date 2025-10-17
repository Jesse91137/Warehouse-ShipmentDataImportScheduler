using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Text.RegularExpressions;

/// <summary>
/// 程式進入點，負責從設定與環境讀取參數、讀取 Excel、並將資料匯入資料庫。
/// </summary>
/// <remarks>
/// 由 <c>Main</c> 控制整個匯入流程：複製檔案至暫存、以 COM 讀取 Excel、分批使用 <see cref="ExcelToSqlBulk"/> 匯入，
/// 並在必要時停用/重建索引以及重設 identity。
/// 所有重要的設定由 `appsettings.json` 或環境變數提供（例如 <c>Excel:SourcePath</c>, <c>Database:ConnectionString</c>）。
/// </remarks>
class Program
{
    // 預編譯並重複使用的 Regex 與已知標頭集合，以減少每次呼叫時的分配與編譯成本
    private static readonly Regex _collapseWhitespace = new Regex("\\s+", RegexOptions.Compiled | RegexOptions.CultureInvariant); // 壓縮多重空白
    private static readonly Regex _trailingSuffix = new Regex("_\\d+$", RegexOptions.Compiled | RegexOptions.CultureInvariant); // 移除尾端自動序號
    private static readonly Regex _gwNwPrefix = new Regex("^(G\\.W\\.|N\\.W\\.)", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.CultureInvariant); // 判斷 G.W./N.W. 開頭
    private static readonly Regex _stripParensSuffix = new Regex("\\s*[\\(（].*?[\\)）]\\s*$", RegexOptions.Compiled | RegexOptions.CultureInvariant); // 移除括號註記
    private static readonly Regex _autoColumnName = new Regex("^Column\\d+$", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.CultureInvariant); // 判斷自動產生欄位名稱

    private static readonly HashSet<string> _canonicalSet = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        "機種","滿箱台數","G.W.(kgs)","N.W.(kgs)","尾箱台數","長寬高","電池標籤","客戶工單","客戶料號","備註"
    }; // 已知標準欄位集合

    /// <summary>
    /// 非同步主程式，執行匯入工作並回傳整數型別的結束碼。
    /// </summary>
    /// <param name="args">可選的命令列參數；若提供，第一個參數會被視為 Excel 檔案來源路徑。</param>
    /// <returns>
    /// 一個表示結果的整數：<c>0</c> 表示成功；其他非零值表示不同類型的錯誤（例如設定缺失或執行失敗）。
    /// </returns>
    /// <remarks>
    /// 方法會將錯誤寫入日誌並以對應的錯誤代碼結束。主要錯誤碼包括：
    /// <list type="bullet">
    /// <item><description>2 - 匯入發生未處理的例外</description></item>
    /// <item><description>3 - 已有其他匯入工作在執行中（無法取得應用鎖）</description></item>
    /// <item><description>4/5/6 - 設定缺失（Excel SourcePath、Database ConnectionString、TargetTable）</description></item>
    /// </list>
    /// </remarks>
    static async Task<int> Main(string[] args)
    {
        // 1. 建立設定來源（appsettings.json 與環境變數）並組建 IConfiguration
        var builder = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory) // 使用執行目錄作為設定檔基底路徑
            .AddJsonFile("appsettings.json", optional: true, reloadOnChange: false) // 從 appsettings.json 載入設定（可選）
            .AddEnvironmentVariables(); // 加入環境變數作為設定來源

        // 2. 處理命令列參數覆寫設定
        if (args is not null && args.Length > 0)
        {
            var overrides = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase); // 建立命令列覆寫字典
            foreach (var a in args)
            {
                if (string.IsNullOrWhiteSpace(a)) continue; // 跳過空白參數
                var s = a.TrimStart('-', '/'); // 移除前綴
                var idx = s.IndexOf('='); // 找到等號位置
                if (idx > 0)
                {
                    var k = s.Substring(0, idx); // 取得 key
                    var v = s.Substring(idx + 1); // 取得 value
                    overrides[k] = v; // 加入覆寫
                }
            }
            if (overrides.Count > 0)
            {
                builder.AddInMemoryCollection(overrides); // 加入命令列覆寫設定
            }
        }

        var config = builder.Build(); // 建構 IConfiguration 實例

        // 3. 建立 loggerFactory 並取得 logger
        using var loggerFactory = LoggerFactory.Create(b => b.AddConsole()); // 建立 loggerFactory 並設定輸出到主控台
        var logger = loggerFactory.CreateLogger<Program>(); // 使用型別化 logger

        // 4. 從設定讀取 Excel 與資料庫相關參數（包含預設值處理）
        string? sourcePath = config["Excel:SourcePath"]; // 讀取 Excel 檔案來源路徑
        string? sheetName = config["Excel:SheetName"]; // 讀取要讀取的工作表名稱
        int waitMs = GetIntConfig(config, "Excel:WaitMsAfterCalc", 500, logger); // 讀取等待時間，預設 500 ms

        // 5. 處理暫存資料夾路徑
        string tempFolder;
        {
            var baseDir = AppContext.BaseDirectory; // 取得執行目錄
            tempFolder = Path.Combine(baseDir, "Temp"); // 預設為執行目錄下 Temp 子資料夾
        }

        string? connStr = config["Database:ConnectionString"]; // 讀取資料庫連線字串
        string? targetTable = config["Database:TargetTable"]; // 讀取目標資料表名稱
        int batchSize = GetIntConfig(config, "Import:BatchSize", 2000, logger); // 讀取批次大小，預設 2000
        int copyRetry = GetIntConfig(config, "Import:CopyRetryCount", 3, logger); // 讀取複製重試次數，預設 3
        int copyRetryBaseDelay = GetIntConfig(config, "Import:CopyRetryBaseDelayMs", 500, logger); // 讀取複製重試基底延遲，預設 500 ms
        bool preserveIdentity = GetBoolConfig(config, "Import:PreserveIdentity", false, logger); // 讀取是否保留 identity，預設 false

        if (args is not null && args.Length > 0) sourcePath = args[0]; // 若有命令列參數則覆寫 sourcePath

        // 6. 驗證必要設定
        if (string.IsNullOrWhiteSpace(sourcePath)) { logger.LogError("Excel SourcePath is not configured."); return 4; } // 檢查來源路徑
        if (string.IsNullOrWhiteSpace(connStr)) { logger.LogError("Database ConnectionString is not configured."); return 5; } // 檢查連線字串
        if (string.IsNullOrWhiteSpace(targetTable)) { logger.LogError("Database TargetTable is not configured."); return 6; } // 檢查目標資料表

        // 7. 建立暫存資料夾
        try
        {
            var baseDir = AppContext.BaseDirectory; // 取得執行目錄
            var systemTemp = Path.GetTempPath(); // 取得系統暫存路徑
            logger.LogInformation("BaseDir={baseDir}, SystemTemp={systemTemp}, TempSetting={setting}, ResolvedTemp={temp}", baseDir, systemTemp, tempFolder, tempFolder); // 記錄路徑
            Directory.CreateDirectory(tempFolder); // 建立暫存資料夾
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Unable to create or access temp folder {t}", tempFolder); // 建立失敗記錄錯誤
            return 2; // 回傳錯誤代碼 2
        }

        string tempFile = string.Empty; // 暫存檔案路徑

        try
        {
            // 8. 複製來源檔案到暫存
            var configuredSource = config["Excel:SourcePath"]; // 取得設定來源
            var copySource = configuredSource;
            if (string.IsNullOrWhiteSpace(copySource))
            {
                copySource = sourcePath; // 若無設定則用命令列來源
            }
            tempFile = Path.Combine(tempFolder, Path.GetFileName(copySource)); // 以來源檔名建立暫存檔名

            logger.LogInformation("Copying {src} -> {dst} (overwrite if exists)", copySource, tempFile); // 記錄複製動作
            await FileHelpers.CopyFileWithRetryAsync(copySource!, tempFile, copyRetry, copyRetryBaseDelay); // 執行複製（含重試）

            // 9. 讀取 Excel
            logger.LogInformation("Reading Excel (COM)..."); // 記錄開始讀取
            var arr = ExcelInteropReader.ReadUsedRangeValue2_ReadOnly(tempFile, sheetName, waitMs); // 以 COM 讀取 UsedRange
            int totalRows = arr.GetLength(0); // 取得總列數
            int cols = arr.GetLength(1); // 取得總欄數
            logger.LogInformation("UsedRange rows={rows}, cols={cols}", totalRows, cols); // 記錄列數與欄數

            // 10. 建立欄位對映（每批次動態建立）

            DbHelpers.AppLockHandle? lockHandle = null; // 應用層鎖把手
            try
            {
                // 11. 取得應用層鎖
                var (handle, rc) = await DbHelpers.AcquireAppLockHandleAsync(connStr, "ImportShipmentDataLock", 0); // 取得 applock
                lockHandle = handle;
                if (lockHandle is null)
                {
                    logger.LogError("Another import is running. Exiting. sp_getapplock rc={rc}", rc); // 若無法取得鎖則記錄錯誤
                    return 3; // 回傳錯誤代碼 3
                }

                List<string> indexNames = new List<string>(); // 索引名稱集合
                long? previousIdent = null; // 先前 identity 值

                // 12. 清空或刪除目標資料表
                previousIdent = await DbHelpers.TruncateOrDeleteAsync(connStr, targetTable, preserveIdentity); // 清空表
                logger.LogInformation("Target {t} truncated/cleared. preserveIdentity={p}", targetTable, preserveIdentity); // 記錄清空結果

                // 13. 取得非聚集索引名稱並停用
                indexNames = await DbHelpers.GetNonClusteredIndexNamesAsync(connStr, targetTable); // 取得索引名稱
                if (indexNames.Count > 0)
                {
                    logger.LogInformation("Disabling {n} indexes...", indexNames.Count); // 記錄停用索引
                    await DbHelpers.DisableIndexesAsync(connStr, targetTable, indexNames); // 停用索引
                }

                var bulk = new ExcelToSqlBulk(connStr); // 建立匯入輔助類別
                bool firstRowIsHeader = true; // 第一列為標題列
                int dataStart = firstRowIsHeader ? 2 : 1; // 資料開始列索引
                int cur = dataStart; // 目前處理列索引
                while (cur <= totalRows) // 迴圈處理所有資料列
                {
                    int end = Math.Min(cur + batchSize - 1, totalRows); // 計算本批次結束列
                    var dt = ExcelInteropReader.ConvertObjectArrayToDataTable(arr, firstRowIsHeader, cur, end); // 轉為 DataTable
                    if (dt.Rows.Count > 0)
                    {
                        logger.LogInformation("Bulk inserting rows {s}..{e} count={c} into {t}", cur, end, dt.Rows.Count, targetTable); // 記錄匯入範圍
                        var columnsMapping = BuildColumnsMappingForDataTable(dt); // 建立欄位對映

                        // 防禦性修正：確保客戶料號右側未命名欄位對映為備註
                        var colsCollection = dt.Columns;
                        int colCount = colsCollection.Count;
                        var dtColumnNames = new string[colCount];
                        for (int i = 0; i < colCount; i++) dtColumnNames[i] = colsCollection[i].ColumnName;

                        for (int ci = 0; ci < dtColumnNames.Length - 1; ci++)
                        {
                            var thisCol = dtColumnNames[ci];
                            var nextCol = dtColumnNames[ci + 1];
                            if (columnsMapping.TryGetValue(thisCol, out var mapped) && string.Equals(mapped, "客戶料號", StringComparison.OrdinalIgnoreCase))
                            {
                                if (!columnsMapping.TryGetValue(nextCol, out var nextMapped) || !string.Equals(nextMapped, "備註", StringComparison.OrdinalIgnoreCase))
                                {
                                    columnsMapping[nextCol] = "備註"; // 強制對映
                                }
                            }
                        }

                        logger.LogInformation("Columns in DataTable: {cols}", string.Join(", ", dtColumnNames)); // 記錄 DataTable 欄位
                        logger.LogInformation("ColumnsMapping: {map}", string.Join(", ", columnsMapping.Select(kv => $"{kv.Key}->{kv.Value}"))); // 記錄欄位對映

                        // 過濾機種為空值的列
                        var machineCols = new List<string>();
                        foreach (var kv in columnsMapping)
                        {
                            if (string.Equals(kv.Value, "機種", StringComparison.OrdinalIgnoreCase) && dt.Columns.Contains(kv.Key))
                                machineCols.Add(kv.Key);
                        }

                        if (machineCols.Count > 0)
                        {
                            var rows = dt.Rows;
                            for (int ri = rows.Count - 1; ri >= 0; ri--)
                            {
                                var row = rows[ri];
                                bool hasMachineValue = false;
                                foreach (var colName in machineCols)
                                {
                                    var val = row[colName];
                                    if (val == null) continue;
                                    if (val is string sVal)
                                    {
                                        if (!string.IsNullOrWhiteSpace(sVal)) { hasMachineValue = true; break; }
                                    }
                                    else
                                    {
                                        hasMachineValue = true; break; // 非字串且不為 null 視為有值
                                    }
                                }
                                if (!hasMachineValue)
                                {
                                    rows.RemoveAt(ri); // 刪除沒有機種的列
                                }
                            }
                        }

                        if (dt.Rows.Count > 0)
                        {
                            await bulk.BulkInsertDataTableAsync(dt, targetTable, columnsMapping, batchSize); // 執行批次匯入
                        }
                    }
                    cur = end + 1; // 前進至下一批
                }

                // 14. 還原 identity
                if (preserveIdentity && previousIdent.HasValue)
                {
                    await DbHelpers.RestoreIdentityAsync(connStr, targetTable, previousIdent.Value); // 還原 identity
                    logger.LogInformation("Identity reseeded to {v}", previousIdent.Value); // 記錄 identity
                }

                // 15. 重建索引
                if (indexNames.Count > 0)
                {
                    logger.LogInformation("Rebuilding {n} indexes...", indexNames.Count); // 記錄重建索引
                    await DbHelpers.RebuildIndexesAsync(connStr, targetTable, indexNames); // 重建索引
                }

                logger.LogInformation("Bulk import complete."); // 記錄匯入完成
            }
            finally
            {
                if (lockHandle is not null)
                {
                    try { await lockHandle.DisposeAsync(); } catch { } // 釋放 applock
                }
            }

            return 0; // 成功回傳 0
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Import failed"); // 記錄匯入失敗
            return 2; // 回傳錯誤代碼 2
        }
        finally
        {
            FileHelpers.SafeDelete(tempFile, logger); // 嘗試刪除暫存檔
        }
    }

    /// <summary>
    /// 建立 DataTable 欄位名稱到目標資料庫欄位名稱的對映字典。
    /// </summary>
    /// <param name="dt">來源 DataTable。</param>
    /// <returns>來源欄位名稱到目標欄位名稱的字典。</returns>
    private static Dictionary<string, string> BuildColumnsMappingForDataTable(System.Data.DataTable dt)
    {
        var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase); // 建立對映字典
        int gwCount = 0, nwCount = 0; // G.W./N.W. 計數器
        var canonicalSetRef = _canonicalSet; // 標準欄位集合參考
        var cols = dt.Columns;
        int colCount = cols.Count;
        var keys = new string[colCount]; // 欄位名稱陣列
        var norms = new string[colCount]; // 標準化名稱陣列
        var isUnnamed = new bool[colCount]; // 是否未命名欄位陣列

        for (int i = 0; i < colCount; i++)
        {
            var src = (cols[i] as System.Data.DataColumn)?.ColumnName ?? string.Empty; // 取得欄位名稱
            keys[i] = src;

            var firstLine = src; // 取第一行（若有換行）
            int rIdx = src.IndexOf('\r');
            int nIdx = src.IndexOf('\n');
            int cut = -1;
            if (rIdx >= 0) cut = rIdx;
            if (nIdx >= 0 && (nIdx < cut || cut == -1)) cut = nIdx;
            if (cut >= 0) firstLine = src.Substring(0, cut);

            var norm = _collapseWhitespace.Replace(firstLine, " ").Trim(); // 壓縮空白
            norm = _trailingSuffix.Replace(norm, string.Empty); // 移除尾端序號

            if (!_gwNwPrefix.IsMatch(norm))
            {
                norm = _stripParensSuffix.Replace(norm, string.Empty).Trim(); // 移除括號註記
            }

            var sp = norm.IndexOf(' ');
            var firstToken = sp >= 0 ? norm.Substring(0, sp) : norm;
            if (canonicalSetRef.Contains(firstToken)) norm = firstToken; // 若在標準集合則用 token

            norms[i] = norm;
            isUnnamed[i] = string.IsNullOrWhiteSpace(src) || string.IsNullOrWhiteSpace(norm) || _autoColumnName.IsMatch(norm); // 判斷是否未命名
        }

        for (int i = 0; i < colCount; i++)
        {
            var key = keys[i];
            var norm = norms[i];

            if (string.Equals(norm, "G.W.(kgs)", StringComparison.OrdinalIgnoreCase))
            {
                gwCount++;
                map[key] = gwCount == 1 ? "G.W.(kgs)滿" : "G.W.(kgs)尾"; // G.W. 欄位對映
                continue;
            }

            if (string.Equals(norm, "N.W.(kgs)", StringComparison.OrdinalIgnoreCase))
            {
                nwCount++;
                map[key] = nwCount == 1 ? "N.W.(kgs)滿" : "N.W.(kgs)尾"; // N.W. 欄位對映
                continue;
            }

            if (!map.ContainsKey(key)) map[key] = norm; // 預設使用標準化名稱
        }

        for (int i = 0; i < colCount - 1; i++)
        {
            if (string.Equals(norms[i], "客戶料號", StringComparison.OrdinalIgnoreCase) && isUnnamed[i + 1])
            {
                map[keys[i + 1]] = "備註"; // 客戶料號右側未命名欄位對映為備註
            }
        }

        return map;
    }

    /// <summary>
    /// 取得 int 設定的安全 helper（會在解析失敗時回傳預設值並寫入警告）
    /// </summary>
    /// <param name="config">設定來源。</param>
    /// <param name="key">設定鍵值。</param>
    /// <param name="defaultValue">預設值。</param>
    /// <param name="logger">日誌器。</param>
    /// <returns>解析後的 int 值。</returns>
    private static int GetIntConfig(IConfiguration config, string key, int defaultValue, ILogger logger)
    {
        var s = config[key]; // 取得設定值
        if (string.IsNullOrWhiteSpace(s)) return defaultValue; // 空值回傳預設
        if (int.TryParse(s, out var v)) return v; // 解析成功回傳值
        logger.LogWarning("Configuration key {k} has invalid integer value '{v}', using default {d}", key, s, defaultValue); // 解析失敗記錄警告
        return defaultValue;
    }

    /// <summary>
    /// 取得 bool 設定的安全 helper（會在解析失敗時回傳預設值並寫入警告）
    /// </summary>
    /// <param name="config">設定來源。</param>
    /// <param name="key">設定鍵值。</param>
    /// <param name="defaultValue">預設值。</param>
    /// <param name="logger">日誌器。</param>
    /// <returns>解析後的 bool 值。</returns>
    private static bool GetBoolConfig(IConfiguration config, string key, bool defaultValue, ILogger logger)
    {
        var s = config[key]; // 取得設定值
        if (string.IsNullOrWhiteSpace(s)) return defaultValue; // 空值回傳預設
        if (bool.TryParse(s, out var v)) return v; // 解析成功回傳值
        if (s == "0") return false; // 支援 0
        if (s == "1") return true; // 支援 1
        logger.LogWarning("Configuration key {k} has invalid boolean value '{v}', using default {d}", key, s, defaultValue); // 解析失敗記錄警告
        return defaultValue;
    }
}
// Program end

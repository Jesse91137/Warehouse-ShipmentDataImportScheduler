using System.Text.RegularExpressions;
using System.Data;
using Microsoft.Data.SqlClient;
using System.Globalization;

/// <summary>
/// 提供將從 Excel 轉換而來的 <see cref="DataTable"/> 使用 <see cref="SqlBulkCopy"/> 匯入到 SQL Server 的功能。
/// </summary>
/// <remarks>
/// 這個類別會嘗試自動推斷欄位的目標型別（整數、十進位或日期），並在匯入前轉換欄位型別。
/// 使用者需提供有效的連線字串且目標資料表需已存在。若需要欄位對應，可透過 <paramref name="columnsMapping"/> 指定。
/// </remarks>
public class ExcelToSqlBulk
{
    private readonly string _connectionString; // SQL Server 連線字串
    // 預編譯 Regex 與共用 CultureInfo/NumberStyles，減少在熱路徑的分配與重複計算
    private static readonly Regex _cellRefRegex = new Regex("^[A-Za-z]{1,3}\\d{1,4}$", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.CultureInvariant); // 判斷 Excel 自動產生的欄位名稱
    private static readonly Regex _autoColumnRegex = new Regex("^Column\\d+$", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.CultureInvariant); // 判斷自動產生的欄位名稱
    private static readonly CultureInfo _invariant = CultureInfo.InvariantCulture; // 使用不變文化設定

    /// <summary>
    /// 初始化新的 <see cref="ExcelToSqlBulk"/> 類別實例。
    /// </summary>
    /// <param name="connectionString">用於連線到 SQL Server 的連線字串。</param>
    public ExcelToSqlBulk(string connectionString) => _connectionString = connectionString; // 建構子，儲存連線字串

    /// <summary>
    /// 非同步將 <paramref name="table"/> 的資料透過 <see cref="SqlBulkCopy"/> 匯入到指定的目標資料表。
    /// </summary>
    /// <param name="table">要匯入的資料表，不能為 <see langword="null"/>。</param>
    /// <param name="targetTableName">目標的資料表名稱，可以包含 schema，如 <c>dbo.MyTable</c>。</param>
    /// <param name="columnsMapping">可選的來源欄位到目標欄位名稱對照字典（來源欄位名 -> 目標欄位名）。</param>
    /// <param name="batchSize">每一批次的列數，預設為 2000；實際使用時會與表列數比較以避免為 0。</param>
    /// <returns>一個表示非同步操作的 <see cref="Task"/>。</returns>
    /// <exception cref="ArgumentNullException">當 <paramref name="table"/> 為 <see langword="null"/> 時拋出。</exception>
    /// <exception cref="InvalidOperationException">當對應的目的欄位在資料庫中找不到時拋出。</exception>
    public async Task BulkInsertDataTableAsync(DataTable table, string targetTableName, Dictionary<string, string>? columnsMapping = null, int batchSize = 2000)
    {
        if (table == null) throw new ArgumentNullException(nameof(table)); // 檢查資料表是否為 null

        // 新增異動時間欄位，記錄匯入時間
        // 確保目標欄位存在且為 DateTime，盡量一次取得 now 減少重複呼叫
        const string modColName = "異動時間";
        var nowUtc = DateTime.Now;
        var existingCol = table.Columns.Contains(modColName) ? table.Columns[modColName] : null;
        if (existingCol == null)
        {
            var modCol = new DataColumn(modColName, typeof(DateTime)); // 建立 DateTime 欄位
            table.Columns.Add(modCol);
            foreach (DataRow r in table.Rows) r[modCol] = nowUtc;
        }
        else if (existingCol.DataType != typeof(DateTime))
        {
            // 若型別不正確，建立新欄位並填入時間，再置換回原位置
            var ord = existingCol.Ordinal;
            var tempName = modColName + "_tmp";
            var newCol = new DataColumn(tempName, typeof(DateTime));
            table.Columns.Add(newCol);
            for (int i = 0; i < table.Rows.Count; i++) table.Rows[i][newCol] = nowUtc;
            try { table.Columns.Remove(existingCol); } catch { }
            newCol.ColumnName = modColName;
            newCol.SetOrdinal(ord);
        }
        else
        {
            // 已為 DateTime，只填補空值
            foreach (DataRow r in table.Rows) if (r.IsNull(modColName)) r[modColName] = nowUtc;
        }

        // 轉換欄位型別（自動推斷 int/decimal/DateTime）
        ConvertColumnsToTargetTypes(table);

        using var conn = new SqlConnection(_connectionString); // 建立 SQL 連線
        await conn.OpenAsync(); // 開啟連線

        await ValidateMappingsAsync(conn, targetTableName, table, columnsMapping); // 驗證欄位對應

        // 預先解析來源欄 -> 目的欄對應
        var resolvedMapping = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (DataColumn col in table.Columns)
        {
            var src = col.ColumnName ?? string.Empty;
            resolvedMapping[src] = ResolveDestinationColumn(src, columnsMapping); // 解析對應
        }

        using var tran = conn.BeginTransaction(); // 開啟交易
        try
        {
            using var bulk = new SqlBulkCopy(conn, SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.KeepNulls, tran)
            {
                DestinationTableName = targetTableName, // 設定目標資料表
                BatchSize = Math.Min(batchSize, Math.Max(1, table.Rows.Count)), // 設定批次大小
                BulkCopyTimeout = 0 // 不限時間
            };

            foreach (DataColumn col in table.Columns)
            {
                string src = col.ColumnName;
                if (!resolvedMapping.TryGetValue(src, out var dest)) dest = src;
                bulk.ColumnMappings.Add(src, dest); // 加入欄位對應
            }

            await bulk.WriteToServerAsync(table); // 執行批次匯入
            tran.Commit(); // 提交交易
        }
        catch
        {
            try { tran.Rollback(); } catch { } // 回滾交易
            throw;
        }
    }

    /// <summary>
    /// 嘗試根據每個欄位的內容推斷其最合適的目標型別（int、decimal 或 DateTime），
    /// 並在必要時建立新的欄位並將資料轉換後取代原欄位。
    /// </summary>
    /// <param name="table">需被檢查與轉換欄位的 <see cref="DataTable"/>。</param>
    /// <remarks>
    /// 本方法會掃描整個欄位的每一列以決定欄位是否可完全被某種型別表示，
    /// 若成功會新增臨時欄位轉換資料後再移除原欄位。
    /// </remarks>
    private void ConvertColumnsToTargetTypes(DataTable table)
    {
        // 第一階段：偵測哪些欄位需要轉型
        var conversions = new List<(string columnName, Type targetType)>();
        const int sampleLimit = 200; // 每欄最多採樣 200 筆資料
        foreach (DataColumn col in table.Columns)
        {
            // 若欄位已經是正確型別，跳過檢測（避免不必要的字串解析）
            var currentType = col.DataType;
            if (currentType == typeof(int) || currentType == typeof(decimal) || currentType == typeof(DateTime))
                continue;
            bool allInt = true; // 是否全為整數
            bool allDecimal = true; // 是否全為十進位
            bool allDate = true; // 是否全為日期
            int samples = 0; // 採樣計數
            var rows = table.Rows;
            int rc = rows.Count;

            // 逐列採樣
            for (int r = 0; r < rc; r++)
            {
                var v = rows[r][col];
                if (v == DBNull.Value || v == null) continue;
                // 盡可能避免分配，先嘗試直接轉型再 fallback 到 ToString()
                string? s = v as string ?? v.ToString();
                if (string.IsNullOrWhiteSpace(s)) continue;
                s = s.Trim();

                samples++;
                if (!int.TryParse(s, NumberStyles.Integer, _invariant, out _)) allInt = false;
                if (!decimal.TryParse(s, NumberStyles.Number, _invariant, out _)) allDecimal = false;
                if (!DateTime.TryParse(s, _invariant, DateTimeStyles.None, out _)) allDate = false;
                if (!allInt && !allDecimal && !allDate) break;
                if (samples >= sampleLimit) break; // 採樣足夠即停止
            }

            if (samples == 0) continue; // 無資料不推斷
            if (allInt) conversions.Add((col.ColumnName, typeof(int))); // 全為整數
            else if (allDecimal) conversions.Add((col.ColumnName, typeof(decimal))); // 全為十進位
            else if (allDate) conversions.Add((col.ColumnName, typeof(DateTime))); // 全為日期
        }

        // 第二階段：執行型別轉換
        foreach (var conv in conversions)
        {
            var oldCol = table.Columns[conv.columnName];
            if (oldCol == null) continue;

            var tempName = conv.columnName + "_tmp";
            var newCol = new DataColumn(tempName, conv.targetType); // 建立新欄位
            int ord = oldCol.Ordinal; // 保留原欄位順序
            table.Columns.Add(newCol);
            var rows2 = table.Rows;
            int rc2 = rows2.Count;
            for (int r = 0; r < rc2; r++)
            {
                var row = rows2[r];
                var v = row[oldCol];
                if (v == DBNull.Value || v == null)
                {
                    row[newCol] = DBNull.Value;
                    continue;
                }

                var s = v as string ?? v.ToString();
                s = s?.Trim() ?? string.Empty;
                try
                {
                    if (conv.targetType == typeof(int))
                    {
                        if (int.TryParse(s, NumberStyles.Integer, _invariant, out var vi))
                            row[newCol] = vi;
                        else row[newCol] = DBNull.Value;
                    }
                    else if (conv.targetType == typeof(decimal))
                    {
                        if (decimal.TryParse(s, NumberStyles.Number, _invariant, out var vd))
                            row[newCol] = vd;
                        else row[newCol] = DBNull.Value;
                    }
                    else if (conv.targetType == typeof(DateTime))
                    {
                        if (DateTime.TryParse(s, _invariant, DateTimeStyles.None, out var dt))
                            row[newCol] = dt;
                        else row[newCol] = DBNull.Value;
                    }
                }
                catch
                {
                    row[newCol] = DBNull.Value;
                }
            }

            // 移除舊欄位，並將新欄位改回原名與順序
            table.Columns.Remove(oldCol);
            newCol.ColumnName = conv.columnName;
            newCol.SetOrdinal(ord);
        }
    }

    /// <summary>
    /// 驗證來源欄位（或透過 <paramref name="columnsMapping"/> 對應後的目標欄位）是否都存在於目標資料表中。
    /// </summary>
    /// <param name="conn">已開啟的 <see cref="SqlConnection"/> 實例。</param>
    /// <param name="targetTableName">目標資料表名稱，可能包含 schema。</param>
    /// <param name="table">來源的 <see cref="DataTable"/>，用於逐一檢查來源欄位。</param>
    /// <param name="columnsMapping">來源至目標欄位名稱的對照字典（可為 <see langword="null"/>）。</param>
    /// <returns>一個表示驗證完成的非同步 <see cref="Task"/>。</returns>
    /// <exception cref="InvalidOperationException">當目標資料表中找不到某個必需欄位時拋出。</exception>
    private async Task ValidateMappingsAsync(SqlConnection conn, string targetTableName, DataTable table, Dictionary<string, string>? columnsMapping)
    {
        var existingCols = new HashSet<string>(StringComparer.OrdinalIgnoreCase); // 取得目標表所有欄位
        using (var cmd = conn.CreateCommand())
        {
            var tn = targetTableName.Trim();
            var parts = tn.Replace("[", "").Replace("]", "").Split('.');
            var tbl = parts.Length == 2 ? parts[1] : parts[0];
            cmd.CommandText = $"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @table";
            cmd.Parameters.AddWithValue("@table", tbl);
            using var r = await cmd.ExecuteReaderAsync();
            while (await r.ReadAsync()) existingCols.Add(r.GetString(0));
        }

        // 建立正規化名稱查詢，便於提示建議
        static string NormalizeName(string s)
        {
            if (string.IsNullOrEmpty(s)) return string.Empty;
            var sb = new System.Text.StringBuilder(s.Length);
            foreach (var ch in s)
            {
                if (char.IsLetterOrDigit(ch)) sb.Append(char.ToLowerInvariant(ch));
            }
            return sb.ToString();
        }

        var existingList = new List<string>(existingCols);
        var normalizedMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var ec in existingList)
        {
            var n = NormalizeName(ec);
            if (!normalizedMap.ContainsKey(n)) normalizedMap[n] = ec;
        }

        // 收集需移除的自動產生或空欄位
        var toRemove = new List<DataColumn>();
        var missingCritical = new List<(string dest, string message)>();

        foreach (DataColumn col in table.Columns)
        {
            // 解析目的欄位名稱
            string srcName = col.ColumnName ?? string.Empty;
            string dest = ResolveDestinationColumn(srcName, columnsMapping);
            if (existingCols.Contains(dest)) continue;

            // 判斷是否為自動產生或空欄位
            bool looksAutoGenerated = false;
            var src = srcName;
            if (string.IsNullOrWhiteSpace(src)) looksAutoGenerated = true;
            else if (_cellRefRegex.IsMatch(src)) looksAutoGenerated = true; // C12, AA10, 等
            else if (_autoColumnRegex.IsMatch(src)) looksAutoGenerated = true;

            var normDest = NormalizeName(dest);
            if (looksAutoGenerated || string.IsNullOrEmpty(normDest))
            {
                toRemove.Add(col);
                continue; // 跳過嚴格驗證
            }

            // 嘗試提供建議
            string? suggestion = null;
            if (normalizedMap.TryGetValue(normDest, out var match))
            {
                suggestion = match;
            }
            else
            {
                foreach (var kv in normalizedMap)
                {
                    if (kv.Key.Contains(normDest) || normDest.Contains(kv.Key))
                    {
                        suggestion = kv.Value;
                        break;
                    }
                }
            }

            var existingColsStr = existingList.Count > 0 ? string.Join(", ", existingList) : "(no columns found)";
            var suggestionPart = suggestion is null ? string.Empty : $" Did you mean: '{suggestion}'?";
            missingCritical.Add((dest, $"Destination column '{dest}' not found in table {targetTableName}. Existing columns: {existingColsStr}.{suggestionPart}"));
        }

        // 移除自動產生或空欄位
        if (toRemove.Count > 0)
        {
            foreach (var c in toRemove)
            {
                try { table.Columns.Remove(c); } catch { }
            }
        }

        if (missingCritical.Count > 0)
        {
            // 若來源欄位對應不到目標欄位，則移除並警告
            foreach (var (dest, message) in missingCritical)
            {
                var colsToRemove = new List<DataColumn>();
                foreach (DataColumn col in table.Columns)
                {
                    string srcName = col.ColumnName ?? string.Empty;
                    string resolved = ResolveDestinationColumn(srcName, columnsMapping);
                    if (string.Equals(resolved, dest, StringComparison.OrdinalIgnoreCase))
                    {
                        colsToRemove.Add(col);
                    }
                }

                foreach (var c in colsToRemove)
                {
                    try
                    {
                        var removedName = c.ColumnName;
                        table.Columns.Remove(c);
                        // 輸出警告到主控台
                        Console.WriteLine($"Warning: removed source column '{removedName}' because destination '{dest}' does not exist in {targetTableName}.");
                    }
                    catch { }
                }
            }
        }
    }

    /// <summary>
    /// 解析來源欄位名稱對應的目的欄位名稱，優先使用來源->目的對應，若無則嘗試目的->來源反向對應，否則回傳原始來源名稱。
    /// </summary>
    /// <param name="sourceColumn">來源欄位名稱。</param>
    /// <param name="columnsMapping">來源到目的欄位對應字典。</param>
    /// <returns>解析後的目的欄位名稱。</returns>
    private static string ResolveDestinationColumn(string sourceColumn, Dictionary<string, string>? columnsMapping)
    {
        if (columnsMapping == null) return sourceColumn; // 無對應則回傳原名

        // 優先來源->目的
        if (columnsMapping.TryGetValue(sourceColumn, out var mapped)) return mapped;

        // 若為反向對應則嘗試目的->來源
        foreach (var kv in columnsMapping)
        {
            if (string.Equals(kv.Value, sourceColumn, StringComparison.OrdinalIgnoreCase))
                return kv.Key; // 反向對應
        }

        // 無對應則回傳原名
        return sourceColumn;
    }
}

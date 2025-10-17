using System.Text;
using Microsoft.Data.SqlClient;

/// <summary>
/// 提供與 SQL Server 相關的輔助方法（例如應用鎖、清空資料表、重設識別序號與索引管理）。
/// </summary>
/// <remarks>
/// 這些方法為靜態輔助函式，會建立並管理自己的 <see cref="SqlConnection"/>。
/// 呼叫端應提供有效的連線字串，且對於敏感操作（例如 TRUNCATE/DELETE）務必小心使用。
/// </remarks>
public static class DbHelpers
{
    // 安全地引用 SQL 識別字（schema、table、index 名稱），避免語法錯誤與 SQL 注入
    private static string QuoteSqlIdentifier(string name)
    {
        if (string.IsNullOrWhiteSpace(name)) throw new ArgumentException("Identifier is null or empty", nameof(name)); // 檢查名稱是否為空
        if (name.Contains("]")) throw new ArgumentException("Identifier contains invalid character ']'", nameof(name)); // 禁止包含右括號
        return "[" + name + "]"; // 加上中括號以引用
    }

    // 分離 schema 與 table 名稱，預設 schema 為 dbo
    private static (string schema, string table) SplitSchemaAndTable(string tableSchemaAndName)
    {
        var cleaned = tableSchemaAndName.Replace("[", "").Replace("]", ""); // 移除中括號
        var parts = cleaned.Split('.'); // 以點分割
        if (parts.Length == 2) return (parts[0], parts[1]); // 有 schema
        return ("dbo", parts[0]); // 無 schema 預設為 dbo
    }

    /// <summary>
    /// 非同步嘗試取得一個 SQL Server 的應用鎖（application lock）。
    /// </summary>
    /// <param name="connStr">用於連線到 SQL Server 的連線字串。</param>
    /// <param name="resourceName">欲上鎖的資源名稱。</param>
    /// <param name="timeoutMs">等待鎖定的逾時時間（毫秒），預設為 <c>0</c>。</param>
    /// <returns>一個表示是否成功取得鎖的布林值：<see langword="true"/> 表示取得成功，否則 <see langword="false"/>。</returns>
    public static async Task<bool> AcquireAppLockAsync(string connStr, string resourceName, int timeoutMs = 0)
    {
        using var conn = new SqlConnection(connStr); // 建立連線
        await conn.OpenAsync(); // 開啟連線
        using var cmd = conn.CreateCommand(); // 建立命令
        cmd.CommandText = @"DECLARE @rc INT;
EXEC @rc = sys.sp_getapplock @Resource = @Resource, @LockMode = N'Exclusive', @LockOwner = N'Session', @LockTimeout = @LockTimeout;
SELECT @rc;"; // 執行 sp_getapplock 並回傳結果
        cmd.CommandType = System.Data.CommandType.Text; // 設定命令型態
        cmd.Parameters.AddWithValue("@Resource", resourceName); // 設定資源名稱參數
        cmd.Parameters.AddWithValue("@LockTimeout", timeoutMs); // 設定逾時參數
        var rcObj = await cmd.ExecuteScalarAsync(); // 執行命令取得回傳值
        var rc = rcObj != DBNull.Value && rcObj != null ? Convert.ToInt32(rcObj) : -999; // 轉換回傳值
        return rc >= 0; // 回傳是否成功
    }

    /// <summary>
    /// 取得一個會在生命週期內維持 SqlConnection 的 AppLock 把手，確保使用者在取得鎖後可在整個匯入期間保有該鎖。
    /// 呼叫端應在作業結束時釋放把手 (Dispose/DisposeAsync)，以釋放 applock 與關閉連線。
    /// </summary>
    /// <param name="connStr">SQL Server 連線字串</param>
    /// <param name="resourceName">applock 資源名稱</param>
    /// <param name="timeoutMs">逾時毫秒</param>
    /// <returns>成功時回傳 AppLockHandle（需釋放），失敗時回傳 null。</returns>
    /// <summary>
    /// 取得 applock 把手並回傳 sp_getapplock 的結果代碼，方便呼叫端做診斷紀錄。
    /// 回傳的 tuple.First 為把手（若成功），tuple.Second 為 sp_getapplock 的整數回傳值（若無法取得則為負值或 -999）。
    /// </summary>
    public static async Task<(AppLockHandle? handle, int resultCode)> AcquireAppLockHandleAsync(string connStr, string resourceName, int timeoutMs = 0)
    {
        var conn = new SqlConnection(connStr); // 建立連線
        await conn.OpenAsync(); // 開啟連線
        try
        {
            using var cmd = conn.CreateCommand(); // 建立命令
            cmd.CommandText = @"DECLARE @rc INT;
EXEC @rc = sys.sp_getapplock @Resource = @Resource, @LockMode = N'Exclusive', @LockOwner = N'Session', @LockTimeout = @LockTimeout;
SELECT @rc;"; // 執行 sp_getapplock 並回傳結果
            cmd.CommandType = System.Data.CommandType.Text; // 設定命令型態
            cmd.Parameters.AddWithValue("@Resource", resourceName); // 設定資源名稱參數
            cmd.Parameters.AddWithValue("@LockTimeout", timeoutMs); // 設定逾時參數
            var rcObj = await cmd.ExecuteScalarAsync(); // 執行命令取得回傳值
            int intRc = rcObj != null && rcObj != DBNull.Value ? Convert.ToInt32(rcObj) : -999; // 轉換回傳值
            var ok = intRc >= 0; // 判斷是否成功
            if (!ok)
            {
                await conn.DisposeAsync(); // 失敗則釋放連線
                return (null, intRc); // 回傳 null 與結果碼
            }
            return (new AppLockHandle(conn, resourceName), intRc); // 成功則回傳把手與結果碼
        }
        catch
        {
            await conn.DisposeAsync(); // 發生例外時釋放連線
            throw; // 向上拋出例外
        }
    }

    /// <summary>
    /// 表示一個持有 applock 的把手，會在 Dispose/DisposeAsync 時呼叫 sp_releaseapplock 並關閉連線。
    /// </summary>
    public sealed class AppLockHandle : IAsyncDisposable
    {
        private readonly SqlConnection _conn; // 連線物件
        private readonly string _resource; // 資源名稱
        private bool _released; // 是否已釋放

        internal AppLockHandle(SqlConnection conn, string resource)
        {
            _conn = conn ?? throw new ArgumentNullException(nameof(conn)); // 檢查連線
            _resource = resource ?? throw new ArgumentNullException(nameof(resource)); // 檢查資源名稱
            _released = false; // 初始未釋放
        }

        /// <summary>
        /// 釋放 applock 並關閉底層連線。
        /// 若已釋放則為 no-op。
        /// </summary>
        public async ValueTask DisposeAsync()
        {
            if (_released) return; // 已釋放則直接返回
            try
            {
                using var cmd = _conn.CreateCommand(); // 建立命令
                cmd.CommandText = "sp_releaseapplock"; // 設定釋放鎖的命令
                cmd.CommandType = System.Data.CommandType.StoredProcedure; // 設定為預存程序
                cmd.Parameters.AddWithValue("@Resource", _resource); // 設定資源名稱參數
                cmd.Parameters.AddWithValue("@LockOwner", "Session"); // 設定鎖擁有者
                await cmd.ExecuteNonQueryAsync(); // 執行釋放鎖
            }
            catch
            {
                // 吞掉例外，確保連線仍會被關閉
            }
            finally
            {
                try { await _conn.DisposeAsync(); } catch { } // 關閉連線
                _released = true; // 標記已釋放
            }
        }

        // 支援同步 Dispose，便於使用 using(...) 模式
        public void Dispose()
        {
            // 同步等候非同步 Dispose 完成
            try
            {
                DisposeAsync().AsTask().GetAwaiter().GetResult();
            }
            catch
            {
                // 不拋出例外，與原本行為一致
            }
        }
    }

    /// <summary>
    /// 非同步嘗試清空指定的資料表，優先使用 <c>TRUNCATE TABLE</c>，若失敗則改用 <c>DELETE FROM</c>。
    /// </summary>
    /// <param name="connStr">用於連線到 SQL Server 的連線字串。</param>
    /// <param name="tableName">要清空的資料表名稱，可能包含 schema（例如 <c>dbo.MyTable</c>）。</param>
    /// <param name="preserveIdentity">若為 <see langword="true"/>，在操作前會先讀取並回傳目前的 IDENT_CURRENT 值，以利後續重設。</param>
    /// <returns>
    /// 當 <paramref name="preserveIdentity"/> 為 <see langword="true"/> 時，回傳目前的 identity 值；
    /// 否則回傳 <c>null</c>。
    /// </returns>
    public static async Task<long?> TruncateOrDeleteAsync(string connStr, string tableName, bool preserveIdentity)
    {
        long? currentIdent = null; // 儲存目前 identity
        using var conn = new SqlConnection(connStr); // 建立連線
        await conn.OpenAsync(); // 開啟連線

        if (preserveIdentity)
        {
            using var cmdId = conn.CreateCommand(); // 建立命令
            var (sch, tbl) = SplitSchemaAndTable(tableName); // 分離 schema 與 table
            cmdId.CommandText = "SELECT IDENT_CURRENT(@fullname)"; // 查詢目前 identity
            cmdId.Parameters.AddWithValue("@fullname", sch + "." + tbl); // 設定參數
            var val = await cmdId.ExecuteScalarAsync(); // 執行查詢
            if (val != DBNull.Value && val != null) currentIdent = Convert.ToInt64(val); // 取得值
        }
        var (schMain, tblMain) = SplitSchemaAndTable(tableName); // 分離 schema 與 table
        var quotedMain = QuoteSqlIdentifier(schMain) + "." + QuoteSqlIdentifier(tblMain); // 組合引用名稱
        using var cmd = conn.CreateCommand(); // 建立命令
        try
        {
            cmd.CommandText = "TRUNCATE TABLE " + quotedMain + ";"; // 嘗試 TRUNCATE
            await cmd.ExecuteNonQueryAsync(); // 執行命令
            return currentIdent; // 回傳 identity
        }
        catch (SqlException)
        {
            cmd.CommandText = "DELETE FROM " + quotedMain + ";"; // 若失敗則改用 DELETE
            cmd.CommandTimeout = 0; // 設定無限逾時
            await cmd.ExecuteNonQueryAsync(); // 執行命令
            return currentIdent; // 回傳 identity
        }
    }

    /// <summary>
    /// 使用 DBCC CHECKIDENT 重設指定資料表的 identity 欄位為提供的值。
    /// </summary>
    /// <param name="connStr">用於連線到 SQL Server 的連線字串。</param>
    /// <param name="tableName">要重設 identity 的資料表名稱，可能包含 schema。</param>
    /// <param name="reseedValue">要重設的數值；若為 <c>null</c> 則不進行任何動作。</param>
    /// <returns>一個表示非同步操作完成的 <see cref="Task"/>。</returns>
    public static async Task RestoreIdentityAsync(string connStr, string tableName, long? reseedValue)
    {
        if (!reseedValue.HasValue) return; // 無值則不處理
        using var conn = new SqlConnection(connStr); // 建立連線
        await conn.OpenAsync(); // 開啟連線
        using var cmd = conn.CreateCommand(); // 建立命令
        var (sch, tbl) = SplitSchemaAndTable(tableName); // 分離 schema 與 table
        var fullname = sch + "." + tbl; // 組合完整名稱
        var quotedFull = QuoteSqlIdentifier(sch) + "." + QuoteSqlIdentifier(tbl); // 組合引用名稱
        cmd.CommandText = "DBCC CHECKIDENT ('" + quotedFull + "', RESEED, @reseed);"; // 設定重設命令
        cmd.Parameters.AddWithValue("@reseed", reseedValue.Value); // 設定重設值參數
        cmd.CommandTimeout = 0; // 設定無限逾時
        await cmd.ExecuteNonQueryAsync(); // 執行命令
    }

    /// <summary>
    /// 取得指定資料表的所有非聚集索引（nonclustered index）名稱清單。
    /// </summary>
    /// <param name="connStr">用於連線到 SQL Server 的連線字串。</param>
    /// <param name="tableSchemaAndName">資料表名稱，允許包含 schema（例如 <c>dbo.MyTable</c>）。</param>
    /// <returns>一個包含索引名稱的清單。</returns>
    public static async Task<List<string>> GetNonClusteredIndexNamesAsync(string connStr, string tableSchemaAndName)
    {
        var list = new List<string>(); // 儲存索引名稱
        var (schema, table) = SplitSchemaAndTable(tableSchemaAndName); // 分離 schema 與 table

        string sql = @"
SELECT i.name
FROM sys.indexes i
JOIN sys.objects o ON i.object_id = o.object_id
JOIN sys.schemas s ON o.schema_id = s.schema_id
WHERE o.name = @table AND s.name = @schema AND i.type_desc = 'NONCLUSTERED' AND i.is_hypothetical = 0 AND i.name IS NOT NULL;
"; // 查詢非聚集索引
        using var conn = new SqlConnection(connStr); // 建立連線
        await conn.OpenAsync(); // 開啟連線
        using var cmd = conn.CreateCommand(); // 建立命令
        cmd.CommandText = sql; // 設定查詢語句
        cmd.Parameters.AddWithValue("@table", table); // 設定 table 參數
        cmd.Parameters.AddWithValue("@schema", schema); // 設定 schema 參數
        using var reader = await cmd.ExecuteReaderAsync(); // 執行查詢
        while (await reader.ReadAsync()) list.Add(reader.GetString(0)); // 讀取索引名稱
        return list; // 回傳索引名稱清單
    }

    /// <summary>
    /// 依序將指定的索引停用（DISABLE）。
    /// </summary>
    /// <param name="connStr">用於連線到 SQL Server 的連線字串。</param>
    /// <param name="tableName">目標資料表名稱，可能包含 schema。</param>
    /// <param name="indexNames">要停用的索引名稱集合。</param>
    /// <returns>一個表示非同步操作完成的 <see cref="Task"/>。</returns>
    public static async Task DisableIndexesAsync(string connStr, string tableName, IEnumerable<string> indexNames)
    {
        if (indexNames == null) return;
        using var conn = new SqlConnection(connStr);
        await conn.OpenAsync();
        var (sch, tbl) = SplitSchemaAndTable(tableName);
        var quotedTbl = QuoteSqlIdentifier(sch) + "." + QuoteSqlIdentifier(tbl);

        // 將多個 ALTER INDEX 合併為一個命令，減少 round-trip
        var sb = new StringBuilder();
        foreach (var idx in indexNames)
        {
            if (string.IsNullOrWhiteSpace(idx)) continue;
            sb.Append("ALTER INDEX ");
            sb.Append(QuoteSqlIdentifier(idx));
            sb.Append(" ON ");
            sb.Append(quotedTbl);
            sb.Append(" DISABLE;");
        }
        var sql = sb.ToString();
        if (string.IsNullOrWhiteSpace(sql)) return;
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// 依序重建指定的索引（REBUILD）。
    /// </summary>
    /// <param name="connStr">用於連線到 SQL Server 的連線字串。</param>
    /// <param name="tableName">目標資料表名稱，可能包含 schema。</param>
    /// <param name="indexNames">要重建的索引名稱集合。</param>
    /// <returns>一個表示非同步操作完成的 <see cref="Task"/>。</returns>
    public static async Task RebuildIndexesAsync(string connStr, string tableName, IEnumerable<string> indexNames)
    {
        if (indexNames == null) return;
        using var conn = new SqlConnection(connStr);
        await conn.OpenAsync();
        var (sch, tbl) = SplitSchemaAndTable(tableName);
        var quotedTbl = QuoteSqlIdentifier(sch) + "." + QuoteSqlIdentifier(tbl);

        // 合併多個 REBUILD 為單次執行以降低 round-trip
        var sb = new StringBuilder();
        foreach (var idx in indexNames)
        {
            if (string.IsNullOrWhiteSpace(idx)) continue;
            sb.Append("ALTER INDEX ");
            sb.Append(QuoteSqlIdentifier(idx));
            sb.Append(" ON ");
            sb.Append(quotedTbl);
            sb.Append(" REBUILD;");
        }
        var sql = sb.ToString();
        if (string.IsNullOrWhiteSpace(sql)) return;
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.CommandTimeout = 0;
        await cmd.ExecuteNonQueryAsync();
    }
}

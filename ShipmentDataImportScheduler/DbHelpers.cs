using System.Text;
using Microsoft.Data.SqlClient;

namespace ShipmentDataImportScheduler;

/// <summary>
/// 提供與 SQL Server 相關的輔助方法（例如應用鎖、清空資料表、重設識別序號與索引管理）。
/// </summary>
/// <remarks>
/// 這些方法為靜態輔助函式，會建立並管理自己的 <see cref="SqlConnection"/>。
/// 呼叫端應提供有效的連線字串，且對於敏感操作（例如 TRUNCATE/DELETE）務必小心使用。
/// </remarks>
public static class DbHelpers
{

    #region 識別字與 Schema 處理
    /// <summary>
    /// 安全地引用 SQL 識別字（如 schema、table、index 名稱），避免語法錯誤與 SQL 注入。
    /// </summary>
    /// <param name="name">要被引用的 SQL 識別字名稱。</param>
    /// <returns>已加上中括號的安全 SQL 識別字。</returns>
    /// <exception cref="ArgumentException">
    /// 當 <paramref name="name"/> 為 null、空字串、僅有空白，或包含不允許的字元 ']' 時拋出。
    /// </exception>
    private static string QuoteSqlIdentifier(string name)
    {
        if (string.IsNullOrWhiteSpace(name)) throw new ArgumentException("Identifier is null or empty", nameof(name));
        if (name.Contains(']')) throw new ArgumentException("Identifier contains invalid character ']'", nameof(name));
        return $"[{name}]";
    }

    /// <summary>
    /// 分離 schema 與 table 名稱，若未指定 schema 則預設為 dbo。
    /// </summary>
    /// <param name="tableSchemaAndName">資料表名稱，格式可為 [schema].[table] 或僅 [table]。</param>
    /// <returns>
    /// Tuple，第一個元素為 schema 名稱，第二個元素為 table 名稱。
    /// 若未指定 schema，則 schema 為 "dbo"。
    /// </returns>
    private static (string schema, string table) SplitSchemaAndTable(string tableSchemaAndName)
    {
        var cleaned = tableSchemaAndName.Replace("[", string.Empty).Replace("]", string.Empty);
        var parts = cleaned.Split('.');
        return parts.Length == 2 ? (parts[0], parts[1]) : ("dbo", parts[0]);
    }

    #endregion

    #region AppLock 相關

    /// <summary>
    /// 非同步嘗試取得一個 SQL Server 的應用鎖（application lock）。
    /// </summary>
    /// <param name="connStr">SQL Server 連線字串。</param>
    /// <param name="resourceName">要鎖定的資源名稱（application lock 的名稱）。</param>
    /// <param name="timeoutMs">取得鎖的逾時（毫秒），預設為 0（立即返回）。</param>
    /// <returns>若成功取得鎖則回傳 true，否則回傳 false。</returns>
    /// <remarks>
    /// 此方法會建立自己的 <see cref="SqlConnection"/>，並於執行結束時自動釋放。
    /// 若鎖取得失敗（例如逾時或資源被其他 session 鎖定），則回傳 false。
    /// </remarks>
    public static async Task<bool> AcquireAppLockAsync(string connStr, string resourceName, int timeoutMs = 0)
    {
        using var conn = new SqlConnection(connStr);
        await conn.OpenAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"DECLARE @rc INT;
    EXEC @rc = sys.sp_getapplock @Resource = @Resource, @LockMode = N'Exclusive', @LockOwner = N'Session', @LockTimeout = @LockTimeout;
    SELECT @rc;";
        cmd.CommandType = System.Data.CommandType.Text;
        cmd.Parameters.AddWithValue("@Resource", resourceName);
        cmd.Parameters.AddWithValue("@LockTimeout", timeoutMs);
        var rcObj = await cmd.ExecuteScalarAsync();
        var rc = rcObj is not null && rcObj != DBNull.Value ? Convert.ToInt32(rcObj) : -999;
        return rc >= 0;
    }


    /// <summary>
    /// 非同步取得一個 SQL Server 應用鎖（application lock）並回傳把手物件。
    /// </summary>
    /// <param name="connStr">SQL Server 連線字串。</param>
    /// <param name="resourceName">要鎖定的資源名稱（application lock 名稱）。</param>
    /// <param name="timeoutMs">取得鎖的逾時（毫秒），預設為 0（立即返回）。</param>
    /// <returns>
    /// 回傳一個 tuple，
    /// 第一個元素為 <see cref="AppLockHandle"/>（若成功取得鎖則不為 null），
    /// 第二個元素為 sp_getapplock 的整數回傳值（成功為 0 或正數，失敗為負數）。
    /// </returns>
    /// <remarks>
    /// 此方法會建立自己的 <see cref="SqlConnection"/>，並於失敗時自動釋放連線。
    /// 若成功取得鎖，需由呼叫端負責釋放 <see cref="AppLockHandle"/>（Dispose/DisposeAsync）。
    /// 若鎖取得失敗（例如逾時或資源被其他 session 鎖定），則回傳 null 把手與負數結果碼。
    /// </remarks>
    public static async Task<(AppLockHandle? handle, int resultCode)> AcquireAppLockHandleAsync(string connStr, string resourceName, int timeoutMs = 0)
    {
        var conn = new SqlConnection(connStr);
        await conn.OpenAsync();
        try
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = @"DECLARE @rc INT;
    EXEC @rc = sys.sp_getapplock @Resource = @Resource, @LockMode = N'Exclusive', @LockOwner = N'Session', @LockTimeout = @LockTimeout;
    SELECT @rc;";
            cmd.CommandType = System.Data.CommandType.Text;
            cmd.Parameters.AddWithValue("@Resource", resourceName);
            cmd.Parameters.AddWithValue("@LockTimeout", timeoutMs);
            var rcObj = await cmd.ExecuteScalarAsync();
            int intRc = rcObj is not null && rcObj != DBNull.Value ? Convert.ToInt32(rcObj) : -999;
            if (intRc < 0)
            {
                await conn.DisposeAsync();
                return (null, intRc);
            }
            return (new AppLockHandle(conn, resourceName), intRc);
        }
        catch
        {
            await conn.DisposeAsync();
            throw;
        }
    }

    /// <summary>
    /// 代表一個持有 SQL Server 應用鎖（application lock）的把手物件。
    /// 在 Dispose 或 DisposeAsync 時會自動釋放 applock 並關閉底層連線。
    /// </summary>
    /// <remarks>
    /// 此類別僅能由 <see cref="DbHelpers.AcquireAppLockHandleAsync"/> 建立。
    /// 使用完畢後請務必呼叫 Dispose 或 DisposeAsync 以釋放資源。
    /// </remarks>
    public sealed class AppLockHandle : IAsyncDisposable
    {
        /// <summary>
        /// 持有的 SQL Server 連線物件。
        /// </summary>
        private readonly SqlConnection _conn;

        /// <summary>
        /// 應用鎖所對應的資源名稱。
        /// </summary>
        private readonly string _resource;

        /// <summary>
        /// 指示是否已釋放 applock 與連線。
        /// </summary>
        private bool _released;

        /// <summary>
        /// 建立 <see cref="AppLockHandle"/> 的執行個體，並指定所持有的 <see cref="SqlConnection"/> 及資源名稱。
        /// </summary>
        /// <param name="conn">已開啟的 SQL Server 連線，將用於持有與釋放應用鎖。</param>
        /// <param name="resource">應用鎖所對應的資源名稱。</param>
        /// <exception cref="ArgumentNullException">
        /// 當 <paramref name="conn"/> 或 <paramref name="resource"/> 為 null 時拋出。
        /// </exception>
        internal AppLockHandle(SqlConnection conn, string resource)

        /// <summary>
        /// 建立 <see cref="AppLockHandle"/> 的執行個體，並指定所持有的 <see cref="SqlConnection"/> 及資源名稱。
        /// </summary>
        /// <param name="conn">已開啟的 SQL Server 連線，將用於持有與釋放應用鎖。</param>
        /// <param name="resource">應用鎖所對應的資源名稱。</param>
        /// <exception cref="ArgumentNullException">
        /// 當 <paramref name="conn"/> 或 <paramref name="resource"/> 為 null 時拋出。
        /// </exception>
        {
            _conn = conn ?? throw new ArgumentNullException(nameof(conn));
            _resource = resource ?? throw new ArgumentNullException(nameof(resource));
            _released = false;
        }

        /// <summary>
        /// 釋放 applock 並關閉底層連線。
        /// 若已釋放則為 no-op。
        /// </summary>
        public async ValueTask DisposeAsync()
        {
            if (_released) return;
            try
            {
                using var cmd = _conn.CreateCommand();
                cmd.CommandText = "sp_releaseapplock";
                cmd.CommandType = System.Data.CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("@Resource", _resource);
                cmd.Parameters.AddWithValue("@LockOwner", "Session");
                await cmd.ExecuteNonQueryAsync();
            }
            catch
            {
                // 保證連線釋放
            }
            finally
            {
                try { await _conn.DisposeAsync(); } catch { }
                _released = true;
            }
        }

        /// <summary>
        /// 釋放 applock 並關閉底層連線。
        /// 若已釋放則為 no-op。
        /// </summary>
        /// <remarks>
        /// 此方法會同步釋放資源，便於使用 using(...) 模式。
        /// 若釋放過程中發生例外，將被吞掉不拋出。
        /// </remarks>
        public void Dispose()
        {
            try
            {
                DisposeAsync().AsTask().GetAwaiter().GetResult();
            }
            catch
            {
                // 不拋出例外
            }
        }
    }

    #endregion

    #region Table 操作

    /// <summary>
    /// 非同步嘗試清空指定的資料表，優先使用 <c>TRUNCATE TABLE</c>，若失敗則改用 <c>DELETE FROM</c>。
    /// </summary>
    /// <param name="connStr">SQL Server 連線字串。</param>
    /// <param name="tableName">要清空的資料表名稱，可含 schema（如 dbo.TableName）。</param>
    /// <param name="preserveIdentity">是否保留目前的 identity 值（若為 true，會先查詢目前 identity，供後續重設）。</param>
    /// <returns>
    /// 若 <paramref name="preserveIdentity"/> 為 true，回傳清空前的 identity 值（若有）；否則回傳 null。
    /// </returns>
    /// <remarks>
    /// 此方法會優先嘗試使用 <c>TRUNCATE TABLE</c>，若因權限或外鍵等限制失敗，則自動改用 <c>DELETE FROM</c>。
    /// 若 <paramref name="preserveIdentity"/> 為 true，建議於清空後呼叫 <see cref="RestoreIdentityAsync"/> 以重設 identity。
    /// </remarks>
    public static async Task<long?> TruncateOrDeleteAsync(string connStr, string tableName, bool preserveIdentity)
    {
        long? currentIdent = null;
        using var conn = new SqlConnection(connStr);
        await conn.OpenAsync();

        if (preserveIdentity)
        {
            using var cmdId = conn.CreateCommand();
            var (sch, tbl) = SplitSchemaAndTable(tableName);
            cmdId.CommandText = "SELECT IDENT_CURRENT(@fullname)";
            cmdId.Parameters.AddWithValue("@fullname", $"{sch}.{tbl}");
            var val = await cmdId.ExecuteScalarAsync();
            if (val is not null && val != DBNull.Value) currentIdent = Convert.ToInt64(val);
        }
        var (schMain, tblMain) = SplitSchemaAndTable(tableName);
        var quotedMain = $"{QuoteSqlIdentifier(schMain)}.{QuoteSqlIdentifier(tblMain)}";
        using var cmd = conn.CreateCommand();
        try
        {
            cmd.CommandText = $"TRUNCATE TABLE {quotedMain};";
            await cmd.ExecuteNonQueryAsync();
            return currentIdent;
        }
        catch (SqlException)
        {
            cmd.CommandText = $"DELETE FROM {quotedMain};";
            cmd.CommandTimeout = 0;
            await cmd.ExecuteNonQueryAsync();
            return currentIdent;
        }
    }


    /// <summary>
    /// 使用 DBCC CHECKIDENT 重設指定資料表的 identity 欄位為提供的值。
    /// </summary>
    /// <param name="connStr">SQL Server 連線字串。</param>
    /// <param name="tableName">要重設 identity 的資料表名稱，可含 schema（如 dbo.TableName）。</param>
    /// <param name="reseedValue">要重設的 identity 值（若為 null 則不執行）。</param>
    /// <returns>非同步作業的 <see cref="Task"/>。</returns>
    /// <remarks>
    /// 此方法會使用 <c>DBCC CHECKIDENT</c> 指令將指定資料表的 identity 欄位重設為 <paramref name="reseedValue"/>。
    /// 若 <paramref name="reseedValue"/> 為 null 則不執行任何動作。
    /// </remarks>
    public static async Task RestoreIdentityAsync(string connStr, string tableName, long? reseedValue)
    {
        if (!reseedValue.HasValue) return;
        using var conn = new SqlConnection(connStr);
        await conn.OpenAsync();
        using var cmd = conn.CreateCommand();
        var (sch, tbl) = SplitSchemaAndTable(tableName);
        var quotedFull = $"{QuoteSqlIdentifier(sch)}.{QuoteSqlIdentifier(tbl)}";
        cmd.CommandText = $"DBCC CHECKIDENT ('{quotedFull}', RESEED, @reseed);";
        cmd.Parameters.AddWithValue("@reseed", reseedValue.Value);
        cmd.CommandTimeout = 0;
        await cmd.ExecuteNonQueryAsync();
    }

    #endregion

    #region Index 操作

    /// <summary>
    /// 取得指定資料表的所有非聚集索引（nonclustered index）名稱清單。
    /// </summary>
    /// <param name="connStr">SQL Server 連線字串。</param>
    /// <param name="tableSchemaAndName">資料表名稱，可含 schema（如 dbo.TableName）。</param>
    /// <returns>回傳該資料表所有非聚集索引名稱的字串清單。</returns>
    /// <remarks>
    /// 此方法會查詢 sys.indexes、sys.objects 及 sys.schemas，僅回傳 type_desc 為 NONCLUSTERED、非 hypothetical 且名稱不為 null 的索引。
    /// </remarks>
    public static async Task<List<string>> GetNonClusteredIndexNamesAsync(string connStr, string tableSchemaAndName)
    {
        var list = new List<string>();
        var (schema, table) = SplitSchemaAndTable(tableSchemaAndName);
        const string sql = @"
    SELECT i.name
    FROM sys.indexes i
    JOIN sys.objects o ON i.object_id = o.object_id
    JOIN sys.schemas s ON o.schema_id = s.schema_id
    WHERE o.name = @table AND s.name = @schema AND i.type_desc = 'NONCLUSTERED' AND i.is_hypothetical = 0 AND i.name IS NOT NULL;
    ";
        using var conn = new SqlConnection(connStr);
        await conn.OpenAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.Parameters.AddWithValue("@table", table);
        cmd.Parameters.AddWithValue("@schema", schema);
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            list.Add(reader.GetString(0));
        }
        return list;
    }


    /// <summary>
    /// 依序將指定的索引停用（DISABLE）。
    /// </summary>
    /// <param name="connStr">SQL Server 連線字串。</param>
    /// <param name="tableName">要操作的資料表名稱，可含 schema（如 dbo.TableName）。</param>
    /// <param name="indexNames">要停用的索引名稱集合。</param>
    /// <returns>非同步作業的 <see cref="Task"/>。</returns>
    /// <remarks>
    /// 此方法會依序對 <paramref name="indexNames"/> 中的每個索引執行 <c>ALTER INDEX ... DISABLE</c>。
    /// 若 <paramref name="indexNames"/> 為 null 或無有效索引名稱，則不執行任何動作。
    /// </remarks>
    public static async Task DisableIndexesAsync(string connStr, string tableName, IEnumerable<string> indexNames)
    {
        if (indexNames is null) return;
        using var conn = new SqlConnection(connStr);
        await conn.OpenAsync();
        var (sch, tbl) = SplitSchemaAndTable(tableName);
        var quotedTbl = $"{QuoteSqlIdentifier(sch)}.{QuoteSqlIdentifier(tbl)}";

        var sb = new StringBuilder();
        foreach (var idx in indexNames)
        {
            if (string.IsNullOrWhiteSpace(idx)) continue;
            sb.Append($"ALTER INDEX {QuoteSqlIdentifier(idx)} ON {quotedTbl} DISABLE;");
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
    /// <param name="connStr">SQL Server 連線字串。</param>
    /// <param name="tableName">要操作的資料表名稱，可含 schema（如 dbo.TableName）。</param>
    /// <param name="indexNames">要重建的索引名稱集合。</param>
    /// <returns>非同步作業的 <see cref="Task"/>。</returns>
    /// <remarks>
    /// 此方法會依序對 <paramref name="indexNames"/> 中的每個索引執行 <c>ALTER INDEX ... REBUILD</c>。
    /// 若 <paramref name="indexNames"/> 為 null 或無有效索引名稱，則不執行任何動作。
    /// </remarks>
    public static async Task RebuildIndexesAsync(string connStr, string tableName, IEnumerable<string> indexNames)
    {
        if (indexNames is null) return;
        using var conn = new SqlConnection(connStr);
        await conn.OpenAsync();
        var (sch, tbl) = SplitSchemaAndTable(tableName);
        var quotedTbl = $"{QuoteSqlIdentifier(sch)}.{QuoteSqlIdentifier(tbl)}";

        var sb = new StringBuilder();
        foreach (var idx in indexNames)
        {
            if (string.IsNullOrWhiteSpace(idx)) continue;
            sb.Append($"ALTER INDEX {QuoteSqlIdentifier(idx)} ON {quotedTbl} REBUILD;");
        }
        var sql = sb.ToString();
        if (string.IsNullOrWhiteSpace(sql)) return;
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.CommandTimeout = 0;
        await cmd.ExecuteNonQueryAsync();
    }

    #endregion
}

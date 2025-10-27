using Microsoft.Extensions.Logging; // 引入日誌記錄相關的命名空間

/// <summary>
/// 提供檔案操作的輔助方法，例如複製重試與安全刪除。
/// </summary>
/// <remarks>
/// 這個類別將常見的檔案處理模式封裝成簡單、可重複使用的方法，
/// 以減少呼叫端對錯誤處理與重試邏輯的負擔。
/// </remarks>

namespace ShipmentDataImportScheduler;

/// <summary>
/// 靜態類別，封裝檔案操作輔助方法。
/// </summary>
public static class FileHelpers
{

    /// <summary>
    /// 以指定帳號密碼連線網路路徑（SMB），用於需要驗證的網路檔案存取。
    /// </summary>
    /// <param name="networkPath">網路路徑（如 \\server\share）</param>
    /// <param name="username">帳號</param>
    /// <param name="password">密碼</param>
    /// <param name="domain">網域（可選）</param>
    /// <exception cref="Exception">連線失敗時拋出</exception>
    public static void ConnectToNetworkPath(string networkPath, string username, string password, string? domain = null)
    {
        var netResource = new NETRESOURCE
        {
            dwType = 1, // RESOURCETYPE_DISK
            lpRemoteName = networkPath
        };
        string user = string.IsNullOrEmpty(domain) ? username : $"{domain}\\{username}";
        int result = WNetAddConnection2(ref netResource, password, user, 0);
        if (result != 0 && result != 1219) // 1219: 已有相同使用者連線
        {
            throw new Exception($"WNetAddConnection2 failed: {result}");
        }
    }

    /// <summary>
    /// 對應 Windows 網路資源結構（NETRESOURCE），用於網路磁碟連線。
    /// </summary>
    /// <remarks>
    /// 此結構體用於 P/Invoke 呼叫 <c>WNetAddConnection2</c> 以建立或管理網路磁碟連線。
    /// 欄位需依照 Windows API 文件正確填寫，否則可能導致連線失敗或行為異常。
    /// </remarks>
    [System.Runtime.InteropServices.StructLayout(System.Runtime.InteropServices.LayoutKind.Sequential)]
    private struct NETRESOURCE
    {
        /// <summary>
        /// 資源範圍（通常不需指定，預設為 0）。
        /// </summary>
        public int dwScope;
        /// <summary>
        /// 資源型態，1 代表磁碟（RESOURCETYPE_DISK）。
        /// </summary>
        public int dwType;
        /// <summary>
        /// 顯示型態（通常不需指定，預設為 0）。
        /// </summary>
        public int dwDisplayType;
        /// <summary>
        /// 使用方式（通常不需指定，預設為 0）。
        /// </summary>
        public int dwUsage;
        /// <summary>
        /// 本機磁碟機代號（如 "Z:"），若不指定則為 null。
        /// </summary>
        public string lpLocalName;
        /// <summary>
        /// 遠端網路路徑（如 "\\server\share"）。
        /// </summary>
        public string lpRemoteName;
        /// <summary>
        /// 備註（可選），通常為 null。
        /// </summary>
        public string lpComment;
        /// <summary>
        /// 提供者名稱（可選），通常為 null。
        /// </summary>
        public string lpProvider;
    }

    /// <summary>
    /// 宣告對 Windows 網路資源進行連線的外部函式 <c>WNetAddConnection2</c>。
    /// </summary>
    /// <param name="netResource">描述網路資源的 <see cref="NETRESOURCE"/> 結構。</param>
    /// <param name="password">用於驗證的密碼。</param>
    /// <param name="username">用於驗證的使用者名稱。</param>
    /// <param name="flags">控制連線行為的旗標，通常設為 0。</param>
    /// <returns>若成功則回傳 0，否則回傳錯誤代碼。</returns>
    /// <remarks>
    /// 此方法透過 P/Invoke 呼叫 Windows API <c>mpr.dll</c>，用於建立或管理網路磁碟連線。
    /// 詳細錯誤代碼請參考 Windows 文件。
    /// </remarks>
    [System.Runtime.InteropServices.DllImport("mpr.dll", CharSet = System.Runtime.InteropServices.CharSet.Auto)]
    private static extern int WNetAddConnection2(ref NETRESOURCE netResource, string password, string username, int flags);

    #region === 同步檔案複製與重試 ===
    /// <summary>
    /// 複製來源檔案到目的地；若複製失敗，會依設定次數重試，採用簡單的指數退避（exponential backoff）。
    /// </summary>
    /// <param name="source">來源檔案路徑。</param>
    /// <param name="destination">目的地檔案路徑。</param>
    /// <param name="retries">最大重試次數；若小於 1 則視為 1。</param>
    /// <param name="baseDelayMs">指數退避的基底延遲（以毫秒為單位），每次重試會乘以 2 的指數倍。</param>
    /// <remarks>
    /// 這個方法使用 <see cref="File.Copy(string,string)"/> 進行檔案複製，
    /// 若發生例外則會依據重試策略等待後再次嘗試。實作中以 <see cref="Thread.Sleep(int)"/> 進行等待，
    /// 因此呼叫端應注意此方法會同步阻塞目前執行緒──在 UI 或高併發環境中請考慮改用非同步或背景工作。
    /// </remarks>
    /// <exception cref="System.Exception">當在最後一次重試後仍無法完成複製時，拋出包含原始例外的例外物件。</exception>
    /// <example>
    /// <code language="csharp">
    /// // 範例：複製檔案並重試 5 次，基底延遲 200ms
    /// FileHelpers.CopyFileWithRetry("C:\\temp\\in.txt", "C:\\temp\\out.txt", retries: 5, baseDelayMs: 200);
    /// </code>
    /// </example>
    public static void CopyFileWithRetry(string source, string destination, int retries = 3, int baseDelayMs = 500)
    {
        if (string.IsNullOrWhiteSpace(source)) throw new ArgumentNullException(nameof(source));
        if (string.IsNullOrWhiteSpace(destination)) throw new ArgumentNullException(nameof(destination));
        if (retries < 1) retries = 1;

        // 若來源與目的地相同，直接返回以避免 lock 與不必要的 IO
        try
        {
            var srcFull = Path.GetFullPath(source);
            var dstFull = Path.GetFullPath(destination);
            if (string.Equals(srcFull, dstFull, StringComparison.OrdinalIgnoreCase)) return;
        }
        catch { /* 若 Path 解析失敗，繼續嘗試讓底層拋出適當例外 */ }

        if (!File.Exists(source)) throw new FileNotFoundException("Source file not found", source);

        const int bufferSize = 1024 * 64; // 64KB
        const int maxDelayMs = 30_000;
        // 使用 static Random 以減少資源消耗
        static int GetJitter(int delay) => Random.Shared.Next(0, Math.Min(100, Math.Max(1, delay / 10)));

        for (var attempt = 1; attempt <= retries; attempt++)
        {
            try
            {
                using var src = new FileStream(source, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, bufferSize, FileOptions.SequentialScan);
                using var dst = new FileStream(destination, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize, FileOptions.None);
                src.CopyTo(dst, bufferSize);
                return;
            }
            catch (IOException) when (attempt < retries)
            {
                int multiplier = 1 << (attempt - 1);
                int delay = Math.Min(baseDelayMs * multiplier, maxDelayMs);
                Thread.Sleep(delay + GetJitter(delay));
            }
            catch (UnauthorizedAccessException)
            {
                throw;
            }
            catch (Exception) when (attempt < retries)
            {
                Thread.Sleep(Math.Min(baseDelayMs, maxDelayMs));
            }
            catch (Exception ex)
            {
                throw new Exception($"Copy failed after {retries} attempts: {ex.Message}", ex);
            }
        }
    }
    #endregion


    #region === 非同步檔案複製與重試 ===
    /// <summary>
    /// 非同步版本的複製函式，避免阻塞執行緒。
    /// </summary>
    /// <param name="source">來源檔案路徑。</param>
    /// <param name="destination">目的地檔案路徑。</param>
    /// <param name="retries">最大重試次數；若小於 1 則視為 1。</param>
    /// <param name="baseDelayMs">指數退避的基底延遲（以毫秒為單位），每次重試會乘以 2 的指數倍。</param>
    /// <remarks>
    /// 使用非同步檔案流進行複製，適合高併發或 UI 環境。
    /// </remarks>
    /// <exception cref="FileNotFoundException">來源檔案不存在時拋出。</exception>
    /// <exception cref="Exception">重試失敗時拋出，包含原始例外。</exception>
    public static async Task CopyFileWithRetryAsync(string source, string destination, int retries = 3, int baseDelayMs = 500)
    {
        if (string.IsNullOrWhiteSpace(source)) throw new ArgumentNullException(nameof(source));
        if (string.IsNullOrWhiteSpace(destination)) throw new ArgumentNullException(nameof(destination));
        if (retries < 1) retries = 1;

        try
        {
            var srcFull = Path.GetFullPath(source);
            var dstFull = Path.GetFullPath(destination);
            if (string.Equals(srcFull, dstFull, StringComparison.OrdinalIgnoreCase)) return;
        }
        catch { /* ignore and proceed with copy attempts */ }

        if (!File.Exists(source)) throw new FileNotFoundException("Source file not found", source);

        const int bufferSize = 1024 * 64;
        const int maxDelayMs = 30_000;
        static int GetJitter(int delay) => Random.Shared.Next(0, Math.Min(100, Math.Max(1, delay / 10)));

        for (var attempt = 1; attempt <= retries; attempt++)
        {
            try
            {
                using var src = new FileStream(source, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, bufferSize, FileOptions.SequentialScan);
                using var dst = new FileStream(destination, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize, FileOptions.None);
                await src.CopyToAsync(dst, bufferSize).ConfigureAwait(false);
                return;
            }
            catch (IOException) when (attempt < retries)
            {
                int multiplier = 1 << (attempt - 1);
                int delay = Math.Min(baseDelayMs * multiplier, maxDelayMs);
                await Task.Delay(delay + GetJitter(delay)).ConfigureAwait(false);
            }
            catch (UnauthorizedAccessException)
            {
                throw;
            }
            catch (Exception) when (attempt < retries)
            {
                await Task.Delay(Math.Min(baseDelayMs, maxDelayMs)).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                throw new Exception($"Copy failed after {retries} attempts: {ex.Message}", ex);
            }
        }
    }
    #endregion


    #region === 安全刪除檔案（無日誌） ===
    /// <summary>
    /// 安全刪除指定路徑的檔案；若檔案不存在或刪除失敗，方法會吞掉例外並靜默返回。
    /// </summary>
    /// <param name="path">欲刪除的檔案完整路徑。</param>
    /// <remarks>
    /// 方法會先檢查 <see cref="File.Exists(string)"/>，若存在則呼叫 <see cref="File.Delete(string)"/>。
    /// 所有在刪除過程中產生的例外都會被捕捉並忽略，因此呼叫端無法從此方法得知刪除是否成功。
    /// </remarks>
    /// <example>
    /// <code language="csharp">
    /// // 範例：嘗試刪除檔案（失敗時不會拋出例外）
    /// FileHelpers.SafeDelete("C:\\temp\\oldfile.tmp");
    /// </code>
    /// </example>
    public static void SafeDelete(string path)
    {
        if (string.IsNullOrWhiteSpace(path)) return;
        try
        {
            File.Delete(path);
        }
        catch (FileNotFoundException) { /* 已不存在，忽略 */ }
        catch { /* 其他錯誤在此版本選擇靜默忽略 */ }
    }
    #endregion


    #region === 安全刪除檔案（有日誌） ===
    /// <summary>
    /// 安全刪除指定路徑的檔案，並可選擇記錄失敗訊息。
    /// </summary>
    /// <param name="path">欲刪除的檔案完整路徑。</param>
    /// <param name="logger">可選的日誌記錄器，失敗時記錄警告。</param>
    /// <remarks>
    /// 若刪除失敗，會將例外記錄到 logger（若有提供）。
    /// </remarks>
    public static void SafeDelete(string path, ILogger? logger)
    {
        if (string.IsNullOrWhiteSpace(path)) return;
        try
        {
            File.Delete(path);
        }
        catch (FileNotFoundException) { /* 已不存在，忽略 */ }
        catch (Exception ex)
        {
            logger?.LogWarning(ex, "SafeDelete failed for {path}", path);
        }
    }
    #endregion
}

using Microsoft.Extensions.Logging; // 引入日誌記錄相關的命名空間

/// <summary>
/// 提供檔案操作的輔助方法，例如複製重試與安全刪除。
/// </summary>
/// <remarks>
/// 這個類別將常見的檔案處理模式封裝成簡單、可重複使用的方法，
/// 以減少呼叫端對錯誤處理與重試邏輯的負擔。
/// </remarks>
public static class FileHelpers // 定義靜態類別 FileHelpers，封裝檔案操作輔助方法
{
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
        if (source is null) throw new ArgumentNullException(nameof(source));
        if (destination is null) throw new ArgumentNullException(nameof(destination));
        if (retries < 1) retries = 1; // 若重試次數小於 1，則設為 1

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
        const int maxDelayMs = 30_000; // 延遲上限
        var rnd = new Random();

        for (int attempt = 1; attempt <= retries; attempt++)
        {
            try
            {
                // 使用 FileStream 做同步複製以利大型檔案效能控制（SequentialScan）
                using var src = new FileStream(source, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, bufferSize, FileOptions.SequentialScan);
                using var dst = new FileStream(destination, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize, FileOptions.None);
                src.CopyTo(dst, bufferSize);
                return;
            }
            catch (IOException ex) // 常見可重試的 IO 錯誤（例如被鎖定）
            {
                if (attempt == retries)
                    throw new IOException($"Copy failed after {retries} attempts: {ex.Message}", ex);
                // 指數退避 + 小量 jitter，並限制最大延遲
                int multiplier = 1 << (attempt - 1);
                int delay = Math.Min(baseDelayMs * multiplier, maxDelayMs);
                int jitter = rnd.Next(0, Math.Min(100, Math.Max(1, delay / 10)));
                Thread.Sleep(delay + jitter);
            }
            catch (UnauthorizedAccessException) // 權限問題通常不是重試能解的
            {
                throw;
            }
            catch (Exception ex) // 其他例外視為不可重試（保守處理）
            {
                if (attempt == retries)
                    throw new Exception($"Copy failed after {retries} attempts: {ex.Message}", ex);
                // 小延遲再重試
                int delay = Math.Min(baseDelayMs, maxDelayMs);
                Thread.Sleep(delay);
            }
        }
    }

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
        if (source is null) throw new ArgumentNullException(nameof(source));
        if (destination is null) throw new ArgumentNullException(nameof(destination));
        if (retries < 1) retries = 1; // 若重試次數小於 1 則設為 1

        // 若來源與目的地相同，直接返回以避免 lock 與不必要的 IO
        try
        {
            var srcFull = Path.GetFullPath(source);
            var dstFull = Path.GetFullPath(destination);
            if (string.Equals(srcFull, dstFull, StringComparison.OrdinalIgnoreCase)) return;
        }
        catch { /* ignore and proceed with copy attempts */ }

        if (!File.Exists(source)) throw new FileNotFoundException("Source file not found", source);

        const int bufferSize = 1024 * 64; // 64KB buffer
        const int maxDelayMs = 30_000;
        var rnd = new Random();

        for (int attempt = 1; attempt <= retries; attempt++) // 依重試次數循環
        {
            try
            {
                // Use FileStream copy to allow async IO; use SequentialScan and larger buffer for better throughput on large files
                using var src = new FileStream(source, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, bufferSize, FileOptions.SequentialScan); // 開啟來源檔案流
                using var dst = new FileStream(destination, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize, FileOptions.None); // 開啟目的地檔案流
                await src.CopyToAsync(dst, bufferSize).ConfigureAwait(false); // 非同步複製檔案內容
                return; // 複製成功則結束方法
            }
            catch (IOException ex) // 可重試的 IO 問題
            {
                if (attempt == retries)
                    throw new IOException($"Copy failed after {retries} attempts: {ex.Message}", ex);
                int multiplier = 1 << (attempt - 1);
                int delay = Math.Min(baseDelayMs * multiplier, maxDelayMs);
                int jitter = rnd.Next(0, Math.Min(100, Math.Max(1, delay / 10)));
                await Task.Delay(delay + jitter).ConfigureAwait(false);
            }
            catch (UnauthorizedAccessException) // 權限錯誤不可重試
            {
                throw;
            }
            catch (Exception ex) // 其他例外視為不可重試（保守處理）
            {
                if (attempt == retries)
                    throw new Exception($"Copy failed after {retries} attempts: {ex.Message}", ex);
                await Task.Delay(baseDelayMs).ConfigureAwait(false);
            }
        }
    }

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
        if (path is null) return;
        try
        {
            // 直接刪除，File.Delete 在檔案不存在時會拋出 FileNotFoundException
            File.Delete(path);
        }
        catch (FileNotFoundException) { /* 已不存在，忽略 */ }
        catch { /* 其他錯誤在此版本選擇靜默忽略 */ }
    }

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
        if (path is null) return;
        try
        {
            File.Delete(path);
        }
        catch (FileNotFoundException) { /* 已不存在，忽略 */ }
        catch (Exception ex) // 捕捉刪除失敗的例外並記錄
        {
            logger?.LogWarning(ex, "SafeDelete failed for {path}", path); // 若有 logger 則記錄警告
        }
    }
}

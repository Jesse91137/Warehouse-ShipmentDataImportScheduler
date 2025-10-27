# ShipmentDataImportScheduler

簡短描述：本專案是一個排程型的 Windows/.NET 應用程式，用來自動讀取指定資料夾中的 Excel 檔案，執行驗證與轉換，然後將出貨資料批次寫入資料庫，以支援倉儲出貨資料的自動化匯入流程。

主要檔案與結構

- `ShipmentDataImportScheduler/`：專案原始程式與專案檔（.csproj）。
- `spec/process-shipment-data-import.md`：流程規格（機器可讀的版本），包含需求與驗收準則。

主要功能

- 偵測指定資料夾中的 Excel 檔案（.xlsx / .xls）。
- 解析並驗證欄位格式與必填性。
- 將驗證通過的資料轉換為資料表結構，並以批次方式寫入資料庫（Bulk Insert）。
- 支援多語系資源檔（請參閱輸出目錄中的語系資料）。

前置需求

- .NET SDK 8.0
- Windows（若要使用 Excel COM 互動，需安裝 Excel）
- 資料庫（例如 SQL Server）存取憑證與連線資訊

設定 (appsettings)

請在 `ShipmentDataImportScheduler/appsettings.json` 或環境變數中設定必要值。以下為範例（請勿在原始碼中硬編機敏憑證）：

```json
{
  "ConnectionStrings": {
    "DefaultConnection": "Server=YOUR_DB_SERVER;Database=YOUR_DB;User Id=YOUR_USER;Password=YOUR_PASSWORD;"
  },
  "WatchFolder": "C:\\path\\to\\incoming\\excel",
  "ProcessedFolder": "C:\\path\\to\\processed",
  "ErrorFolder": "C:\\path\\to\\errors",
  "LogLevel": "Information"
}
```

執行方式（開發 / 測試）

1. 使用 Visual Studio 開啟 `ShipmentDataImportScheduler.sln`，以 Debug 模式執行。
2. 或使用命令列（PowerShell）：

```powershell
dotnet build .\ShipmentDataImportScheduler\ShipmentDataImportScheduler.csproj
dotnet run --project .\ShipmentDataImportScheduler\ShipmentDataImportScheduler.csproj
```

1. 若要執行已發佈的可執行檔，請將 `appsettings.json` 放在同一目錄，或使用環境變數覆寫設定。

日常操作與監控

- 請確認 `WatchFolder` 有新的 Excel 檔案到達時，程式會處理並把成功的檔案移到 `ProcessedFolder`，失敗會移到 `ErrorFolder`。
- 檢查應用程式產生的日誌（視專案設定而定）以取得處理結果與錯誤訊息。

安全與祕密管理

- 切勿在原始碼或版本控制中硬編資料庫帳密。請使用環境變數、Windows 憑證保管庫或專用機密管理服務（例如 Azure Key Vault）。

除錯技巧

- 若遇到 Excel COM 相關錯誤，確認執行環境是否安裝相容版本的 Excel，並以具有互動式桌面的帳戶執行（非服務帳戶）以便於 COM 互動。
- 若是資料庫連線或權限錯誤，先使用 Database 客戶端測試 `DefaultConnection` 字串是否可連線。

貢獻指南

- 若要回報錯誤或提出功能建議，請使用 repository 的 Issue 功能。
- 接受 Pull Request；請在 PR 中包含測試或重現步驟。

授權

請依專案根目錄的授權檔案為準（若無請聯絡專案管理者）。

聯絡

如需進一步協助或部署相關問題，請聯絡專案所有者或維運團隊。

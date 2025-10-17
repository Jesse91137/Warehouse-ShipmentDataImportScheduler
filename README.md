# ShipmentDataImportScheduler

簡短描述：本專案為一個排程型 Windows/.NET 應用，用來讀取指定資料夾中的 Excel 檔案，驗證、轉換，並將出貨資料批次寫入資料庫。

主要檔案與說明：

- `ShipmentDataImportScheduler/`：程式原始碼與專案檔。
- `spec/process-shipment-data-import.md`：流程規格（機器可讀的版本），請參閱以取得詳細需求、介面與驗收準則。

如何執行（開發/測試環境）：

1. 開啟 Visual Studio 並載入 `ShipmentDataImportScheduler.sln`。
2. 在 `appsettings.json` 或環境變數中設定資料庫連線字串與檔案路徑。
3. 以 Debug 模式執行或從命令列啟動可執行檔。

注意事項：不要在原始碼中硬編資料庫憑證，請使用環境變數或機密管理機制。

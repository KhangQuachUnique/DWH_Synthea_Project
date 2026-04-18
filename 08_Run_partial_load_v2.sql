USE DW_Synthea_DWH;
GO

-- Chỉ lấy 4 năm gần đây cho nhẹ (từ đầu 2023 đến cuối 2026)
DECLARE @FromDate    DATE = '2023-01-01'; 
DECLARE @ToDate      DATE = '2026-12-31';
DECLARE @Today       DATE = CONVERT(DATE, GETDATE());

PRINT '=== START LOAD DWH V2 (RECENT DATA) - ' + CONVERT(VARCHAR, GETDATE(), 120) + ' ===';
PRINT 'From: ' + CONVERT(VARCHAR, @FromDate) + ' | To: ' + CONVERT(VARCHAR, @ToDate);

-- Run Load Orchestrator
EXEC dbo.usp_run_dwh_load
    @FromDate = @FromDate,
    @ToDate   = @ToDate,
    @AsOfDate = @Today;

PRINT '=== FINISH LOAD DWH V2 - ' + CONVERT(VARCHAR, GETDATE(), 120) + ' ===';
GO
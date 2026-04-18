USE DW_Synthea_DWH;
GO

-- Chỉ lấy 2 năm (từ đầu 2019 đến cuối 2020)
DECLARE @FromDate    DATE = '2019-01-01'; 
DECLARE @ToDate      DATE = '2020-12-31';
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
USE DW_Synthea_DWH;
GO

DECLARE @FromDate    DATE = '2019-01-01';
DECLARE @ToDate      DATE = '2020-7-31';
DECLARE @Today       DATE = CONVERT(DATE, GETDATE());
DECLARE @batch_id    UNIQUEIDENTIFIER = NEWID();

PRINT '=== START LOAD DWH V2 (RECENT DATA) - ' + CONVERT(VARCHAR, GETDATE(), 120) + ' ===';
PRINT 'From: ' + CONVERT(VARCHAR, @FromDate) + ' | To: ' + CONVERT(VARCHAR, @ToDate);
PRINT 'Batch ID: ' + CONVERT(VARCHAR(36), @batch_id);

-- Run Load Orchestrator
EXEC dbo.usp_run_dwh_load
    @FromDate = @FromDate,
    @ToDate   = @ToDate,
    @AsOfDate = @Today,
    @batch_id = @batch_id;

PRINT '=== FINISH LOAD DWH V2 - ' + CONVERT(VARCHAR, GETDATE(), 120) + ' ===';
GO
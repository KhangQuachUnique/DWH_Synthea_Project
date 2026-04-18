USE DW_Synthea_DWH;
GO

-- FIX CỨNG khoảng thời gian an toàn tuyệt đối cho DWH
DECLARE @MinDate     DATE = '1900-01-01'; 
DECLARE @MaxDate     DATE = '2099-12-31';
DECLARE @Today       DATE = CONVERT(DATE, GETDATE());

PRINT '=== START FULL LOAD DWH V2 - ' + CONVERT(VARCHAR, GETDATE(), 120) + ' ===';

-- CHẠY LOAD ORCHESTRATOR V2
EXEC dbo.usp_run_dwh_load
    @FromDate = @MinDate,
    @ToDate   = @MaxDate,
    @AsOfDate = @Today;

PRINT '=== FINISH FULL LOAD DWH V2 - ' + CONVERT(VARCHAR, GETDATE(), 120) + ' ===';
GO


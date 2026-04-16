USE DW_Synthea_DWH;
GO

DECLARE @Today DATE = CONVERT(DATE, GETDATE());

-- SCD2 dims
EXEC dbo.usp_load_dim_patient_scd2      @AsOfDate = @Today;
EXEC dbo.usp_load_dim_organization_scd2 @AsOfDate = @Today;
EXEC dbo.usp_load_dim_provider_scd2     @AsOfDate = @Today;
EXEC dbo.usp_load_dim_payer_scd2        @AsOfDate = @Today;

-- Type 1 dims + facts
EXEC dbo.usp_load_dim_encounter @FromDate='2010-01-01', @ToDate='2026-12-31';
EXEC dbo.usp_load_fact_utilization @FromDate='2010-01-01', @ToDate='2026-12-31';
EXEC dbo.usp_load_fact_conditions @FromDate='2010-01-01', @ToDate='2026-12-31';
EXEC dbo.usp_load_fact_condition_daily_snapshot @FromDate='2010-01-01', @ToDate='2026-12-31';
EXEC dbo.usp_load_fact_costs @FromDate='2010-01-01', @ToDate='2026-12-31';
GO
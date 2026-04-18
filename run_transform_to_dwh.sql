USE DW_Synthea_DWH;
GO

-- =============================================
-- FULL LOAD DWH - TỰ ĐỘNG TÌM NGÀY BẮT ĐẦU
-- =============================================

DECLARE @Today       DATE = CONVERT(DATE, GETDATE());
DECLARE @MinDate     DATE;
DECLARE @MaxDate     DATE = '2026-12-31';   -- Bạn có thể giữ hoặc cũng tính động

PRINT '=== BẮT ĐẦU FULL LOAD DWH - ' + CONVERT(VARCHAR, GETDATE(), 120) + ' ===';

-- ==================== TÍNH TỰ ĐỘNG MIN DATE ====================
SELECT @MinDate = MIN(dt)
FROM (
    -- Ngày sinh của bệnh nhân (thường sớm nhất)
    SELECT MIN(BIRTHDATE) AS dt FROM [DW_Synthea_Staging].[dbo].[Staging_Patients]
    
    UNION ALL
    -- Ngày bắt đầu của Encounter
    SELECT MIN(CONVERT(DATE, [START])) FROM [DW_Synthea_Staging].[dbo].[Staging_Encounters]
    
    UNION ALL
    -- Ngày bắt đầu của Condition
    SELECT MIN(CONVERT(DATE, [START])) FROM [DW_Synthea_Staging].[dbo].[Staging_Conditions]
    
    UNION ALL
    -- Ngày của Medication, Procedure, Immunization (để an toàn)
    SELECT MIN(CONVERT(DATE, [START])) FROM [DW_Synthea_Staging].[dbo].[Staging_Medications]
    UNION ALL
    SELECT MIN(CONVERT(DATE, [DATE]))  FROM [DW_Synthea_Staging].[dbo].[Staging_Procedures]
    UNION ALL
    SELECT MIN(CONVERT(DATE, [DATE]))  FROM [DW_Synthea_Staging].[dbo].[Staging_Immunizations]
) AS AllDates(dt)
WHERE dt IS NOT NULL;

-- Nếu không tìm thấy thì fallback về 2010-01-01
IF @MinDate IS NULL
    SET @MinDate = '2010-01-01';

PRINT 'Min Date tự động tìm được: ' + CONVERT(VARCHAR(10), @MinDate, 120);
PRINT 'Max Date sử dụng: ' + CONVERT(VARCHAR(10), @MaxDate, 120);

-- ==================== CHẠY LOAD ====================

EXEC dbo.usp_load_dim_date 
    @StartDate = @MinDate, 
    @EndDate   = @MaxDate;

EXEC dbo.usp_load_dim_condition_code;

-- SCD2 Dimensions (dùng @Today để expire nếu có thay đổi)
EXEC dbo.usp_load_dim_patient_scd2      @AsOfDate = @Today;
EXEC dbo.usp_load_dim_organization_scd2 @AsOfDate = @Today;
EXEC dbo.usp_load_dim_provider_scd2     @AsOfDate = @Today;
EXEC dbo.usp_load_dim_payer_scd2        @AsOfDate = @Today;

-- Dim Encounter và Facts
EXEC dbo.usp_load_dim_encounter 
    @FromDate = @MinDate, 
    @ToDate   = @MaxDate;

EXEC dbo.usp_load_fact_utilization 
    @FromDate = @MinDate, 
    @ToDate   = @MaxDate;

EXEC dbo.usp_load_fact_conditions 
    @FromDate = @MinDate, 
    @ToDate   = @MaxDate;

EXEC dbo.usp_load_fact_condition_daily_snapshot 
    @FromDate = @MinDate, 
    @ToDate   = @MaxDate;

EXEC dbo.usp_load_fact_costs 
    @FromDate = @MinDate, 
    @ToDate   = @MaxDate;

PRINT '=== HOÀN TẤT FULL LOAD DWH - ' + CONVERT(VARCHAR, GETDATE(), 120) + ' ===';
GO
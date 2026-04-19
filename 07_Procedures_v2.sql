-- ============================================================================
-- Stored Procedures: Load from [DW_Synthea_Staging] -> [DW_Synthea_DWH] (v6 - Incremental + Logging)
-- ============================================================================

-- 1. XÓA HOÀN TOÀN tất cả code liên quan đến ICU
-- 2. usp_load_fact_encounter_daily và usp_load_fact_condition_daily đã dọn sạch
-- 3. SỬA LẠI dim_provider dùng cột UTILIZATION (chuẩn dataset của bạn)
-- ============================================================================

USE [DW_Synthea_DWH];
GO

-- ETL_Run_Log table (nếu chưa có)
IF OBJECT_ID('dbo.ETL_Run_Log', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.ETL_Run_Log (
        log_id INT IDENTITY(1,1) PRIMARY KEY,
        proc_name NVARCHAR(128),
        batch_id UNIQUEIDENTIFIER,
        start_time DATETIME2,
        end_time DATETIME2,
        status NVARCHAR(20),
        rows_affected INT,
        error_msg NVARCHAR(MAX),
        params NVARCHAR(4000),
        created_at DATETIME2 DEFAULT SYSUTCDATETIME()
    );
END
GO

CREATE OR ALTER FUNCTION dbo.fn_date_key (@d DATE)
RETURNS INT
AS
BEGIN
    RETURN (CONVERT(INT, CONVERT(CHAR(8), @d, 112)));
END;
GO

-- ============================================================================
-- 1. DIM_DATE
-- ============================================================================
CREATE OR ALTER PROCEDURE dbo.usp_load_dim_date
    @StartDate DATE, 
    @EndDate   DATE, 
    @batch_id  UNIQUEIDENTIFIER = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @log_id INT, @start_time DATETIME2 = SYSUTCDATETIME();
    IF @batch_id IS NULL SET @batch_id = NEWID();

    BEGIN TRY
        IF @StartDate IS NULL OR @EndDate IS NULL OR @EndDate < @StartDate
            THROW 50000, 'Invalid date range', 1;

        ;WITH n AS (
            SELECT TOP (DATEDIFF(DAY, @StartDate, @EndDate) + 1)
                ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS n
            FROM sys.all_objects a CROSS JOIN sys.all_objects b
        ),
        d AS (SELECT DATEADD(DAY, n.n, @StartDate) AS full_date FROM n)
        INSERT INTO dbo.dim_date 
            (date_key, full_date, [year], [quarter], [month], month_name, 
             [week_of_year], day_of_month, day_of_week, day_name, is_weekend)
        SELECT
            dbo.fn_date_key(d.full_date), 
            d.full_date,
            DATEPART(YEAR, d.full_date), 
            DATEPART(QUARTER, d.full_date), 
            DATEPART(MONTH, d.full_date),
            DATENAME(MONTH, d.full_date), 
            DATEPART(ISOWK, d.full_date), 
            DATEPART(DAY, d.full_date),
            ((DATEPART(WEEKDAY, d.full_date) + @@DATEFIRST - 2) % 7) + 1,
            DATENAME(WEEKDAY, d.full_date),
            CASE WHEN (((DATEPART(WEEKDAY, d.full_date) + @@DATEFIRST - 2) % 7) + 1) IN (6,7) 
                 THEN 1 ELSE 0 END
        FROM d
        WHERE NOT EXISTS (SELECT 1 FROM dbo.dim_date x WHERE x.full_date = d.full_date);

        INSERT INTO dbo.ETL_Run_Log(proc_name, batch_id, start_time, end_time, status, rows_affected, params)
        VALUES ('usp_load_dim_date', @batch_id, @start_time, SYSUTCDATETIME(), 'SUCCESS', @@ROWCOUNT, 
                CONCAT('StartDate=',@StartDate,';EndDate=',@EndDate));
    END TRY
    BEGIN CATCH
        INSERT INTO dbo.ETL_Run_Log(proc_name, batch_id, start_time, end_time, status, error_msg, params)
        VALUES ('usp_load_dim_date', @batch_id, @start_time, SYSUTCDATETIME(), 'FAILED', ERROR_MESSAGE(), 
                CONCAT('StartDate=',@StartDate,';EndDate=',@EndDate));
        THROW;
    END CATCH
END;
GO

CREATE OR ALTER PROCEDURE dbo.usp_load_dim_condition_code
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO dbo.dim_condition_code (code, [description])
    SELECT s.code, MAX(s.[description])
    FROM [DW_Synthea_Staging].[dbo].[Staging_Conditions] s
    WHERE s.code IS NOT NULL
    GROUP BY s.code
    HAVING NOT EXISTS (SELECT 1 FROM dbo.dim_condition_code d WHERE d.code = s.code);
END;
GO

CREATE OR ALTER PROCEDURE dbo.usp_load_dim_medication
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO dbo.dim_medication (medication_code, medication_description)
    SELECT s.CODE, MAX(s.[DESCRIPTION])
    FROM [DW_Synthea_Staging].[dbo].[Staging_Medications] s
    WHERE s.CODE IS NOT NULL
    GROUP BY s.CODE
    HAVING NOT EXISTS (SELECT 1 FROM dbo.dim_medication d WHERE d.medication_code = s.CODE);
END;
GO

CREATE OR ALTER PROCEDURE dbo.usp_load_dim_patient_scd2
    @AsOfDate DATE = NULL
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    IF @AsOfDate IS NULL SET @AsOfDate = CONVERT(DATE, GETDATE());
    DECLARE @OpenEnded DATE = '9999-12-31', @InitialValidFrom DATE = '1900-01-01';

    BEGIN TRAN;
    IF OBJECT_ID('tempdb..#src_patient') IS NOT NULL DROP TABLE #src_patient;

    SELECT 
        p.Id AS patient_id, 
        p.FIRST AS first_name, 
        p.LAST AS last_name, 
        p.BIRTHDATE AS birthdate, 
        p.DEATHDATE AS deathdate,
        p.GENDER AS gender, 
        p.RACE AS race, 
        p.ETHNICITY AS ethnicity, 
        p.CITY AS city, 
        p.STATE AS state, 
        p.ZIP AS zip, 
        p.COUNTY AS county,
        p.HEALTHCARE_EXPENSES AS healthcare_expenses, 
        p.HEALTHCARE_COVERAGE AS healthcare_coverage,
        HASHBYTES('SHA2_256', CONCAT_WS('|', 
            ISNULL(p.CITY,''), ISNULL(p.STATE,''), ISNULL(p.ZIP,''), ISNULL(p.COUNTY,''),
            ISNULL(CONVERT(VARCHAR(10), p.DEATHDATE, 23), ''),
            ISNULL(CONVERT(VARCHAR(30), p.HEALTHCARE_EXPENSES), ''), 
            ISNULL(CONVERT(VARCHAR(30), p.HEALTHCARE_COVERAGE), ''))) AS row_hash
    INTO #src_patient 
    FROM [DW_Synthea_Staging].[dbo].[Staging_Patients] p 
    WHERE p.Id IS NOT NULL;

    CREATE INDEX IX_src_patient_id ON #src_patient(patient_id);

    IF OBJECT_ID('tempdb..#chg_patient') IS NOT NULL DROP TABLE #chg_patient;
    SELECT s.patient_id 
    INTO #chg_patient 
    FROM #src_patient s 
    JOIN dbo.dim_patient d ON d.patient_id = s.patient_id AND d.is_current = 1 
    WHERE d.row_hash <> s.row_hash;

    CREATE UNIQUE CLUSTERED INDEX IX_chg_patient ON #chg_patient(patient_id);

    UPDATE d 
    SET d.valid_to = DATEADD(DAY, -1, @AsOfDate), 
        d.is_current = 0, 
        d.update_at = SYSUTCDATETIME()
    FROM dbo.dim_patient d 
    JOIN #chg_patient c ON c.patient_id = d.patient_id 
    WHERE d.is_current = 1;

    INSERT INTO dbo.dim_patient 
        (patient_id, first_name, last_name, birthdate, deathdate, gender, race, ethnicity, 
         city, state, zip, county, healthcare_expenses, healthcare_coverage, 
         valid_from, valid_to, is_current, row_hash)
    SELECT 
        s.patient_id, s.first_name, s.last_name, s.birthdate, s.deathdate, 
        s.gender, s.race, s.ethnicity, s.city, s.state, s.zip, s.county, 
        s.healthcare_expenses, s.healthcare_coverage, 
        @InitialValidFrom, @OpenEnded, 1, s.row_hash
    FROM #src_patient s 
    LEFT JOIN dbo.dim_patient d ON d.patient_id = s.patient_id AND d.is_current = 1
    WHERE d.patient_id IS NULL 
       OR EXISTS (SELECT 1 FROM #chg_patient c WHERE c.patient_id = s.patient_id);

    COMMIT;
END;
GO

CREATE OR ALTER PROCEDURE dbo.usp_load_dim_organization_scd2
    @AsOfDate DATE = NULL
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    IF @AsOfDate IS NULL SET @AsOfDate = CONVERT(DATE, GETDATE());
    DECLARE @OpenEnded DATE = '9999-12-31', @InitialValidFrom DATE = '1900-01-01';

    BEGIN TRAN;
    IF OBJECT_ID('tempdb..#src_org') IS NOT NULL DROP TABLE #src_org;

    SELECT o.Id AS organization_id, o.NAME AS [name], o.CITY AS city, o.STATE AS [state], o.ZIP AS zip, o.PHONE AS phone, o.REVENUE AS revenue, o.UTILIZATION AS utilization,
        HASHBYTES('SHA2_256', CONCAT_WS('|', ISNULL(o.NAME,''), ISNULL(o.CITY,''), ISNULL(o.STATE,''), ISNULL(CONVERT(VARCHAR(30), o.REVENUE), ''), ISNULL(CONVERT(VARCHAR(30), o.UTILIZATION), ''))) AS row_hash
    INTO #src_org FROM [DW_Synthea_Staging].[dbo].[Staging_Organizations] o WHERE o.Id IS NOT NULL;

    CREATE INDEX IX_src_org_id ON #src_org(organization_id);
    IF OBJECT_ID('tempdb..#chg_org') IS NOT NULL DROP TABLE #chg_org;
    SELECT s.organization_id INTO #chg_org FROM #src_org s JOIN dbo.dim_organization d ON d.organization_id = s.organization_id AND d.is_current = 1 WHERE d.row_hash <> s.row_hash;
    CREATE UNIQUE CLUSTERED INDEX IX_chg_org ON #chg_org(organization_id);

    UPDATE d SET d.valid_to = DATEADD(DAY, -1, @AsOfDate), d.is_current = 0, d.update_at = SYSUTCDATETIME()
    FROM dbo.dim_organization d JOIN #chg_org c ON c.organization_id = d.organization_id WHERE d.is_current = 1;

    INSERT INTO dbo.dim_organization (organization_id, [name], city, [state], zip, phone, revenue, utilization, valid_from, valid_to, is_current, row_hash)
    SELECT s.organization_id, s.[name], s.city, s.[state], s.zip, s.phone, s.revenue, s.utilization, @InitialValidFrom, @OpenEnded, 1, s.row_hash
    FROM #src_org s LEFT JOIN dbo.dim_organization d ON d.organization_id = s.organization_id AND d.is_current = 1
    WHERE d.organization_id IS NULL OR EXISTS (SELECT 1 FROM #chg_org c WHERE c.organization_id = s.organization_id);

    COMMIT;
END;
GO

CREATE OR ALTER PROCEDURE dbo.usp_load_dim_provider_scd2
    @AsOfDate DATE = NULL
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    IF @AsOfDate IS NULL SET @AsOfDate = CONVERT(DATE, GETDATE());
    DECLARE @OpenEnded DATE = '9999-12-31', @InitialValidFrom DATE = '1900-01-01';

    BEGIN TRAN;
    IF OBJECT_ID('tempdb..#src_provider') IS NOT NULL DROP TABLE #src_provider;

    SELECT p.Id AS provider_id, p.ORGANIZATION AS organization_id, p.NAME AS [name], p.GENDER AS gender, p.SPECIALITY AS speciality, p.CITY AS city, p.STATE AS [state], p.ZIP AS zip, 
           p.UTILIZATION AS utilization,   -- ← ĐÃ SỬA LẠI ĐÚNG CỘT
           HASHBYTES('SHA2_256', CONCAT_WS('|', ISNULL(p.ORGANIZATION,''), ISNULL(p.SPECIALITY,''), ISNULL(p.CITY,''), ISNULL(p.STATE,''), ISNULL(CONVERT(VARCHAR(30), p.UTILIZATION), ''))) AS row_hash
    INTO #src_provider FROM [DW_Synthea_Staging].[dbo].[Staging_Providers] p WHERE p.Id IS NOT NULL;

    CREATE INDEX IX_src_provider_id ON #src_provider(provider_id);
    IF OBJECT_ID('tempdb..#chg_provider') IS NOT NULL DROP TABLE #chg_provider;
    SELECT s.provider_id INTO #chg_provider FROM #src_provider s JOIN dbo.dim_provider d ON d.provider_id = s.provider_id AND d.is_current = 1 WHERE d.row_hash <> s.row_hash;
    CREATE UNIQUE CLUSTERED INDEX IX_chg_provider ON #chg_provider(provider_id);

    UPDATE d SET d.valid_to = DATEADD(DAY, -1, @AsOfDate), d.is_current = 0, d.update_at = SYSUTCDATETIME()
    FROM dbo.dim_provider d JOIN #chg_provider c ON c.provider_id = d.provider_id WHERE d.is_current = 1;

    INSERT INTO dbo.dim_provider (provider_id, organization_id, [name], gender, speciality, city, [state], zip, utilization, valid_from, valid_to, is_current, row_hash)
    SELECT s.provider_id, s.organization_id, s.[name], s.gender, s.speciality, s.city, s.[state], s.zip, s.utilization, @InitialValidFrom, @OpenEnded, 1, s.row_hash
    FROM #src_provider s LEFT JOIN dbo.dim_provider d ON d.provider_id = s.provider_id AND d.is_current = 1
    WHERE d.provider_id IS NULL OR EXISTS (SELECT 1 FROM #chg_provider c WHERE c.provider_id = s.provider_id);

    COMMIT;
END;
GO

CREATE OR ALTER PROCEDURE dbo.usp_load_dim_payer_scd2
    @AsOfDate DATE = NULL
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    IF @AsOfDate IS NULL SET @AsOfDate = CONVERT(DATE, GETDATE());
    DECLARE @OpenEnded DATE = '9999-12-31', @InitialValidFrom DATE = '1900-01-01';

    BEGIN TRAN;
    IF OBJECT_ID('tempdb..#src_payer') IS NOT NULL DROP TABLE #src_payer;

    ;WITH ownership AS (SELECT t.PAYER AS payer_id, MAX(t.OWNERSHIP) AS ownership FROM [DW_Synthea_Staging].[dbo].[Staging_Payer_Transitions] t WHERE t.PAYER IS NOT NULL GROUP BY t.PAYER)
    SELECT p.Id AS payer_id, p.NAME AS [name], p.CITY AS city, p.STATE_HEADQUARTERED AS state_headquartered, p.ZIP AS zip, p.PHONE AS phone, o.ownership AS ownership, 
           p.AMOUNT_COVERED AS amount_covered, p.AMOUNT_UNCOVERED AS amount_uncovered, p.REVENUE AS revenue, p.UNIQUE_CUSTOMERS AS unique_customers, p.MEMBER_MONTHS AS member_months,
           HASHBYTES('SHA2_256', CONCAT_WS('|', ISNULL(o.ownership,''), ISNULL(CONVERT(VARCHAR(30), p.AMOUNT_COVERED), ''), ISNULL(CONVERT(VARCHAR(30), p.REVENUE), ''), ISNULL(CONVERT(VARCHAR(30), p.UNIQUE_CUSTOMERS), ''))) AS row_hash
    INTO #src_payer FROM [DW_Synthea_Staging].[dbo].[Staging_Payers] p LEFT JOIN ownership o ON o.payer_id = p.Id WHERE p.Id IS NOT NULL;

    CREATE INDEX IX_src_payer_id ON #src_payer(payer_id);
    IF OBJECT_ID('tempdb..#chg_payer') IS NOT NULL DROP TABLE #chg_payer;
    SELECT s.payer_id INTO #chg_payer FROM #src_payer s JOIN dbo.dim_payer d ON d.payer_id = s.payer_id AND d.is_current = 1 WHERE d.row_hash <> s.row_hash;
    CREATE UNIQUE CLUSTERED INDEX IX_chg_payer ON #chg_payer(payer_id);

    UPDATE d SET d.valid_to = DATEADD(DAY, -1, @AsOfDate), d.is_current = 0, d.update_at = SYSUTCDATETIME()
    FROM dbo.dim_payer d JOIN #chg_payer c ON c.payer_id = d.payer_id WHERE d.is_current = 1;

    INSERT INTO dbo.dim_payer (payer_id, [name], city, state_headquartered, zip, phone, ownership, amount_covered, amount_uncovered, revenue, unique_customers, member_months, valid_from, valid_to, is_current, row_hash)
    SELECT s.payer_id, s.[name], s.city, s.state_headquartered, s.zip, s.phone, s.ownership, s.amount_covered, s.amount_uncovered, s.revenue, s.unique_customers, s.member_months, @InitialValidFrom, @OpenEnded, 1, s.row_hash
    FROM #src_payer s LEFT JOIN dbo.dim_payer d ON d.payer_id = s.payer_id AND d.is_current = 1
    WHERE d.payer_id IS NULL OR EXISTS (SELECT 1 FROM #chg_payer c WHERE c.payer_id = s.payer_id);

    COMMIT;
END;
GO

CREATE OR ALTER PROCEDURE dbo.usp_load_fact_encounter
    @FromDate DATE, 
    @ToDate   DATE
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;

    EXEC dbo.usp_load_dim_date @FromDate, @ToDate;

    -- Incremental delete
    DELETE FROM dbo.fact_encounter 
    WHERE encounter_date_key BETWEEN dbo.fn_date_key(@FromDate) AND dbo.fn_date_key(@ToDate);

    ;WITH src AS (
        SELECT 
            e.Id AS encounter_id,
            CONVERT(DATE, e.[START]) AS encounter_date,
            e.PATIENT AS patient_id,
            e.PROVIDER AS provider_id,
            e.PAYER AS payer_id,
            e.ORGANIZATION AS organization_id,
            e.ENCOUNTERCLASS AS encounter_class,
            e.CODE AS encounter_code,
            e.DESCRIPTION AS encounter_description,
            e.REASONCODE AS reason_code,
            ISNULL(e.BASE_ENCOUNTER_COST, 0) AS base_cost,
            ISNULL(e.TOTAL_CLAIM_COST, 0) AS total_claim_cost,
            ISNULL(e.PAYER_COVERAGE, 0) AS payer_coverage
        FROM [DW_Synthea_Staging].[dbo].[Staging_Encounters] e
        WHERE e.Id IS NOT NULL 
          AND CONVERT(DATE, e.[START]) BETWEEN @FromDate AND @ToDate
    )
    INSERT INTO dbo.fact_encounter (
        patient_key, encounter_date_key, provider_key, organization_key, payer_key,
        encounter_id, encounter_code, encounter_class, encounter_description, 
        reason_code, base_cost, total_claim_cost, payer_coverage
    )
    SELECT 
        p.patient_key,
        dbo.fn_date_key(s.encounter_date),
        pr.provider_key,
        o.organization_key,
        pa.payer_key,
        s.encounter_id,
        s.encounter_code,
        s.encounter_class,
        s.encounter_description,
        s.reason_code,
        s.base_cost,
        s.total_claim_cost,
        s.payer_coverage
    FROM src s
    OUTER APPLY (SELECT TOP 1 patient_key FROM dbo.dim_patient d 
                 WHERE d.patient_id = s.patient_id 
                   AND s.encounter_date BETWEEN d.valid_from AND d.valid_to 
                 ORDER BY d.valid_from DESC) p
    OUTER APPLY (SELECT TOP 1 provider_key FROM dbo.dim_provider d 
                 WHERE d.provider_id = s.provider_id 
                   AND s.encounter_date BETWEEN d.valid_from AND d.valid_to 
                 ORDER BY d.valid_from DESC) pr
    OUTER APPLY (SELECT TOP 1 organization_key FROM dbo.dim_organization d 
                 WHERE d.organization_id = s.organization_id 
                   AND s.encounter_date BETWEEN d.valid_from AND d.valid_to 
                 ORDER BY d.valid_from DESC) o
    OUTER APPLY (SELECT TOP 1 payer_key FROM dbo.dim_payer d 
                 WHERE d.payer_id = s.payer_id 
                   AND s.encounter_date BETWEEN d.valid_from AND d.valid_to 
                 ORDER BY d.valid_from DESC) pa;
END;
GO

CREATE OR ALTER PROCEDURE dbo.usp_load_fact_encounter_daily
    @FromDate DATE, 
    @ToDate   DATE
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;

    DELETE FROM dbo.fact_encounter_daily 
    WHERE date_key BETWEEN dbo.fn_date_key(@FromDate) AND dbo.fn_date_key(@ToDate);

    INSERT INTO dbo.fact_encounter_daily (
        date_key, organization_key, payer_key, encounter_class,
        encounter_count, unique_patient_count,
        total_base_cost, total_claim_cost, total_payer_coverage, total_out_of_pocket,
        hospitalized_count, total_los_days
    )
    SELECT 
        encounter_date_key AS date_key,
        organization_key, 
        payer_key, 
        encounter_class,
        COUNT(*) AS encounter_count,
        COUNT(DISTINCT patient_key) AS unique_patient_count,
        SUM(base_cost) AS total_base_cost,
        SUM(total_claim_cost) AS total_claim_cost,
        SUM(payer_coverage) AS total_payer_coverage,
        SUM(out_of_pocket) AS total_out_of_pocket,
        SUM(CASE WHEN encounter_class IN ('inpatient') THEN 1 ELSE 0 END) AS hospitalized_count,
        0 AS total_los_days                     -- Không lưu stop_date trong fact_encounter v2
    FROM dbo.fact_encounter
    WHERE encounter_date_key BETWEEN dbo.fn_date_key(@FromDate) AND dbo.fn_date_key(@ToDate)
      AND organization_key IS NOT NULL 
      AND payer_key IS NOT NULL
    GROUP BY encounter_date_key, organization_key, payer_key, encounter_class;
END;
GO

-- ============================================================================
-- FACT CONDITION (đã fix tên table + columns theo schema v2)
-- ============================================================================
CREATE OR ALTER PROCEDURE dbo.usp_load_fact_conditions
    @FromDate DATE, 
    @ToDate   DATE
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;

    EXEC dbo.usp_load_dim_condition_code;

    DELETE FROM dbo.fact_condition 
    WHERE start_date_key BETWEEN dbo.fn_date_key(@FromDate) AND dbo.fn_date_key(@ToDate);

    ;WITH src AS (
        SELECT 
            c.[START] AS start_date, 
            c.[STOP] AS stop_date, 
            c.PATIENT AS patient_id, 
            c.ENCOUNTER AS encounter_id, 
            c.CODE AS condition_code, 
            c.[DESCRIPTION] AS condition_description,
            e.PROVIDER AS provider_id, 
            e.ORGANIZATION AS organization_id
        FROM [DW_Synthea_Staging].[dbo].[Staging_Conditions] c
        LEFT JOIN [DW_Synthea_Staging].[dbo].[Staging_Encounters] e ON e.Id = c.ENCOUNTER
        WHERE c.[START] BETWEEN @FromDate AND @ToDate 
          AND c.CODE IS NOT NULL
    )
    INSERT INTO dbo.fact_condition (
        patient_key, start_date_key, stop_date_key, condition_code_key,
        provider_key, organization_key, encounter_id, 
        condition_code, condition_description
    )
    SELECT 
        p.patient_key, 
        dbo.fn_date_key(s.start_date),
        CASE WHEN s.stop_date IS NULL OR s.stop_date < '1900-01-01' OR s.stop_date > '2099-12-31' 
             THEN NULL ELSE dbo.fn_date_key(s.stop_date) END,
        cc.condition_code_key,
        pr.provider_key, 
        o.organization_key, 
        s.encounter_id, 
        s.condition_code, 
        s.condition_description
    FROM src s
    JOIN dbo.dim_condition_code cc ON cc.code = s.condition_code
    OUTER APPLY (SELECT TOP 1 patient_key FROM dbo.dim_patient d 
                 WHERE d.patient_id = s.patient_id AND s.start_date BETWEEN d.valid_from AND d.valid_to 
                 ORDER BY d.valid_from DESC) p
    OUTER APPLY (SELECT TOP 1 provider_key FROM dbo.dim_provider d 
                 WHERE d.provider_id = s.provider_id AND s.start_date BETWEEN d.valid_from AND d.valid_to 
                 ORDER BY d.valid_from DESC) pr
    OUTER APPLY (SELECT TOP 1 organization_key FROM dbo.dim_organization d 
                 WHERE d.organization_id = s.organization_id AND s.start_date BETWEEN d.valid_from AND d.valid_to 
                 ORDER BY d.valid_from DESC) o;
END;
GO

-- ============================================================================
-- FACT CONDITION DAILY (fix hospitalized logic - không còn is_hospitalized)
-- ============================================================================
CREATE OR ALTER PROCEDURE dbo.usp_load_fact_condition_daily
    @FromDate DATE, 
    @ToDate   DATE
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;

    DELETE FROM dbo.fact_condition_daily 
    WHERE date_key BETWEEN dbo.fn_date_key(@FromDate) AND dbo.fn_date_key(@ToDate);

    ;WITH daily_calc AS (
        SELECT 
            d.date_key, 
            fc.condition_code_key, 
            fc.organization_key,
            SUM(CASE WHEN fc.start_date_key = d.date_key THEN 1 ELSE 0 END) AS new_cases,
            SUM(CASE WHEN fc.stop_date_key = d.date_key THEN 1 ELSE 0 END) AS resolved_cases,
            SUM(CASE WHEN fc.start_date_key <= d.date_key 
                     AND (fc.stop_date_key > d.date_key OR fc.stop_date_key IS NULL) 
                     THEN 1 ELSE 0 END) AS active_cases,
            -- Hospitalized = condition đang active + encounter liên quan là inpatient
            SUM(CASE WHEN e.encounter_class IN ('inpatient') 
                     AND fc.start_date_key <= d.date_key 
                     AND (fc.stop_date_key IS NULL OR fc.stop_date_key >= d.date_key) 
                     THEN 1 ELSE 0 END) AS hospitalized_cases
        FROM dbo.fact_condition fc
        CROSS JOIN dbo.dim_date d
        LEFT JOIN dbo.fact_encounter e ON e.encounter_id = fc.encounter_id
        WHERE d.full_date BETWEEN @FromDate AND @ToDate
          AND fc.start_date_key <= dbo.fn_date_key(@ToDate)
          AND fc.organization_key IS NOT NULL
        GROUP BY d.date_key, fc.condition_code_key, fc.organization_key
    )
    INSERT INTO dbo.fact_condition_daily (
        date_key, condition_code_key, organization_key, 
        new_cases, active_cases, resolved_cases, cumulative_cases, hospitalized_cases
    )
    SELECT 
        date_key, condition_code_key, organization_key, 
        new_cases, active_cases, resolved_cases, 
        SUM(new_cases) OVER(PARTITION BY condition_code_key, organization_key 
                            ORDER BY date_key ROWS UNBOUNDED PRECEDING) AS cumulative_cases,
        hospitalized_cases
    FROM daily_calc 
    WHERE (new_cases > 0 OR active_cases > 0 OR resolved_cases > 0 OR hospitalized_cases > 0);
END;
GO

-- ============================================================================
-- FACT MEDICATIONS (đã fix theo schema v2 + gọi dim_medication)
-- ============================================================================
CREATE OR ALTER PROCEDURE dbo.usp_load_fact_medications
    @FromDate DATE, 
    @ToDate   DATE
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;

    EXEC dbo.usp_load_dim_medication;

    -- Đảm bảo dim_date bao quát stop_date
    DECLARE @minDate DATE = @FromDate, @maxDate DATE = @ToDate;
    SELECT @minDate = MIN(CONVERT(DATE, m.[START])),
           @maxDate = MAX(COALESCE(CONVERT(DATE, m.[STOP]), CONVERT(DATE, m.[START])))
    FROM [DW_Synthea_Staging].[dbo].[Staging_Medications] m
    WHERE m.[START] BETWEEN @FromDate AND @ToDate;

    IF @minDate IS NULL SET @minDate = @FromDate;
    EXEC dbo.usp_load_dim_date @minDate, @maxDate;

    DELETE FROM dbo.fact_medications 
    WHERE start_date_key BETWEEN dbo.fn_date_key(@FromDate) AND dbo.fn_date_key(@ToDate);

    ;WITH src AS (
        SELECT 
            m.PATIENT AS patient_id, 
            CONVERT(DATE, m.[START]) AS start_date, 
            ISNULL(m.CODE, 'UNKNOWN') AS medication_code,
            MAX(CONVERT(DATE, m.[STOP])) AS stop_date, 
            MAX(m.PAYER) AS payer_id, 
            MAX(m.ENCOUNTER) AS encounter_id,
            MAX(m.REASONCODE) AS reason_code,
            MAX(e.PROVIDER) AS provider_id, 
            MAX(e.ORGANIZATION) AS organization_id,
            SUM(ISNULL(m.BASE_COST, 0)) AS base_cost, 
            SUM(ISNULL(m.PAYER_COVERAGE, 0)) AS payer_coverage, 
            SUM(ISNULL(m.DISPENSES, 1)) AS dispense_count
        FROM [DW_Synthea_Staging].[dbo].[Staging_Medications] m
        LEFT JOIN [DW_Synthea_Staging].[dbo].[Staging_Encounters] e ON e.Id = m.ENCOUNTER
        WHERE m.[START] BETWEEN @FromDate AND @ToDate
        GROUP BY m.PATIENT, CONVERT(DATE, m.[START]), ISNULL(m.CODE, 'UNKNOWN')
    )
    INSERT INTO dbo.fact_medications (
        patient_key, medication_key, start_date_key, stop_date_key,
        provider_key, organization_key, payer_key, encounter_id,
        reason_code, base_cost, payer_coverage, dispense_count
    )
    SELECT 
        p.patient_key, 
        dm.medication_key,
        dbo.fn_date_key(s.start_date),
        CASE WHEN s.stop_date IS NULL OR s.stop_date < '1900-01-01' OR s.stop_date > '2099-12-31' 
             THEN NULL ELSE dbo.fn_date_key(s.stop_date) END,
        pr.provider_key, 
        o.organization_key, 
        pa.payer_key, 
        s.encounter_id, 
        s.reason_code, 
        s.base_cost, 
        s.payer_coverage, 
        s.dispense_count
    FROM src s
    INNER JOIN dbo.dim_medication dm ON dm.medication_code = s.medication_code
    OUTER APPLY (SELECT TOP 1 patient_key FROM dbo.dim_patient d 
                 WHERE d.patient_id = s.patient_id AND s.start_date BETWEEN d.valid_from AND d.valid_to 
                 ORDER BY d.valid_from DESC) p
    OUTER APPLY (SELECT TOP 1 provider_key FROM dbo.dim_provider d 
                 WHERE d.provider_id = s.provider_id AND s.start_date BETWEEN d.valid_from AND d.valid_to 
                 ORDER BY d.valid_from DESC) pr
    OUTER APPLY (SELECT TOP 1 organization_key FROM dbo.dim_organization d 
                 WHERE d.organization_id = s.organization_id AND s.start_date BETWEEN d.valid_from AND d.valid_to 
                 ORDER BY d.valid_from DESC) o
    OUTER APPLY (SELECT TOP 1 payer_key FROM dbo.dim_payer d 
                 WHERE d.payer_id = s.payer_id AND s.start_date BETWEEN d.valid_from AND d.valid_to 
                 ORDER BY d.valid_from DESC) pa
    WHERE p.patient_key IS NOT NULL;
END;
GO

CREATE OR ALTER PROCEDURE dbo.usp_load_fact_medication_daily
    @FromDate DATE, 
    @ToDate   DATE
AS
BEGIN
    SET NOCOUNT ON; 
    SET XACT_ABORT ON;

    -- 1. Xóa dữ liệu cũ trong khoảng ngày (incremental)
    DELETE FROM dbo.fact_medication_daily 
    WHERE date_key BETWEEN dbo.fn_date_key(@FromDate) AND dbo.fn_date_key(@ToDate);

    -- 2. Tính toán daily snapshot
    ;WITH daily_calc AS (
        SELECT 
            d.date_key,
            fm.medication_key,
            fm.organization_key,
            fm.payer_key,

            -- New prescriptions (bắt đầu trong ngày)
            SUM(CASE WHEN fm.start_date_key = d.date_key THEN 1 ELSE 0 END) AS new_prescriptions,

            -- Stopped prescriptions (kết thúc trong ngày)
            SUM(CASE WHEN fm.stop_date_key = d.date_key THEN 1 ELSE 0 END) AS stopped_prescriptions,

            -- Active patient count: số bệnh nhân đang dùng thuốc trong ngày đó
            COUNT(DISTINCT CASE 
                    WHEN fm.start_date_key <= d.date_key 
                         AND (fm.stop_date_key IS NULL OR fm.stop_date_key > d.date_key) 
                    THEN fm.patient_key 
                 END) AS active_patient_count,

            -- Các chỉ số chi phí / dispense chỉ tính cho toa BẮT ĐẦU trong ngày
            SUM(CASE WHEN fm.start_date_key = d.date_key THEN ISNULL(fm.dispense_count, 0) ELSE 0 END) AS total_dispense_count,
            SUM(CASE WHEN fm.start_date_key = d.date_key THEN ISNULL(fm.base_cost, 0)       ELSE 0 END) AS total_base_cost,
            SUM(CASE WHEN fm.start_date_key = d.date_key THEN ISNULL(fm.payer_coverage, 0) ELSE 0 END) AS total_payer_coverage,
            SUM(CASE WHEN fm.start_date_key = d.date_key 
                     THEN ISNULL(fm.base_cost, 0) - ISNULL(fm.payer_coverage, 0) 
                     ELSE 0 END) AS total_out_of_pocket

        FROM dbo.fact_medications fm
        CROSS JOIN dbo.dim_date d
        WHERE d.full_date BETWEEN @FromDate AND @ToDate
          AND fm.start_date_key <= dbo.fn_date_key(@ToDate)   -- chỉ lấy toa liên quan
        GROUP BY 
            d.date_key, 
            fm.medication_key, 
            fm.organization_key, 
            fm.payer_key
    )
    -- 3. Insert chỉ những dòng có dữ liệu thực sự
    INSERT INTO dbo.fact_medication_daily (
        date_key, 
        medication_key, 
        organization_key, 
        payer_key,
        active_patient_count,
        new_prescriptions,
        stopped_prescriptions,
        total_dispense_count,
        total_base_cost,
        total_payer_coverage,
        total_out_of_pocket
    )
    SELECT 
        date_key, 
        medication_key, 
        organization_key, 
        payer_key,
        active_patient_count,
        new_prescriptions,
        stopped_prescriptions,
        total_dispense_count,
        total_base_cost,
        total_payer_coverage,
        total_out_of_pocket
    FROM daily_calc 
    WHERE new_prescriptions > 0 
       OR stopped_prescriptions > 0 
       OR active_patient_count > 0 
       OR total_dispense_count > 0 
       OR total_base_cost > 0;
END;
GO

CREATE OR ALTER PROCEDURE dbo.usp_run_dwh_load
    @FromDate DATE, 
    @ToDate   DATE, 
    @AsOfDate DATE = NULL, 
    @batch_id UNIQUEIDENTIFIER = NULL
AS
BEGIN
    SET NOCOUNT ON;
    IF @AsOfDate IS NULL SET @AsOfDate = CONVERT(DATE, GETDATE());
    IF @batch_id IS NULL SET @batch_id = NEWID();

    -- Idempotency guard
    IF EXISTS (SELECT 1 FROM dbo.ETL_Run_Log 
               WHERE proc_name = 'usp_run_dwh_load' 
                 AND batch_id = @batch_id 
                 AND status = 'SUCCESS')
        RETURN;

    DECLARE @log_id INT, @start_time DATETIME2 = SYSUTCDATETIME();

    BEGIN TRY
        EXEC dbo.usp_load_dim_date @FromDate, @ToDate, @batch_id;
        EXEC dbo.usp_load_dim_condition_code;
        EXEC dbo.usp_load_dim_medication;               -- MỚI

        EXEC dbo.usp_load_dim_patient_scd2 @AsOfDate;
        EXEC dbo.usp_load_dim_organization_scd2 @AsOfDate;
        EXEC dbo.usp_load_dim_provider_scd2 @AsOfDate;
        EXEC dbo.usp_load_dim_payer_scd2 @AsOfDate;

        EXEC dbo.usp_load_fact_encounter @FromDate, @ToDate;
        EXEC dbo.usp_load_fact_conditions @FromDate, @ToDate;
        EXEC dbo.usp_load_fact_medications @FromDate, @ToDate;

        EXEC dbo.usp_load_fact_encounter_daily @FromDate, @ToDate;
        EXEC dbo.usp_load_fact_condition_daily @FromDate, @ToDate;
        EXEC dbo.usp_load_fact_medication_daily @FromDate, @ToDate;

        INSERT INTO dbo.ETL_Run_Log(proc_name, batch_id, start_time, end_time, status, params)
        VALUES ('usp_run_dwh_load', @batch_id, @start_time, SYSUTCDATETIME(), 'SUCCESS', 
                CONCAT('FromDate=',@FromDate,';ToDate=',@ToDate,';AsOfDate=',@AsOfDate));
    END TRY
    BEGIN CATCH
        INSERT INTO dbo.ETL_Run_Log(proc_name, batch_id, start_time, end_time, status, error_msg, params)
        VALUES ('usp_run_dwh_load', @batch_id, @start_time, SYSUTCDATETIME(), 'FAILED', ERROR_MESSAGE(), 
                CONCAT('FromDate=',@FromDate,';ToDate=',@ToDate,';AsOfDate=',@AsOfDate));
        THROW;
    END CATCH
END;
GO

PRINT '=== STORED PROCEDURES v6.1 (MATCH SCHEMA v2) CREATED SUCCESSFULLY ===';

-- ============================================================================
-- END OF PROCEDURES v5
-- ============================================================================

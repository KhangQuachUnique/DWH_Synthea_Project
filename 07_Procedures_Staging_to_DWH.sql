-- ============================================================================
-- Stored Procedures: Load from [DW_Synthea_Staging] -> [DW_Synthea_DWH]
-- ============================================================================

USE [DW_Synthea_DWH];
GO

-- ============================================================================
-- Helper: Make date_key from DATE
-- ============================================================================
CREATE OR ALTER FUNCTION dbo.fn_date_key (@d DATE)
RETURNS INT
AS
BEGIN
    RETURN (CONVERT(INT, CONVERT(CHAR(8), @d, 112)));
END;
GO

-- ============================================================================
-- 1) DIM DATE
-- ============================================================================
CREATE OR ALTER PROCEDURE dbo.usp_load_dim_date
    @StartDate DATE,
    @EndDate   DATE
AS
BEGIN
    SET NOCOUNT ON;

    IF @StartDate IS NULL OR @EndDate IS NULL OR @EndDate < @StartDate
        THROW 50000, 'Invalid date range', 1;

    ;WITH
    n AS (
        SELECT TOP (DATEDIFF(DAY, @StartDate, @EndDate) + 1)
            ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS n
        FROM sys.all_objects a
        CROSS JOIN sys.all_objects b
    ),
    d AS (
        SELECT DATEADD(DAY, n.n, @StartDate) AS full_date
        FROM n
    )
    INSERT INTO dbo.dim_date (date_key, full_date, [year], [month], month_name, [quarter], day_of_month, day_of_week, day_name, is_weekend)
    SELECT
        dbo.fn_date_key(d.full_date) AS date_key,
        d.full_date,
        DATEPART(YEAR, d.full_date) AS [year],
        DATEPART(MONTH, d.full_date) AS [month],
        DATENAME(MONTH, d.full_date) AS month_name,
        DATEPART(QUARTER, d.full_date) AS [quarter],
        DATEPART(DAY, d.full_date) AS day_of_month,
        ((DATEPART(WEEKDAY, d.full_date) + @@DATEFIRST - 2) % 7) + 1 AS day_of_week,
        DATENAME(WEEKDAY, d.full_date) AS day_name,
        CASE WHEN (((DATEPART(WEEKDAY, d.full_date) + @@DATEFIRST - 2) % 7) + 1) IN (6,7) THEN 1 ELSE 0 END AS is_weekend
    FROM d
    WHERE NOT EXISTS (
        SELECT 1
        FROM dbo.dim_date x
        WHERE x.full_date = d.full_date
    );
END;
GO

-- ============================================================================
-- 2) DIM CONDITION CODE (Type 1)
-- ============================================================================
CREATE OR ALTER PROCEDURE dbo.usp_load_dim_condition_code
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO dbo.dim_condition_code (code, [description])
    SELECT s.code, MAX(s.[description])
    FROM [DW_Synthea_Staging].[dbo].[Staging_Conditions] s
    WHERE s.code IS NOT NULL
    GROUP BY s.code
    HAVING NOT EXISTS (
        SELECT 1
        FROM dbo.dim_condition_code d
        WHERE d.code = s.code
    );
END;
GO

-- ============================================================================
-- 3) DIM PATIENT (SCD2)
-- ============================================================================
CREATE OR ALTER PROCEDURE dbo.usp_load_dim_patient_scd2
    @AsOfDate DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @AsOfDate IS NULL
        SET @AsOfDate = CONVERT(DATE, GETDATE());

    DECLARE @OpenEnded DATE = '9999-12-31';

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
        p.HEALTHCARE_EXPENSES AS healthcare_expenses,
        p.HEALTHCARE_COVERAGE AS healthcare_coverage,
        HASHBYTES(
            'SHA2_256',
            CONCAT_WS('|',
                ISNULL(p.FIRST,''), ISNULL(p.LAST,''),
                ISNULL(CONVERT(VARCHAR(10), p.BIRTHDATE, 23), ''),
                ISNULL(CONVERT(VARCHAR(10), p.DEATHDATE, 23), ''),
                ISNULL(p.GENDER,''), ISNULL(p.RACE,''), ISNULL(p.ETHNICITY,''),
                ISNULL(p.CITY,''), ISNULL(p.STATE,''), ISNULL(p.ZIP,''),
                ISNULL(CONVERT(VARCHAR(30), p.HEALTHCARE_EXPENSES), ''),
                ISNULL(CONVERT(VARCHAR(30), p.HEALTHCARE_COVERAGE), '')
            )
        ) AS row_hash
    INTO #src_patient
    FROM [DW_Synthea_Staging].[dbo].[Staging_Patients] p
    WHERE p.Id IS NOT NULL;

    CREATE INDEX IX_src_patient_id ON #src_patient(patient_id);

    -- Changed keys
    IF OBJECT_ID('tempdb..#chg_patient') IS NOT NULL DROP TABLE #chg_patient;
    SELECT s.patient_id
    INTO #chg_patient
    FROM #src_patient s
    JOIN dbo.dim_patient d
        ON d.patient_id = s.patient_id
       AND d.is_current = 1
    WHERE d.row_hash <> s.row_hash;

    CREATE UNIQUE CLUSTERED INDEX IX_chg_patient ON #chg_patient(patient_id);

    -- Expire current rows
    UPDATE d
        SET d.valid_to = DATEADD(DAY, -1, @AsOfDate),
            d.is_current = 0,
            d.update_at = SYSUTCDATETIME()
    FROM dbo.dim_patient d
    JOIN #chg_patient c
        ON c.patient_id = d.patient_id
    WHERE d.is_current = 1;

    -- Insert new + changed
    INSERT INTO dbo.dim_patient
    (
        patient_id, first_name, last_name, birthdate, deathdate, gender, race, ethnicity, city, state, zip,
        healthcare_expenses, healthcare_coverage,
        valid_from, valid_to, is_current, row_hash
    )
    SELECT
        s.patient_id, s.first_name, s.last_name, s.birthdate, s.deathdate, s.gender, s.race, s.ethnicity, s.city, s.state, s.zip,
        s.healthcare_expenses, s.healthcare_coverage,
        @AsOfDate, @OpenEnded, 1, s.row_hash
    FROM #src_patient s
    LEFT JOIN dbo.dim_patient d
        ON d.patient_id = s.patient_id
       AND d.is_current = 1
    WHERE d.patient_id IS NULL
       OR EXISTS (SELECT 1 FROM #chg_patient c WHERE c.patient_id = s.patient_id);

    COMMIT;
END;
GO

-- ============================================================================
-- 4) DIM ORGANIZATION (SCD2)
-- ============================================================================
CREATE OR ALTER PROCEDURE dbo.usp_load_dim_organization_scd2
    @AsOfDate DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @AsOfDate IS NULL
        SET @AsOfDate = CONVERT(DATE, GETDATE());

    DECLARE @OpenEnded DATE = '9999-12-31';

    BEGIN TRAN;

    IF OBJECT_ID('tempdb..#src_org') IS NOT NULL DROP TABLE #src_org;

    SELECT
        o.Id AS organization_id,
        o.NAME AS [name],
        o.CITY AS city,
        o.STATE AS [state],
        o.ZIP AS zip,
        o.PHONE AS phone,
        o.REVENUE AS revenue,
        o.UTILIZATION AS utilization,
        HASHBYTES(
            'SHA2_256',
            CONCAT_WS('|',
                ISNULL(o.NAME,''), ISNULL(o.CITY,''), ISNULL(o.STATE,''), ISNULL(o.ZIP,''), ISNULL(o.PHONE,''),
                ISNULL(CONVERT(VARCHAR(30), o.REVENUE), ''),
                ISNULL(CONVERT(VARCHAR(30), o.UTILIZATION), '')
            )
        ) AS row_hash
    INTO #src_org
    FROM [DW_Synthea_Staging].[dbo].[Staging_Organizations] o
    WHERE o.Id IS NOT NULL;

    CREATE INDEX IX_src_org_id ON #src_org(organization_id);

    IF OBJECT_ID('tempdb..#chg_org') IS NOT NULL DROP TABLE #chg_org;
    SELECT s.organization_id
    INTO #chg_org
    FROM #src_org s
    JOIN dbo.dim_organization d
        ON d.organization_id = s.organization_id
       AND d.is_current = 1
    WHERE d.row_hash <> s.row_hash;

    CREATE UNIQUE CLUSTERED INDEX IX_chg_org ON #chg_org(organization_id);

    UPDATE d
        SET d.valid_to = DATEADD(DAY, -1, @AsOfDate),
            d.is_current = 0,
            d.update_at = SYSUTCDATETIME()
    FROM dbo.dim_organization d
    JOIN #chg_org c
        ON c.organization_id = d.organization_id
    WHERE d.is_current = 1;

    INSERT INTO dbo.dim_organization
    (
        organization_id, [name], city, [state], zip, phone, revenue, utilization,
        valid_from, valid_to, is_current, row_hash
    )
    SELECT
        s.organization_id, s.[name], s.city, s.[state], s.zip, s.phone, s.revenue, s.utilization,
        @AsOfDate, @OpenEnded, 1, s.row_hash
    FROM #src_org s
    LEFT JOIN dbo.dim_organization d
        ON d.organization_id = s.organization_id
       AND d.is_current = 1
    WHERE d.organization_id IS NULL
       OR EXISTS (SELECT 1 FROM #chg_org c WHERE c.organization_id = s.organization_id);

    COMMIT;
END;
GO

-- ============================================================================
-- 5) DIM PROVIDER (SCD2)
-- ============================================================================
CREATE OR ALTER PROCEDURE dbo.usp_load_dim_provider_scd2
    @AsOfDate DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @AsOfDate IS NULL
        SET @AsOfDate = CONVERT(DATE, GETDATE());

    DECLARE @OpenEnded DATE = '9999-12-31';

    BEGIN TRAN;

    IF OBJECT_ID('tempdb..#src_provider') IS NOT NULL DROP TABLE #src_provider;

    SELECT
        p.Id AS provider_id,
        p.ORGANIZATION AS organization_id,
        p.NAME AS [name],
        p.GENDER AS gender,
        p.SPECIALITY AS speciality,
        p.CITY AS city,
        p.STATE AS [state],
        p.ZIP AS zip,
        p.UTILIZATION AS utilization,
        HASHBYTES(
            'SHA2_256',
            CONCAT_WS('|',
                ISNULL(p.ORGANIZATION,''), ISNULL(p.NAME,''), ISNULL(p.GENDER,''), ISNULL(p.SPECIALITY,''),
                ISNULL(p.CITY,''), ISNULL(p.STATE,''), ISNULL(p.ZIP,''),
                ISNULL(CONVERT(VARCHAR(30), p.UTILIZATION), '')
            )
        ) AS row_hash
    INTO #src_provider
    FROM [DW_Synthea_Staging].[dbo].[Staging_Providers] p
    WHERE p.Id IS NOT NULL;

    CREATE INDEX IX_src_provider_id ON #src_provider(provider_id);

    IF OBJECT_ID('tempdb..#chg_provider') IS NOT NULL DROP TABLE #chg_provider;
    SELECT s.provider_id
    INTO #chg_provider
    FROM #src_provider s
    JOIN dbo.dim_provider d
        ON d.provider_id = s.provider_id
       AND d.is_current = 1
    WHERE d.row_hash <> s.row_hash;

    CREATE UNIQUE CLUSTERED INDEX IX_chg_provider ON #chg_provider(provider_id);

    UPDATE d
        SET d.valid_to = DATEADD(DAY, -1, @AsOfDate),
            d.is_current = 0,
            d.update_at = SYSUTCDATETIME()
    FROM dbo.dim_provider d
    JOIN #chg_provider c
        ON c.provider_id = d.provider_id
    WHERE d.is_current = 1;

    INSERT INTO dbo.dim_provider
    (
        provider_id, organization_id, [name], gender, speciality, city, [state], zip, utilization,
        valid_from, valid_to, is_current, row_hash
    )
    SELECT
        s.provider_id, s.organization_id, s.[name], s.gender, s.speciality, s.city, s.[state], s.zip, s.utilization,
        @AsOfDate, @OpenEnded, 1, s.row_hash
    FROM #src_provider s
    LEFT JOIN dbo.dim_provider d
        ON d.provider_id = s.provider_id
       AND d.is_current = 1
    WHERE d.provider_id IS NULL
       OR EXISTS (SELECT 1 FROM #chg_provider c WHERE c.provider_id = s.provider_id);

    COMMIT;
END;
GO

-- ============================================================================
-- 6) DIM PAYER (SCD2)
-- ============================================================================
CREATE OR ALTER PROCEDURE dbo.usp_load_dim_payer_scd2
    @AsOfDate DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @AsOfDate IS NULL
        SET @AsOfDate = CONVERT(DATE, GETDATE());

    DECLARE @OpenEnded DATE = '9999-12-31';

    BEGIN TRAN;

    IF OBJECT_ID('tempdb..#src_payer') IS NOT NULL DROP TABLE #src_payer;

    ;WITH ownership AS (
        SELECT
            t.PAYER AS payer_id,
            MAX(t.OWNERSHIP) AS ownership
        FROM [DW_Synthea_Staging].[dbo].[Staging_Payer_Transitions] t
        WHERE t.PAYER IS NOT NULL
        GROUP BY t.PAYER
    )
    SELECT
        p.Id AS payer_id,
        p.NAME AS [name],
        p.CITY AS city,
        p.STATE_HEADQUARTERED AS state_headquartered,
        p.ZIP AS zip,
        p.PHONE AS phone,
        o.ownership AS ownership,
        p.AMOUNT_COVERED AS amount_covered,
        p.AMOUNT_UNCOVERED AS amount_uncovered,
        p.REVENUE AS revenue,
        p.UNIQUE_CUSTOMERS AS unique_customers,
        p.MEMBER_MONTHS AS member_months,
        HASHBYTES(
            'SHA2_256',
            CONCAT_WS('|',
                ISNULL(p.NAME,''), ISNULL(p.CITY,''), ISNULL(p.STATE_HEADQUARTERED,''), ISNULL(p.ZIP,''), ISNULL(p.PHONE,''),
                ISNULL(o.ownership,''),
                ISNULL(CONVERT(VARCHAR(30), p.AMOUNT_COVERED), ''),
                ISNULL(CONVERT(VARCHAR(30), p.AMOUNT_UNCOVERED), ''),
                ISNULL(CONVERT(VARCHAR(30), p.REVENUE), ''),
                ISNULL(CONVERT(VARCHAR(30), p.UNIQUE_CUSTOMERS), ''),
                ISNULL(CONVERT(VARCHAR(30), p.MEMBER_MONTHS), '')
            )
        ) AS row_hash
    INTO #src_payer
    FROM [DW_Synthea_Staging].[dbo].[Staging_Payers] p
    LEFT JOIN ownership o
        ON o.payer_id = p.Id
    WHERE p.Id IS NOT NULL;

    CREATE INDEX IX_src_payer_id ON #src_payer(payer_id);

    IF OBJECT_ID('tempdb..#chg_payer') IS NOT NULL DROP TABLE #chg_payer;
    SELECT s.payer_id
    INTO #chg_payer
    FROM #src_payer s
    JOIN dbo.dim_payer d
        ON d.payer_id = s.payer_id
       AND d.is_current = 1
    WHERE d.row_hash <> s.row_hash;

    CREATE UNIQUE CLUSTERED INDEX IX_chg_payer ON #chg_payer(payer_id);

    UPDATE d
        SET d.valid_to = DATEADD(DAY, -1, @AsOfDate),
            d.is_current = 0,
            d.update_at = SYSUTCDATETIME()
    FROM dbo.dim_payer d
    JOIN #chg_payer c
        ON c.payer_id = d.payer_id
    WHERE d.is_current = 1;

    INSERT INTO dbo.dim_payer
    (
        payer_id, [name], city, state_headquartered, zip, phone, ownership,
        amount_covered, amount_uncovered, revenue, unique_customers, member_months,
        valid_from, valid_to, is_current, row_hash
    )
    SELECT
        s.payer_id, s.[name], s.city, s.state_headquartered, s.zip, s.phone, s.ownership,
        s.amount_covered, s.amount_uncovered, s.revenue, s.unique_customers, s.member_months,
        @AsOfDate, @OpenEnded, 1, s.row_hash
    FROM #src_payer s
    LEFT JOIN dbo.dim_payer d
        ON d.payer_id = s.payer_id
       AND d.is_current = 1
    WHERE d.payer_id IS NULL
       OR EXISTS (SELECT 1 FROM #chg_payer c WHERE c.payer_id = s.payer_id);

    COMMIT;
END;
GO

-- ============================================================================
-- 7) DIM ENCOUNTER (Type 1)
-- ============================================================================
CREATE OR ALTER PROCEDURE dbo.usp_load_dim_encounter
    @FromDate DATE = NULL,
    @ToDate   DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @FromDate IS NULL
        SELECT @FromDate = MIN(CONVERT(DATE, [START]))
        FROM [DW_Synthea_Staging].[dbo].[Staging_Encounters];

    IF @ToDate IS NULL
        SELECT @ToDate = MAX(CONVERT(DATE, [START]))
        FROM [DW_Synthea_Staging].[dbo].[Staging_Encounters];

    -- ensure dim_date range exists
    EXEC dbo.usp_load_dim_date @FromDate, @ToDate;

    IF OBJECT_ID('tempdb..#resolved_encounter') IS NOT NULL DROP TABLE #resolved_encounter;

    ;WITH src AS (
        SELECT
            e.Id AS encounter_id,
            CONVERT(DATE, e.[START]) AS start_date,
            CONVERT(DATE, e.[STOP]) AS stop_date,
            e.PATIENT AS patient_id,
            e.PROVIDER AS provider_id,
            e.PAYER AS payer_id,
            e.ORGANIZATION AS organization_id,
            e.ENCOUNTERCLASS AS encounter_class,
            e.CODE AS code,
            e.[DESCRIPTION] AS [description],
            e.BASE_ENCOUNTER_COST AS base_encounter_cost,
            e.TOTAL_CLAIM_COST AS total_claim_cost,
            e.PAYER_COVERAGE AS payer_coverage,
            e.REASONCODE AS reason_code,
            e.REASONDESCRIPTION AS reason_description
        FROM [DW_Synthea_Staging].[dbo].[Staging_Encounters] e
        WHERE e.Id IS NOT NULL
          AND CONVERT(DATE, e.[START]) BETWEEN @FromDate AND @ToDate
    ),
    resolved AS (
        SELECT
            s.encounter_id,
            dbo.fn_date_key(s.start_date) AS start_date_key,
            CASE WHEN s.stop_date IS NULL THEN NULL ELSE dbo.fn_date_key(s.stop_date) END AS stop_date_key,

            p.patient_key,
            pr.provider_key,
            pa.payer_key,
            o.organization_key,

            s.encounter_class,
            s.code,
            s.[description],
            s.base_encounter_cost,
            s.total_claim_cost,
            s.payer_coverage,
            s.reason_code,
            s.reason_description
        FROM src s
        OUTER APPLY (
            SELECT TOP (1) d.patient_key
            FROM dbo.dim_patient d
            WHERE d.patient_id = s.patient_id
              AND s.start_date BETWEEN d.valid_from AND d.valid_to
            ORDER BY d.valid_from DESC
        ) p
        OUTER APPLY (
            SELECT TOP (1) d.provider_key
            FROM dbo.dim_provider d
            WHERE d.provider_id = s.provider_id
              AND s.start_date BETWEEN d.valid_from AND d.valid_to
            ORDER BY d.valid_from DESC
        ) pr
        OUTER APPLY (
            SELECT TOP (1) d.payer_key
            FROM dbo.dim_payer d
            WHERE d.payer_id = s.payer_id
              AND s.start_date BETWEEN d.valid_from AND d.valid_to
            ORDER BY d.valid_from DESC
        ) pa
        OUTER APPLY (
            SELECT TOP (1) d.organization_key
            FROM dbo.dim_organization d
            WHERE d.organization_id = s.organization_id
              AND s.start_date BETWEEN d.valid_from AND d.valid_to
            ORDER BY d.valid_from DESC
        ) o
    )
    SELECT *
    INTO #resolved_encounter
    FROM resolved;

    CREATE UNIQUE CLUSTERED INDEX IX_resolved_encounter_id ON #resolved_encounter(encounter_id);

    -- Update existing
    UPDATE tgt
        SET tgt.start_date_key = src.start_date_key,
            tgt.stop_date_key = src.stop_date_key,
            tgt.patient_key = src.patient_key,
            tgt.provider_key = src.provider_key,
            tgt.payer_key = src.payer_key,
            tgt.organization_key = src.organization_key,
            tgt.encounter_class = src.encounter_class,
            tgt.code = src.code,
            tgt.[description] = src.[description],
            tgt.base_encounter_cost = src.base_encounter_cost,
            tgt.total_claim_cost = src.total_claim_cost,
            tgt.payer_coverage = src.payer_coverage,
            tgt.reason_code = src.reason_code,
            tgt.reason_description = src.reason_description,
            tgt.update_at = SYSUTCDATETIME()
    FROM dbo.dim_encounter tgt
    JOIN #resolved_encounter src
        ON src.encounter_id = tgt.encounter_id;

    -- Insert missing
    INSERT INTO dbo.dim_encounter
    (
        encounter_id,
        start_date_key,
        stop_date_key,
        patient_key,
        provider_key,
        payer_key,
        organization_key,
        encounter_class,
        code,
        [description],
        base_encounter_cost,
        total_claim_cost,
        payer_coverage,
        reason_code,
        reason_description
    )
    SELECT
        src.encounter_id,
        src.start_date_key,
        src.stop_date_key,
        src.patient_key,
        src.provider_key,
        src.payer_key,
        src.organization_key,
        src.encounter_class,
        src.code,
        src.[description],
        src.base_encounter_cost,
        src.total_claim_cost,
        src.payer_coverage,
        src.reason_code,
        src.reason_description
    FROM #resolved_encounter src
    WHERE NOT EXISTS (
        SELECT 1
        FROM dbo.dim_encounter tgt
        WHERE tgt.encounter_id = src.encounter_id
    );
END;
GO

-- ============================================================================
-- 8) FACT UTILIZATION (daily rollup from encounters)
-- ============================================================================
CREATE OR ALTER PROCEDURE dbo.usp_load_fact_utilization
    @FromDate DATE,
    @ToDate   DATE
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @FromDate IS NULL OR @ToDate IS NULL OR @ToDate < @FromDate
        THROW 50000, 'Invalid date range', 1;

    EXEC dbo.usp_load_dim_date @FromDate, @ToDate;

    -- refresh window
    DELETE f
    FROM dbo.fact_utilization f
    WHERE f.date_key BETWEEN dbo.fn_date_key(@FromDate) AND dbo.fn_date_key(@ToDate);

    ;WITH src AS (
        SELECT
            CONVERT(DATE, e.[START]) AS d,
            e.PROVIDER AS provider_id,
            e.PAYER AS payer_id,
            e.ORGANIZATION AS organization_id,
            COUNT(*) AS encounter_count,
            SUM(ISNULL(e.BASE_ENCOUNTER_COST, 0)) AS base_encounter_cost,
            SUM(ISNULL(e.TOTAL_CLAIM_COST, 0)) AS total_claim_cost,
            SUM(ISNULL(e.PAYER_COVERAGE, 0)) AS payer_coverage
        FROM [DW_Synthea_Staging].[dbo].[Staging_Encounters] e
        WHERE e.[START] IS NOT NULL
          AND CONVERT(DATE, e.[START]) BETWEEN @FromDate AND @ToDate
        GROUP BY
            CONVERT(DATE, e.[START]),
            e.PROVIDER,
            e.PAYER,
            e.ORGANIZATION
    ),
    resolved AS (
        SELECT
            dbo.fn_date_key(s.d) AS date_key,
            pr.provider_key,
            pa.payer_key,
            o.organization_key,
            s.encounter_count,
            s.base_encounter_cost,
            s.total_claim_cost,
            s.payer_coverage
        FROM src s
        OUTER APPLY (
            SELECT TOP (1) d.provider_key
            FROM dbo.dim_provider d
            WHERE d.provider_id = s.provider_id
              AND s.d BETWEEN d.valid_from AND d.valid_to
            ORDER BY d.valid_from DESC
        ) pr
        OUTER APPLY (
            SELECT TOP (1) d.payer_key
            FROM dbo.dim_payer d
            WHERE d.payer_id = s.payer_id
              AND s.d BETWEEN d.valid_from AND d.valid_to
            ORDER BY d.valid_from DESC
        ) pa
        OUTER APPLY (
            SELECT TOP (1) d.organization_key
            FROM dbo.dim_organization d
            WHERE d.organization_id = s.organization_id
              AND s.d BETWEEN d.valid_from AND d.valid_to
            ORDER BY d.valid_from DESC
        ) o
    )
    INSERT INTO dbo.fact_utilization
    (
        date_key, provider_key, payer_key, organization_key,
        encounter_count, base_encounter_cost, total_claim_cost, payer_coverage
    )
    SELECT
        r.date_key, r.provider_key, r.payer_key, r.organization_key,
        r.encounter_count,
        ISNULL(r.base_encounter_cost, 0),
        ISNULL(r.total_claim_cost, 0),
        ISNULL(r.payer_coverage, 0)
    FROM resolved r;
END;
GO

-- ============================================================================
-- 9) FACT CONDITIONS (one row per condition event)
-- ============================================================================
CREATE OR ALTER PROCEDURE dbo.usp_load_fact_conditions
    @FromDate DATE,
    @ToDate   DATE
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @FromDate IS NULL OR @ToDate IS NULL OR @ToDate < @FromDate
        THROW 50000, 'Invalid date range', 1;

    EXEC dbo.usp_load_dim_date @FromDate, @ToDate;
    EXEC dbo.usp_load_dim_condition_code;
    EXEC dbo.usp_load_dim_encounter @FromDate, @ToDate;

    DELETE f
    FROM dbo.fact_conditions f
    WHERE f.condition_start_date_key BETWEEN dbo.fn_date_key(@FromDate) AND dbo.fn_date_key(@ToDate);

    ;WITH src AS (
        SELECT
            c.[START] AS start_date,
            c.[STOP] AS stop_date,
            c.PATIENT AS patient_id,
            c.ENCOUNTER AS encounter_id,
            c.CODE AS condition_code,
            e.PROVIDER AS provider_id,
            e.ORGANIZATION AS organization_id
        FROM [DW_Synthea_Staging].[dbo].[Staging_Conditions] c
        LEFT JOIN [DW_Synthea_Staging].[dbo].[Staging_Encounters] e
            ON e.Id = c.ENCOUNTER
        WHERE c.[START] IS NOT NULL
          AND c.CODE IS NOT NULL
          AND c.[START] BETWEEN @FromDate AND @ToDate
    ),
    resolved AS (
        SELECT
            cc.condition_code_key,
            dbo.fn_date_key(s.start_date) AS condition_start_date_key,
            CASE WHEN s.stop_date IS NULL THEN NULL ELSE dbo.fn_date_key(s.stop_date) END AS condition_stop_date_key,
            CASE
                WHEN s.stop_date IS NULL THEN NULL
                ELSE DATEDIFF(DAY, s.start_date, s.stop_date)
            END AS duration_days,

            p.patient_key,
            pr.provider_key,
            o.organization_key,
            de.encounter_key
        FROM src s
        JOIN dbo.dim_condition_code cc
            ON cc.code = s.condition_code
        OUTER APPLY (
            SELECT TOP (1) d.patient_key
            FROM dbo.dim_patient d
            WHERE d.patient_id = s.patient_id
              AND s.start_date BETWEEN d.valid_from AND d.valid_to
            ORDER BY d.valid_from DESC
        ) p
        OUTER APPLY (
            SELECT TOP (1) d.provider_key
            FROM dbo.dim_provider d
            WHERE d.provider_id = s.provider_id
              AND s.start_date BETWEEN d.valid_from AND d.valid_to
            ORDER BY d.valid_from DESC
        ) pr
        OUTER APPLY (
            SELECT TOP (1) d.organization_key
            FROM dbo.dim_organization d
            WHERE d.organization_id = s.organization_id
              AND s.start_date BETWEEN d.valid_from AND d.valid_to
            ORDER BY d.valid_from DESC
        ) o
        LEFT JOIN dbo.dim_encounter de
            ON de.encounter_id = s.encounter_id
    )
    INSERT INTO dbo.fact_conditions
    (
        condition_code_key,
        patient_key,
        provider_key,
        organization_key,
        encounter_key,
        condition_start_date_key,
        condition_stop_date_key,
        duration_days,
        condition_count
    )
    SELECT
        r.condition_code_key,
        r.patient_key,
        r.provider_key,
        r.organization_key,
        r.encounter_key,
        r.condition_start_date_key,
        r.condition_stop_date_key,
        r.duration_days,
        1
    FROM resolved r;
END;
GO

-- ============================================================================
-- 10) FACT CONDITION DAILY SNAPSHOT (daily rollup)
-- ============================================================================
CREATE OR ALTER PROCEDURE dbo.usp_load_fact_condition_daily_snapshot
    @FromDate DATE,
    @ToDate   DATE
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @FromDate IS NULL OR @ToDate IS NULL OR @ToDate < @FromDate
        THROW 50000, 'Invalid date range', 1;

    EXEC dbo.usp_load_dim_date @FromDate, @ToDate;
    EXEC dbo.usp_load_dim_condition_code;

    DELETE f
    FROM dbo.fact_condition_daily_snapshot f
    WHERE f.date_key BETWEEN dbo.fn_date_key(@FromDate) AND dbo.fn_date_key(@ToDate);

    ;WITH base_cond AS (
        SELECT
            c.CODE AS condition_code,
            CONVERT(DATE, c.[START]) AS start_date,
            CONVERT(DATE, c.[STOP]) AS stop_date,
            e.ORGANIZATION AS organization_id
        FROM [DW_Synthea_Staging].[dbo].[Staging_Conditions] c
        LEFT JOIN [DW_Synthea_Staging].[dbo].[Staging_Encounters] e
            ON e.Id = c.ENCOUNTER
        WHERE c.CODE IS NOT NULL
          AND c.[START] IS NOT NULL
          AND CONVERT(DATE, c.[START]) <= @ToDate
          AND (c.[STOP] IS NULL OR CONVERT(DATE, c.[STOP]) >= @FromDate)
    ),
    new_events AS (
        SELECT
            bc.start_date AS d,
            bc.condition_code,
            bc.organization_id,
            COUNT_BIG(1) AS new_cases
        FROM base_cond bc
        WHERE bc.start_date BETWEEN @FromDate AND @ToDate
        GROUP BY
            bc.start_date,
            bc.condition_code,
            bc.organization_id
    ),
    resolved_events AS (
        SELECT
            bc.stop_date AS d,
            bc.condition_code,
            bc.organization_id,
            COUNT_BIG(1) AS resolved_cases
        FROM base_cond bc
        WHERE bc.stop_date IS NOT NULL
          AND bc.stop_date BETWEEN @FromDate AND @ToDate
        GROUP BY
            bc.stop_date,
            bc.condition_code,
            bc.organization_id
    ),
    new_keys AS (
        SELECT
            ne.d,
            cc.condition_code_key,
            org.organization_key,
            CONVERT(INT, ne.new_cases) AS new_cases,
            CONVERT(INT, 0) AS resolved_cases
        FROM new_events ne
        JOIN dbo.dim_condition_code cc
            ON cc.code = ne.condition_code
        OUTER APPLY (
            SELECT TOP (1) d.organization_key
            FROM dbo.dim_organization d
            WHERE d.organization_id = ne.organization_id
              AND ne.d BETWEEN d.valid_from AND d.valid_to
            ORDER BY d.valid_from DESC
        ) org
    ),
    resolved_keys AS (
        SELECT
            re.d,
            cc.condition_code_key,
            org.organization_key,
            CONVERT(INT, 0) AS new_cases,
            CONVERT(INT, re.resolved_cases) AS resolved_cases
        FROM resolved_events re
        JOIN dbo.dim_condition_code cc
            ON cc.code = re.condition_code
        OUTER APPLY (
            SELECT TOP (1) d.organization_key
            FROM dbo.dim_organization d
            WHERE d.organization_id = re.organization_id
              AND re.d BETWEEN d.valid_from AND d.valid_to
            ORDER BY d.valid_from DESC
        ) org
    ),
    daily_events AS (
        SELECT
            x.d,
            x.condition_code_key,
            x.organization_key,
            SUM(x.new_cases) AS new_cases,
            SUM(x.resolved_cases) AS resolved_cases
        FROM (
            SELECT d, condition_code_key, organization_key, new_cases, resolved_cases
            FROM new_keys
            UNION ALL
            SELECT d, condition_code_key, organization_key, new_cases, resolved_cases
            FROM resolved_keys
        ) x
        GROUP BY
            x.d,
            x.condition_code_key,
            x.organization_key
    ),
    calendar AS (
        SELECT dd.full_date AS d
        FROM dbo.dim_date dd
        WHERE dd.full_date BETWEEN @FromDate AND @ToDate
    ),
    pairs AS (
        SELECT DISTINCT
            de.condition_code_key,
            de.organization_key
        FROM daily_events de
    ),
    grid AS (
        SELECT
            c.d,
            p.condition_code_key,
            p.organization_key
        FROM calendar c
        CROSS JOIN pairs p
    ),
    filled AS (
        SELECT
            g.d,
            g.condition_code_key,
            g.organization_key,
            ISNULL(de.new_cases, 0) AS new_cases,
            ISNULL(de.resolved_cases, 0) AS resolved_cases
        FROM grid g
        LEFT JOIN daily_events de
            ON de.d = g.d
           AND de.condition_code_key = g.condition_code_key
           AND (de.organization_key = g.organization_key OR (de.organization_key IS NULL AND g.organization_key IS NULL))
    ),
    calc AS (
        SELECT
            f.d,
            f.condition_code_key,
            f.organization_key,
            f.new_cases,
            f.resolved_cases,
            SUM(f.new_cases - f.resolved_cases) OVER (
                PARTITION BY f.condition_code_key, f.organization_key
                ORDER BY f.d
                ROWS UNBOUNDED PRECEDING
            ) AS active_cases
        FROM filled f
    )
    INSERT INTO dbo.fact_condition_daily_snapshot
    (
        condition_code_key,
        organization_key,
        date_key,
        local_cases,
        new_cases,
        active_cases,
        resolved_cases
    )
    SELECT
        c.condition_code_key,
        c.organization_key,
        dbo.fn_date_key(c.d) AS date_key,
        c.active_cases AS local_cases,
        c.new_cases,
        c.active_cases,
        c.resolved_cases
    FROM calc c
    WHERE c.active_cases > 0
       OR c.new_cases <> 0
       OR c.resolved_cases <> 0;
END;
GO

-- ============================================================================
-- 11) FACT COSTS (daily rollup across encounter + meds + procedures + immunizations)
-- ============================================================================
CREATE OR ALTER PROCEDURE dbo.usp_load_fact_costs
    @FromDate DATE,
    @ToDate   DATE
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @FromDate IS NULL OR @ToDate IS NULL OR @ToDate < @FromDate
        THROW 50000, 'Invalid date range', 1;

    EXEC dbo.usp_load_dim_date @FromDate, @ToDate;

    DELETE f
    FROM dbo.fact_costs f
    WHERE f.date_key BETWEEN dbo.fn_date_key(@FromDate) AND dbo.fn_date_key(@ToDate);

    ;WITH events AS (
        -- Encounters
        SELECT
            CONVERT(DATE, e.[START]) AS d,
            e.PROVIDER AS provider_id,
            e.ORGANIZATION AS organization_id,
            ISNULL(e.PAYER_COVERAGE, 0) AS payer_coverage,
            ISNULL(e.TOTAL_CLAIM_COST, 0) AS total_cost
        FROM [DW_Synthea_Staging].[dbo].[Staging_Encounters] e
        WHERE e.[START] IS NOT NULL
          AND CONVERT(DATE, e.[START]) BETWEEN @FromDate AND @ToDate

        UNION ALL

        -- Medications
        SELECT
            m.[START] AS d,
            e.PROVIDER AS provider_id,
            e.ORGANIZATION AS organization_id,
            ISNULL(m.PAYER_COVERAGE, 0) AS payer_coverage,
            ISNULL(m.TOTALCOST, 0) AS total_cost
        FROM [DW_Synthea_Staging].[dbo].[Staging_Medications] m
        LEFT JOIN [DW_Synthea_Staging].[dbo].[Staging_Encounters] e
            ON e.Id = m.ENCOUNTER
        WHERE m.[START] IS NOT NULL
          AND m.[START] BETWEEN @FromDate AND @ToDate

        UNION ALL

        -- Procedures
        SELECT
            p.[DATE] AS d,
            e.PROVIDER AS provider_id,
            e.ORGANIZATION AS organization_id,
            CAST(0 AS DECIMAL(18,2)) AS payer_coverage,
            ISNULL(p.BASE_COST, 0) AS total_cost
        FROM [DW_Synthea_Staging].[dbo].[Staging_Procedures] p
        LEFT JOIN [DW_Synthea_Staging].[dbo].[Staging_Encounters] e
            ON e.Id = p.ENCOUNTER
        WHERE p.[DATE] IS NOT NULL
          AND p.[DATE] BETWEEN @FromDate AND @ToDate

        UNION ALL

        -- Immunizations
        SELECT
            i.[DATE] AS d,
            e.PROVIDER AS provider_id,
            e.ORGANIZATION AS organization_id,
            CAST(0 AS DECIMAL(18,2)) AS payer_coverage,
            ISNULL(i.BASE_COST, 0) AS total_cost
        FROM [DW_Synthea_Staging].[dbo].[Staging_Immunizations] i
        LEFT JOIN [DW_Synthea_Staging].[dbo].[Staging_Encounters] e
            ON e.Id = i.ENCOUNTER
        WHERE i.[DATE] IS NOT NULL
          AND i.[DATE] BETWEEN @FromDate AND @ToDate
    ),
    agg AS (
        SELECT
            e.d,
            e.provider_id,
            e.organization_id,
            SUM(ISNULL(e.payer_coverage, 0)) AS local_payer_coverage,
            SUM(ISNULL(e.total_cost, 0)) AS total_costs
        FROM events e
        WHERE e.d BETWEEN @FromDate AND @ToDate
        GROUP BY e.d, e.provider_id, e.organization_id
    ),
    resolved AS (
        SELECT
            dbo.fn_date_key(a.d) AS date_key,
            pr.provider_key,
            org.organization_key,
            CAST(a.local_payer_coverage AS DECIMAL(18,2)) AS local_payer_coverage,
            CAST(a.total_costs AS DECIMAL(18,2)) AS total_costs,
            CAST(a.total_costs - a.local_payer_coverage AS DECIMAL(18,2)) AS total_out_of_pocket
        FROM agg a
        OUTER APPLY (
            SELECT TOP (1) d.provider_key
            FROM dbo.dim_provider d
            WHERE d.provider_id = a.provider_id
              AND a.d BETWEEN d.valid_from AND d.valid_to
            ORDER BY d.valid_from DESC
        ) pr
        OUTER APPLY (
            SELECT TOP (1) d.organization_key
            FROM dbo.dim_organization d
            WHERE d.organization_id = a.organization_id
              AND a.d BETWEEN d.valid_from AND d.valid_to
            ORDER BY d.valid_from DESC
        ) org
    )
    INSERT INTO dbo.fact_costs
    (
        date_key,
        provider_key,
        organization_key,
        local_payer_coverage,
        total_out_of_pocket,
        total_costs
    )
    SELECT
        r.date_key,
        r.provider_key,
        r.organization_key,
        ISNULL(r.local_payer_coverage, 0),
        ISNULL(r.total_out_of_pocket, 0),
        ISNULL(r.total_costs, 0)
    FROM resolved r;
END;
GO

-- ============================================================================
-- 12) Orchestrator
-- ============================================================================
CREATE OR ALTER PROCEDURE dbo.usp_run_dwh_load
    @FromDate DATE,
    @ToDate   DATE,
    @AsOfDate DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;

    IF @AsOfDate IS NULL
        SET @AsOfDate = CONVERT(DATE, GETDATE());

    -- SCD2 dims
    EXEC dbo.usp_load_dim_patient_scd2 @AsOfDate;
    EXEC dbo.usp_load_dim_organization_scd2 @AsOfDate;
    EXEC dbo.usp_load_dim_provider_scd2 @AsOfDate;
    EXEC dbo.usp_load_dim_payer_scd2 @AsOfDate;

    -- Type 1 dims
    EXEC dbo.usp_load_dim_condition_code;
    EXEC dbo.usp_load_dim_encounter @FromDate, @ToDate;

    -- Facts
    EXEC dbo.usp_load_fact_utilization @FromDate, @ToDate;
    EXEC dbo.usp_load_fact_conditions @FromDate, @ToDate;
    EXEC dbo.usp_load_fact_condition_daily_snapshot @FromDate, @ToDate;
    EXEC dbo.usp_load_fact_costs @FromDate, @ToDate;
END;
GO

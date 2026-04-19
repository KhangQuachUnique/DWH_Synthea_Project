-- ============================================================================
-- SYNTHEA DWH — KIMBALL STAR SCHEMA (v5)
-- ============================================================================
-- Target DB  : [DW_Synthea_DWH]
-- Author     : Khang / Data Engineering Team
-- Version    : 5.0
--
-- CHANGES IN v5 (so với v4)
-- ================
-- 1. XÓA HOÀN TOÀN tất cả ICU-related columns vì Synthea COVID gốc KHÔNG có encounter_class = 'icu'
--    → Xóa: is_icu (fact_encounter), icu_count (fact_encounter_daily), icu_cases (fact_condition_daily)
-- 2. Schema sạch hơn, không còn cột thừa
-- 3. Comment rõ ràng hơn về hospitalized
-- ============================================================================

USE [master];
GO

IF DB_ID(N'DW_Synthea_DWH') IS NULL
    CREATE DATABASE [DW_Synthea_DWH];
GO

USE [DW_Synthea_DWH];
GO

-- ============================================================================
-- SECTION 1: DIMENSION TABLES
-- ============================================================================

IF OBJECT_ID(N'dbo.dim_date', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.dim_date (
        date_key            INT         NOT NULL,
        full_date           DATE        NOT NULL,
        [year]              SMALLINT    NOT NULL,
        [quarter]           TINYINT     NOT NULL,
        [month]             TINYINT     NOT NULL,
        month_name          VARCHAR(10) NOT NULL,
        [week_of_year]      TINYINT     NOT NULL,
        day_of_month        TINYINT     NOT NULL,
        day_of_week         TINYINT     NOT NULL,
        day_name            VARCHAR(10) NOT NULL,
        is_weekend          BIT         NOT NULL,
        fiscal_year         SMALLINT    NULL,
        fiscal_quarter      TINYINT     NULL,
        create_at           DATETIME2   NOT NULL CONSTRAINT DF_dim_date_create_at DEFAULT SYSUTCDATETIME(),
        update_at           DATETIME2   NOT NULL CONSTRAINT DF_dim_date_update_at DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_dim_date PRIMARY KEY CLUSTERED (date_key),
        CONSTRAINT UQ_dim_date_full_date UNIQUE (full_date)
    );
END;
GO

IF OBJECT_ID(N'dbo.dim_patient', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.dim_patient (
        patient_key             INT             IDENTITY(1,1) NOT NULL,
        patient_id              VARCHAR(36)     NOT NULL,
        first_name              VARCHAR(100)    NULL,
        last_name               VARCHAR(100)    NULL,
        birthdate               DATE            NULL,
        deathdate               DATE            NULL,
        gender                  CHAR(1)         NULL,
        race                    VARCHAR(50)     NULL,
        ethnicity               VARCHAR(50)     NULL,
        city                    VARCHAR(100)    NULL,
        [state]                 VARCHAR(50)     NULL,
        zip                     VARCHAR(10)     NULL,
        county                  VARCHAR(100)    NULL,
        healthcare_expenses     DECIMAL(18,2)   NULL,
        healthcare_coverage     DECIMAL(18,2)   NULL,
        birth_year              AS YEAR(birthdate) PERSISTED,
        valid_from              DATE            NOT NULL,
        valid_to                DATE            NOT NULL,
        is_current              BIT             NOT NULL,
        row_hash                VARBINARY(32)   NOT NULL,
        create_at               DATETIME2       NOT NULL CONSTRAINT DF_dim_patient_create_at DEFAULT SYSUTCDATETIME(),
        update_at               DATETIME2       NOT NULL CONSTRAINT DF_dim_patient_update_at DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_dim_patient PRIMARY KEY CLUSTERED (patient_key)
    );
    CREATE INDEX IX_dim_patient_nk_current ON dbo.dim_patient (patient_id, is_current) INCLUDE (patient_key, valid_from, valid_to);
    CREATE UNIQUE INDEX UX_dim_patient_nk_from ON dbo.dim_patient (patient_id, valid_from);
END;
GO

IF OBJECT_ID(N'dbo.dim_organization', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.dim_organization (
        organization_key        INT             IDENTITY(1,1) NOT NULL,
        organization_id         VARCHAR(36)     NOT NULL,
        [name]                  VARCHAR(255)    NULL,
        city                    VARCHAR(100)    NULL,
        [state]                 VARCHAR(50)     NULL,
        zip                     VARCHAR(10)     NULL,
        phone                   VARCHAR(20)     NULL,
        revenue                 DECIMAL(18,2)   NULL,
        utilization             INT             NULL,
        valid_from              DATE            NOT NULL,
        valid_to                DATE            NOT NULL,
        is_current              BIT             NOT NULL,
        row_hash                VARBINARY(32)   NOT NULL,
        create_at               DATETIME2       NOT NULL CONSTRAINT DF_dim_org_create_at DEFAULT SYSUTCDATETIME(),
        update_at               DATETIME2       NOT NULL CONSTRAINT DF_dim_org_update_at DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_dim_organization PRIMARY KEY CLUSTERED (organization_key)
    );
    CREATE INDEX IX_dim_org_nk_current ON dbo.dim_organization (organization_id, is_current) INCLUDE (organization_key, valid_from, valid_to);
    CREATE UNIQUE INDEX UX_dim_org_nk_from ON dbo.dim_organization (organization_id, valid_from);
END;
GO

IF OBJECT_ID(N'dbo.dim_provider', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.dim_provider (
        provider_key            INT             IDENTITY(1,1) NOT NULL,
        provider_id             VARCHAR(36)     NOT NULL,
        organization_id         VARCHAR(36)     NULL,
        [name]                  VARCHAR(255)    NULL,
        gender                  CHAR(1)         NULL,
        speciality              VARCHAR(100)    NULL,
        city                    VARCHAR(100)    NULL,
        [state]                 VARCHAR(50)     NULL,
        zip                     VARCHAR(10)     NULL,
        utilization             INT             NULL,
        valid_from              DATE            NOT NULL,
        valid_to                DATE            NOT NULL,
        is_current              BIT             NOT NULL,
        row_hash                VARBINARY(32)   NOT NULL,
        create_at               DATETIME2       NOT NULL CONSTRAINT DF_dim_provider_create_at DEFAULT SYSUTCDATETIME(),
        update_at               DATETIME2       NOT NULL CONSTRAINT DF_dim_provider_update_at DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_dim_provider PRIMARY KEY CLUSTERED (provider_key)
    );
    CREATE INDEX IX_dim_provider_nk_current ON dbo.dim_provider (provider_id, is_current) INCLUDE (provider_key, valid_from, valid_to);
    CREATE UNIQUE INDEX UX_dim_provider_nk_from ON dbo.dim_provider (provider_id, valid_from);
END;
GO

IF OBJECT_ID(N'dbo.dim_payer', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.dim_payer (
        payer_key               INT             IDENTITY(1,1) NOT NULL,
        payer_id                VARCHAR(36)     NOT NULL,
        [name]                  VARCHAR(255)    NULL,
        city                    VARCHAR(100)    NULL,
        state_headquartered     VARCHAR(50)     NULL,
        zip                     VARCHAR(10)     NULL,
        phone                   VARCHAR(20)     NULL,
        ownership               VARCHAR(50)     NULL,
        amount_covered          DECIMAL(18,2)   NULL,
        amount_uncovered        DECIMAL(18,2)   NULL,
        revenue                 DECIMAL(18,2)   NULL,
        unique_customers        INT             NULL,
        member_months           INT             NULL,
        valid_from              DATE            NOT NULL,
        valid_to                DATE            NOT NULL,
        is_current              BIT             NOT NULL,
        row_hash                VARBINARY(32)   NOT NULL,
        create_at               DATETIME2       NOT NULL CONSTRAINT DF_dim_payer_create_at DEFAULT SYSUTCDATETIME(),
        update_at               DATETIME2       NOT NULL CONSTRAINT DF_dim_payer_update_at DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_dim_payer PRIMARY KEY CLUSTERED (payer_key)
    );
    CREATE INDEX IX_dim_payer_nk_current ON dbo.dim_payer (payer_id, is_current) INCLUDE (payer_key, valid_from, valid_to);
    CREATE UNIQUE INDEX UX_dim_payer_nk_from ON dbo.dim_payer (payer_id, valid_from);
END;
GO

IF OBJECT_ID(N'dbo.dim_condition_code', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.dim_condition_code (
        condition_code_key      INT             IDENTITY(1,1) NOT NULL,
        code                    VARCHAR(20)     NOT NULL,
        [description]           VARCHAR(500)    NULL,
        body_system             VARCHAR(100)    NULL,
        condition_category      VARCHAR(100)    NULL,
        is_chronic              BIT             NULL,
        is_infectious           BIT             NULL,
        create_at               DATETIME2       NOT NULL CONSTRAINT DF_dim_cond_code_create_at DEFAULT SYSUTCDATETIME(),
        update_at               DATETIME2       NOT NULL CONSTRAINT DF_dim_cond_code_update_at DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_dim_condition_code PRIMARY KEY CLUSTERED (condition_code_key),
        CONSTRAINT UQ_dim_condition_code_code UNIQUE (code)
    );
END;
GO

-- ============================================================================
-- SECTION 2: FACT TABLES
-- ============================================================================

IF OBJECT_ID(N'dbo.fact_encounter', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.fact_encounter (
        encounter_id            VARCHAR(36)     NOT NULL,
        start_date_key          INT             NOT NULL,
        stop_date_key           INT             NULL,
        patient_key             INT             NOT NULL,
        provider_key            INT             NULL,
        payer_key               INT             NULL,
        organization_key        INT             NULL,
        encounter_class         VARCHAR(50)     NULL,
        encounter_code          VARCHAR(20)     NULL,
        reason_code             VARCHAR(20)     NULL,
        reason_description      VARCHAR(255)    NULL,
        base_encounter_cost     DECIMAL(18,2)   NOT NULL DEFAULT 0,
        total_claim_cost        DECIMAL(18,2)   NOT NULL DEFAULT 0,
        payer_coverage          DECIMAL(18,2)   NOT NULL DEFAULT 0,
        out_of_pocket           AS (total_claim_cost - payer_coverage) PERSISTED,
        is_hospitalized         BIT             NULL,
        length_of_stay_days     INT             NULL,
        encounter_count         INT             NOT NULL DEFAULT 1,
        load_dts                DATETIME2       NOT NULL CONSTRAINT DF_fact_enc_load_dts DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_fact_encounter PRIMARY KEY CLUSTERED (encounter_id)
    );
    CREATE INDEX IX_fact_enc_patient_date ON dbo.fact_encounter (patient_key, start_date_key) INCLUDE (total_claim_cost, payer_coverage, encounter_class);
    CREATE INDEX IX_fact_enc_org_date     ON dbo.fact_encounter (organization_key, start_date_key) INCLUDE (total_claim_cost, encounter_count, encounter_class);
    CREATE INDEX IX_fact_enc_payer_date   ON dbo.fact_encounter (payer_key, start_date_key) INCLUDE (payer_coverage, total_claim_cost);
    CREATE INDEX IX_fact_enc_provider_date ON dbo.fact_encounter (provider_key, start_date_key) INCLUDE (total_claim_cost, encounter_count);
END;
GO

IF OBJECT_ID(N'dbo.fact_encounter_daily', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.fact_encounter_daily (
        daily_key               BIGINT          IDENTITY(1,1) NOT NULL,
        date_key                INT             NOT NULL,
        organization_key        INT             NULL,
        payer_key               INT             NULL,
        encounter_class         VARCHAR(50)     NULL,
        encounter_count         INT             NOT NULL DEFAULT 0,
        unique_patient_count    INT             NOT NULL DEFAULT 0,
        total_base_cost         DECIMAL(18,2)   NOT NULL DEFAULT 0,
        total_claim_cost        DECIMAL(18,2)   NOT NULL DEFAULT 0,
        total_payer_coverage    DECIMAL(18,2)   NOT NULL DEFAULT 0,
        total_out_of_pocket     DECIMAL(18,2)   NOT NULL DEFAULT 0,
        hospitalized_count      INT             NOT NULL DEFAULT 0,
        total_los_days          INT             NOT NULL DEFAULT 0,
        load_dts                DATETIME2       NOT NULL CONSTRAINT DF_fact_enc_daily_load_dts DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_fact_encounter_daily PRIMARY KEY CLUSTERED (daily_key)
    );
    CREATE UNIQUE INDEX UX_fact_enc_daily_grain
        ON dbo.fact_encounter_daily (date_key, organization_key, payer_key, encounter_class)
        WHERE organization_key IS NOT NULL AND payer_key IS NOT NULL;
    CREATE INDEX IX_fact_enc_daily_date    ON dbo.fact_encounter_daily (date_key) INCLUDE (total_claim_cost, encounter_count);
    CREATE INDEX IX_fact_enc_daily_org_date ON dbo.fact_encounter_daily (organization_key, date_key);
END;
GO

IF OBJECT_ID(N'dbo.fact_conditions', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.fact_conditions (
        condition_code_key      INT             NOT NULL,
        patient_key             INT             NOT NULL,
        condition_start_date_key INT            NOT NULL,
        provider_key            INT             NULL,
        organization_key        INT             NULL,
        encounter_id            VARCHAR(36)     NULL,
        condition_stop_date_key INT             NULL,
        duration_days           INT             NULL,
        condition_count         INT             NOT NULL DEFAULT 1,
        is_active               AS CASE WHEN condition_stop_date_key IS NULL THEN 1 ELSE 0 END PERSISTED,
        load_dts                DATETIME2       NOT NULL CONSTRAINT DF_fact_cond_load_dts DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_fact_conditions PRIMARY KEY CLUSTERED (condition_code_key, patient_key, condition_start_date_key)
    );
    CREATE INDEX IX_fact_cond_patient_code ON dbo.fact_conditions (patient_key, condition_code_key) INCLUDE (condition_start_date_key, is_active, duration_days);
    CREATE INDEX IX_fact_cond_code_date    ON dbo.fact_conditions (condition_code_key, condition_start_date_key) INCLUDE (patient_key, organization_key, is_active);
    CREATE INDEX IX_fact_cond_org_date     ON dbo.fact_conditions (organization_key, condition_start_date_key);
END;
GO

IF OBJECT_ID(N'dbo.fact_condition_daily', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.fact_condition_daily (
        daily_condition_key     BIGINT          IDENTITY(1,1) NOT NULL,
        date_key                INT             NOT NULL,
        condition_code_key      INT             NOT NULL,
        organization_key        INT             NULL,
        new_cases               INT             NOT NULL DEFAULT 0,
        active_cases            INT             NOT NULL DEFAULT 0,
        resolved_cases          INT             NOT NULL DEFAULT 0,
        cumulative_cases        INT             NOT NULL DEFAULT 0,
        hospitalized_cases      INT             NOT NULL DEFAULT 0,
        load_dts                DATETIME2       NOT NULL CONSTRAINT DF_fact_cond_daily_load_dts DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_fact_condition_daily PRIMARY KEY CLUSTERED (daily_condition_key)
    );
    CREATE UNIQUE INDEX UX_fact_cond_daily_grain ON dbo.fact_condition_daily (date_key, condition_code_key, organization_key) WHERE organization_key IS NOT NULL;
    CREATE INDEX IX_fact_cond_daily_code_date ON dbo.fact_condition_daily (condition_code_key, date_key) INCLUDE (new_cases, active_cases, resolved_cases);
    CREATE INDEX IX_fact_cond_daily_date      ON dbo.fact_condition_daily (date_key);
END;
GO

IF OBJECT_ID(N'dbo.fact_medications', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.fact_medications (
        patient_key             INT             NOT NULL,
        start_date_key          INT             NOT NULL,
        medication_code         VARCHAR(20)     NOT NULL,
        stop_date_key           INT             NULL,
        provider_key            INT             NULL,
        organization_key        INT             NULL,
        payer_key               INT             NULL,
        encounter_id            VARCHAR(36)     NULL,
        medication_description  VARCHAR(255)    NULL,
        reason_code             VARCHAR(20)     NULL,
        dispense_as_written     BIT             NULL,
        base_cost               DECIMAL(18,2)   NULL DEFAULT 0,
        payer_coverage          DECIMAL(18,2)   NULL DEFAULT 0,
        out_of_pocket           AS (base_cost - payer_coverage) PERSISTED,
        duration_days           INT             NULL,
        dispense_count          INT             NOT NULL DEFAULT 1,
        is_active               AS CASE WHEN stop_date_key IS NULL THEN 1 ELSE 0 END PERSISTED,
        load_dts                DATETIME2       NOT NULL CONSTRAINT DF_fact_med_load_dts DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_fact_medications PRIMARY KEY CLUSTERED (patient_key, start_date_key, medication_code)
    );
    CREATE INDEX IX_fact_med_patient_date ON dbo.fact_medications (patient_key, start_date_key) INCLUDE (medication_code, base_cost, payer_coverage);
    CREATE INDEX IX_fact_med_code_date    ON dbo.fact_medications (medication_code, start_date_key) INCLUDE (patient_key, organization_key, base_cost);
    CREATE INDEX IX_fact_med_payer        ON dbo.fact_medications (payer_key, start_date_key) INCLUDE (payer_coverage, base_cost);
    CREATE INDEX IX_fact_med_org_date     ON dbo.fact_medications (organization_key, start_date_key) INCLUDE (base_cost, payer_coverage);
END;
GO

-- ============================================================================
-- SECTION 3: FOREIGN KEY CONSTRAINTS
-- ============================================================================

ALTER TABLE dbo.fact_encounter WITH CHECK ADD CONSTRAINT FK_enc_start_date FOREIGN KEY (start_date_key) REFERENCES dbo.dim_date(date_key);
ALTER TABLE dbo.fact_encounter WITH CHECK ADD CONSTRAINT FK_enc_stop_date FOREIGN KEY (stop_date_key) REFERENCES dbo.dim_date(date_key);
ALTER TABLE dbo.fact_encounter WITH CHECK ADD CONSTRAINT FK_enc_patient FOREIGN KEY (patient_key) REFERENCES dbo.dim_patient(patient_key);
ALTER TABLE dbo.fact_encounter WITH CHECK ADD CONSTRAINT FK_enc_provider FOREIGN KEY (provider_key) REFERENCES dbo.dim_provider(provider_key);
ALTER TABLE dbo.fact_encounter WITH CHECK ADD CONSTRAINT FK_enc_payer FOREIGN KEY (payer_key) REFERENCES dbo.dim_payer(payer_key);
ALTER TABLE dbo.fact_encounter WITH CHECK ADD CONSTRAINT FK_enc_org FOREIGN KEY (organization_key) REFERENCES dbo.dim_organization(organization_key);
GO

ALTER TABLE dbo.fact_encounter_daily WITH CHECK ADD CONSTRAINT FK_enc_daily_date FOREIGN KEY (date_key) REFERENCES dbo.dim_date(date_key);
ALTER TABLE dbo.fact_encounter_daily WITH CHECK ADD CONSTRAINT FK_enc_daily_org FOREIGN KEY (organization_key) REFERENCES dbo.dim_organization(organization_key);
ALTER TABLE dbo.fact_encounter_daily WITH CHECK ADD CONSTRAINT FK_enc_daily_payer FOREIGN KEY (payer_key) REFERENCES dbo.dim_payer(payer_key);
GO

ALTER TABLE dbo.fact_conditions WITH CHECK ADD CONSTRAINT FK_cond_code FOREIGN KEY (condition_code_key) REFERENCES dbo.dim_condition_code(condition_code_key);
ALTER TABLE dbo.fact_conditions WITH CHECK ADD CONSTRAINT FK_cond_patient FOREIGN KEY (patient_key) REFERENCES dbo.dim_patient(patient_key);
ALTER TABLE dbo.fact_conditions WITH CHECK ADD CONSTRAINT FK_cond_provider FOREIGN KEY (provider_key) REFERENCES dbo.dim_provider(provider_key);
ALTER TABLE dbo.fact_conditions WITH CHECK ADD CONSTRAINT FK_cond_org FOREIGN KEY (organization_key) REFERENCES dbo.dim_organization(organization_key);
ALTER TABLE dbo.fact_conditions WITH CHECK ADD CONSTRAINT FK_cond_start_date FOREIGN KEY (condition_start_date_key) REFERENCES dbo.dim_date(date_key);
ALTER TABLE dbo.fact_conditions WITH CHECK ADD CONSTRAINT FK_cond_stop_date FOREIGN KEY (condition_stop_date_key) REFERENCES dbo.dim_date(date_key);
GO

ALTER TABLE dbo.fact_condition_daily WITH CHECK ADD CONSTRAINT FK_cond_daily_date FOREIGN KEY (date_key) REFERENCES dbo.dim_date(date_key);
ALTER TABLE dbo.fact_condition_daily WITH CHECK ADD CONSTRAINT FK_cond_daily_code FOREIGN KEY (condition_code_key) REFERENCES dbo.dim_condition_code(condition_code_key);
ALTER TABLE dbo.fact_condition_daily WITH CHECK ADD CONSTRAINT FK_cond_daily_org FOREIGN KEY (organization_key) REFERENCES dbo.dim_organization(organization_key);
GO

ALTER TABLE dbo.fact_medications WITH CHECK ADD CONSTRAINT FK_med_patient FOREIGN KEY (patient_key) REFERENCES dbo.dim_patient(patient_key);
ALTER TABLE dbo.fact_medications WITH CHECK ADD CONSTRAINT FK_med_start_date FOREIGN KEY (start_date_key) REFERENCES dbo.dim_date(date_key);
ALTER TABLE dbo.fact_medications WITH CHECK ADD CONSTRAINT FK_med_stop_date FOREIGN KEY (stop_date_key) REFERENCES dbo.dim_date(date_key);
ALTER TABLE dbo.fact_medications WITH CHECK ADD CONSTRAINT FK_med_provider FOREIGN KEY (provider_key) REFERENCES dbo.dim_provider(provider_key);
ALTER TABLE dbo.fact_medications WITH CHECK ADD CONSTRAINT FK_med_org FOREIGN KEY (organization_key) REFERENCES dbo.dim_organization(organization_key);
ALTER TABLE dbo.fact_medications WITH CHECK ADD CONSTRAINT FK_med_payer FOREIGN KEY (payer_key) REFERENCES dbo.dim_payer(payer_key);
GO

-- ============================================================================
-- SECTION 4: SAMPLE ANALYTICAL QUERIES
-- ============================================================================

/*
-- [Q1] Chi phí theo payer × tháng
SELECT d.[year], d.[quarter], d.[month], p.[name] AS payer_name,
       SUM(f.total_claim_cost) AS total_claim,
       SUM(f.total_payer_coverage) AS payer_covered,
       SUM(f.total_out_of_pocket) AS patient_oop
FROM dbo.fact_encounter_daily f
JOIN dbo.dim_date d ON d.date_key = f.date_key
JOIN dbo.dim_payer p ON p.payer_key = f.payer_key AND p.is_current = 1
GROUP BY ROLLUP (d.[year], d.[quarter], d.[month], p.[name]);

-- [Q4] Trend COVID-19
SELECT d.full_date,
       SUM(cd.new_cases) AS new_cases,
       SUM(cd.active_cases) AS active_cases,
       SUM(cd.resolved_cases) AS resolved_cases
FROM dbo.fact_condition_daily cd
JOIN dbo.dim_date d ON d.date_key = cd.date_key
JOIN dbo.dim_condition_code cc ON cc.condition_code_key = cd.condition_code_key
WHERE cc.code = '840539006'
GROUP BY d.full_date
ORDER BY d.full_date;
*/

-- ============================================================================
-- END OF SCHEMA v5
-- ============================================================================

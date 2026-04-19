-- ============================================================================
-- SYNTHEA DWH — KIMBALL STAR SCHEMA (v5.1 - FIXED)
-- ============================================================================
-- Target DB  : [DW_Synthea_DWH]
-- Author     : Khang / Data Engineering Team
-- Version    : 5.1 (Fixed Syntax & naming consistency)
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

IF OBJECT_ID(N'dbo.dim_medication', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.dim_medication (
        medication_key        INT IDENTITY(1,1) NOT NULL,
        medication_code       VARCHAR(20)     NOT NULL,
        medication_description VARCHAR(255)   NULL,
        CONSTRAINT PK_dim_medication PRIMARY KEY CLUSTERED (medication_key),
        CONSTRAINT UQ_dim_medication_code UNIQUE (medication_code)
    );
END;
GO

-- ============================================================================
-- SECTION 2: FACT TABLES
-- ============================================================================

-- 

IF OBJECT_ID(N'dbo.fact_encounter', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.fact_encounter (
        fact_encounter_key      BIGINT IDENTITY(1,1) PRIMARY KEY,
        patient_key             INT          NOT NULL,
        encounter_date_key      INT          NOT NULL,
        provider_key            INT          NULL,
        organization_key        INT          NULL,
        payer_key               INT          NULL,
        encounter_id            VARCHAR(36)  NOT NULL,
        encounter_code          VARCHAR(20)  NULL,
        encounter_class         VARCHAR(50)  NULL,
        encounter_description   VARCHAR(255) NULL,
        reason_code             VARCHAR(20)  NULL,
        base_cost               DECIMAL(18,2) NULL,
        total_claim_cost        DECIMAL(18,2) NULL,
        payer_coverage          DECIMAL(18,2) NULL,
        out_of_pocket AS (ISNULL(total_claim_cost,0) - ISNULL(payer_coverage,0)) PERSISTED,
        load_dts DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE UNIQUE INDEX UX_fact_encounter_source ON dbo.fact_encounter(encounter_id);
    CREATE INDEX IX_fact_encounter_patient_date ON dbo.fact_encounter(patient_key, encounter_date_key);
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
END;
GO

IF OBJECT_ID(N'dbo.fact_condition', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.fact_condition (
        fact_condition_key      BIGINT IDENTITY(1,1) PRIMARY KEY,
        patient_key             INT          NOT NULL,
        start_date_key          INT          NOT NULL,
        stop_date_key           INT          NULL,
        condition_code_key      INT          NULL, -- Thêm key để join dim_condition_code
        provider_key            INT          NULL,
        organization_key        INT          NULL,
        encounter_id            VARCHAR(36)  NULL,
        condition_code          VARCHAR(20)  NOT NULL,
        condition_description   VARCHAR(255) NULL,
        is_active AS CASE WHEN stop_date_key IS NULL THEN 1 ELSE 0 END PERSISTED,
        load_dts DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX IX_fact_condition_patient_date ON dbo.fact_condition(patient_key, start_date_key);
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
END;
GO

IF OBJECT_ID(N'dbo.fact_medications', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.fact_medications (
        fact_medication_key     BIGINT IDENTITY(1,1) PRIMARY KEY,
        patient_key             INT NOT NULL,
        medication_key          INT NOT NULL,
        start_date_key          INT NOT NULL,
        stop_date_key           INT NULL,
        provider_key            INT NULL,
        organization_key        INT NULL,
        payer_key               INT NULL,
        encounter_id            VARCHAR(36) NULL,
        reason_code             VARCHAR(20) NULL,
        base_cost               DECIMAL(18,2) NULL,
        payer_coverage          DECIMAL(18,2) NULL,
        dispense_count          INT NULL,
        is_active               AS CASE WHEN stop_date_key IS NULL THEN 1 ELSE 0 END PERSISTED,
        out_of_pocket           AS (ISNULL(base_cost,0) - ISNULL(payer_coverage,0)) PERSISTED,
        load_dts                DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
END;
GO

IF OBJECT_ID(N'dbo.fact_medication_daily', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.fact_medication_daily (
        medication_daily_key   BIGINT IDENTITY(1,1) PRIMARY KEY,
        date_key               INT NOT NULL,
        medication_key         INT NOT NULL,
        organization_key       INT NULL,
        payer_key              INT NULL,
        active_patient_count   INT NOT NULL DEFAULT 0,
        new_prescriptions      INT NOT NULL DEFAULT 0,
        stopped_prescriptions  INT NOT NULL DEFAULT 0,
        total_dispense_count   INT NOT NULL DEFAULT 0,
        total_base_cost        DECIMAL(18,2) NOT NULL DEFAULT 0,
        total_payer_coverage   DECIMAL(18,2) NOT NULL DEFAULT 0,
        total_out_of_pocket    DECIMAL(18,2) NOT NULL DEFAULT 0,
        load_dts               DATETIME2 NOT NULL CONSTRAINT DF_fact_med_daily_load_dts DEFAULT SYSUTCDATETIME()
    );
    -- Fix: Unique Index không sử dụng hàm ISNULL ở đây
    CREATE UNIQUE INDEX UX_fact_med_daily_grain ON dbo.fact_medication_daily (date_key, medication_key, organization_key, payer_key)
    WHERE organization_key IS NOT NULL AND payer_key IS NOT NULL;
END;
GO

-- ============================================================================
-- SECTION 3: FOREIGN KEY CONSTRAINTS
-- ============================================================================

-- Fact Encounter
IF OBJECT_ID(N'dbo.fact_encounter', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_enc_date' AND parent_object_id = OBJECT_ID(N'dbo.fact_encounter'))
    ALTER TABLE dbo.fact_encounter WITH CHECK ADD CONSTRAINT FK_enc_date FOREIGN KEY (encounter_date_key) REFERENCES dbo.dim_date(date_key);

IF OBJECT_ID(N'dbo.fact_encounter', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_enc_patient' AND parent_object_id = OBJECT_ID(N'dbo.fact_encounter'))
    ALTER TABLE dbo.fact_encounter WITH CHECK ADD CONSTRAINT FK_enc_patient FOREIGN KEY (patient_key) REFERENCES dbo.dim_patient(patient_key);

IF OBJECT_ID(N'dbo.fact_encounter', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_enc_provider' AND parent_object_id = OBJECT_ID(N'dbo.fact_encounter'))
    ALTER TABLE dbo.fact_encounter WITH CHECK ADD CONSTRAINT FK_enc_provider FOREIGN KEY (provider_key) REFERENCES dbo.dim_provider(provider_key);

IF OBJECT_ID(N'dbo.fact_encounter', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_enc_payer' AND parent_object_id = OBJECT_ID(N'dbo.fact_encounter'))
    ALTER TABLE dbo.fact_encounter WITH CHECK ADD CONSTRAINT FK_enc_payer FOREIGN KEY (payer_key) REFERENCES dbo.dim_payer(payer_key);

IF OBJECT_ID(N'dbo.fact_encounter', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_enc_org' AND parent_object_id = OBJECT_ID(N'dbo.fact_encounter'))
    ALTER TABLE dbo.fact_encounter WITH CHECK ADD CONSTRAINT FK_enc_org FOREIGN KEY (organization_key) REFERENCES dbo.dim_organization(organization_key);
GO

-- Fact Encounter Daily
IF OBJECT_ID(N'dbo.fact_encounter_daily', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_enc_daily_date' AND parent_object_id = OBJECT_ID(N'dbo.fact_encounter_daily'))
    ALTER TABLE dbo.fact_encounter_daily WITH CHECK ADD CONSTRAINT FK_enc_daily_date FOREIGN KEY (date_key) REFERENCES dbo.dim_date(date_key);

IF OBJECT_ID(N'dbo.fact_encounter_daily', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_enc_daily_org' AND parent_object_id = OBJECT_ID(N'dbo.fact_encounter_daily'))
    ALTER TABLE dbo.fact_encounter_daily WITH CHECK ADD CONSTRAINT FK_enc_daily_org FOREIGN KEY (organization_key) REFERENCES dbo.dim_organization(organization_key);

IF OBJECT_ID(N'dbo.fact_encounter_daily', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_enc_daily_payer' AND parent_object_id = OBJECT_ID(N'dbo.fact_encounter_daily'))
    ALTER TABLE dbo.fact_encounter_daily WITH CHECK ADD CONSTRAINT FK_enc_daily_payer FOREIGN KEY (payer_key) REFERENCES dbo.dim_payer(payer_key);
GO

-- Fact Condition (Fixing naming consistency)
IF OBJECT_ID(N'dbo.fact_condition', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_cond_patient' AND parent_object_id = OBJECT_ID(N'dbo.fact_condition'))
    ALTER TABLE dbo.fact_condition WITH CHECK ADD CONSTRAINT FK_cond_patient FOREIGN KEY (patient_key) REFERENCES dbo.dim_patient(patient_key);

IF OBJECT_ID(N'dbo.fact_condition', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_cond_start_date' AND parent_object_id = OBJECT_ID(N'dbo.fact_condition'))
    ALTER TABLE dbo.fact_condition WITH CHECK ADD CONSTRAINT FK_cond_start_date FOREIGN KEY (start_date_key) REFERENCES dbo.dim_date(date_key);

IF OBJECT_ID(N'dbo.fact_condition', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_cond_code_dim' AND parent_object_id = OBJECT_ID(N'dbo.fact_condition'))
    ALTER TABLE dbo.fact_condition WITH CHECK ADD CONSTRAINT FK_cond_code_dim FOREIGN KEY (condition_code_key) REFERENCES dbo.dim_condition_code(condition_code_key);

IF OBJECT_ID(N'dbo.fact_condition', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_cond_org' AND parent_object_id = OBJECT_ID(N'dbo.fact_condition'))
    ALTER TABLE dbo.fact_condition WITH CHECK ADD CONSTRAINT FK_cond_org FOREIGN KEY (organization_key) REFERENCES dbo.dim_organization(organization_key);
GO

-- Fact Condition Daily
IF OBJECT_ID(N'dbo.fact_condition_daily', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_cond_daily_date' AND parent_object_id = OBJECT_ID(N'dbo.fact_condition_daily'))
    ALTER TABLE dbo.fact_condition_daily WITH CHECK ADD CONSTRAINT FK_cond_daily_date FOREIGN KEY (date_key) REFERENCES dbo.dim_date(date_key);

IF OBJECT_ID(N'dbo.fact_condition_daily', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_cond_daily_code' AND parent_object_id = OBJECT_ID(N'dbo.fact_condition_daily'))
    ALTER TABLE dbo.fact_condition_daily WITH CHECK ADD CONSTRAINT FK_cond_daily_code FOREIGN KEY (condition_code_key) REFERENCES dbo.dim_condition_code(condition_code_key);

IF OBJECT_ID(N'dbo.fact_condition_daily', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_cond_daily_org' AND parent_object_id = OBJECT_ID(N'dbo.fact_condition_daily'))
    ALTER TABLE dbo.fact_condition_daily WITH CHECK ADD CONSTRAINT FK_cond_daily_org FOREIGN KEY (organization_key) REFERENCES dbo.dim_organization(organization_key);
GO

-- Fact Medications
IF OBJECT_ID(N'dbo.fact_medications', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_med_patient' AND parent_object_id = OBJECT_ID(N'dbo.fact_medications'))
    ALTER TABLE dbo.fact_medications WITH CHECK ADD CONSTRAINT FK_med_patient FOREIGN KEY (patient_key) REFERENCES dbo.dim_patient(patient_key);

IF OBJECT_ID(N'dbo.fact_medications', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_med_medication' AND parent_object_id = OBJECT_ID(N'dbo.fact_medications'))
    ALTER TABLE dbo.fact_medications WITH CHECK ADD CONSTRAINT FK_med_medication FOREIGN KEY (medication_key) REFERENCES dbo.dim_medication(medication_key);

IF OBJECT_ID(N'dbo.fact_medications', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_med_start_date' AND parent_object_id = OBJECT_ID(N'dbo.fact_medications'))
    ALTER TABLE dbo.fact_medications WITH CHECK ADD CONSTRAINT FK_med_start_date FOREIGN KEY (start_date_key) REFERENCES dbo.dim_date(date_key);

IF OBJECT_ID(N'dbo.fact_medications', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_med_org' AND parent_object_id = OBJECT_ID(N'dbo.fact_medications'))
    ALTER TABLE dbo.fact_medications WITH CHECK ADD CONSTRAINT FK_med_org FOREIGN KEY (organization_key) REFERENCES dbo.dim_organization(organization_key);

IF OBJECT_ID(N'dbo.fact_medications', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_med_payer' AND parent_object_id = OBJECT_ID(N'dbo.fact_medications'))
    ALTER TABLE dbo.fact_medications WITH CHECK ADD CONSTRAINT FK_med_payer FOREIGN KEY (payer_key) REFERENCES dbo.dim_payer(payer_key);
GO

-- Fact Medication Daily
IF OBJECT_ID(N'dbo.fact_medication_daily', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_med_daily_date' AND parent_object_id = OBJECT_ID(N'dbo.fact_medication_daily'))
    ALTER TABLE dbo.fact_medication_daily WITH CHECK ADD CONSTRAINT FK_med_daily_date FOREIGN KEY (date_key) REFERENCES dbo.dim_date(date_key);

IF OBJECT_ID(N'dbo.fact_medication_daily', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_med_daily_med' AND parent_object_id = OBJECT_ID(N'dbo.fact_medication_daily'))
    ALTER TABLE dbo.fact_medication_daily WITH CHECK ADD CONSTRAINT FK_med_daily_med FOREIGN KEY (medication_key) REFERENCES dbo.dim_medication(medication_key);

IF OBJECT_ID(N'dbo.fact_medication_daily', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_med_daily_org' AND parent_object_id = OBJECT_ID(N'dbo.fact_medication_daily'))
    ALTER TABLE dbo.fact_medication_daily WITH CHECK ADD CONSTRAINT FK_med_daily_org FOREIGN KEY (organization_key) REFERENCES dbo.dim_organization(organization_key);

IF OBJECT_ID(N'dbo.fact_medication_daily', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_med_daily_payer' AND parent_object_id = OBJECT_ID(N'dbo.fact_medication_daily'))
    ALTER TABLE dbo.fact_medication_daily WITH CHECK ADD CONSTRAINT FK_med_daily_payer FOREIGN KEY (payer_key) REFERENCES dbo.dim_payer(payer_key);
GO

-- ============================================================================
-- END OF SCHEMA v5.1
-- ============================================================================
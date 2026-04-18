-- ============================================================================
-- SYNTHEA DWH DATABASE SCHEMA (Star Schema)
-- ============================================================================
-- Source DB : [DW_Synthea_Staging]
-- Target DB : [DW_Synthea_DWH]
-- Notes:
--   - DIM SCD2: dim_patient, dim_provider, dim_organization, dim_payer
--   - Run this script in SSMS/Azure Data Studio
-- ============================================================================

USE [master];
GO

IF DB_ID(N'DW_Synthea_DWH') IS NULL
BEGIN
    CREATE DATABASE [DW_Synthea_DWH];
END;
GO

USE [DW_Synthea_DWH];
GO

-- Create schema (optional, keep dbo for simplicity)

-- ============================================================================
-- DIM: Date
-- ============================================================================
IF OBJECT_ID(N'dbo.dim_date', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.dim_date (
        date_key        INT         NOT NULL, -- yyyymmdd
        full_date       DATE        NOT NULL,
        [year]          SMALLINT    NOT NULL,
        [month]         TINYINT     NOT NULL,
        month_name      VARCHAR(10) NOT NULL,
        [quarter]       TINYINT     NOT NULL,
        day_of_month    TINYINT     NOT NULL,
        day_of_week     TINYINT     NOT NULL, -- 1=Mon ... (depends on DATEFIRST in loader)
        day_name        VARCHAR(10) NOT NULL,
        is_weekend      BIT         NOT NULL,
        create_at       DATETIME2   NOT NULL CONSTRAINT DF_dim_date_create_at DEFAULT SYSUTCDATETIME(),
        update_at       DATETIME2   NOT NULL CONSTRAINT DF_dim_date_update_at DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_dim_date PRIMARY KEY CLUSTERED (date_key),
        CONSTRAINT UQ_dim_date_full_date UNIQUE (full_date)
    );
END;
GO

-- ============================================================================
-- DIM: Patient (SCD2)
-- ============================================================================
IF OBJECT_ID(N'dbo.dim_patient', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.dim_patient (
        patient_key     INT             IDENTITY(1,1) NOT NULL,
        patient_id      VARCHAR(36)     NOT NULL,  -- natural key from Synthea

        first_name      VARCHAR(100)    NULL,
        last_name       VARCHAR(100)    NULL,
        birthdate       DATE            NULL,
        deathdate       DATE            NULL,
        gender          CHAR(1)         NULL,
        race            VARCHAR(50)     NULL,
        ethnicity       VARCHAR(50)     NULL,
        city            VARCHAR(100)    NULL,
        state           VARCHAR(50)     NULL,
        zip             VARCHAR(10)     NULL,
        healthcare_expenses DECIMAL(18,2) NULL,
        healthcare_coverage DECIMAL(18,2) NULL,

        valid_from      DATE            NOT NULL,
        valid_to        DATE            NOT NULL,
        is_current      BIT             NOT NULL,
        row_hash        VARBINARY(32)   NOT NULL,

        create_at       DATETIME2       NOT NULL CONSTRAINT DF_dim_patient_create_at DEFAULT SYSUTCDATETIME(),
        update_at       DATETIME2       NOT NULL CONSTRAINT DF_dim_patient_update_at DEFAULT SYSUTCDATETIME(),

        CONSTRAINT PK_dim_patient PRIMARY KEY CLUSTERED (patient_key)
    );

    CREATE INDEX IX_dim_patient_nk_current
        ON dbo.dim_patient (patient_id, is_current)
        INCLUDE (valid_from, valid_to, row_hash);

    CREATE UNIQUE INDEX UX_dim_patient_nk_from
        ON dbo.dim_patient (patient_id, valid_from);
END;
GO

-- ============================================================================
-- DIM: Organization (SCD2)
-- ============================================================================
IF OBJECT_ID(N'dbo.dim_organization', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.dim_organization (
        organization_key    INT             IDENTITY(1,1) NOT NULL,
        organization_id     VARCHAR(36)     NOT NULL, -- natural key

        [name]              VARCHAR(255)    NULL,
        city                VARCHAR(100)    NULL,
        [state]             VARCHAR(50)     NULL,
        zip                 VARCHAR(10)     NULL,
        phone               VARCHAR(20)     NULL,

        revenue             DECIMAL(18,2)   NULL,
        utilization         INT             NULL,

        valid_from          DATE            NOT NULL,
        valid_to            DATE            NOT NULL,
        is_current          BIT             NOT NULL,
        row_hash            VARBINARY(32)   NOT NULL,

        create_at           DATETIME2       NOT NULL CONSTRAINT DF_dim_org_create_at DEFAULT SYSUTCDATETIME(),
        update_at           DATETIME2       NOT NULL CONSTRAINT DF_dim_org_update_at DEFAULT SYSUTCDATETIME(),

        CONSTRAINT PK_dim_organization PRIMARY KEY CLUSTERED (organization_key)
    );

    CREATE INDEX IX_dim_org_nk_current
        ON dbo.dim_organization (organization_id, is_current)
        INCLUDE (valid_from, valid_to, row_hash);

    CREATE UNIQUE INDEX UX_dim_org_nk_from
        ON dbo.dim_organization (organization_id, valid_from);
END;
GO

-- ============================================================================
-- DIM: Provider (SCD2)
-- ============================================================================
IF OBJECT_ID(N'dbo.dim_provider', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.dim_provider (
        provider_key        INT             IDENTITY(1,1) NOT NULL,
        provider_id         VARCHAR(36)     NOT NULL, -- natural key

        organization_id     VARCHAR(36)     NULL,     -- keep NK for lineage
        [name]              VARCHAR(255)    NULL,
        gender              CHAR(1)         NULL,
        speciality          VARCHAR(100)    NULL,
        city                VARCHAR(100)    NULL,
        [state]             VARCHAR(50)     NULL,
        zip                 VARCHAR(10)     NULL,
        utilization         INT             NULL,

        valid_from          DATE            NOT NULL,
        valid_to            DATE            NOT NULL,
        is_current          BIT             NOT NULL,
        row_hash            VARBINARY(32)   NOT NULL,

        create_at           DATETIME2       NOT NULL CONSTRAINT DF_dim_provider_create_at DEFAULT SYSUTCDATETIME(),
        update_at           DATETIME2       NOT NULL CONSTRAINT DF_dim_provider_update_at DEFAULT SYSUTCDATETIME(),

        CONSTRAINT PK_dim_provider PRIMARY KEY CLUSTERED (provider_key)
    );

    CREATE INDEX IX_dim_provider_nk_current
        ON dbo.dim_provider (provider_id, is_current)
        INCLUDE (valid_from, valid_to, row_hash);

    CREATE UNIQUE INDEX UX_dim_provider_nk_from
        ON dbo.dim_provider (provider_id, valid_from);
END;
GO

-- ============================================================================
-- DIM: Payer (SCD2)
-- ============================================================================
IF OBJECT_ID(N'dbo.dim_payer', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.dim_payer (
        payer_key           INT             IDENTITY(1,1) NOT NULL,
        payer_id            VARCHAR(36)     NOT NULL, -- natural key

        [name]              VARCHAR(255)    NULL,
        city                VARCHAR(100)    NULL,
        state_headquartered VARCHAR(50)     NULL,
        zip                 VARCHAR(10)     NULL,
        phone               VARCHAR(20)     NULL,
        ownership           VARCHAR(50)     NULL, -- from Staging_Payer_Transitions (best-effort)

        amount_covered       DECIMAL(18,2)  NULL,
        amount_uncovered     DECIMAL(18,2)  NULL,
        revenue              DECIMAL(18,2)  NULL,
        unique_customers     INT            NULL,
        member_months        INT            NULL,

        valid_from          DATE            NOT NULL,
        valid_to            DATE            NOT NULL,
        is_current          BIT             NOT NULL,
        row_hash            VARBINARY(32)   NOT NULL,

        create_at           DATETIME2       NOT NULL CONSTRAINT DF_dim_payer_create_at DEFAULT SYSUTCDATETIME(),
        update_at           DATETIME2       NOT NULL CONSTRAINT DF_dim_payer_update_at DEFAULT SYSUTCDATETIME(),

        CONSTRAINT PK_dim_payer PRIMARY KEY CLUSTERED (payer_key)
    );

    CREATE INDEX IX_dim_payer_nk_current
        ON dbo.dim_payer (payer_id, is_current)
        INCLUDE (valid_from, valid_to, row_hash);

    CREATE UNIQUE INDEX UX_dim_payer_nk_from
        ON dbo.dim_payer (payer_id, valid_from);
END;
GO

-- ============================================================================
-- DIM: Condition Code (Type 1)
-- ============================================================================
IF OBJECT_ID(N'dbo.dim_condition_code', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.dim_condition_code (
        condition_code_key  INT             IDENTITY(1,1) NOT NULL,
        code                VARCHAR(20)     NOT NULL,
        [description]       VARCHAR(255)    NULL,
        create_at           DATETIME2       NOT NULL CONSTRAINT DF_dim_cond_code_create_at DEFAULT SYSUTCDATETIME(),
        update_at           DATETIME2       NOT NULL CONSTRAINT DF_dim_cond_code_update_at DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_dim_condition_code PRIMARY KEY CLUSTERED (condition_code_key),
        CONSTRAINT UQ_dim_condition_code UNIQUE (code)
    );
END;
GO

-- ============================================================================
-- DIM: Encounter (Type 1; one row per encounter event)
-- ============================================================================
IF OBJECT_ID(N'dbo.dim_encounter', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.dim_encounter (
        encounter_key       UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_dim_encounter_key DEFAULT NEWID(),
        encounter_id        VARCHAR(36)      NOT NULL,

        start_date_key      INT              NULL,
        stop_date_key       INT              NULL,

        patient_key         INT              NULL,
        provider_key        INT              NULL,
        payer_key           INT              NULL,
        organization_key    INT              NULL,

        encounter_class     VARCHAR(50)      NULL,
        code                VARCHAR(20)      NULL,
        [description]       VARCHAR(255)     NULL,

        base_encounter_cost DECIMAL(18,2)    NULL,
        total_claim_cost    DECIMAL(18,2)    NULL,
        payer_coverage      DECIMAL(18,2)    NULL,

        reason_code         VARCHAR(20)      NULL,
        reason_description  VARCHAR(255)     NULL,

        create_at           DATETIME2        NOT NULL CONSTRAINT DF_dim_encounter_create_at DEFAULT SYSUTCDATETIME(),
        update_at           DATETIME2        NOT NULL CONSTRAINT DF_dim_encounter_update_at DEFAULT SYSUTCDATETIME(),

        CONSTRAINT PK_dim_encounter PRIMARY KEY CLUSTERED (encounter_key),
        CONSTRAINT UQ_dim_encounter_id UNIQUE (encounter_id)
    );

    CREATE INDEX IX_dim_encounter_start_date
        ON dbo.dim_encounter (start_date_key)
        INCLUDE (provider_key, payer_key, organization_key, patient_key);
END;
GO

-- ============================================================================
-- FACT: Utilization (daily rollup)
-- ============================================================================
IF OBJECT_ID(N'dbo.fact_utilization', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.fact_utilization (
        utilization_key     BIGINT          IDENTITY(1,1) NOT NULL,
        date_key            INT             NOT NULL,
        provider_key        INT             NULL,
        payer_key           INT             NULL,
        organization_key    INT             NULL,

        encounter_count     INT             NOT NULL,
        base_encounter_cost DECIMAL(18,2)   NOT NULL,
        total_claim_cost    DECIMAL(18,2)   NOT NULL,
        payer_coverage      DECIMAL(18,2)   NOT NULL,

        load_dts            DATETIME2       NOT NULL CONSTRAINT DF_fact_util_load_dts DEFAULT SYSUTCDATETIME(),

        CONSTRAINT PK_fact_utilization PRIMARY KEY CLUSTERED (utilization_key)
    );

    CREATE INDEX IX_fact_util_date ON dbo.fact_utilization (date_key);
END;
GO

-- ============================================================================
-- FACT: Conditions (one row per condition event)
-- ============================================================================
IF OBJECT_ID(N'dbo.fact_conditions', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.fact_conditions (
        condition_key            UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_fact_condition_key DEFAULT NEWID(),
        condition_code_key       INT              NOT NULL,

        patient_key              INT              NULL,
        provider_key             INT              NULL,
        organization_key         INT              NULL,
        encounter_key            UNIQUEIDENTIFIER NULL,

        condition_start_date_key INT              NOT NULL,
        condition_stop_date_key  INT              NULL,

        duration_days            INT              NULL,
        condition_count          INT              NOT NULL CONSTRAINT DF_fact_cond_count DEFAULT (1),

        load_dts                 DATETIME2        NOT NULL CONSTRAINT DF_fact_cond_load_dts DEFAULT SYSUTCDATETIME(),

        CONSTRAINT PK_fact_conditions PRIMARY KEY CLUSTERED (condition_key)
    );

    CREATE INDEX IX_fact_conditions_start_date ON dbo.fact_conditions (condition_start_date_key);
    CREATE INDEX IX_fact_conditions_code_date ON dbo.fact_conditions (condition_code_key, condition_start_date_key);
END;
GO

-- ============================================================================
-- FACT: Condition Daily Snapshot (daily rollup)
-- ============================================================================
IF OBJECT_ID(N'dbo.fact_condition_daily_snapshot', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.fact_condition_daily_snapshot (
        [key]               UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_fact_cond_snap_key DEFAULT NEWID(),
        condition_code_key  INT              NOT NULL,
        organization_key    INT              NULL,
        date_key            INT              NOT NULL,

        local_cases         INT              NOT NULL,
        new_cases           INT              NOT NULL,
        active_cases        INT              NOT NULL,
        resolved_cases      INT              NOT NULL,

        load_dts            DATETIME2        NOT NULL CONSTRAINT DF_fact_cond_snap_load_dts DEFAULT SYSUTCDATETIME(),

        CONSTRAINT PK_fact_condition_daily_snapshot PRIMARY KEY CLUSTERED ([key])
    );

    CREATE INDEX IX_fact_cond_snap_date ON dbo.fact_condition_daily_snapshot (date_key);
END;
GO

-- ============================================================================
-- FACT: Costs (daily rollup across encounters/meds/procedures/immunizations)
-- ============================================================================
IF OBJECT_ID(N'dbo.fact_costs', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.fact_costs (
        cost_key            BIGINT          IDENTITY(1,1) NOT NULL,
        date_key            INT             NOT NULL,
        provider_key        INT             NULL,
        organization_key    INT             NULL,

        local_payer_coverage DECIMAL(18,2)  NOT NULL,
        total_out_of_pocket  DECIMAL(18,2)  NOT NULL,
        total_costs          DECIMAL(18,2)  NOT NULL,

        load_dts             DATETIME2      NOT NULL CONSTRAINT DF_fact_costs_load_dts DEFAULT SYSUTCDATETIME(),

        CONSTRAINT PK_fact_costs PRIMARY KEY CLUSTERED (cost_key)
    );

    CREATE INDEX IX_fact_costs_date ON dbo.fact_costs (date_key);
END;
GO

-- ============================================================================
-- Add Foreign Key Constraints to DWH
-- ============================================================================
USE [DW_Synthea_DWH];
GO

-- 1) fact_utilization → dimensions
ALTER TABLE dbo.fact_utilization WITH CHECK
    ADD CONSTRAINT FK_util_date FOREIGN KEY (date_key)
        REFERENCES dbo.dim_date(date_key);

ALTER TABLE dbo.fact_utilization WITH CHECK
    ADD CONSTRAINT FK_util_provider FOREIGN KEY (provider_key)
        REFERENCES dbo.dim_provider(provider_key);

ALTER TABLE dbo.fact_utilization WITH CHECK
    ADD CONSTRAINT FK_util_payer FOREIGN KEY (payer_key)
        REFERENCES dbo.dim_payer(payer_key);

ALTER TABLE dbo.fact_utilization WITH CHECK
    ADD CONSTRAINT FK_util_org FOREIGN KEY (organization_key)
        REFERENCES dbo.dim_organization(organization_key);

-- 2) fact_conditions → dimensions
ALTER TABLE dbo.fact_conditions WITH CHECK
    ADD CONSTRAINT FK_cond_code FOREIGN KEY (condition_code_key)
        REFERENCES dbo.dim_condition_code(condition_code_key);

ALTER TABLE dbo.fact_conditions WITH CHECK
    ADD CONSTRAINT FK_cond_patient FOREIGN KEY (patient_key)
        REFERENCES dbo.dim_patient(patient_key);

ALTER TABLE dbo.fact_conditions WITH CHECK
    ADD CONSTRAINT FK_cond_provider FOREIGN KEY (provider_key)
        REFERENCES dbo.dim_provider(provider_key);

ALTER TABLE dbo.fact_conditions WITH CHECK
    ADD CONSTRAINT FK_cond_org FOREIGN KEY (organization_key)
        REFERENCES dbo.dim_organization(organization_key);

ALTER TABLE dbo.fact_conditions WITH CHECK
    ADD CONSTRAINT FK_cond_encounter FOREIGN KEY (encounter_key)
        REFERENCES dbo.dim_encounter(encounter_key);

ALTER TABLE dbo.fact_conditions WITH CHECK
    ADD CONSTRAINT FK_cond_start_date FOREIGN KEY (condition_start_date_key)
        REFERENCES dbo.dim_date(date_key);

-- 3) dim_encounter → dimensions
ALTER TABLE dbo.dim_encounter WITH CHECK
    ADD CONSTRAINT FK_enc_patient FOREIGN KEY (patient_key)
        REFERENCES dbo.dim_patient(patient_key);

ALTER TABLE dbo.dim_encounter WITH CHECK
    ADD CONSTRAINT FK_enc_provider FOREIGN KEY (provider_key)
        REFERENCES dbo.dim_provider(provider_key);

ALTER TABLE dbo.dim_encounter WITH CHECK
    ADD CONSTRAINT FK_enc_payer FOREIGN KEY (payer_key)
        REFERENCES dbo.dim_payer(payer_key);

ALTER TABLE dbo.dim_encounter WITH CHECK
    ADD CONSTRAINT FK_enc_org FOREIGN KEY (organization_key)
        REFERENCES dbo.dim_organization(organization_key);

ALTER TABLE dbo.dim_encounter WITH CHECK
    ADD CONSTRAINT FK_enc_start_date FOREIGN KEY (start_date_key)
        REFERENCES dbo.dim_date(date_key);

-- 4) dim_encounter -> dimesions
ALTER TABLE dbo.dim_encounter WITH CHECK
    ADD CONSTRAINT FK_enc_stop_date FOREIGN KEY (stop_date_key)
        REFERENCES dbo.dim_date(date_key);
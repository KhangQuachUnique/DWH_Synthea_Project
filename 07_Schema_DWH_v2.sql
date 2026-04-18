-- ============================================================================
-- SYNTHEA DWH — KIMBALL STAR SCHEMA (Redesigned)
-- ============================================================================
-- Target DB  : [DW_Synthea_DWH]
-- Author     : Khang / Data Engineering Team
-- Version    : 2.0
--
-- DESIGN DECISIONS
-- ================
-- 5 FACT TABLES (không hơn):
--   1. fact_encounter          — grain: 1 row per encounter (transaction fact)
--   2. fact_encounter_daily    — grain: date x org x payer x class (periodic rollup)
--   3. fact_conditions         — grain: 1 row per condition event (transaction fact)
--   4. fact_condition_daily    — grain: date x condition x org (accumulating snapshot)
--   5. fact_medications        — grain: 1 row per dispense event (transaction fact)
--
-- 6 DIMENSION TABLES:
--   dim_date, dim_patient (SCD2), dim_provider (SCD2),
--   dim_organization (SCD2), dim_payer (SCD2), dim_condition_code (Type1)
--
-- ANTI-PATTERNS REMOVED vs v1:
--   - dim_encounter (had measures → moved to fact_encounter)
--   - fact_costs / fact_utilization / fact_cost_daily (merged → fact_encounter_daily)
--   - fact_covid_* (hardcoded disease → use fact_condition_daily + filter)
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

-- ----------------------------------------------------------------------------
-- DIM: Date  (Type 0 — never changes)
-- ----------------------------------------------------------------------------
IF OBJECT_ID(N'dbo.dim_date', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.dim_date (
        date_key            INT         NOT NULL,   -- yyyymmdd, e.g. 20240115
        full_date           DATE        NOT NULL,
        [year]              SMALLINT    NOT NULL,
        [quarter]           TINYINT     NOT NULL,   -- 1..4
        [month]             TINYINT     NOT NULL,   -- 1..12
        month_name          VARCHAR(10) NOT NULL,   -- 'January'
        [week_of_year]      TINYINT     NOT NULL,   -- ISO week
        day_of_month        TINYINT     NOT NULL,
        day_of_week         TINYINT     NOT NULL,   -- 1=Mon (set via DATEFIRST in loader)
        day_name            VARCHAR(10) NOT NULL,
        is_weekend          BIT         NOT NULL,

        -- Healthcare-specific attributes (hữu ích cho reporting)
        fiscal_year         SMALLINT    NULL,       -- nếu org dùng fiscal year khác calendar
        fiscal_quarter      TINYINT     NULL,

        create_at           DATETIME2   NOT NULL CONSTRAINT DF_dim_date_create_at DEFAULT SYSUTCDATETIME(),
        update_at           DATETIME2   NOT NULL CONSTRAINT DF_dim_date_update_at DEFAULT SYSUTCDATETIME(),

        CONSTRAINT PK_dim_date PRIMARY KEY CLUSTERED (date_key),
        CONSTRAINT UQ_dim_date_full_date UNIQUE (full_date)
    );
END;
GO

-- ----------------------------------------------------------------------------
-- DIM: Patient  (SCD Type 2)
-- Track: deathdate, city, state, zip, healthcare_expenses/coverage
-- ----------------------------------------------------------------------------
IF OBJECT_ID(N'dbo.dim_patient', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.dim_patient (
        patient_key             INT             IDENTITY(1,1) NOT NULL,
        patient_id              VARCHAR(36)     NOT NULL,   -- Synthea natural key (UUID)

        -- Descriptive attributes
        first_name              VARCHAR(100)    NULL,
        last_name               VARCHAR(100)    NULL,
        birthdate               DATE            NULL,
        deathdate               DATE            NULL,
        gender                  CHAR(1)         NULL,       -- 'M' / 'F'
        race                    VARCHAR(50)     NULL,
        ethnicity               VARCHAR(50)     NULL,

        -- Location (tracked: patients move)
        city                    VARCHAR(100)    NULL,
        [state]                 VARCHAR(50)     NULL,
        zip                     VARCHAR(10)     NULL,
        county                  VARCHAR(100)    NULL,

        -- Financials (tracked: plan changes)
        healthcare_expenses     DECIMAL(18,2)   NULL,
        healthcare_coverage     DECIMAL(18,2)   NULL,

        -- Computed helper (không track, dùng cho age-band reporting)
        birth_year              AS YEAR(birthdate) PERSISTED,

        -- SCD2 metadata
        valid_from              DATE            NOT NULL,
        valid_to                DATE            NOT NULL,   -- 9999-12-31 nếu current
        is_current              BIT             NOT NULL,
        row_hash                VARBINARY(32)   NOT NULL,   -- HASHBYTES('SHA2_256', concat of tracked cols)

        create_at               DATETIME2       NOT NULL CONSTRAINT DF_dim_patient_create_at DEFAULT SYSUTCDATETIME(),
        update_at               DATETIME2       NOT NULL CONSTRAINT DF_dim_patient_update_at DEFAULT SYSUTCDATETIME(),

        CONSTRAINT PK_dim_patient PRIMARY KEY CLUSTERED (patient_key)
    );

    CREATE INDEX IX_dim_patient_nk_current
        ON dbo.dim_patient (patient_id, is_current)
        INCLUDE (patient_key, valid_from, valid_to);

    CREATE UNIQUE INDEX UX_dim_patient_nk_from
        ON dbo.dim_patient (patient_id, valid_from);
END;
GO

-- ----------------------------------------------------------------------------
-- DIM: Organization  (SCD Type 2)
-- Track: name, revenue, utilization (org có thể đổi tên hoặc merge)
-- ----------------------------------------------------------------------------
IF OBJECT_ID(N'dbo.dim_organization', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.dim_organization (
        organization_key        INT             IDENTITY(1,1) NOT NULL,
        organization_id         VARCHAR(36)     NOT NULL,   -- Synthea natural key

        [name]                  VARCHAR(255)    NULL,
        city                    VARCHAR(100)    NULL,
        [state]                 VARCHAR(50)     NULL,
        zip                     VARCHAR(10)     NULL,
        phone                   VARCHAR(20)     NULL,

        -- Financials (tracked)
        revenue                 DECIMAL(18,2)   NULL,
        utilization             INT             NULL,       -- encounter count từ source

        -- SCD2 metadata
        valid_from              DATE            NOT NULL,
        valid_to                DATE            NOT NULL,
        is_current              BIT             NOT NULL,
        row_hash                VARBINARY(32)   NOT NULL,

        create_at               DATETIME2       NOT NULL CONSTRAINT DF_dim_org_create_at DEFAULT SYSUTCDATETIME(),
        update_at               DATETIME2       NOT NULL CONSTRAINT DF_dim_org_update_at DEFAULT SYSUTCDATETIME(),

        CONSTRAINT PK_dim_organization PRIMARY KEY CLUSTERED (organization_key)
    );

    CREATE INDEX IX_dim_org_nk_current
        ON dbo.dim_organization (organization_id, is_current)
        INCLUDE (organization_key, valid_from, valid_to);

    CREATE UNIQUE INDEX UX_dim_org_nk_from
        ON dbo.dim_organization (organization_id, valid_from);
END;
GO

-- ----------------------------------------------------------------------------
-- DIM: Provider  (SCD Type 2)
-- Track: speciality, organization_id (provider đổi nơi làm việc)
-- ----------------------------------------------------------------------------
IF OBJECT_ID(N'dbo.dim_provider', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.dim_provider (
        provider_key            INT             IDENTITY(1,1) NOT NULL,
        provider_id             VARCHAR(36)     NOT NULL,   -- Synthea natural key

        organization_id         VARCHAR(36)     NULL,       -- NK giữ lại cho lineage
        [name]                  VARCHAR(255)    NULL,
        gender                  CHAR(1)         NULL,
        speciality              VARCHAR(100)    NULL,       -- tracked: provider có thể đổi chuyên khoa
        city                    VARCHAR(100)    NULL,
        [state]                 VARCHAR(50)     NULL,
        zip                     VARCHAR(10)     NULL,
        utilization             INT             NULL,

        -- SCD2 metadata
        valid_from              DATE            NOT NULL,
        valid_to                DATE            NOT NULL,
        is_current              BIT             NOT NULL,
        row_hash                VARBINARY(32)   NOT NULL,

        create_at               DATETIME2       NOT NULL CONSTRAINT DF_dim_provider_create_at DEFAULT SYSUTCDATETIME(),
        update_at               DATETIME2       NOT NULL CONSTRAINT DF_dim_provider_update_at DEFAULT SYSUTCDATETIME(),

        CONSTRAINT PK_dim_provider PRIMARY KEY CLUSTERED (provider_key)
    );

    CREATE INDEX IX_dim_provider_nk_current
        ON dbo.dim_provider (provider_id, is_current)
        INCLUDE (provider_key, valid_from, valid_to);

    CREATE UNIQUE INDEX UX_dim_provider_nk_from
        ON dbo.dim_provider (provider_id, valid_from);
END;
GO

-- ----------------------------------------------------------------------------
-- DIM: Payer  (SCD Type 2)
-- Track: amount_covered, revenue, unique_customers (plan metrics thay đổi)
-- ----------------------------------------------------------------------------
IF OBJECT_ID(N'dbo.dim_payer', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.dim_payer (
        payer_key               INT             IDENTITY(1,1) NOT NULL,
        payer_id                VARCHAR(36)     NOT NULL,   -- Synthea natural key

        [name]                  VARCHAR(255)    NULL,
        city                    VARCHAR(100)    NULL,
        state_headquartered     VARCHAR(50)     NULL,
        zip                     VARCHAR(10)     NULL,
        phone                   VARCHAR(20)     NULL,
        ownership               VARCHAR(50)     NULL,       -- 'Government', 'Private', etc.

        -- Financial metrics (tracked)
        amount_covered          DECIMAL(18,2)   NULL,
        amount_uncovered        DECIMAL(18,2)   NULL,
        revenue                 DECIMAL(18,2)   NULL,
        unique_customers        INT             NULL,
        member_months           INT             NULL,

        -- SCD2 metadata
        valid_from              DATE            NOT NULL,
        valid_to                DATE            NOT NULL,
        is_current              BIT             NOT NULL,
        row_hash                VARBINARY(32)   NOT NULL,

        create_at               DATETIME2       NOT NULL CONSTRAINT DF_dim_payer_create_at DEFAULT SYSUTCDATETIME(),
        update_at               DATETIME2       NOT NULL CONSTRAINT DF_dim_payer_update_at DEFAULT SYSUTCDATETIME(),

        CONSTRAINT PK_dim_payer PRIMARY KEY CLUSTERED (payer_key)
    );

    CREATE INDEX IX_dim_payer_nk_current
        ON dbo.dim_payer (payer_id, is_current)
        INCLUDE (payer_key, valid_from, valid_to);

    CREATE UNIQUE INDEX UX_dim_payer_nk_from
        ON dbo.dim_payer (payer_id, valid_from);
END;
GO

-- ----------------------------------------------------------------------------
-- DIM: Condition Code  (Type 1 — code descriptions không thay đổi nghĩa)
-- Dùng chung cho conditions, medications (reason_code), procedures (reason_code)
-- Có thể extend thêm ICD-10 chapter/category để roll up theo nhóm bệnh
-- ----------------------------------------------------------------------------
IF OBJECT_ID(N'dbo.dim_condition_code', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.dim_condition_code (
        condition_code_key      INT             IDENTITY(1,1) NOT NULL,
        code                    VARCHAR(20)     NOT NULL,   -- SNOMED-CT code

        -- Standard description
        [description]           VARCHAR(500)    NULL,

        -- Roll-up hierarchy cho drill down analysis
        -- (populate từ SNOMED hierarchy hoặc ICD mapping)
        body_system             VARCHAR(100)    NULL,       -- 'Respiratory', 'Cardiovascular', etc.
        condition_category      VARCHAR(100)    NULL,       -- 'Chronic', 'Acute', 'Mental Health'
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

-- ----------------------------------------------------------------------------
-- FACT 1: fact_encounter  (Transaction Fact)
-- Grain  : 1 row per encounter
-- Source : encounters.csv (Synthea)
-- Use    : Chi phí per-encounter, clinical severity, cost drill-down to patient/provider
--
-- NOTE: Replaces dim_encounter (anti-pattern) + generalizes fact_covid_severity
-- ----------------------------------------------------------------------------
IF OBJECT_ID(N'dbo.fact_encounter', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.fact_encounter (
        -- Degenerate dimension (NK từ source, không cần surrogate)
        encounter_id            VARCHAR(36)     NOT NULL,

        -- Dimension FKs
        start_date_key          INT             NOT NULL,
        stop_date_key           INT             NULL,
        patient_key             INT             NOT NULL,
        provider_key            INT             NULL,
        payer_key               INT             NULL,
        organization_key        INT             NULL,

        -- Degenerate dimensions (low cardinality — không tạo dim table riêng)
        encounter_class         VARCHAR(50)     NULL,   -- 'ambulatory','inpatient','emergency','wellness'
        encounter_code          VARCHAR(20)     NULL,   -- SNOMED procedure code
        reason_code             VARCHAR(20)     NULL,   -- link tới dim_condition_code nếu cần
        reason_description      VARCHAR(255)    NULL,

        -- *** MEASURES: Cost (additive) ***
        base_encounter_cost     DECIMAL(18,2)   NOT NULL DEFAULT 0,
        total_claim_cost        DECIMAL(18,2)   NOT NULL DEFAULT 0,
        payer_coverage          DECIMAL(18,2)   NOT NULL DEFAULT 0,
        out_of_pocket           AS (total_claim_cost - payer_coverage) PERSISTED,  -- computed, đừng ETL thủ công

        -- *** MEASURES: Clinical severity (additive/semi-additive) ***
        -- Semi-additive: SUM across encounters ok, AVG across patients cho LOS
        is_hospitalized         BIT             NULL,   -- 1 = inpatient stay
        is_icu                  BIT             NULL,
        length_of_stay_days     INT             NULL,   -- 0 nếu same-day, NULL nếu ambulatory

        -- *** MEASURES: Volume ***
        encounter_count         INT             NOT NULL DEFAULT 1,  -- luôn 1, dùng để SUM COUNT(*) dễ hơn

        load_dts                DATETIME2       NOT NULL CONSTRAINT DF_fact_enc_load_dts DEFAULT SYSUTCDATETIME(),

        CONSTRAINT PK_fact_encounter PRIMARY KEY CLUSTERED (encounter_id)
    );

    -- Index theo chiều phân tích phổ biến nhất
    CREATE INDEX IX_fact_enc_patient_date
        ON dbo.fact_encounter (patient_key, start_date_key)
        INCLUDE (total_claim_cost, payer_coverage, encounter_class);

    CREATE INDEX IX_fact_enc_org_date
        ON dbo.fact_encounter (organization_key, start_date_key)
        INCLUDE (total_claim_cost, encounter_count, encounter_class);

    CREATE INDEX IX_fact_enc_payer_date
        ON dbo.fact_encounter (payer_key, start_date_key)
        INCLUDE (payer_coverage, total_claim_cost);

    CREATE INDEX IX_fact_enc_provider_date
        ON dbo.fact_encounter (provider_key, start_date_key)
        INCLUDE (total_claim_cost, encounter_count);
END;
GO

-- ----------------------------------------------------------------------------
-- FACT 2: fact_encounter_daily  (Periodic Rollup Fact)
-- Grain  : date × organization × payer × encounter_class
-- Source : Aggregated từ fact_encounter
-- Use    : BI dashboard, trend chi phí, drill up/down theo thời gian & tổ chức
--
-- NOTE: Replaces fact_utilization + fact_costs + fact_cost_daily (3 tables → 1)
--       Thêm payer_key (thiếu ở fact_costs cũ), thêm encounter_class để filter
-- ----------------------------------------------------------------------------
IF OBJECT_ID(N'dbo.fact_encounter_daily', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.fact_encounter_daily (
        daily_key               BIGINT          IDENTITY(1,1) NOT NULL,

        -- Dimension FKs (grain keys)
        date_key                INT             NOT NULL,
        organization_key        INT             NULL,
        payer_key               INT             NULL,
        provider_key            INT             NULL,
        encounter_class         VARCHAR(50)     NULL,   -- degenerate dim tại rollup level

        -- *** MEASURES: Volume ***
        encounter_count         INT             NOT NULL DEFAULT 0,
        unique_patient_count    INT             NOT NULL DEFAULT 0,   -- distinct patients trong ngày

        -- *** MEASURES: Cost (fully additive) ***
        total_base_cost         DECIMAL(18,2)   NOT NULL DEFAULT 0,
        total_claim_cost        DECIMAL(18,2)   NOT NULL DEFAULT 0,
        total_payer_coverage    DECIMAL(18,2)   NOT NULL DEFAULT 0,
        total_out_of_pocket     DECIMAL(18,2)   NOT NULL DEFAULT 0,   -- = claim - payer_coverage

        -- *** MEASURES: Clinical severity (additive at daily level) ***
        hospitalized_count      INT             NOT NULL DEFAULT 0,
        icu_count               INT             NOT NULL DEFAULT 0,
        total_los_days          INT             NOT NULL DEFAULT 0,

        load_dts                DATETIME2       NOT NULL CONSTRAINT DF_fact_enc_daily_load_dts DEFAULT SYSUTCDATETIME(),

        CONSTRAINT PK_fact_encounter_daily PRIMARY KEY CLUSTERED (daily_key)
    );

    -- Unique constraint theo grain — dùng để upsert idempotent
    CREATE UNIQUE INDEX UX_fact_enc_daily_grain
        ON dbo.fact_encounter_daily (date_key, organization_key, payer_key, provider_key, encounter_class)
        WHERE organization_key IS NOT NULL
          AND payer_key IS NOT NULL
          AND provider_key IS NOT NULL;

    CREATE INDEX IX_fact_enc_daily_date
        ON dbo.fact_encounter_daily (date_key)
        INCLUDE (total_claim_cost, encounter_count);

    CREATE INDEX IX_fact_enc_daily_org_date
        ON dbo.fact_encounter_daily (organization_key, date_key);
END;
GO

-- ----------------------------------------------------------------------------
-- FACT 3: fact_conditions  (Transaction Fact)
-- Grain  : 1 row per condition onset event (start of a condition per patient)
-- Source : conditions.csv (Synthea)
-- Use    : Phân tích bệnh per-patient, duration, bệnh đồng mắc (comorbidity)
--          Roll up theo condition code → body_system → category
-- ----------------------------------------------------------------------------
IF OBJECT_ID(N'dbo.fact_conditions', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.fact_conditions (
        -- Composite NK làm PK (Synthea không có condition UUID riêng)
        condition_code_key      INT             NOT NULL,
        patient_key             INT             NOT NULL,
        condition_start_date_key INT            NOT NULL,

        -- Additional FKs
        provider_key            INT             NULL,
        organization_key        INT             NULL,
        encounter_id            VARCHAR(36)     NULL,   -- degenerate dim, link tới fact_encounter

        condition_stop_date_key INT             NULL,   -- NULL = còn active

        -- *** MEASURES ***
        duration_days           INT             NULL,   -- NULL nếu còn active, computed khi stop
        condition_count         INT             NOT NULL DEFAULT 1,

        -- Clinical flags (thêm mới — giá trị cho analysis)
        is_active               AS CASE WHEN condition_stop_date_key IS NULL THEN 1 ELSE 0 END PERSISTED,

        load_dts                DATETIME2       NOT NULL CONSTRAINT DF_fact_cond_load_dts DEFAULT SYSUTCDATETIME(),

        CONSTRAINT PK_fact_conditions PRIMARY KEY CLUSTERED (condition_code_key, patient_key, condition_start_date_key)
    );

    CREATE INDEX IX_fact_cond_patient_code
        ON dbo.fact_conditions (patient_key, condition_code_key)
        INCLUDE (condition_start_date_key, is_active, duration_days);

    CREATE INDEX IX_fact_cond_code_date
        ON dbo.fact_conditions (condition_code_key, condition_start_date_key)
        INCLUDE (patient_key, organization_key, is_active);

    CREATE INDEX IX_fact_cond_org_date
        ON dbo.fact_conditions (organization_key, condition_start_date_key);
END;
GO

-- ----------------------------------------------------------------------------
-- FACT 4: fact_condition_daily  (Accumulating Snapshot Fact)
-- Grain  : date × condition_code × organization
-- Source : Aggregated từ fact_conditions (active conditions per day)
-- Use    : Trend bệnh theo thời gian (new/active/resolved per ngày)
--          Đây là table đúng để query COVID-19 stats — chỉ cần filter condition_code
--          Roll up: drill up theo body_system, category trong dim_condition_code
-- ----------------------------------------------------------------------------
IF OBJECT_ID(N'dbo.fact_condition_daily', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.fact_condition_daily (
        daily_condition_key     BIGINT          IDENTITY(1,1) NOT NULL,

        -- Dimension FKs (grain keys)
        date_key                INT             NOT NULL,
        condition_code_key      INT             NOT NULL,
        organization_key        INT             NULL,

        -- *** MEASURES: Disease surveillance (additive) ***
        new_cases               INT             NOT NULL DEFAULT 0,   -- onset trong ngày này
        active_cases            INT             NOT NULL DEFAULT 0,   -- chưa resolved tính đến date
        resolved_cases          INT             NOT NULL DEFAULT 0,   -- stop trong ngày này
        cumulative_cases        INT             NOT NULL DEFAULT 0,   -- total từ đầu đến date

        -- *** MEASURES: Severity enrichment ***
        hospitalized_cases      INT             NOT NULL DEFAULT 0,   -- join với fact_encounter
        icu_cases               INT             NOT NULL DEFAULT 0,

        load_dts                DATETIME2       NOT NULL CONSTRAINT DF_fact_cond_daily_load_dts DEFAULT SYSUTCDATETIME(),

        CONSTRAINT PK_fact_condition_daily PRIMARY KEY CLUSTERED (daily_condition_key)
    );

    -- Unique constraint theo grain
    CREATE UNIQUE INDEX UX_fact_cond_daily_grain
        ON dbo.fact_condition_daily (date_key, condition_code_key, organization_key)
        WHERE organization_key IS NOT NULL;

    CREATE INDEX IX_fact_cond_daily_code_date
        ON dbo.fact_condition_daily (condition_code_key, date_key)
        INCLUDE (new_cases, active_cases, resolved_cases);

    CREATE INDEX IX_fact_cond_daily_date
        ON dbo.fact_condition_daily (date_key);
END;
GO

-- ----------------------------------------------------------------------------
-- FACT 5: fact_medications  (Transaction Fact)  ← CRITICAL
-- Grain  : 1 row per medication dispense event
-- Source : medications.csv (Synthea)
-- Use    : Chi phí thuốc (thường 40–60% total healthcare cost), adherence analysis
--          Drug utilization review, payer coverage per drug class
-- ----------------------------------------------------------------------------
IF OBJECT_ID(N'dbo.fact_medications', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.fact_medications (
        -- Synthea không có medication UUID → dùng composite NK
        patient_key             INT             NOT NULL,
        start_date_key          INT             NOT NULL,
        medication_code         VARCHAR(20)     NOT NULL,   -- RxNorm code (degenerate dim)

        -- Additional FKs
        stop_date_key           INT             NULL,
        provider_key            INT             NULL,
        organization_key        INT             NULL,
        payer_key               INT             NULL,
        encounter_id            VARCHAR(36)     NULL,       -- link tới fact_encounter

        -- Degenerate dims
        medication_description  VARCHAR(255)    NULL,
        reason_code             VARCHAR(20)     NULL,       -- FK logic tới dim_condition_code
        dispense_as_written     BIT             NULL,

        -- *** MEASURES: Cost (additive) ***
        base_cost               DECIMAL(18,2)   NULL DEFAULT 0,
        payer_coverage          DECIMAL(18,2)   NULL DEFAULT 0,
        out_of_pocket           AS (base_cost - payer_coverage) PERSISTED,

        -- *** MEASURES: Duration/adherence ***
        duration_days           INT             NULL,       -- stop - start
        dispense_count          INT             NOT NULL DEFAULT 1,
        is_active               AS CASE WHEN stop_date_key IS NULL THEN 1 ELSE 0 END PERSISTED,

        load_dts                DATETIME2       NOT NULL CONSTRAINT DF_fact_med_load_dts DEFAULT SYSUTCDATETIME(),

        CONSTRAINT PK_fact_medications PRIMARY KEY CLUSTERED (patient_key, start_date_key, medication_code)
    );

    CREATE INDEX IX_fact_med_patient_date
        ON dbo.fact_medications (patient_key, start_date_key)
        INCLUDE (medication_code, base_cost, payer_coverage);

    CREATE INDEX IX_fact_med_code_date
        ON dbo.fact_medications (medication_code, start_date_key)
        INCLUDE (patient_key, organization_key, base_cost);

    CREATE INDEX IX_fact_med_payer
        ON dbo.fact_medications (payer_key, start_date_key)
        INCLUDE (payer_coverage, base_cost);

    CREATE INDEX IX_fact_med_org_date
        ON dbo.fact_medications (organization_key, start_date_key)
        INCLUDE (base_cost, payer_coverage);
END;
GO

-- ============================================================================
-- SECTION 3: FOREIGN KEY CONSTRAINTS
-- ============================================================================

-- *** fact_encounter ***
ALTER TABLE dbo.fact_encounter WITH CHECK
    ADD CONSTRAINT FK_enc_start_date    FOREIGN KEY (start_date_key)    REFERENCES dbo.dim_date(date_key);
ALTER TABLE dbo.fact_encounter WITH CHECK
    ADD CONSTRAINT FK_enc_stop_date     FOREIGN KEY (stop_date_key)     REFERENCES dbo.dim_date(date_key);
ALTER TABLE dbo.fact_encounter WITH CHECK
    ADD CONSTRAINT FK_enc_patient       FOREIGN KEY (patient_key)       REFERENCES dbo.dim_patient(patient_key);
ALTER TABLE dbo.fact_encounter WITH CHECK
    ADD CONSTRAINT FK_enc_provider      FOREIGN KEY (provider_key)      REFERENCES dbo.dim_provider(provider_key);
ALTER TABLE dbo.fact_encounter WITH CHECK
    ADD CONSTRAINT FK_enc_payer         FOREIGN KEY (payer_key)         REFERENCES dbo.dim_payer(payer_key);
ALTER TABLE dbo.fact_encounter WITH CHECK
    ADD CONSTRAINT FK_enc_org           FOREIGN KEY (organization_key)  REFERENCES dbo.dim_organization(organization_key);
GO

-- *** fact_encounter_daily ***
ALTER TABLE dbo.fact_encounter_daily WITH CHECK
    ADD CONSTRAINT FK_enc_daily_date    FOREIGN KEY (date_key)          REFERENCES dbo.dim_date(date_key);
ALTER TABLE dbo.fact_encounter_daily WITH CHECK
    ADD CONSTRAINT FK_enc_daily_org     FOREIGN KEY (organization_key)  REFERENCES dbo.dim_organization(organization_key);
ALTER TABLE dbo.fact_encounter_daily WITH CHECK
    ADD CONSTRAINT FK_enc_daily_payer   FOREIGN KEY (payer_key)         REFERENCES dbo.dim_payer(payer_key);
ALTER TABLE dbo.fact_encounter_daily WITH CHECK
    ADD CONSTRAINT FK_enc_daily_provider FOREIGN KEY (provider_key)     REFERENCES dbo.dim_provider(provider_key);
GO

-- *** fact_conditions ***
ALTER TABLE dbo.fact_conditions WITH CHECK
    ADD CONSTRAINT FK_cond_code         FOREIGN KEY (condition_code_key)        REFERENCES dbo.dim_condition_code(condition_code_key);
ALTER TABLE dbo.fact_conditions WITH CHECK
    ADD CONSTRAINT FK_cond_patient      FOREIGN KEY (patient_key)               REFERENCES dbo.dim_patient(patient_key);
ALTER TABLE dbo.fact_conditions WITH CHECK
    ADD CONSTRAINT FK_cond_provider     FOREIGN KEY (provider_key)              REFERENCES dbo.dim_provider(provider_key);
ALTER TABLE dbo.fact_conditions WITH CHECK
    ADD CONSTRAINT FK_cond_org          FOREIGN KEY (organization_key)          REFERENCES dbo.dim_organization(organization_key);
ALTER TABLE dbo.fact_conditions WITH CHECK
    ADD CONSTRAINT FK_cond_start_date   FOREIGN KEY (condition_start_date_key)  REFERENCES dbo.dim_date(date_key);
ALTER TABLE dbo.fact_conditions WITH CHECK
    ADD CONSTRAINT FK_cond_stop_date    FOREIGN KEY (condition_stop_date_key)   REFERENCES dbo.dim_date(date_key);
GO

-- *** fact_condition_daily ***
ALTER TABLE dbo.fact_condition_daily WITH CHECK
    ADD CONSTRAINT FK_cond_daily_date   FOREIGN KEY (date_key)              REFERENCES dbo.dim_date(date_key);
ALTER TABLE dbo.fact_condition_daily WITH CHECK
    ADD CONSTRAINT FK_cond_daily_code   FOREIGN KEY (condition_code_key)    REFERENCES dbo.dim_condition_code(condition_code_key);
ALTER TABLE dbo.fact_condition_daily WITH CHECK
    ADD CONSTRAINT FK_cond_daily_org    FOREIGN KEY (organization_key)      REFERENCES dbo.dim_organization(organization_key);
GO

-- *** fact_medications ***
ALTER TABLE dbo.fact_medications WITH CHECK
    ADD CONSTRAINT FK_med_patient       FOREIGN KEY (patient_key)       REFERENCES dbo.dim_patient(patient_key);
ALTER TABLE dbo.fact_medications WITH CHECK
    ADD CONSTRAINT FK_med_start_date    FOREIGN KEY (start_date_key)    REFERENCES dbo.dim_date(date_key);
ALTER TABLE dbo.fact_medications WITH CHECK
    ADD CONSTRAINT FK_med_stop_date     FOREIGN KEY (stop_date_key)     REFERENCES dbo.dim_date(date_key);
ALTER TABLE dbo.fact_medications WITH CHECK
    ADD CONSTRAINT FK_med_provider      FOREIGN KEY (provider_key)      REFERENCES dbo.dim_provider(provider_key);
ALTER TABLE dbo.fact_medications WITH CHECK
    ADD CONSTRAINT FK_med_org           FOREIGN KEY (organization_key)  REFERENCES dbo.dim_organization(organization_key);
ALTER TABLE dbo.fact_medications WITH CHECK
    ADD CONSTRAINT FK_med_payer         FOREIGN KEY (payer_key)         REFERENCES dbo.dim_payer(payer_key);
GO

-- ============================================================================
-- SECTION 4: SAMPLE ANALYTICAL QUERIES (reference)
-- ============================================================================

/*
-- [Q1] Chi phí theo payer × tháng (roll up → quý → năm)
SELECT
    d.[year], d.[quarter], d.[month],
    p.[name]                AS payer_name,
    SUM(f.total_claim_cost) AS total_claim,
    SUM(f.total_payer_coverage) AS payer_covered,
    SUM(f.total_out_of_pocket)  AS patient_oop
FROM dbo.fact_encounter_daily f
JOIN dbo.dim_date d          ON d.date_key        = f.date_key
JOIN dbo.dim_payer p         ON p.payer_key       = f.payer_key AND p.is_current = 1
GROUP BY ROLLUP (d.[year], d.[quarter], d.[month], p.[name]);

-- [Q2] Top 10 bệnh theo số ca active (drill down: category → code)
SELECT TOP 10
    cc.condition_category,
    cc.[description],
    SUM(cd.active_cases) AS total_active
FROM dbo.fact_condition_daily cd
JOIN dbo.dim_condition_code cc ON cc.condition_code_key = cd.condition_code_key
JOIN dbo.dim_date d            ON d.date_key = cd.date_key
WHERE d.full_date = CAST(GETDATE() AS DATE)
GROUP BY cc.condition_category, cc.[description]
ORDER BY total_active DESC;

-- [Q3] Chi phí thuốc vs chi phí encounter theo org (cost split)
SELECT
    o.[name]                    AS org_name,
    SUM(e.total_claim_cost)     AS encounter_cost,
    SUM(m.base_cost)            AS medication_cost,
    ROUND(100.0 * SUM(m.base_cost)
          / NULLIF(SUM(e.total_claim_cost) + SUM(m.base_cost), 0), 1) AS med_pct
FROM dbo.dim_organization o
LEFT JOIN dbo.fact_encounter_daily e ON e.organization_key = o.organization_key
LEFT JOIN dbo.fact_medications m     ON m.organization_key = o.organization_key
WHERE o.is_current = 1
GROUP BY o.[name]
ORDER BY med_pct DESC;

-- [Q4] Trend COVID-19 (hoặc bất kỳ bệnh nào — chỉ đổi code)
SELECT
    d.full_date,
    SUM(cd.new_cases)       AS new_cases,
    SUM(cd.active_cases)    AS active_cases,
    SUM(cd.resolved_cases)  AS resolved_cases
FROM dbo.fact_condition_daily cd
JOIN dbo.dim_date d            ON d.date_key          = cd.date_key
JOIN dbo.dim_condition_code cc ON cc.condition_code_key = cd.condition_code_key
WHERE cc.code = '840539006'  -- COVID-19 SNOMED code
GROUP BY d.full_date
ORDER BY d.full_date;
*/

-- ============================================================================
-- END OF SCHEMA
-- ============================================================================

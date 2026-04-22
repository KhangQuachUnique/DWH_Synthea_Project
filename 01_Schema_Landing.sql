-- ============================================================================
-- SYNTHEA LANDING DATABASE SCHEMA
-- ============================================================================
-- Mục đích: Tạo Landing Database để lưu trữ dữ liệu thô từ CSV
-- Dữ kiện: Ngày tạo: 2026-04-15
-- ============================================================================

USE master;
GO

-- Tạo Landing Database nếu chưa tồn tại
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'DW_Synthea_Landing')
BEGIN
    CREATE DATABASE [DW_Synthea_Landing]
END;
GO

USE [DW_Synthea_Landing];
GO

-- ============================================================================
-- 1. BẢNG PATIENTS - 12,352 dòng
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Landing_Patients]'))
BEGIN
    CREATE TABLE [dbo].[Landing_Patients] (
        [Id] VARCHAR(36) NOT NULL,
        [BIRTHDATE] VARCHAR(10),
        [DEATHDATE] VARCHAR(10),
        [SSN] VARCHAR(11),
        [DRIVERS] VARCHAR(50),
        [PASSPORT] VARCHAR(50),
        [PREFIX] VARCHAR(10),
        [FIRST] VARCHAR(100),
        [LAST] VARCHAR(100),
        [SUFFIX] VARCHAR(10),
        [MAIDEN] VARCHAR(100),
        [MARITAL] VARCHAR(20),
        [RACE] VARCHAR(50),
        [ETHNICITY] VARCHAR(50),
        [GENDER] CHAR(1),
        [BIRTHPLACE] VARCHAR(255),
        [ADDRESS] VARCHAR(255),
        [CITY] VARCHAR(100),
        [STATE] VARCHAR(50),
        [COUNTY] VARCHAR(100),
        [ZIP] VARCHAR(10),
        [LAT] VARCHAR(50),
        [LON] VARCHAR(50),
        [HEALTHCARE_EXPENSES] VARCHAR(20),
        [HEALTHCARE_COVERAGE] VARCHAR(20),
        [create_at] DATETIME NOT NULL DEFAULT GETDATE(),
        [update_at] DATETIME NOT NULL DEFAULT GETDATE()
    );
    CREATE CLUSTERED INDEX [IX_Landing_Patients_Id] ON [dbo].[Landing_Patients]([Id]);
END;
GO

-- ============================================================================
-- 2. BẢNG ENCOUNTERS - 321,528 dòng
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Landing_Encounters]'))
BEGIN
    CREATE TABLE [dbo].[Landing_Encounters] (
        [Id] VARCHAR(36),
        [START] VARCHAR(50),
        [STOP] VARCHAR(50),
        [PATIENT] VARCHAR(36),
        [ORGANIZATION] VARCHAR(36),
        [PROVIDER] VARCHAR(36),
        [PAYER] VARCHAR(36),
        [ENCOUNTERCLASS] VARCHAR(50),
        [CODE] VARCHAR(20),
        [DESCRIPTION] VARCHAR(255),
        [BASE_ENCOUNTER_COST] VARCHAR(20),
        [TOTAL_CLAIM_COST] VARCHAR(20),
        [PAYER_COVERAGE] VARCHAR(20),
        [REASONCODE] VARCHAR(20),
        [REASONDESCRIPTION] VARCHAR(255),
        [create_at] DATETIME NOT NULL DEFAULT GETDATE(),
        [update_at] DATETIME NOT NULL DEFAULT GETDATE()
    );
    CREATE CLUSTERED INDEX [IX_Landing_Encounters_Id] ON [dbo].[Landing_Encounters]([Id]);
    CREATE NONCLUSTERED INDEX [IX_Landing_Encounters_Patient] ON [dbo].[Landing_Encounters]([PATIENT]);
END;
GO

-- ============================================================================
-- 3. BẢNG CONDITIONS - 114,544 dòng
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Landing_Conditions]'))
BEGIN
    CREATE TABLE [dbo].[Landing_Conditions] (
        [START] VARCHAR(10),
        [STOP] VARCHAR(10),
        [PATIENT] VARCHAR(36),
        [ENCOUNTER] VARCHAR(36),
        [CODE] VARCHAR(20),
        [DESCRIPTION] VARCHAR(255),
        [create_at] DATETIME NOT NULL DEFAULT GETDATE(),
        [update_at] DATETIME NOT NULL DEFAULT GETDATE()
    );
    CREATE NONCLUSTERED INDEX [IX_Landing_Conditions_Patient] ON [dbo].[Landing_Conditions]([PATIENT]);
END;
GO

-- ============================================================================
-- 4. BẢNG MEDICATIONS - 431,262 dòng (LỚN)
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Landing_Medications]'))
BEGIN
    CREATE TABLE [dbo].[Landing_Medications] (
        [START] VARCHAR(10),
        [STOP] VARCHAR(10),
        [PATIENT] VARCHAR(36),
        [PAYER] VARCHAR(36),
        [ENCOUNTER] VARCHAR(36),
        [CODE] VARCHAR(20),
        [DESCRIPTION] VARCHAR(255),
        [BASE_COST] VARCHAR(20),
        [PAYER_COVERAGE] VARCHAR(20),
        [DISPENSES] VARCHAR(10),
        [TOTALCOST] VARCHAR(20),
        [REASONCODE] VARCHAR(20),
        [REASONDESCRIPTION] VARCHAR(255),
        [create_at] DATETIME NOT NULL DEFAULT GETDATE(),
        [update_at] DATETIME NOT NULL DEFAULT GETDATE()
    );
    CREATE NONCLUSTERED INDEX [IX_Landing_Medications_Patient] ON [dbo].[Landing_Medications]([PATIENT]);
END;
GO

-- ============================================================================
-- 5. BẢNG ORGANIZATIONS - 5,499 dòng
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Landing_Organizations]'))
BEGIN
    CREATE TABLE [dbo].[Landing_Organizations] (
        [Id] VARCHAR(100),
        [NAME] VARCHAR(255),
        [ADDRESS] VARCHAR(255),
        [CITY] VARCHAR(100),
        [STATE] VARCHAR(100),
        [ZIP] VARCHAR(100),
        [LAT] VARCHAR(100),
        [LON] VARCHAR(100),
        [PHONE] VARCHAR(100),
        [REVENUE] VARCHAR(100),
        [UTILIZATION] VARCHAR(100),
        [create_at] DATETIME NOT NULL DEFAULT GETDATE(),
        [update_at] DATETIME NOT NULL DEFAULT GETDATE()
    );
    CREATE CLUSTERED INDEX [IX_Landing_Organizations_Id] ON [dbo].[Landing_Organizations]([Id]);
END;
GO

-- ============================================================================
-- 6. BẢNG PROVIDERS - 31,764 dòng
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Landing_Providers]'))
BEGIN
    CREATE TABLE [dbo].[Landing_Providers] (
        [Id] VARCHAR(100),
        [ORGANIZATION] VARCHAR(100),
        [NAME] VARCHAR(255),
        [GENDER] CHAR(1),
        [SPECIALITY] VARCHAR(100),
        [ADDRESS] VARCHAR(255),
        [CITY] VARCHAR(100),
        [STATE] VARCHAR(50),
        [ZIP] VARCHAR(10),
        [LAT] VARCHAR(50),
        [LON] VARCHAR(50),
        [UTILIZATION] VARCHAR(10),
        [create_at] DATETIME NOT NULL DEFAULT GETDATE(),
        [update_at] DATETIME NOT NULL DEFAULT GETDATE()
    );
    CREATE CLUSTERED INDEX [IX_Landing_Providers_Id] ON [dbo].[Landing_Providers]([Id]);
END;
GO

-- ============================================================================
-- 7. BẢNG PAYERS - 10 dòng
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Landing_Payers]'))
BEGIN
    CREATE TABLE [dbo].[Landing_Payers] (
        [Id] VARCHAR(100),
        [NAME] VARCHAR(255),
        [ADDRESS] VARCHAR(255),
        [CITY] VARCHAR(100),
        [STATE_HEADQUARTERED] VARCHAR(50),
        [ZIP] VARCHAR(100),
        [PHONE] VARCHAR(100),
        [AMOUNT_COVERED] VARCHAR(100),
        [AMOUNT_UNCOVERED] VARCHAR(100),
        [REVENUE] VARCHAR(100),
        [COVERED_ENCOUNTERS] VARCHAR(100),
        [UNCOVERED_ENCOUNTERS] VARCHAR(100),
        [COVERED_MEDICATIONS] VARCHAR(100),
        [UNCOVERED_MEDICATIONS] VARCHAR(100),
        [COVERED_PROCEDURES] VARCHAR(100),
        [UNCOVERED_PROCEDURES] VARCHAR(100),
        [COVERED_IMMUNIZATIONS] VARCHAR(100),
        [UNCOVERED_IMMUNIZATIONS] VARCHAR(100),
        [UNIQUE_CUSTOMERS] VARCHAR(100),
        [QOLS_AVG] VARCHAR(100),
        [MEMBER_MONTHS] VARCHAR(100),
        [create_at] DATETIME NOT NULL DEFAULT GETDATE(),
        [update_at] DATETIME NOT NULL DEFAULT GETDATE()
    );
    CREATE CLUSTERED INDEX [IX_Landing_Payers_Id] ON [dbo].[Landing_Payers]([Id]);
END;
GO

-- ============================================================================
-- 8. BẢNG PAYER_TRANSITIONS - 41,392 dòng
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Landing_Payer_Transitions]'))
BEGIN
    CREATE TABLE [dbo].[Landing_Payer_Transitions] (
        [PATIENT] VARCHAR(100),
        [START_YEAR] VARCHAR(10),
        [END_YEAR] VARCHAR(10),
        [PAYER] VARCHAR(100),
        [OWNERSHIP] VARCHAR(50),
        [create_at] DATETIME NOT NULL DEFAULT GETDATE(),
        [update_at] DATETIME NOT NULL DEFAULT GETDATE()
    );
    CREATE NONCLUSTERED INDEX [IX_Landing_Payer_Transitions_Patient] ON [dbo].[Landing_Payer_Transitions]([PATIENT]);
END;
GO

-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[LoadLog]'))
BEGIN
    CREATE TABLE [dbo].[LoadLog] (
        [LoadLogId] INT IDENTITY(1,1) PRIMARY KEY,
        [TableName] VARCHAR(100),
        [StartTime] DATETIME,
        [EndTime] DATETIME,
        [RowsLoaded] INT,
        [Status] VARCHAR(50), -- 'SUCCESS', 'FAILED', 'PARTIAL'
        [ErrorMessage] VARCHAR(MAX),
        [CreatedDate] DATETIME DEFAULT GETDATE()
    );
END;
GO

-- ============================================================================ 
-- BƯỚC 2: Tạo bảng ETL_Control để lưu watermark/batch cho từng bảng Landing
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ETL_Control]'))
BEGIN
    CREATE TABLE dbo.ETL_Control (
        ControlId       INT IDENTITY(1,1) PRIMARY KEY,
        TableName       VARCHAR(100)  NOT NULL,
        LastBatchId     VARCHAR(36)   NULL,
        LastLoadedAt    DATETIME      NOT NULL DEFAULT GETDATE(),
        RowsLoaded      INT           NOT NULL DEFAULT 0,
        Status          VARCHAR(20)   NOT NULL DEFAULT 'SUCCESS', -- SUCCESS/FAILED
        CONSTRAINT UQ_ETL_Control_Table UNIQUE (TableName)
    );
END;
GO

-- ============================================================================
-- Thêm cột batch_id vào các bảng Landing đang dùng (nếu chưa có)
-- ============================================================================
IF OBJECT_ID(N'dbo.Landing_Patients', N'U') IS NOT NULL
   AND COL_LENGTH('dbo.Landing_Patients', 'batch_id') IS NULL
    ALTER TABLE dbo.Landing_Patients ADD batch_id VARCHAR(36) NULL;

IF OBJECT_ID(N'dbo.Landing_Encounters', N'U') IS NOT NULL
   AND COL_LENGTH('dbo.Landing_Encounters', 'batch_id') IS NULL
    ALTER TABLE dbo.Landing_Encounters ADD batch_id VARCHAR(36) NULL;

IF OBJECT_ID(N'dbo.Landing_Conditions', N'U') IS NOT NULL
   AND COL_LENGTH('dbo.Landing_Conditions', 'batch_id') IS NULL
    ALTER TABLE dbo.Landing_Conditions ADD batch_id VARCHAR(36) NULL;

IF OBJECT_ID(N'dbo.Landing_Medications', N'U') IS NOT NULL
   AND COL_LENGTH('dbo.Landing_Medications', 'batch_id') IS NULL
    ALTER TABLE dbo.Landing_Medications ADD batch_id VARCHAR(36) NULL;

IF OBJECT_ID(N'dbo.Landing_Organizations', N'U') IS NOT NULL
   AND COL_LENGTH('dbo.Landing_Organizations', 'batch_id') IS NULL
    ALTER TABLE dbo.Landing_Organizations ADD batch_id VARCHAR(36) NULL;

IF OBJECT_ID(N'dbo.Landing_Providers', N'U') IS NOT NULL
   AND COL_LENGTH('dbo.Landing_Providers', 'batch_id') IS NULL
    ALTER TABLE dbo.Landing_Providers ADD batch_id VARCHAR(36) NULL;

IF OBJECT_ID(N'dbo.Landing_Payers', N'U') IS NOT NULL
   AND COL_LENGTH('dbo.Landing_Payers', 'batch_id') IS NULL
    ALTER TABLE dbo.Landing_Payers ADD batch_id VARCHAR(36) NULL;

IF OBJECT_ID(N'dbo.Landing_Payer_Transitions', N'U') IS NOT NULL
   AND COL_LENGTH('dbo.Landing_Payer_Transitions', 'batch_id') IS NULL
    ALTER TABLE dbo.Landing_Payer_Transitions ADD batch_id VARCHAR(36) NULL;
GO

-- ============================================================================ 
-- BƯỚC 3: Khởi tạo 8 dòng cho 8 bảng Landing vào ETL_Control (nếu chưa có)
-- ============================================================================
INSERT INTO dbo.ETL_Control (TableName, LastLoadedAt, RowsLoaded)
SELECT src.TableName, CAST('1900-01-01' AS DATETIME), 0
FROM (
    VALUES ('Landing_Patients'),
           ('Landing_Encounters'),
           ('Landing_Conditions'),
           ('Landing_Medications'),
           ('Landing_Organizations'),
           ('Landing_Providers'),
           ('Landing_Payers'),
           ('Landing_Payer_Transitions')
) AS src(TableName)
WHERE NOT EXISTS (
    SELECT 1
    FROM dbo.ETL_Control ctl
    WHERE ctl.TableName = src.TableName
);
GO

PRINT 'Landing Database Schema Created Successfully!';

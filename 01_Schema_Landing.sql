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
    COLLATE SQL_Latin1_General_CP1_CI_AS;
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
        [Id] NVARCHAR(36) NOT NULL,
        [BIRTHDATE] NVARCHAR(10),
        [DEATHDATE] NVARCHAR(10),
        [SSN] NVARCHAR(11),
        [DRIVERS] NVARCHAR(50),
        [PASSPORT] NVARCHAR(50),
        [PREFIX] NVARCHAR(10),
        [FIRST] NVARCHAR(100),
        [LAST] NVARCHAR(100),
        [SUFFIX] NVARCHAR(10),
        [MAIDEN] NVARCHAR(100),
        [MARITAL] NVARCHAR(20),
        [RACE] NVARCHAR(50),
        [ETHNICITY] NVARCHAR(50),
        [GENDER] CHAR(1),
        [BIRTHPLACE] NVARCHAR(255),
        [ADDRESS] NVARCHAR(255),
        [CITY] NVARCHAR(100),
        [STATE] NVARCHAR(50),
        [COUNTY] NVARCHAR(100),
        [ZIP] NVARCHAR(10),
        [LAT] NVARCHAR(50),
        [LON] NVARCHAR(50),
        [HEALTHCARE_EXPENSES] NVARCHAR(20),
        [HEALTHCARE_COVERAGE] NVARCHAR(20),
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
        [Id] NVARCHAR(36),
        [START] NVARCHAR(50),
        [STOP] NVARCHAR(50),
        [PATIENT] NVARCHAR(36),
        [ORGANIZATION] NVARCHAR(36),
        [PROVIDER] NVARCHAR(36),
        [PAYER] NVARCHAR(36),
        [ENCOUNTERCLASS] NVARCHAR(50),
        [CODE] NVARCHAR(20),
        [DESCRIPTION] NVARCHAR(255),
        [BASE_ENCOUNTER_COST] NVARCHAR(20),
        [TOTAL_CLAIM_COST] NVARCHAR(20),
        [PAYER_COVERAGE] NVARCHAR(20),
        [REASONCODE] NVARCHAR(20),
        [REASONDESCRIPTION] NVARCHAR(255),
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
        [START] NVARCHAR(10),
        [STOP] NVARCHAR(10),
        [PATIENT] NVARCHAR(36),
        [ENCOUNTER] NVARCHAR(36),
        [CODE] NVARCHAR(20),
        [DESCRIPTION] NVARCHAR(255),
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
        [START] NVARCHAR(10),
        [STOP] NVARCHAR(10),
        [PATIENT] NVARCHAR(36),
        [PAYER] NVARCHAR(36),
        [ENCOUNTER] NVARCHAR(36),
        [CODE] NVARCHAR(20),
        [DESCRIPTION] NVARCHAR(255),
        [BASE_COST] NVARCHAR(20),
        [PAYER_COVERAGE] NVARCHAR(20),
        [DISPENSES] NVARCHAR(10),
        [TOTALCOST] NVARCHAR(20),
        [REASONCODE] NVARCHAR(20),
        [REASONDESCRIPTION] NVARCHAR(255),
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
        [Id] NVARCHAR(100),
        [NAME] NVARCHAR(255),
        [ADDRESS] NVARCHAR(255),
        [CITY] NVARCHAR(100),
        [STATE] NVARCHAR(100),
        [ZIP] NVARCHAR(100),
        [LAT] NVARCHAR(100),
        [LON] NVARCHAR(100),
        [PHONE] NVARCHAR(100),
        [REVENUE] NVARCHAR(100),
        [UTILIZATION] NVARCHAR(100),
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
        [Id] NVARCHAR(100),
        [ORGANIZATION] NVARCHAR(100),
        [NAME] NVARCHAR(255),
        [GENDER] CHAR(1),
        [SPECIALITY] NVARCHAR(100),
        [ADDRESS] NVARCHAR(255),
        [CITY] NVARCHAR(100),
        [STATE] NVARCHAR(50),
        [ZIP] NVARCHAR(10),
        [LAT] NVARCHAR(50),
        [LON] NVARCHAR(50),
        [UTILIZATION] NVARCHAR(10),
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
        [Id] NVARCHAR(100),
        [NAME] NVARCHAR(255),
        [ADDRESS] NVARCHAR(255),
        [CITY] NVARCHAR(100),
        [STATE_HEADQUARTERED] NVARCHAR(50),
        [ZIP] NVARCHAR(100),
        [PHONE] NVARCHAR(100),
        [AMOUNT_COVERED] NVARCHAR(100),
        [AMOUNT_UNCOVERED] NVARCHAR(100),
        [REVENUE] NVARCHAR(100),
        [COVERED_ENCOUNTERS] NVARCHAR(100),
        [UNCOVERED_ENCOUNTERS] NVARCHAR(100),
        [COVERED_MEDICATIONS] NVARCHAR(100),
        [UNCOVERED_MEDICATIONS] NVARCHAR(100),
        [COVERED_PROCEDURES] NVARCHAR(100),
        [UNCOVERED_PROCEDURES] NVARCHAR(100),
        [COVERED_IMMUNIZATIONS] NVARCHAR(100),
        [UNCOVERED_IMMUNIZATIONS] NVARCHAR(100),
        [UNIQUE_CUSTOMERS] NVARCHAR(100),
        [QOLS_AVG] NVARCHAR(100),
        [MEMBER_MONTHS] NVARCHAR(100),
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
        [PATIENT] NVARCHAR(100),
        [START_YEAR] NVARCHAR(10),
        [END_YEAR] NVARCHAR(10),
        [PAYER] NVARCHAR(100),
        [OWNERSHIP] NVARCHAR(50),
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
        [TableName] NVARCHAR(100),
        [StartTime] DATETIME,
        [EndTime] DATETIME,
        [RowsLoaded] INT,
        [Status] NVARCHAR(50), -- 'SUCCESS', 'FAILED', 'PARTIAL'
        [ErrorMessage] NVARCHAR(MAX),
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
        TableName       NVARCHAR(100)  NOT NULL,
        LastBatchId     NVARCHAR(36)   NULL,
        LastLoadedAt    DATETIME      NOT NULL DEFAULT GETDATE(),
        RowsLoaded      INT           NOT NULL DEFAULT 0,
        Status          NVARCHAR(20)   NOT NULL DEFAULT 'SUCCESS', -- SUCCESS/FAILED
        CONSTRAINT UQ_ETL_Control_Table UNIQUE (TableName)
    );
END;
GO

-- ============================================================================
-- Thêm cột batch_id vào các bảng Landing đang dùng (nếu chưa có)
-- ============================================================================
IF OBJECT_ID(N'dbo.Landing_Patients', N'U') IS NOT NULL
   AND COL_LENGTH('dbo.Landing_Patients', 'batch_id') IS NULL
    ALTER TABLE dbo.Landing_Patients ADD batch_id NVARCHAR(36) NULL;

IF OBJECT_ID(N'dbo.Landing_Encounters', N'U') IS NOT NULL
   AND COL_LENGTH('dbo.Landing_Encounters', 'batch_id') IS NULL
    ALTER TABLE dbo.Landing_Encounters ADD batch_id NVARCHAR(36) NULL;

IF OBJECT_ID(N'dbo.Landing_Conditions', N'U') IS NOT NULL
   AND COL_LENGTH('dbo.Landing_Conditions', 'batch_id') IS NULL
    ALTER TABLE dbo.Landing_Conditions ADD batch_id NVARCHAR(36) NULL;

IF OBJECT_ID(N'dbo.Landing_Medications', N'U') IS NOT NULL
   AND COL_LENGTH('dbo.Landing_Medications', 'batch_id') IS NULL
    ALTER TABLE dbo.Landing_Medications ADD batch_id NVARCHAR(36) NULL;

IF OBJECT_ID(N'dbo.Landing_Organizations', N'U') IS NOT NULL
   AND COL_LENGTH('dbo.Landing_Organizations', 'batch_id') IS NULL
    ALTER TABLE dbo.Landing_Organizations ADD batch_id NVARCHAR(36) NULL;

IF OBJECT_ID(N'dbo.Landing_Providers', N'U') IS NOT NULL
   AND COL_LENGTH('dbo.Landing_Providers', 'batch_id') IS NULL
    ALTER TABLE dbo.Landing_Providers ADD batch_id NVARCHAR(36) NULL;

IF OBJECT_ID(N'dbo.Landing_Payers', N'U') IS NOT NULL
   AND COL_LENGTH('dbo.Landing_Payers', 'batch_id') IS NULL
    ALTER TABLE dbo.Landing_Payers ADD batch_id NVARCHAR(36) NULL;

IF OBJECT_ID(N'dbo.Landing_Payer_Transitions', N'U') IS NOT NULL
   AND COL_LENGTH('dbo.Landing_Payer_Transitions', 'batch_id') IS NULL
    ALTER TABLE dbo.Landing_Payer_Transitions ADD batch_id NVARCHAR(36) NULL;
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

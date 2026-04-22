-- ============================================================================
-- SYNTHEA STAGING DATABASE SCHEMA
-- ============================================================================
-- Mục đích: Tạo Staging Database để lưu dữ liệu đã clean + đúng kiểu dữ liệu
-- Dữ kiện: Ngày tạo: 2026-04-15
-- ============================================================================

USE master;
GO

-- Tạo Staging Database nếu chưa tồn tại
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'DW_Synthea_Staging')
BEGIN
    CREATE DATABASE [DW_Synthea_Staging]
    COLLATE SQL_Latin1_General_CP1_CI_AS;
END;
GO

USE [DW_Synthea_Staging];
GO

-- ============================================================================
-- 1. STAGING_PATIENTS
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Staging_Patients]'))
BEGIN
    CREATE TABLE [dbo].[Staging_Patients] (
        [Id] NVARCHAR(100) NOT NULL,
        [BIRTHDATE] DATE NULL,
        [DEATHDATE] DATE NULL,
        [SSN] NVARCHAR(11) NULL,
        [DRIVERS] NVARCHAR(50) NULL,
        [PASSPORT] NVARCHAR(50) NULL,
        [PREFIX] NVARCHAR(10) NULL,
        [FIRST] NVARCHAR(100) NULL,
        [LAST] NVARCHAR(100) NULL,
        [SUFFIX] NVARCHAR(10) NULL,
        [MAIDEN] NVARCHAR(100) NULL,
        [MARITAL] NVARCHAR(20) NULL,
        [RACE] NVARCHAR(50) NULL,
        [ETHNICITY] NVARCHAR(50) NULL,
        [GENDER] CHAR(1) NULL,
        [BIRTHPLACE] NVARCHAR(255) NULL,
        [ADDRESS] NVARCHAR(255) NULL,
        [CITY] NVARCHAR(100) NULL,
        [STATE] NVARCHAR(100) NULL,
        [COUNTY] NVARCHAR(100) NULL,
        [ZIP] NVARCHAR(100) NULL,
        [LAT] DECIMAL(18,2) NULL,
        [LON] DECIMAL(18,2) NULL,
        [HEALTHCARE_EXPENSES] DECIMAL(18,2) NULL,
        [HEALTHCARE_COVERAGE] DECIMAL(18,2) NULL,
        [create_at] DATETIME NOT NULL DEFAULT GETDATE(),
        [update_at] DATETIME NOT NULL DEFAULT GETDATE()
    );
    CREATE CLUSTERED INDEX [IX_Staging_Patients_Id] ON [dbo].[Staging_Patients]([Id]);
END;
GO

-- ============================================================================
-- 2. STAGING_ENCOUNTERS
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Staging_Encounters]'))
BEGIN
    CREATE TABLE [dbo].[Staging_Encounters] (
        [Id] NVARCHAR(36) NULL,
        [START] DATETIME NULL,
        [STOP] DATETIME NULL,
        [PATIENT] NVARCHAR(36) NULL,
        [ORGANIZATION] NVARCHAR(36) NULL,
        [PROVIDER] NVARCHAR(36) NULL,
        [PAYER] NVARCHAR(36) NULL,
        [ENCOUNTERCLASS] NVARCHAR(50) NULL,
        [CODE] NVARCHAR(20) NULL,
        [DESCRIPTION] NVARCHAR(255) NULL,
        [BASE_ENCOUNTER_COST] DECIMAL(18,2) NULL,
        [TOTAL_CLAIM_COST] DECIMAL(18,2) NULL,
        [PAYER_COVERAGE] DECIMAL(18,2) NULL,
        [REASONCODE] NVARCHAR(20) NULL,
        [REASONDESCRIPTION] NVARCHAR(255) NULL,
        [create_at] DATETIME NOT NULL DEFAULT GETDATE(),
        [update_at] DATETIME NOT NULL DEFAULT GETDATE()
    );
    CREATE NONCLUSTERED INDEX [IX_Staging_Encounters_Patient] ON [dbo].[Staging_Encounters]([PATIENT]);
END;
GO

-- ============================================================================
-- 3. STAGING_CONDITIONS
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Staging_Conditions]'))
BEGIN
    CREATE TABLE [dbo].[Staging_Conditions] (
        [START] DATE NULL,
        [STOP] DATE NULL,
        [PATIENT] NVARCHAR(36) NULL,
        [ENCOUNTER] NVARCHAR(36) NULL,
        [CODE] NVARCHAR(20) NULL,
        [DESCRIPTION] NVARCHAR(255) NULL,
        [create_at] DATETIME NOT NULL DEFAULT GETDATE(),
        [update_at] DATETIME NOT NULL DEFAULT GETDATE()
    );
    CREATE NONCLUSTERED INDEX [IX_Staging_Conditions_Patient] ON [dbo].[Staging_Conditions]([PATIENT]);
END;
GO

-- ============================================================================
-- 4. STAGING_MEDICATIONS
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Staging_Medications]'))
BEGIN
    CREATE TABLE [dbo].[Staging_Medications] (
        [START] DATE NULL,
        [STOP] DATE NULL,
        [PATIENT] NVARCHAR(36) NULL,
        [PAYER] NVARCHAR(36) NULL,
        [ENCOUNTER] NVARCHAR(36) NULL,
        [CODE] NVARCHAR(20) NULL,
        [DESCRIPTION] NVARCHAR(255) NULL,
        [BASE_COST] DECIMAL(18,2) NULL,
        [PAYER_COVERAGE] DECIMAL(18,2) NULL,
        [DISPENSES] INT NULL,
        [TOTALCOST] DECIMAL(18,2) NULL,
        [REASONCODE] NVARCHAR(20) NULL,
        [REASONDESCRIPTION] NVARCHAR(255) NULL,
        [create_at] DATETIME NOT NULL DEFAULT GETDATE(),
        [update_at] DATETIME NOT NULL DEFAULT GETDATE()
    );
    CREATE NONCLUSTERED INDEX [IX_Staging_Medications_Patient] ON [dbo].[Staging_Medications]([PATIENT]);
END;
GO

-- ============================================================================
-- 5. STAGING_ORGANIZATIONS
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Staging_Organizations]'))
BEGIN
    CREATE TABLE [dbo].[Staging_Organizations] (
        [Id] NVARCHAR(100) NULL,
        [NAME] NVARCHAR(255) NULL,
        [ADDRESS] NVARCHAR(255) NULL,
        [CITY] NVARCHAR(100) NULL,
        [STATE] NVARCHAR(100) NULL,
        [ZIP] NVARCHAR(100) NULL,
        [LAT] DECIMAL(18,10) NULL,
        [LON] DECIMAL(18,10) NULL,
        [PHONE] NVARCHAR(100) NULL,
        [REVENUE] DECIMAL(18,2) NULL,
        [UTILIZATION] INT NULL,
        [create_at] DATETIME NOT NULL DEFAULT GETDATE(),
        [update_at] DATETIME NOT NULL DEFAULT GETDATE()
    );
    CREATE NONCLUSTERED INDEX [IX_Staging_Organizations_Id] ON [dbo].[Staging_Organizations]([Id]);
END;
GO

-- ============================================================================
-- 6. STAGING_PROVIDERS
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Staging_Providers]'))
BEGIN
    CREATE TABLE [dbo].[Staging_Providers] (
        [Id] NVARCHAR(100) NULL,
        [ORGANIZATION] NVARCHAR(100) NULL,
        [NAME] NVARCHAR(255) NULL,
        [GENDER] CHAR(1) NULL,
        [SPECIALITY] NVARCHAR(100) NULL,
        [ADDRESS] NVARCHAR(255) NULL,
        [CITY] NVARCHAR(100) NULL,
        [STATE] NVARCHAR(100) NULL,
        [ZIP] NVARCHAR(100) NULL,
        [LAT] DECIMAL(18,10) NULL,
        [LON] DECIMAL(18,10) NULL,
        [UTILIZATION] INT NULL,
        [create_at] DATETIME NOT NULL DEFAULT GETDATE(),
        [update_at] DATETIME NOT NULL DEFAULT GETDATE()
    );
    CREATE NONCLUSTERED INDEX [IX_Staging_Providers_Id] ON [dbo].[Staging_Providers]([Id]);
END;
GO

-- ============================================================================
-- 7. STAGING_PAYERS
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Staging_Payers]'))
BEGIN
    CREATE TABLE [dbo].[Staging_Payers] (
        [Id] NVARCHAR(100) NULL,
        [NAME] NVARCHAR(255) NULL,
        [ADDRESS] NVARCHAR(255) NULL,
        [CITY] NVARCHAR(100) NULL,
        [STATE_HEADQUARTERED] NVARCHAR(50) NULL,
        [ZIP] NVARCHAR(100) NULL,
        [PHONE] NVARCHAR(100) NULL,
        [AMOUNT_COVERED] DECIMAL(18,2) NULL,
        [AMOUNT_UNCOVERED] DECIMAL(18,2) NULL,
        [REVENUE] DECIMAL(18,2) NULL,
        [COVERED_ENCOUNTERS] INT NULL,
        [UNCOVERED_ENCOUNTERS] INT NULL,
        [COVERED_MEDICATIONS] INT NULL,
        [UNCOVERED_MEDICATIONS] INT NULL,
        [COVERED_PROCEDURES] INT NULL,
        [UNCOVERED_PROCEDURES] INT NULL,
        [COVERED_IMMUNIZATIONS] INT NULL,
        [UNCOVERED_IMMUNIZATIONS] INT NULL,
        [UNIQUE_CUSTOMERS] INT NULL,
        [QOLS_AVG] DECIMAL(18,4) NULL,
        [MEMBER_MONTHS] INT NULL,
        [create_at] DATETIME NOT NULL DEFAULT GETDATE(),
        [update_at] DATETIME NOT NULL DEFAULT GETDATE()
    );
    CREATE NONCLUSTERED INDEX [IX_Staging_Payers_Id] ON [dbo].[Staging_Payers]([Id]);
END;
GO

-- ============================================================================
-- 8. STAGING_PAYER_TRANSITIONS
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Staging_Payer_Transitions]'))
BEGIN
    CREATE TABLE [dbo].[Staging_Payer_Transitions] (
        [PATIENT] NVARCHAR(100) NULL,
        [START_YEAR] INT NULL,
        [END_YEAR] INT NULL,
        [PAYER] NVARCHAR(100) NULL,
        [OWNERSHIP] NVARCHAR(50) NULL,
        [create_at] DATETIME NOT NULL DEFAULT GETDATE(),
        [update_at] DATETIME NOT NULL DEFAULT GETDATE()
    );
    CREATE NONCLUSTERED INDEX [IX_Staging_Payer_Transitions_Patient] ON [dbo].[Staging_Payer_Transitions]([PATIENT]);
END;
GO

PRINT 'Staging Database Schema Created Successfully!';

IF OBJECT_ID(N'dbo.Staging_Patients', N'U') IS NOT NULL
   AND COL_LENGTH('dbo.Staging_Patients', 'batch_id') IS NULL
    ALTER TABLE dbo.Staging_Patients ADD batch_id NVARCHAR(36) NULL;

IF OBJECT_ID(N'dbo.Staging_Encounters', N'U') IS NOT NULL
   AND COL_LENGTH('dbo.Staging_Encounters', 'batch_id') IS NULL
    ALTER TABLE dbo.Staging_Encounters ADD batch_id NVARCHAR(36) NULL;

IF OBJECT_ID(N'dbo.Staging_Conditions', N'U') IS NOT NULL   
   AND COL_LENGTH('dbo.Staging_Conditions', 'batch_id') IS NULL
    ALTER TABLE dbo.Staging_Conditions ADD batch_id NVARCHAR(36) NULL;

IF OBJECT_ID(N'dbo.Staging_Medications', N'U') IS NOT NULL
   AND COL_LENGTH('dbo.Staging_Medications', 'batch_id') IS NULL
    ALTER TABLE dbo.Staging_Medications ADD batch_id NVARCHAR(36) NULL;

IF OBJECT_ID(N'dbo.Staging_Organizations', N'U') IS NOT NULL
   AND COL_LENGTH('dbo.Staging_Organizations', 'batch_id') IS NULL
    ALTER TABLE dbo.Staging_Organizations ADD batch_id NVARCHAR(36) NULL;

IF OBJECT_ID(N'dbo.Staging_Providers', N'U') IS NOT NULL
   AND COL_LENGTH('dbo.Staging_Providers', 'batch_id') IS NULL
    ALTER TABLE dbo.Staging_Providers ADD batch_id NVARCHAR(36) NULL;

IF OBJECT_ID(N'dbo.Staging_Payers', N'U') IS NOT NULL
   AND COL_LENGTH('dbo.Staging_Payers', 'batch_id') IS NULL
    ALTER TABLE dbo.Staging_Payers ADD batch_id NVARCHAR(36) NULL;

IF OBJECT_ID(N'dbo.Staging_Payer_Transitions', N'U') IS NOT NULL
   AND COL_LENGTH('dbo.Staging_Payer_Transitions', 'batch_id') IS NULL
    ALTER TABLE dbo.Staging_Payer_Transitions ADD batch_id NVARCHAR(36) NULL;
GO
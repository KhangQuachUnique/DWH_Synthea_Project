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
-- 5. BẢNG OBSERVATIONS - 1,659,750 dòng (RẤT LỚN - 58% dữ liệu)
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Landing_Observations]'))
BEGIN
    CREATE TABLE [dbo].[Landing_Observations] (
        [DATE] VARCHAR(10),
        [PATIENT] VARCHAR(36),
        [ENCOUNTER] VARCHAR(36),
        [CODE] VARCHAR(20),
        [DESCRIPTION] VARCHAR(255),
        [VALUE] VARCHAR(255),
        [UNITS] VARCHAR(20),
        [TYPE] VARCHAR(50),
        [create_at] DATETIME NOT NULL DEFAULT GETDATE(),
        [update_at] DATETIME NOT NULL DEFAULT GETDATE()
    );
    CREATE NONCLUSTERED INDEX [IX_Landing_Observations_Patient] ON [dbo].[Landing_Observations]([PATIENT]);
    CREATE NONCLUSTERED INDEX [IX_Landing_Observations_Date] ON [dbo].[Landing_Observations]([DATE]);
END;
GO

-- ============================================================================
-- 6. BẢNG PROCEDURES - 100,427 dòng
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Landing_Procedures]'))
BEGIN
    CREATE TABLE [dbo].[Landing_Procedures] (
        [DATE] VARCHAR(10),
        [PATIENT] VARCHAR(36),
        [ENCOUNTER] VARCHAR(36),
        [CODE] VARCHAR(20),
        [DESCRIPTION] VARCHAR(255),
        [BASE_COST] VARCHAR(20),
        [REASONCODE] VARCHAR(20),
        [REASONDESCRIPTION] VARCHAR(255),
        [create_at] DATETIME NOT NULL DEFAULT GETDATE(),
        [update_at] DATETIME NOT NULL DEFAULT GETDATE()
    );
    CREATE NONCLUSTERED INDEX [IX_Landing_Procedures_Patient] ON [dbo].[Landing_Procedures]([PATIENT]);
END;
GO

-- ============================================================================
-- 7. BẢNG IMMUNIZATIONS - 16,481 dòng
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Landing_Immunizations]'))
BEGIN
    CREATE TABLE [dbo].[Landing_Immunizations] (
        [DATE] VARCHAR(10),
        [PATIENT] VARCHAR(36),
        [ENCOUNTER] VARCHAR(36),
        [CODE] VARCHAR(20),
        [DESCRIPTION] VARCHAR(255),
        [BASE_COST] VARCHAR(20),
        [create_at] DATETIME NOT NULL DEFAULT GETDATE(),
        [update_at] DATETIME NOT NULL DEFAULT GETDATE()
    );
    CREATE NONCLUSTERED INDEX [IX_Landing_Immunizations_Patient] ON [dbo].[Landing_Immunizations]([PATIENT]);
END;
GO

-- ============================================================================
-- 8. BẢNG ALLERGIES - 5,417 dòng
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Landing_Allergies]'))
BEGIN
    CREATE TABLE [dbo].[Landing_Allergies] (
        [START] VARCHAR(10),
        [STOP] VARCHAR(10),
        [PATIENT] VARCHAR(36),
        [ENCOUNTER] VARCHAR(36),
        [CODE] VARCHAR(20),
        [DESCRIPTION] VARCHAR(255),
        [create_at] DATETIME NOT NULL DEFAULT GETDATE(),
        [update_at] DATETIME NOT NULL DEFAULT GETDATE()
    );
    CREATE NONCLUSTERED INDEX [IX_Landing_Allergies_Patient] ON [dbo].[Landing_Allergies]([PATIENT]);
END;
GO

-- ============================================================================
-- 9. BẢNG CAREPLANS - 37,715 dòng
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Landing_Careplans]'))
BEGIN
    CREATE TABLE [dbo].[Landing_Careplans] (
        [Id] VARCHAR(36),
        [START] VARCHAR(10),
        [STOP] VARCHAR(10),
        [PATIENT] VARCHAR(36),
        [ENCOUNTER] VARCHAR(36),
        [CODE] VARCHAR(20),
        [DESCRIPTION] VARCHAR(255),
        [REASONCODE] VARCHAR(20),
        [REASONDESCRIPTION] VARCHAR(255),
        [create_at] DATETIME NOT NULL DEFAULT GETDATE(),
        [update_at] DATETIME NOT NULL DEFAULT GETDATE()
    );
    CREATE NONCLUSTERED INDEX [IX_Landing_Careplans_Patient] ON [dbo].[Landing_Careplans]([PATIENT]);
END;
GO

-- ============================================================================
-- 10. BẢNG DEVICES - 2,360 dòng
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Landing_Devices]'))
BEGIN
    CREATE TABLE [dbo].[Landing_Devices] (
        [START] VARCHAR(10),
        [STOP] VARCHAR(10),
        [PATIENT] VARCHAR(36),
        [ENCOUNTER] VARCHAR(36),
        [CODE] VARCHAR(20),
        [DESCRIPTION] VARCHAR(255),
        [UDI] VARCHAR(500),
        [create_at] DATETIME NOT NULL DEFAULT GETDATE(),
        [update_at] DATETIME NOT NULL DEFAULT GETDATE()
    );
    CREATE NONCLUSTERED INDEX [IX_Landing_Devices_Patient] ON [dbo].[Landing_Devices]([PATIENT]);
END;
GO

-- ============================================================================
-- 11. BẢNG IMAGING_STUDIES - 4,504 dòng
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Landing_Imaging_Studies]'))
BEGIN
    CREATE TABLE [dbo].[Landing_Imaging_Studies] (
        [Id] VARCHAR(36),
        [DATE] VARCHAR(10),
        [PATIENT] VARCHAR(36),
        [ENCOUNTER] VARCHAR(36),
        [BODYSITE_CODE] VARCHAR(20),
        [BODYSITE_DESCRIPTION] VARCHAR(255),
        [MODALITY_CODE] VARCHAR(5),
        [MODALITY_DESCRIPTION] VARCHAR(50),
        [SOP_CODE] VARCHAR(64),
        [SOP_DESCRIPTION] VARCHAR(255),
        [create_at] DATETIME NOT NULL DEFAULT GETDATE(),
        [update_at] DATETIME NOT NULL DEFAULT GETDATE()
    );
    CREATE NONCLUSTERED INDEX [IX_Landing_Imaging_Studies_Patient] ON [dbo].[Landing_Imaging_Studies]([PATIENT]);
END;
GO

-- ============================================================================
-- 12. BẢNG SUPPLIES - 143,110 dòng
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Landing_Supplies]'))
BEGIN
    CREATE TABLE [dbo].[Landing_Supplies] (
        [DATE] VARCHAR(10),
        [PATIENT] VARCHAR(36),
        [ENCOUNTER] VARCHAR(36),
        [CODE] VARCHAR(20),
        [DESCRIPTION] VARCHAR(255),
        [QUANTITY] VARCHAR(10),
        [create_at] DATETIME NOT NULL DEFAULT GETDATE(),
        [update_at] DATETIME NOT NULL DEFAULT GETDATE()
    );
    CREATE NONCLUSTERED INDEX [IX_Landing_Supplies_Patient] ON [dbo].[Landing_Supplies]([PATIENT]);
END;
GO

-- ============================================================================
-- 13. BẢNG ORGANIZATIONS - 5,499 dòng
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Landing_Organizations]'))
BEGIN
    CREATE TABLE [dbo].[Landing_Organizations] (
        [Id] VARCHAR(36),
        [NAME] VARCHAR(255),
        [ADDRESS] VARCHAR(255),
        [CITY] VARCHAR(100),
        [STATE] VARCHAR(50),
        [ZIP] VARCHAR(10),
        [LAT] VARCHAR(50),
        [LON] VARCHAR(50),
        [PHONE] VARCHAR(20),
        [REVENUE] VARCHAR(20),
        [UTILIZATION] VARCHAR(10),
        [create_at] DATETIME NOT NULL DEFAULT GETDATE(),
        [update_at] DATETIME NOT NULL DEFAULT GETDATE()
    );
    CREATE CLUSTERED INDEX [IX_Landing_Organizations_Id] ON [dbo].[Landing_Organizations]([Id]);
END;
GO

-- ============================================================================
-- 14. BẢNG PROVIDERS - 31,764 dòng
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Landing_Providers]'))
BEGIN
    CREATE TABLE [dbo].[Landing_Providers] (
        [Id] VARCHAR(36),
        [ORGANIZATION] VARCHAR(36),
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
-- 15. BẢNG PAYERS - 10 dòng
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Landing_Payers]'))
BEGIN
    CREATE TABLE [dbo].[Landing_Payers] (
        [Id] VARCHAR(36),
        [NAME] VARCHAR(255),
        [ADDRESS] VARCHAR(255),
        [CITY] VARCHAR(100),
        [STATE_HEADQUARTERED] VARCHAR(50),
        [ZIP] VARCHAR(10),
        [PHONE] VARCHAR(20),
        [AMOUNT_COVERED] VARCHAR(20),
        [AMOUNT_UNCOVERED] VARCHAR(20),
        [REVENUE] VARCHAR(20),
        [COVERED_ENCOUNTERS] VARCHAR(10),
        [UNCOVERED_ENCOUNTERS] VARCHAR(10),
        [COVERED_MEDICATIONS] VARCHAR(10),
        [UNCOVERED_MEDICATIONS] VARCHAR(10),
        [COVERED_PROCEDURES] VARCHAR(10),
        [UNCOVERED_PROCEDURES] VARCHAR(10),
        [COVERED_IMMUNIZATIONS] VARCHAR(10),
        [UNCOVERED_IMMUNIZATIONS] VARCHAR(10),
        [UNIQUE_CUSTOMERS] VARCHAR(10),
        [QOLS_AVG] VARCHAR(10),
        [MEMBER_MONTHS] VARCHAR(10),
        [create_at] DATETIME NOT NULL DEFAULT GETDATE(),
        [update_at] DATETIME NOT NULL DEFAULT GETDATE()
    );
    CREATE CLUSTERED INDEX [IX_Landing_Payers_Id] ON [dbo].[Landing_Payers]([Id]);
END;
GO

-- ============================================================================
-- 16. BẢNG PAYER_TRANSITIONS - 41,392 dòng
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Landing_Payer_Transitions]'))
BEGIN
    CREATE TABLE [dbo].[Landing_Payer_Transitions] (
        [PATIENT] VARCHAR(36),
        [START_YEAR] VARCHAR(10),
        [END_YEAR] VARCHAR(10),
        [PAYER] VARCHAR(36),
        [OWNERSHIP] VARCHAR(50),
        [create_at] DATETIME NOT NULL DEFAULT GETDATE(),
        [update_at] DATETIME NOT NULL DEFAULT GETDATE()
    );
    CREATE NONCLUSTERED INDEX [IX_Landing_Payer_Transitions_Patient] ON [dbo].[Landing_Payer_Transitions]([PATIENT]);
END;
GO

-- ============================================================================
-- BẢNG TRACKING - Ghi lại tiến độ load
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

PRINT 'Landing Database Schema Created Successfully!';

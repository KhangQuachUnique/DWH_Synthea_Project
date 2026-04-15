-- ============================================================================
-- SYNTHEA STAGING DATABASE SCHEMA
-- ============================================================================
-- Mục đích: Tạo Staging Database với dữ liệu đã được transform
-- Đặc điểm:
--   - Dữ kiểu dữ liệu chính xác (INT, DATETIME, DECIMAL, DATE...)
--   - Cột AddLoadDate = GETDATE() tự động
--   - Cấu trúc chuẩn bị cho DW
-- Ngày tạo: 2026-04-15
-- ============================================================================

USE master;
GO

-- Tạo Staging Database nếu chưa tồn tại
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'DW_Synthea_Staging')
BEGIN
    CREATE DATABASE [DW_Synthea_Staging]
    ON PRIMARY (
        NAME = 'DW_Synthea_Staging_Data',
        FILENAME = 'C:\SQLServerData\DW_Synthea_Staging.mdf',
        SIZE = 1024MB,
        FILEGROWTH = 512MB
    )
    LOG ON (
        NAME = 'DW_Synthea_Staging_Log',
        FILENAME = 'C:\SQLServerData\DW_Synthea_Staging.ldf',
        SIZE = 256MB,
        FILEGROWTH = 256MB
    );
END;
GO

USE [DW_Synthea_Staging];
GO

-- ============================================================================
-- 1. BẢNG PATIENTS - 12,352 dòng
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Staging_Patients]'))
BEGIN
    CREATE TABLE [dbo].[Staging_Patients] (
        [Id] VARCHAR(36) NOT NULL PRIMARY KEY,
        [BIRTHDATE] DATE,
        [DEATHDATE] DATE,
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
        [LAT] DECIMAL(15,10),
        [LON] DECIMAL(15,10),
        [HEALTHCARE_EXPENSES] DECIMAL(10,2),
        [HEALTHCARE_COVERAGE] DECIMAL(10,2),
        [create_at] DATETIME NOT NULL DEFAULT GETDATE(),
        [update_at] DATETIME NOT NULL DEFAULT GETDATE()
    );
    CREATE NONCLUSTERED INDEX [IX_Staging_Patients_Name] ON [dbo].[Staging_Patients]([FIRST], [LAST]);
END;
GO

-- ============================================================================
-- 2. BẢNG ENCOUNTERS - 321,528 dòng
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Staging_Encounters]'))
BEGIN
    CREATE TABLE [dbo].[Staging_Encounters] (
        [Id] VARCHAR(36) NOT NULL PRIMARY KEY,
        [START] DATETIME,
        [STOP] DATETIME,
        [PATIENT] VARCHAR(36) NOT NULL,
        [ORGANIZATION] VARCHAR(36),
        [PROVIDER] VARCHAR(36),
        [PAYER] VARCHAR(36),
        [ENCOUNTERCLASS] VARCHAR(50),
        [CODE] VARCHAR(20),
        [DESCRIPTION] VARCHAR(255),
        [BASE_ENCOUNTER_COST] DECIMAL(12,2),
        [TOTAL_CLAIM_COST] DECIMAL(12,2),
        [PAYER_COVERAGE] DECIMAL(12,2),
        [REASONCODE] VARCHAR(20),
        [REASONDESCRIPTION] VARCHAR(255),
        [create_at] DATETIME NOT NULL DEFAULT GETDATE(),
        [update_at] DATETIME NOT NULL DEFAULT GETDATE()
    );
    CREATE NONCLUSTERED INDEX [IX_Staging_Encounters_Patient] ON [dbo].[Staging_Encounters]([PATIENT]);
    CREATE NONCLUSTERED INDEX [IX_Staging_Encounters_Date] ON [dbo].[Staging_Encounters]([START]);
END;
GO

-- ============================================================================
-- 3. BẢNG CONDITIONS - 114,544 dòng
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Staging_Conditions]'))
BEGIN
    CREATE TABLE [dbo].[Staging_Conditions] (
        [START] DATE,
        [STOP] DATE,
        [PATIENT] VARCHAR(36) NOT NULL,
        [ENCOUNTER] VARCHAR(36),
        [CODE] VARCHAR(20) NOT NULL,
        [DESCRIPTION] VARCHAR(255),
        [create_at] DATETIME NOT NULL DEFAULT GETDATE(),
        [update_at] DATETIME NOT NULL DEFAULT GETDATE()
    );
    CREATE NONCLUSTERED INDEX [IX_Staging_Conditions_Patient] ON [dbo].[Staging_Conditions]([PATIENT]);
    CREATE NONCLUSTERED INDEX [IX_Staging_Conditions_Code] ON [dbo].[Staging_Conditions]([CODE]);
END;
GO

-- ============================================================================
-- 4. BẢNG MEDICATIONS - 431,262 dòng (LỚN)
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Staging_Medications]'))
BEGIN
    CREATE TABLE [dbo].[Staging_Medications] (
        [START] DATE,
        [STOP] DATE,
        [PATIENT] VARCHAR(36) NOT NULL,
        [PAYER] VARCHAR(36),
        [ENCOUNTER] VARCHAR(36),
        [CODE] VARCHAR(20) NOT NULL,
        [DESCRIPTION] VARCHAR(255),
        [BASE_COST] DECIMAL(10,2),
        [PAYER_COVERAGE] DECIMAL(10,2),
        [DISPENSES] INT,
        [TOTALCOST] DECIMAL(10,2),
        [REASONCODE] VARCHAR(20),
        [REASONDESCRIPTION] VARCHAR(255),
        [create_at] DATETIME NOT NULL DEFAULT GETDATE(),
        [update_at] DATETIME NOT NULL DEFAULT GETDATE()
    );
    CREATE NONCLUSTERED INDEX [IX_Staging_Medications_Patient] ON [dbo].[Staging_Medications]([PATIENT]);
    CREATE NONCLUSTERED INDEX [IX_Staging_Medications_Code] ON [dbo].[Staging_Medications]([CODE]);
END;
GO

-- ============================================================================
-- 5. BẢNG OBSERVATIONS - 1,659,750 dòng (RẤT LỚN - 58% dữ liệu)
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Staging_Observations]'))
BEGIN
    CREATE TABLE [dbo].[Staging_Observations] (
        [DATE] DATE,
        [PATIENT] VARCHAR(36) NOT NULL,
        [ENCOUNTER] VARCHAR(36),
        [CODE] VARCHAR(20) NOT NULL,
        [DESCRIPTION] VARCHAR(255),
        [VALUE] VARCHAR(50),
        [UNITS] VARCHAR(20),
        [TYPE] VARCHAR(50),
        [create_at] DATETIME NOT NULL DEFAULT GETDATE(),
        [update_at] DATETIME NOT NULL DEFAULT GETDATE()
    );
    CREATE NONCLUSTERED INDEX [IX_Staging_Observations_Patient] ON [dbo].[Staging_Observations]([PATIENT]);
    CREATE NONCLUSTERED INDEX [IX_Staging_Observations_Date] ON [dbo].[Staging_Observations]([DATE]);
    CREATE NONCLUSTERED INDEX [IX_Staging_Observations_Code] ON [dbo].[Staging_Observations]([CODE]);
END;
GO

-- ============================================================================
-- 6. BẢNG PROCEDURES - 100,427 dòng
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Staging_Procedures]'))
BEGIN
    CREATE TABLE [dbo].[Staging_Procedures] (
        [DATE] DATE,
        [PATIENT] VARCHAR(36) NOT NULL,
        [ENCOUNTER] VARCHAR(36),
        [CODE] VARCHAR(20) NOT NULL,
        [DESCRIPTION] VARCHAR(255),
        [BASE_COST] DECIMAL(12,2),
        [REASONCODE] VARCHAR(20),
        [REASONDESCRIPTION] VARCHAR(255),
        [create_at] DATETIME NOT NULL DEFAULT GETDATE(),
        [update_at] DATETIME NOT NULL DEFAULT GETDATE()
    );
    CREATE NONCLUSTERED INDEX [IX_Staging_Procedures_Patient] ON [dbo].[Staging_Procedures]([PATIENT]);
    CREATE NONCLUSTERED INDEX [IX_Staging_Procedures_Code] ON [dbo].[Staging_Procedures]([CODE]);
END;
GO

-- ============================================================================
-- 7. BẢNG IMMUNIZATIONS - 16,481 dòng
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Staging_Immunizations]'))
BEGIN
    CREATE TABLE [dbo].[Staging_Immunizations] (
        [DATE] DATE,
        [PATIENT] VARCHAR(36) NOT NULL,
        [ENCOUNTER] VARCHAR(36),
        [CODE] VARCHAR(20) NOT NULL,
        [DESCRIPTION] VARCHAR(255),
        [BASE_COST] DECIMAL(10,2),
        [create_at] DATETIME NOT NULL DEFAULT GETDATE(),
        [update_at] DATETIME NOT NULL DEFAULT GETDATE()
    );
    CREATE NONCLUSTERED INDEX [IX_Staging_Immunizations_Patient] ON [dbo].[Staging_Immunizations]([PATIENT]);
END;
GO

-- ============================================================================
-- 8. BẢNG ALLERGIES - 5,417 dòng
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Staging_Allergies]'))
BEGIN
    CREATE TABLE [dbo].[Staging_Allergies] (
        [START] DATE,
        [STOP] DATE,
        [PATIENT] VARCHAR(36) NOT NULL,
        [ENCOUNTER] VARCHAR(36),
        [CODE] VARCHAR(20) NOT NULL,
        [DESCRIPTION] VARCHAR(255),
        [create_at] DATETIME NOT NULL DEFAULT GETDATE(),
        [update_at] DATETIME NOT NULL DEFAULT GETDATE()
    );
    CREATE NONCLUSTERED INDEX [IX_Staging_Allergies_Patient] ON [dbo].[Staging_Allergies]([PATIENT]);
END;
GO

-- ============================================================================
-- 9. BẢNG CAREPLANS - 37,715 dòng
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Staging_Careplans]'))
BEGIN
    CREATE TABLE [dbo].[Staging_Careplans] (
        [Id] VARCHAR(36) NOT NULL PRIMARY KEY,
        [START] DATE,
        [STOP] DATE,
        [PATIENT] VARCHAR(36) NOT NULL,
        [ENCOUNTER] VARCHAR(36),
        [CODE] VARCHAR(20) NOT NULL,
        [DESCRIPTION] VARCHAR(255),
        [REASONCODE] VARCHAR(20),
        [REASONDESCRIPTION] VARCHAR(255),
        [create_at] DATETIME NOT NULL DEFAULT GETDATE(),
        [update_at] DATETIME NOT NULL DEFAULT GETDATE()
    );
    CREATE NONCLUSTERED INDEX [IX_Staging_Careplans_Patient] ON [dbo].[Staging_Careplans]([PATIENT]);
END;
GO

-- ============================================================================
-- 10. BẢNG DEVICES - 2,360 dòng
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Staging_Devices]'))
BEGIN
    CREATE TABLE [dbo].[Staging_Devices] (
        [START] DATE,
        [STOP] DATE,
        [PATIENT] VARCHAR(36) NOT NULL,
        [ENCOUNTER] VARCHAR(36),
        [CODE] VARCHAR(20) NOT NULL,
        [DESCRIPTION] VARCHAR(255),
        [UDI] VARCHAR(500),
        [create_at] DATETIME NOT NULL DEFAULT GETDATE(),
        [update_at] DATETIME NOT NULL DEFAULT GETDATE()
    );
    CREATE NONCLUSTERED INDEX [IX_Staging_Devices_Patient] ON [dbo].[Staging_Devices]([PATIENT]);
END;
GO

-- ============================================================================
-- 11. BẢNG IMAGING_STUDIES - 4,504 dòng
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Staging_Imaging_Studies]'))
BEGIN
    CREATE TABLE [dbo].[Staging_Imaging_Studies] (
        [Id] VARCHAR(36) NOT NULL PRIMARY KEY,
        [DATE] DATE,
        [PATIENT] VARCHAR(36) NOT NULL,
        [ENCOUNTER] VARCHAR(36),
        [BODYSITE_CODE] VARCHAR(20),
        [BODYSITE_DESCRIPTION] VARCHAR(255),
        [MODALITY_CODE] VARCHAR(5),
        [MODALITY_DESCRIPTION] VARCHAR(50),
        [SOP_CODE] VARCHAR(30),
        [SOP_DESCRIPTION] VARCHAR(255),
        [create_at] DATETIME NOT NULL DEFAULT GETDATE(),
        [update_at] DATETIME NOT NULL DEFAULT GETDATE()
    );
    CREATE NONCLUSTERED INDEX [IX_Staging_Imaging_Studies_Patient] ON [dbo].[Staging_Imaging_Studies]([PATIENT]);
END;
GO

-- ============================================================================
-- 12. BẢNG SUPPLIES - 143,110 dòng
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Staging_Supplies]'))
BEGIN
    CREATE TABLE [dbo].[Staging_Supplies] (
        [DATE] DATE,
        [PATIENT] VARCHAR(36) NOT NULL,
        [ENCOUNTER] VARCHAR(36),
        [CODE] VARCHAR(20) NOT NULL,
        [DESCRIPTION] VARCHAR(255),
        [QUANTITY] INT,
        [create_at] DATETIME NOT NULL DEFAULT GETDATE(),
        [update_at] DATETIME NOT NULL DEFAULT GETDATE()
    );
    CREATE NONCLUSTERED INDEX [IX_Staging_Supplies_Patient] ON [dbo].[Staging_Supplies]([PATIENT]);
END;
GO

-- ============================================================================
-- 13. BẢNG ORGANIZATIONS - 5,499 dòng
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Staging_Organizations]'))
BEGIN
    CREATE TABLE [dbo].[Staging_Organizations] (
        [Id] VARCHAR(36) NOT NULL PRIMARY KEY,
        [NAME] VARCHAR(255),
        [ADDRESS] VARCHAR(255),
        [CITY] VARCHAR(100),
        [STATE] VARCHAR(50),
        [ZIP] VARCHAR(10),
        [LAT] DECIMAL(15,10),
        [LON] DECIMAL(15,10),
        [PHONE] VARCHAR(20),
        [REVENUE] DECIMAL(15,2),
        [UTILIZATION] INT,
        [create_at] DATETIME NOT NULL DEFAULT GETDATE(),
        [update_at] DATETIME NOT NULL DEFAULT GETDATE()
    );
    CREATE NONCLUSTERED INDEX [IX_Staging_Organizations_Name] ON [dbo].[Staging_Organizations]([NAME]);
END;
GO

-- ============================================================================
-- 14. BẢNG PROVIDERS - 31,764 dòng
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Staging_Providers]'))
BEGIN
    CREATE TABLE [dbo].[Staging_Providers] (
        [Id] VARCHAR(36) NOT NULL PRIMARY KEY,
        [ORGANIZATION] VARCHAR(36),
        [NAME] VARCHAR(255),
        [GENDER] CHAR(1),
        [SPECIALITY] VARCHAR(100),
        [ADDRESS] VARCHAR(255),
        [CITY] VARCHAR(100),
        [STATE] VARCHAR(50),
        [ZIP] VARCHAR(10),
        [LAT] DECIMAL(15,10),
        [LON] DECIMAL(15,10),
        [UTILIZATION] INT,
        [create_at] DATETIME NOT NULL DEFAULT GETDATE(),
        [update_at] DATETIME NOT NULL DEFAULT GETDATE()
    );
    CREATE NONCLUSTERED INDEX [IX_Staging_Providers_Organization] ON [dbo].[Staging_Providers]([ORGANIZATION]);
    CREATE NONCLUSTERED INDEX [IX_Staging_Providers_Name] ON [dbo].[Staging_Providers]([NAME]);
END;
GO

-- ============================================================================
-- 15. BẢNG PAYERS - 10 dòng
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Staging_Payers]'))
BEGIN
    CREATE TABLE [dbo].[Staging_Payers] (
        [Id] VARCHAR(36) NOT NULL PRIMARY KEY,
        [NAME] VARCHAR(255),
        [ADDRESS] VARCHAR(255),
        [CITY] VARCHAR(100),
        [STATE_HEADQUARTERED] VARCHAR(50),
        [ZIP] VARCHAR(10),
        [PHONE] VARCHAR(20),
        [AMOUNT_COVERED] DECIMAL(15,2),
        [AMOUNT_UNCOVERED] DECIMAL(15,2),
        [REVENUE] DECIMAL(15,2),
        [COVERED_ENCOUNTERS] INT,
        [UNCOVERED_ENCOUNTERS] INT,
        [COVERED_MEDICATIONS] INT,
        [UNCOVERED_MEDICATIONS] INT,
        [COVERED_PROCEDURES] INT,
        [UNCOVERED_PROCEDURES] INT,
        [COVERED_IMMUNIZATIONS] INT,
        [UNCOVERED_IMMUNIZATIONS] INT,
        [UNIQUE_CUSTOMERS] INT,
        [QOLS_AVG] DECIMAL(5,4),
        [MEMBER_MONTHS] INT,
        [create_at] DATETIME NOT NULL DEFAULT GETDATE(),
        [update_at] DATETIME NOT NULL DEFAULT GETDATE()
    );
END;
GO

-- ============================================================================
-- 16. BẢNG PAYER_TRANSITIONS - 41,392 dòng
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Staging_Payer_Transitions]'))
BEGIN
    CREATE TABLE [dbo].[Staging_Payer_Transitions] (
        [PATIENT] VARCHAR(36) NOT NULL,
        [START_YEAR] INT,
        [END_YEAR] INT,
        [PAYER] VARCHAR(36),
        [OWNERSHIP] VARCHAR(50),
        [create_at] DATETIME NOT NULL DEFAULT GETDATE(),
        [update_at] DATETIME NOT NULL DEFAULT GETDATE()
    );
    CREATE NONCLUSTERED INDEX [IX_Staging_Payer_Transitions_Patient] ON [dbo].[Staging_Payer_Transitions]([PATIENT]);
    CREATE NONCLUSTERED INDEX [IX_Staging_Payer_Transitions_Payer] ON [dbo].[Staging_Payer_Transitions]([PAYER]);
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
        [Status] VARCHAR(50),
        [ErrorMessage] VARCHAR(MAX),
        [CreatedDate] DATETIME DEFAULT GETDATE()
    );
END;
GO

PRINT 'Staging Database Schema Created Successfully!';

-- Transform Landing_Patients to Staging_Patients (chuẩn hóa, làm sạch)
CREATE OR ALTER PROCEDURE dbo.usp_transform_landing_to_staging_patients
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    BEGIN TRAN;
    BEGIN TRY
        IF OBJECT_ID(N'DW_Synthea_Landing.dbo.Landing_Patients', N'U') IS NULL
           OR OBJECT_ID(N'DW_Synthea_Staging.dbo.Staging_Patients', N'U') IS NULL
            THROW 50001, 'Missing required table: Landing_Patients or Staging_Patients.', 1;

        DROP TABLE IF EXISTS #src;

        SELECT
            LEFT(LTRIM(RTRIM(Id)),36) AS Id,
            TRY_CAST(BIRTHDATE AS DATE) AS BIRTHDATE,
            TRY_CAST(DEATHDATE AS DATE) AS DEATHDATE,
            NULLIF(LEFT(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(SSN)),'\r',' '),'\n',' '),'\t',' '),11),'') AS SSN,
            NULLIF(LEFT(LTRIM(RTRIM(DRIVERS)),50),'') AS DRIVERS,
            NULLIF(LEFT(LTRIM(RTRIM(PASSPORT)),50),'') AS PASSPORT,
            NULLIF(LEFT(LTRIM(RTRIM(PREFIX)),10),'') AS PREFIX,
            NULLIF(LEFT(LTRIM(RTRIM(FIRST)),100),'') AS FIRST,
            NULLIF(LEFT(LTRIM(RTRIM(LAST)),100),'') AS LAST,
            NULLIF(LEFT(LTRIM(RTRIM(SUFFIX)),10),'') AS SUFFIX,
            NULLIF(LEFT(LTRIM(RTRIM(MAIDEN)),100),'') AS MAIDEN,
            NULLIF(LEFT(LTRIM(RTRIM(MARITAL)),20),'') AS MARITAL,
            NULLIF(LEFT(LTRIM(RTRIM(RACE)),50),'') AS RACE,
            NULLIF(LEFT(LTRIM(RTRIM(ETHNICITY)),50),'') AS ETHNICITY,
            NULLIF(LEFT(LTRIM(RTRIM(GENDER)),1),'') AS GENDER,
            NULLIF(LEFT(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(BIRTHPLACE)),'\r',' '),'\n',' '),'\t',' '),255),'') AS BIRTHPLACE,
            NULLIF(LEFT(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(ADDRESS)),'\r',' '),'\n',' '),'\t',' '),255),'') AS ADDRESS,
            NULLIF(LEFT(LTRIM(RTRIM(CITY)),100),'') AS CITY,
            NULLIF(LEFT(LTRIM(RTRIM(STATE)),50),'') AS STATE,
            NULLIF(LEFT(LTRIM(RTRIM(COUNTY)),100),'') AS COUNTY,
            NULLIF(LEFT(LTRIM(RTRIM(ZIP)),10),'') AS ZIP,
            TRY_CAST(NULLIF(LAT,'') AS DECIMAL(18,2)) AS LAT,
            TRY_CAST(NULLIF(LON,'') AS DECIMAL(18,2)) AS LON,
            TRY_CAST(NULLIF(HEALTHCARE_EXPENSES,'') AS DECIMAL(18,2)) AS HEALTHCARE_EXPENSES,
            TRY_CAST(NULLIF(HEALTHCARE_COVERAGE,'') AS DECIMAL(18,2)) AS HEALTHCARE_COVERAGE,
            batch_id,
            update_at
        INTO #src
        FROM DW_Synthea_Landing.dbo.Landing_Patients
        WHERE Id IS NOT NULL AND LTRIM(RTRIM(Id)) <> '';

        MERGE DW_Synthea_Staging.dbo.Staging_Patients AS t
        USING #src AS s
        ON t.Id = s.Id
        WHEN MATCHED AND (t.update_at < s.update_at) THEN
            UPDATE SET
                BIRTHDATE = s.BIRTHDATE,
                DEATHDATE = s.DEATHDATE,
                SSN = s.SSN,
                DRIVERS = s.DRIVERS,
                PASSPORT = s.PASSPORT,
                PREFIX = s.PREFIX,
                FIRST = s.FIRST,
                LAST = s.LAST,
                SUFFIX = s.SUFFIX,
                MAIDEN = s.MAIDEN,
                MARITAL = s.MARITAL,
                RACE = s.RACE,
                ETHNICITY = s.ETHNICITY,
                GENDER = s.GENDER,
                BIRTHPLACE = s.BIRTHPLACE,
                ADDRESS = s.ADDRESS,
                CITY = s.CITY,
                STATE = s.STATE,
                COUNTY = s.COUNTY,
                ZIP = s.ZIP,
                LAT = s.LAT,
                LON = s.LON,
                HEALTHCARE_EXPENSES = s.HEALTHCARE_EXPENSES,
                HEALTHCARE_COVERAGE = s.HEALTHCARE_COVERAGE,
                update_at = s.update_at,
                batch_id = s.batch_id

        WHEN NOT MATCHED BY TARGET THEN
            INSERT (Id,BIRTHDATE,DEATHDATE,SSN,DRIVERS,PASSPORT,PREFIX,FIRST,LAST,SUFFIX,MAIDEN,MARITAL,RACE,ETHNICITY,GENDER,BIRTHPLACE,ADDRESS,CITY,STATE,COUNTY,ZIP,LAT,LON,HEALTHCARE_EXPENSES,HEALTHCARE_COVERAGE,create_at,update_at,batch_id)
            VALUES (s.Id,s.BIRTHDATE,s.DEATHDATE,s.SSN,s.DRIVERS,s.PASSPORT,s.PREFIX,s.FIRST,s.LAST,s.SUFFIX,s.MAIDEN,s.MARITAL,s.RACE,s.ETHNICITY,s.GENDER,s.BIRTHPLACE,s.ADDRESS,s.CITY,s.STATE,s.COUNTY,s.ZIP,s.LAT,s.LON,s.HEALTHCARE_EXPENSES,s.HEALTHCARE_COVERAGE,SYSUTCDATETIME(),s.update_at,s.batch_id);

        DROP TABLE IF EXISTS #src;
        COMMIT;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;
        DROP TABLE IF EXISTS #src;
        THROW;
    END CATCH
END
GO

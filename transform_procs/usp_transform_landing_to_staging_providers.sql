-- Transform Landing_Providers to Staging_Providers (chuẩn hóa, làm sạch)
CREATE OR ALTER PROCEDURE dbo.usp_transform_landing_to_staging_providers
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    BEGIN TRAN;
    BEGIN TRY
        IF OBJECT_ID(N'DW_Synthea_Landing.dbo.Landing_Providers', N'U') IS NULL
           OR OBJECT_ID(N'DW_Synthea_Staging.dbo.Staging_Providers', N'U') IS NULL
            THROW 50001, 'Missing required table: Landing_Providers or Staging_Providers.', 1;

        DROP TABLE IF EXISTS #src;

        SELECT
            LEFT(LTRIM(RTRIM(Id)),36) AS Id,
            NULLIF(LEFT(LTRIM(RTRIM(ORGANIZATION)),36),'') AS ORGANIZATION,
            NULLIF(LEFT(LTRIM(RTRIM(NAME)),255),'') AS NAME,
            NULLIF(LEFT(LTRIM(RTRIM(GENDER)),1),'') AS GENDER,
            NULLIF(LEFT(LTRIM(RTRIM(SPECIALITY)),100),'') AS SPECIALITY,
            NULLIF(LEFT(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(ADDRESS)),'\r',' '),'\n',' '),'\t',' '),255),'') AS ADDRESS,
            NULLIF(LEFT(LTRIM(RTRIM(CITY)),100),'') AS CITY,
            NULLIF(LEFT(LTRIM(RTRIM(STATE)),50),'') AS STATE,
            NULLIF(LEFT(LTRIM(RTRIM(ZIP)),10),'') AS ZIP,
            TRY_CAST(NULLIF(LAT,'') AS DECIMAL(18,10)) AS LAT,
            TRY_CAST(NULLIF(LON,'') AS DECIMAL(18,10)) AS LON,
            TRY_CAST(NULLIF(UTILIZATION,'') AS INT) AS UTILIZATION,
            update_at
        INTO #src
        FROM DW_Synthea_Landing.dbo.Landing_Providers
        WHERE Id IS NOT NULL AND LTRIM(RTRIM(Id)) <> '';

        MERGE DW_Synthea_Staging.dbo.Staging_Providers AS t
        USING #src AS s
        ON t.Id = s.Id
        WHEN MATCHED AND (t.update_at < s.update_at) THEN
            UPDATE SET
                ORGANIZATION = s.ORGANIZATION,
                NAME = s.NAME,
                GENDER = s.GENDER,
                SPECIALITY = s.SPECIALITY,
                ADDRESS = s.ADDRESS,
                CITY = s.CITY,
                STATE = s.STATE,
                ZIP = s.ZIP,
                LAT = s.LAT,
                LON = s.LON,
                UTILIZATION = s.UTILIZATION,
                update_at = s.update_at
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (Id,ORGANIZATION,NAME,GENDER,SPECIALITY,ADDRESS,CITY,STATE,ZIP,LAT,LON,UTILIZATION,create_at,update_at)
            VALUES (s.Id,s.ORGANIZATION,s.NAME,s.GENDER,s.SPECIALITY,s.ADDRESS,s.CITY,s.STATE,s.ZIP,s.LAT,s.LON,s.UTILIZATION,SYSUTCDATETIME(),s.update_at);

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

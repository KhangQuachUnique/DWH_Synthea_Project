-- Transform Landing_Organizations to Staging_Organizations (chuẩn hóa, làm sạch)
CREATE OR ALTER PROCEDURE dbo.usp_transform_landing_to_staging_organizations
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    BEGIN TRAN;
    BEGIN TRY
        IF OBJECT_ID(N'DW_Synthea_Landing.dbo.Landing_Organizations', N'U') IS NULL
           OR OBJECT_ID(N'DW_Synthea_Staging.dbo.Staging_Organizations', N'U') IS NULL
            THROW 50001, 'Missing required table: Landing_Organizations or Staging_Organizations.', 1;

        DROP TABLE IF EXISTS #src;

        SELECT
            LEFT(LTRIM(RTRIM(Id)),36) AS Id,
            NULLIF(LEFT(LTRIM(RTRIM(NAME)),255),'') AS NAME,
            NULLIF(LEFT(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(ADDRESS)),'\r',' '),'\n',' '),'\t',' '),255),'') AS ADDRESS,
            NULLIF(LEFT(LTRIM(RTRIM(CITY)),100),'') AS CITY,
            NULLIF(LEFT(LTRIM(RTRIM(STATE)),50),'') AS STATE,
            NULLIF(LEFT(LTRIM(RTRIM(ZIP)),10),'') AS ZIP,
            TRY_CAST(NULLIF(LAT,'') AS DECIMAL(18,10)) AS LAT,
            TRY_CAST(NULLIF(LON,'') AS DECIMAL(18,10)) AS LON,
            NULLIF(LEFT(LTRIM(RTRIM(PHONE)),20),'') AS PHONE,
            TRY_CAST(NULLIF(REVENUE,'') AS DECIMAL(18,2)) AS REVENUE,
            TRY_CAST(NULLIF(UTILIZATION,'') AS INT) AS UTILIZATION,
            batch_id,
            update_at
        INTO #src
        FROM DW_Synthea_Landing.dbo.Landing_Organizations
        WHERE Id IS NOT NULL AND LTRIM(RTRIM(Id)) <> '';

        MERGE DW_Synthea_Staging.dbo.Staging_Organizations AS t
        USING #src AS s
        ON t.Id = s.Id
        WHEN MATCHED AND (t.update_at < s.update_at) THEN
            UPDATE SET
                NAME = s.NAME,
                ADDRESS = s.ADDRESS,
                CITY = s.CITY,
                STATE = s.STATE,
                ZIP = s.ZIP,
                LAT = s.LAT,
                LON = s.LON,
                PHONE = s.PHONE,
                REVENUE = s.REVENUE,
                UTILIZATION = s.UTILIZATION,
                update_at = s.update_at
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (Id,NAME,ADDRESS,CITY,STATE,ZIP,LAT,LON,PHONE,REVENUE,UTILIZATION,create_at,update_at)
            VALUES (s.Id,s.NAME,s.ADDRESS,s.CITY,s.STATE,s.ZIP,s.LAT,s.LON,s.PHONE,s.REVENUE,s.UTILIZATION,SYSUTCDATETIME(),s.update_at);

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

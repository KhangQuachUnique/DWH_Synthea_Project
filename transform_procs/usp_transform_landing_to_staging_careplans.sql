-- Transform Landing_Careplans to Staging_Careplans (chuẩn hóa, làm sạch)
CREATE OR ALTER PROCEDURE dbo.usp_transform_landing_to_staging_careplans
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    BEGIN TRAN;
    BEGIN TRY
        SELECT
            LEFT(LTRIM(RTRIM(Id)),36) AS Id,
            TRY_CAST(START AS DATE) AS START,
            TRY_CAST(STOP AS DATE) AS STOP,
            NULLIF(LEFT(LTRIM(RTRIM(PATIENT)),36),'') AS PATIENT,
            NULLIF(LEFT(LTRIM(RTRIM(ENCOUNTER)),36),'') AS ENCOUNTER,
            NULLIF(LEFT(LTRIM(RTRIM(CODE)),20),'') AS CODE,
            NULLIF(LEFT(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(DESCRIPTION)),'\r',' '),'\n',' '),'\t',' '),255),'') AS DESCRIPTION,
            NULLIF(LEFT(LTRIM(RTRIM(REASONCODE)),20),'') AS REASONCODE,
            NULLIF(LEFT(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(REASONDESCRIPTION)),'\r',' '),'\n',' '),'\t',' '),255),'') AS REASONDESCRIPTION,
            update_at
        INTO #src
        FROM DW_Synthea_Landing.dbo.Landing_Careplans
        WHERE Id IS NOT NULL AND LTRIM(RTRIM(Id)) <> '';

        MERGE DW_Synthea_Staging.dbo.Staging_Careplans AS t
        USING #src AS s
        ON t.Id = s.Id
        WHEN MATCHED AND (t.update_at < s.update_at) THEN
            UPDATE SET
                START = s.START,
                STOP = s.STOP,
                PATIENT = s.PATIENT,
                ENCOUNTER = s.ENCOUNTER,
                CODE = s.CODE,
                DESCRIPTION = s.DESCRIPTION,
                REASONCODE = s.REASONCODE,
                REASONDESCRIPTION = s.REASONDESCRIPTION,
                update_at = s.update_at
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (Id,START,STOP,PATIENT,ENCOUNTER,CODE,DESCRIPTION,REASONCODE,REASONDESCRIPTION,create_at,update_at)
            VALUES (s.Id,s.START,s.STOP,s.PATIENT,s.ENCOUNTER,s.CODE,s.DESCRIPTION,s.REASONCODE,s.REASONDESCRIPTION,SYSUTCDATETIME(),s.update_at);

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

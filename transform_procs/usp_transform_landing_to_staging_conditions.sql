-- Transform Landing_Conditions to Staging_Conditions (chuẩn hóa, làm sạch)
CREATE OR ALTER PROCEDURE dbo.usp_transform_landing_to_staging_conditions
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    BEGIN TRAN;
    BEGIN TRY
        IF OBJECT_ID(N'DW_Synthea_Landing.dbo.Landing_Conditions', N'U') IS NULL
           OR OBJECT_ID(N'DW_Synthea_Staging.dbo.Staging_Conditions', N'U') IS NULL
            THROW 50001, 'Missing required table: Landing_Conditions or Staging_Conditions.', 1;

        DROP TABLE IF EXISTS #src;

        SELECT
            TRY_CAST(START AS DATE) AS START,
            TRY_CAST(STOP AS DATE) AS STOP,
            NULLIF(LEFT(LTRIM(RTRIM(PATIENT)),36),'') AS PATIENT,
            NULLIF(LEFT(LTRIM(RTRIM(ENCOUNTER)),36),'') AS ENCOUNTER,
            NULLIF(LEFT(LTRIM(RTRIM(CODE)),20),'') AS CODE,
            NULLIF(LEFT(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(DESCRIPTION)),'\r',' '),'\n',' '),'\t',' '),255),'') AS DESCRIPTION,
            batch_id,
            update_at
        INTO #src
        FROM DW_Synthea_Landing.dbo.Landing_Conditions
        WHERE PATIENT IS NOT NULL AND LTRIM(RTRIM(PATIENT)) <> '';

        MERGE DW_Synthea_Staging.dbo.Staging_Conditions AS t
        USING #src AS s
        ON ISNULL(t.PATIENT,'') = ISNULL(s.PATIENT,'') AND ISNULL(t.START,'1900-01-01') = ISNULL(s.START,'1900-01-01') AND ISNULL(t.CODE,'') = ISNULL(s.CODE,'')
        WHEN MATCHED AND (t.update_at < s.update_at) THEN
            UPDATE SET
                STOP = s.STOP,
                ENCOUNTER = s.ENCOUNTER,
                DESCRIPTION = s.DESCRIPTION,
                update_at = s.update_at
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (START,STOP,PATIENT,ENCOUNTER,CODE,DESCRIPTION,create_at,update_at)
            VALUES (s.START,s.STOP,s.PATIENT,s.ENCOUNTER,s.CODE,s.DESCRIPTION,SYSUTCDATETIME(),s.update_at);

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

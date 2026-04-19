-- Transform Landing_Observations to Staging_Observations (chuẩn hóa, làm sạch)
CREATE OR ALTER PROCEDURE dbo.usp_transform_landing_to_staging_observations
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    BEGIN TRAN;
    BEGIN TRY
        SELECT
            TRY_CAST([DATE] AS DATE) AS [DATE],
            NULLIF(LEFT(LTRIM(RTRIM(PATIENT)),36),'') AS PATIENT,
            NULLIF(LEFT(LTRIM(RTRIM(ENCOUNTER)),36),'') AS ENCOUNTER,
            NULLIF(LEFT(LTRIM(RTRIM(CODE)),20),'') AS CODE,
            NULLIF(LEFT(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(DESCRIPTION)),'\r',' '),'\n',' '),'\t',' '),255),'') AS DESCRIPTION,
            NULLIF(LEFT(LTRIM(RTRIM([VALUE])),255),'') AS [VALUE],
            NULLIF(LEFT(LTRIM(RTRIM(UNITS)),20),'') AS UNITS,
            NULLIF(LEFT(LTRIM(RTRIM([TYPE])),50),'') AS [TYPE],
            update_at
        INTO #src
        FROM DW_Synthea_Landing.dbo.Landing_Observations
        WHERE PATIENT IS NOT NULL AND LTRIM(RTRIM(PATIENT)) <> '';

        MERGE DW_Synthea_Staging.dbo.Staging_Observations AS t
        USING #src AS s
        ON ISNULL(t.PATIENT,'') = ISNULL(s.PATIENT,'') AND ISNULL(t.DATE,'1900-01-01') = ISNULL(s.DATE,'1900-01-01') AND ISNULL(t.CODE,'') = ISNULL(s.CODE,'')
        WHEN MATCHED AND (t.update_at < s.update_at) THEN
            UPDATE SET
                ENCOUNTER = s.ENCOUNTER,
                DESCRIPTION = s.DESCRIPTION,
                VALUE = s.[VALUE],
                UNITS = s.UNITS,
                [TYPE] = s.[TYPE],
                update_at = s.update_at
        WHEN NOT MATCHED BY TARGET THEN
            INSERT ([DATE],PATIENT,ENCOUNTER,CODE,DESCRIPTION,VALUE,UNITS,[TYPE],create_at,update_at)
            VALUES (s.[DATE],s.PATIENT,s.ENCOUNTER,s.CODE,s.DESCRIPTION,s.[VALUE],s.UNITS,s.[TYPE],SYSUTCDATETIME(),s.update_at);

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

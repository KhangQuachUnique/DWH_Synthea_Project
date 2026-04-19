-- Transform Landing_Devices to Staging_Devices (chuẩn hóa, làm sạch)
CREATE OR ALTER PROCEDURE dbo.usp_transform_landing_to_staging_devices
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    BEGIN TRAN;
    BEGIN TRY
        SELECT
            TRY_CAST(START AS DATE) AS START,
            TRY_CAST(STOP AS DATE) AS STOP,
            NULLIF(LEFT(LTRIM(RTRIM(PATIENT)),36),'') AS PATIENT,
            NULLIF(LEFT(LTRIM(RTRIM(ENCOUNTER)),36),'') AS ENCOUNTER,
            NULLIF(LEFT(LTRIM(RTRIM(CODE)),20),'') AS CODE,
            NULLIF(LEFT(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(DESCRIPTION)),'\r',' '),'\n',' '),'\t',' '),255),'') AS DESCRIPTION,
            NULLIF(LEFT(LTRIM(RTRIM(UDI)),500),'') AS UDI,
            update_at
        INTO #src
        FROM DW_Synthea_Landing.dbo.Landing_Devices
        WHERE PATIENT IS NOT NULL AND LTRIM(RTRIM(PATIENT)) <> '';

        MERGE DW_Synthea_Staging.dbo.Staging_Devices AS t
        USING #src AS s
        ON ISNULL(t.PATIENT,'') = ISNULL(s.PATIENT,'') AND ISNULL(t.START,'1900-01-01') = ISNULL(s.START,'1900-01-01') AND ISNULL(t.CODE,'') = ISNULL(s.CODE,'')
        WHEN MATCHED AND (t.update_at < s.update_at) THEN
            UPDATE SET
                STOP = s.STOP,
                ENCOUNTER = s.ENCOUNTER,
                DESCRIPTION = s.DESCRIPTION,
                UDI = s.UDI,
                update_at = s.update_at
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (START,STOP,PATIENT,ENCOUNTER,CODE,DESCRIPTION,UDI,create_at,update_at)
            VALUES (s.START,s.STOP,s.PATIENT,s.ENCOUNTER,s.CODE,s.DESCRIPTION,s.UDI,SYSUTCDATETIME(),s.update_at);

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

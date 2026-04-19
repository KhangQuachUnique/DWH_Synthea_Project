-- Transform Landing_Imaging_Studies to Staging_Imaging_Studies (chuẩn hóa, làm sạch)
CREATE OR ALTER PROCEDURE dbo.usp_transform_landing_to_staging_imaging_studies
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    BEGIN TRAN;
    BEGIN TRY
        SELECT
            LEFT(LTRIM(RTRIM(Id)),36) AS Id,
            TRY_CAST([DATE] AS DATE) AS [DATE],
            NULLIF(LEFT(LTRIM(RTRIM(PATIENT)),36),'') AS PATIENT,
            NULLIF(LEFT(LTRIM(RTRIM(ENCOUNTER)),36),'') AS ENCOUNTER,
            NULLIF(LEFT(LTRIM(RTRIM(BODYSITE_CODE)),20),'') AS BODYSITE_CODE,
            NULLIF(LEFT(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(BODYSITE_DESCRIPTION)),'\r',' '),'\n',' '),'\t',' '),255),'') AS BODYSITE_DESCRIPTION,
            NULLIF(LEFT(LTRIM(RTRIM(MODALITY_CODE)),5),'') AS MODALITY_CODE,
            NULLIF(LEFT(LTRIM(RTRIM(MODALITY_DESCRIPTION)),50),'') AS MODALITY_DESCRIPTION,
            NULLIF(LEFT(LTRIM(RTRIM(SOP_CODE)),64),'') AS SOP_CODE,
            NULLIF(LEFT(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(SOP_DESCRIPTION)),'\r',' '),'\n',' '),'\t',' '),255),'') AS SOP_DESCRIPTION,
            update_at
        INTO #src
        FROM DW_Synthea_Landing.dbo.Landing_Imaging_Studies
        WHERE Id IS NOT NULL AND LTRIM(RTRIM(Id)) <> '';

        MERGE DW_Synthea_Staging.dbo.Staging_Imaging_Studies AS t
        USING #src AS s
        ON t.Id = s.Id
        WHEN MATCHED AND (t.update_at < s.update_at) THEN
            UPDATE SET
                [DATE] = s.[DATE],
                PATIENT = s.PATIENT,
                ENCOUNTER = s.ENCOUNTER,
                BODYSITE_CODE = s.BODYSITE_CODE,
                BODYSITE_DESCRIPTION = s.BODYSITE_DESCRIPTION,
                MODALITY_CODE = s.MODALITY_CODE,
                MODALITY_DESCRIPTION = s.MODALITY_DESCRIPTION,
                SOP_CODE = s.SOP_CODE,
                SOP_DESCRIPTION = s.SOP_DESCRIPTION,
                update_at = s.update_at
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (Id,[DATE],PATIENT,ENCOUNTER,BODYSITE_CODE,BODYSITE_DESCRIPTION,MODALITY_CODE,MODALITY_DESCRIPTION,SOP_CODE,SOP_DESCRIPTION,create_at,update_at)
            VALUES (s.Id,s.[DATE],s.PATIENT,s.ENCOUNTER,s.BODYSITE_CODE,s.BODYSITE_DESCRIPTION,s.MODALITY_CODE,s.MODALITY_DESCRIPTION,s.SOP_CODE,s.SOP_DESCRIPTION,SYSUTCDATETIME(),s.update_at);

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

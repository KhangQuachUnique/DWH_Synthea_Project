-- Transform Landing_Encounters to Staging_Encounters (chuẩn hóa, làm sạch)
CREATE OR ALTER PROCEDURE dbo.usp_transform_landing_to_staging_encounters
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    BEGIN TRAN;
    BEGIN TRY
        IF OBJECT_ID(N'DW_Synthea_Landing.dbo.Landing_Encounters', N'U') IS NULL
           OR OBJECT_ID(N'DW_Synthea_Staging.dbo.Staging_Encounters', N'U') IS NULL
            THROW 50001, 'Missing required table: Landing_Encounters or Staging_Encounters.', 1;

        DROP TABLE IF EXISTS #src;

        SELECT
            LEFT(LTRIM(RTRIM(Id)),36) AS Id,
            TRY_CAST(START AS DATETIME2) AS START,
            TRY_CAST(STOP AS DATETIME2) AS STOP,
            NULLIF(LEFT(LTRIM(RTRIM(PATIENT)),36),'') AS PATIENT,
            NULLIF(LEFT(LTRIM(RTRIM(ORGANIZATION)),36),'') AS ORGANIZATION,
            NULLIF(LEFT(LTRIM(RTRIM(PROVIDER)),36),'') AS PROVIDER,
            NULLIF(LEFT(LTRIM(RTRIM(PAYER)),36),'') AS PAYER,
            NULLIF(LEFT(LTRIM(RTRIM(ENCOUNTERCLASS)),50),'') AS ENCOUNTERCLASS,
            NULLIF(LEFT(LTRIM(RTRIM(CODE)),20),'') AS CODE,
            NULLIF(LEFT(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(DESCRIPTION)),'\r',' '),'\n',' '),'\t',' '),255),'') AS DESCRIPTION,
            TRY_CAST(NULLIF(BASE_ENCOUNTER_COST,'') AS DECIMAL(18,2)) AS BASE_ENCOUNTER_COST,
            TRY_CAST(NULLIF(TOTAL_CLAIM_COST,'') AS DECIMAL(18,2)) AS TOTAL_CLAIM_COST,
            TRY_CAST(NULLIF(PAYER_COVERAGE,'') AS DECIMAL(18,2)) AS PAYER_COVERAGE,
            NULLIF(LEFT(LTRIM(RTRIM(REASONCODE)),20),'') AS REASONCODE,
            NULLIF(LEFT(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(REASONDESCRIPTION)),'\r',' '),'\n',' '),'\t',' '),255),'') AS REASONDESCRIPTION,
            update_at
        INTO #src
        FROM DW_Synthea_Landing.dbo.Landing_Encounters
        WHERE Id IS NOT NULL AND LTRIM(RTRIM(Id)) <> '';

        MERGE DW_Synthea_Staging.dbo.Staging_Encounters AS t
        USING #src AS s
        ON t.Id = s.Id
        WHEN MATCHED AND (t.update_at < s.update_at) THEN
            UPDATE SET
                START = s.START,
                STOP = s.STOP,
                PATIENT = s.PATIENT,
                ORGANIZATION = s.ORGANIZATION,
                PROVIDER = s.PROVIDER,
                PAYER = s.PAYER,
                ENCOUNTERCLASS = s.ENCOUNTERCLASS,
                CODE = s.CODE,
                DESCRIPTION = s.DESCRIPTION,
                BASE_ENCOUNTER_COST = s.BASE_ENCOUNTER_COST,
                TOTAL_CLAIM_COST = s.TOTAL_CLAIM_COST,
                PAYER_COVERAGE = s.PAYER_COVERAGE,
                REASONCODE = s.REASONCODE,
                REASONDESCRIPTION = s.REASONDESCRIPTION,
                update_at = s.update_at
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (Id,START,STOP,PATIENT,ORGANIZATION,PROVIDER,PAYER,ENCOUNTERCLASS,CODE,DESCRIPTION,BASE_ENCOUNTER_COST,TOTAL_CLAIM_COST,PAYER_COVERAGE,REASONCODE,REASONDESCRIPTION,create_at,update_at)
            VALUES (s.Id,s.START,s.STOP,s.PATIENT,s.ORGANIZATION,s.PROVIDER,s.PAYER,s.ENCOUNTERCLASS,s.CODE,s.DESCRIPTION,s.BASE_ENCOUNTER_COST,s.TOTAL_CLAIM_COST,s.PAYER_COVERAGE,s.REASONCODE,s.REASONDESCRIPTION,SYSUTCDATETIME(),s.update_at);

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

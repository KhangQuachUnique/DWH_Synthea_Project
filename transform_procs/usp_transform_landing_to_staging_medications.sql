-- Transform Landing_Medications to Staging_Medications (chuẩn hóa, làm sạch)
CREATE OR ALTER PROCEDURE dbo.usp_transform_landing_to_staging_medications
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    BEGIN TRAN;
    BEGIN TRY
        IF OBJECT_ID(N'DW_Synthea_Landing.dbo.Landing_Medications', N'U') IS NULL
           OR OBJECT_ID(N'DW_Synthea_Staging.dbo.Staging_Medications', N'U') IS NULL
            THROW 50001, 'Missing required table: Landing_Medications or Staging_Medications.', 1;

        DROP TABLE IF EXISTS #src;

        SELECT
            TRY_CAST(START AS DATE) AS START,
            TRY_CAST(STOP AS DATE) AS STOP,
            NULLIF(LEFT(LTRIM(RTRIM(PATIENT)),36),'') AS PATIENT,
            NULLIF(LEFT(LTRIM(RTRIM(PAYER)),36),'') AS PAYER,
            NULLIF(LEFT(LTRIM(RTRIM(ENCOUNTER)),36),'') AS ENCOUNTER,
            NULLIF(LEFT(LTRIM(RTRIM(CODE)),20),'') AS CODE,
            NULLIF(LEFT(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(DESCRIPTION)),'\r',' '),'\n',' '),'\t',' '),255),'') AS DESCRIPTION,
            TRY_CAST(NULLIF(BASE_COST,'') AS DECIMAL(18,2)) AS BASE_COST,
            TRY_CAST(NULLIF(PAYER_COVERAGE,'') AS DECIMAL(18,2)) AS PAYER_COVERAGE,
            TRY_CAST(NULLIF(DISPENSES,'') AS INT) AS DISPENSES,
            TRY_CAST(NULLIF(TOTALCOST,'') AS DECIMAL(18,2)) AS TOTALCOST,
            NULLIF(LEFT(LTRIM(RTRIM(REASONCODE)),20),'') AS REASONCODE,
            NULLIF(LEFT(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(REASONDESCRIPTION)),'\r',' '),'\n',' '),'\t',' '),255),'') AS REASONDESCRIPTION,
            update_at
        INTO #src
        FROM DW_Synthea_Landing.dbo.Landing_Medications
        WHERE PATIENT IS NOT NULL AND LTRIM(RTRIM(PATIENT)) <> '';

        MERGE DW_Synthea_Staging.dbo.Staging_Medications AS t
        USING #src AS s
        ON ISNULL(t.PATIENT,'') = ISNULL(s.PATIENT,'') AND ISNULL(t.START,'1900-01-01') = ISNULL(s.START,'1900-01-01') AND ISNULL(t.CODE,'') = ISNULL(s.CODE,'')
        WHEN MATCHED AND (t.update_at < s.update_at) THEN
            UPDATE SET
                STOP = s.STOP,
                PAYER = s.PAYER,
                ENCOUNTER = s.ENCOUNTER,
                DESCRIPTION = s.DESCRIPTION,
                BASE_COST = s.BASE_COST,
                PAYER_COVERAGE = s.PAYER_COVERAGE,
                DISPENSES = s.DISPENSES,
                TOTALCOST = s.TOTALCOST,
                REASONCODE = s.REASONCODE,
                REASONDESCRIPTION = s.REASONDESCRIPTION,
                update_at = s.update_at
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (START,STOP,PATIENT,PAYER,ENCOUNTER,CODE,DESCRIPTION,BASE_COST,PAYER_COVERAGE,DISPENSES,TOTALCOST,REASONCODE,REASONDESCRIPTION,create_at,update_at)
            VALUES (s.START,s.STOP,s.PATIENT,s.PAYER,s.ENCOUNTER,s.CODE,s.DESCRIPTION,s.BASE_COST,s.PAYER_COVERAGE,s.DISPENSES,s.TOTALCOST,s.REASONCODE,s.REASONDESCRIPTION,SYSUTCDATETIME(),s.update_at);

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

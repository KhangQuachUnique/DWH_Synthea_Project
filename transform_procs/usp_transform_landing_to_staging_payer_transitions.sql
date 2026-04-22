-- Transform Landing_Payer_Transitions to Staging_Payer_Transitions (chuẩn hóa, làm sạch)
CREATE OR ALTER PROCEDURE dbo.usp_transform_landing_to_staging_payer_transitions
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    BEGIN TRAN;
    BEGIN TRY
        IF OBJECT_ID(N'DW_Synthea_Landing.dbo.Landing_Payer_Transitions', N'U') IS NULL
           OR OBJECT_ID(N'DW_Synthea_Staging.dbo.Staging_Payer_Transitions', N'U') IS NULL
            THROW 50001, 'Missing required table: Landing_Payer_Transitions or Staging_Payer_Transitions.', 1;

        DROP TABLE IF EXISTS #src;

        SELECT
            NULLIF(LEFT(LTRIM(RTRIM(PATIENT)),36),'') AS PATIENT,
            TRY_CAST(NULLIF(START_YEAR,'') AS INT) AS START_YEAR,
            TRY_CAST(NULLIF(END_YEAR,'') AS INT) AS END_YEAR,
            NULLIF(LEFT(LTRIM(RTRIM(PAYER)),36),'') AS PAYER,
            NULLIF(LEFT(LTRIM(RTRIM(OWNERSHIP)),50),'') AS OWNERSHIP,
            batch_id,
            update_at
        INTO #src
        FROM DW_Synthea_Landing.dbo.Landing_Payer_Transitions
        WHERE PATIENT IS NOT NULL AND LTRIM(RTRIM(PATIENT)) <> '';

        MERGE DW_Synthea_Staging.dbo.Staging_Payer_Transitions AS t
        USING #src AS s
        ON ISNULL(t.PATIENT,'') = ISNULL(s.PATIENT,'') AND ISNULL(t.START_YEAR,-1) = ISNULL(s.START_YEAR,-1) AND ISNULL(t.PAYER,'') = ISNULL(s.PAYER,'')
        WHEN MATCHED AND (t.update_at < s.update_at) THEN
            UPDATE SET
                END_YEAR = s.END_YEAR,
                OWNERSHIP = s.OWNERSHIP,
                update_at = s.update_at
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (PATIENT,START_YEAR,END_YEAR,PAYER,OWNERSHIP,create_at,update_at)
            VALUES (s.PATIENT,s.START_YEAR,s.END_YEAR,s.PAYER,s.OWNERSHIP,SYSUTCDATETIME(),s.update_at);

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

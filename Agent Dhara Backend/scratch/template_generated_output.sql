-- ETL SQL — Agent Dhara — plan_id=plan_1779876861
-- dialect=tsql — review before executing against production.

-- ⚠ 1 item(s) flagged for manual review before production run.
--   [data_1.xml] phone: Phone column has very high cardinality (100 unique / 100 rows)

-- ============================================================
-- Create configuration, watermark and logging tables if not exists
-- ============================================================
IF OBJECT_ID('dbo.etl_log', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.etl_log (
        id INT IDENTITY(1,1) PRIMARY KEY,
        process_name VARCHAR(100) NOT NULL,
        start_time DATETIME NOT NULL,
        end_time DATETIME NULL,
        status VARCHAR(20) NOT NULL,
        error_message VARCHAR(MAX) NULL
    );
END;
GO


IF OBJECT_ID('dbo.etl_invalid_values', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.etl_invalid_values (
        column_name VARCHAR(256),
        invalid_value VARCHAR(256),
        PRIMARY KEY (column_name, invalid_value)
    );
END;
GO

IF OBJECT_ID('dbo.etl_rejects', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.etl_rejects (
        id INT IDENTITY(1,1) PRIMARY KEY,
        process_name VARCHAR(100) NOT NULL,
        table_name VARCHAR(100) NOT NULL,
        row_data VARCHAR(MAX) NOT NULL,
        error_reason VARCHAR(MAX) NOT NULL,
        rejected_at DATETIME DEFAULT GETDATE()
    );
END;
GO

IF OBJECT_ID('dbo.etl_watermark', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.etl_watermark (
        process_name VARCHAR(256) PRIMARY KEY,
        last_run_time DATETIME NOT NULL
    );
END;
GO



-- === dataset: data_1.json === 
IF OBJECT_ID('dbo.etl_clean_json', 'P') IS NOT NULL DROP PROCEDURE dbo.etl_clean_json;
GO
CREATE PROCEDURE dbo.etl_clean_json
    @load_type VARCHAR(20) = 'FULL',
    @last_run DATETIME = NULL
AS BEGIN
    SET NOCOUNT ON;
    -- Retrieve last run watermark if not provided
    IF @load_type = 'INCREMENTAL' AND @last_run IS NULL
    BEGIN
        SELECT @last_run = last_run_time FROM dbo.etl_watermark WHERE process_name = 'etl_clean_json';
    END;

    INSERT INTO dbo.etl_log (process_name, start_time, status)
    VALUES ('etl_clean_json', GETDATE(), 'RUNNING');
    DECLARE @run_id INT = SCOPE_IDENTITY();

    BEGIN TRY
        BEGIN TRAN;

        -- Initialize Clean Table Structure
        IF OBJECT_ID('data_1.json_Clean', 'U') IS NULL
        BEGIN
            SELECT * INTO [data_1].[json_Clean] FROM [data_1].[json] WHERE 1=0;
            ALTER TABLE [data_1].[json_Clean] ADD etl_created_at DATETIME DEFAULT GETDATE();
            ALTER TABLE [data_1].[json_Clean] ADD etl_updated_at DATETIME DEFAULT GETDATE();
            ALTER TABLE [data_1].[json_Clean] ADD etl_batch_id INT;
            ALTER TABLE [data_1].[json_Clean] ADD CONSTRAINT [PK_json_Clean] PRIMARY KEY ([id]);
            CREATE NONCLUSTERED INDEX idx_json_Clean_id ON [data_1].[json_Clean]([id]);
        END

        -- Create Staging Table with raw column types to preserve raw strings
        IF OBJECT_ID('tempdb..#json_Staging') IS NOT NULL DROP TABLE #json_Staging;
        CREATE TABLE #json_Staging ([id] NVARCHAR(MAX) NULL, [name] NVARCHAR(MAX) NULL, [department] NVARCHAR(MAX) NULL, [phone] NVARCHAR(MAX) NULL, [email] NVARCHAR(MAX) NULL, [age] NVARCHAR(MAX) NULL, [salary] NVARCHAR(MAX) NULL, etl_batch_id INT NULL);

        -- Copy data from Raw to Staging
            INSERT INTO #json_Staging ([id], [name], [department], [phone], [email], [age], [salary], etl_batch_id)
            SELECT [id], [name], [department], [phone], [email], [age], [salary], @run_id FROM [data_1].[json];

        -- Single-Pass expression updates on #json_Staging
        UPDATE #json_Staging
        SET [age] = LOWER(LTRIM(RTRIM(CAST([age] AS NVARCHAR(MAX))))),
            [department] = LOWER(LTRIM(RTRIM(CAST([department] AS NVARCHAR(MAX))))),
            [email] = LOWER(LTRIM(RTRIM(LTRIM(RTRIM(CAST([email] AS NVARCHAR(MAX))))))),
            [name] = LOWER(LTRIM(RTRIM(CAST([name] AS NVARCHAR(MAX))))),
            [phone] = REPLACE(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(CAST([phone] AS NVARCHAR(MAX)))), N'-', N''), N' ', N''), N'(', N''), N')', N'')
        WHERE 1=1;

        -- Quarantine rows where primary key [id] is NULL to dbo.etl_rejects
        INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)
        SELECT 'etl_clean_json', 'data_1.json_Clean',
               (SELECT r.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
               'Primary key [id] is NULL'
        FROM #json_Staging r
        WHERE r.[id] IS NULL;

        DELETE FROM #json_Staging WHERE [id] IS NULL;

        -- Quarantine invalid emails from #json_Staging.[email] to dbo.etl_rejects
        INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)
        SELECT 'etl_clean_json', 'data_1.json_Clean',
               (SELECT r.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
               'Column [email] with value ' + CAST(r.[email] AS NVARCHAR(MAX)) + ' is not a valid email format'
        FROM #json_Staging r
        WHERE r.[email] IS NOT NULL AND NOT (CAST(r.[email] AS NVARCHAR(MAX)) LIKE '%_@_%._%');

        DELETE FROM #json_Staging WHERE [email] IS NOT NULL AND NOT (CAST([email] AS NVARCHAR(MAX)) LIKE '%_@_%._%');

        -- Quarantine invalid phones from #json_Staging.[phone] to dbo.etl_rejects
        INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)
        SELECT 'etl_clean_json', 'data_1.json_Clean',
               (SELECT r.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
               'Column [phone] with value ' + CAST(r.[phone] AS NVARCHAR(MAX)) + ' is not a valid phone format'
        FROM #json_Staging r
        WHERE r.[phone] IS NOT NULL AND (LEN(REPLACE(REPLACE(REPLACE(REPLACE(CAST(r.[phone] AS NVARCHAR(200)), N'-', N''), N' ', N''), N'(', N''), N')', N'')) < 7 OR REPLACE(REPLACE(REPLACE(REPLACE(CAST(r.[phone] AS NVARCHAR(200)), N'-', N''), N' ', N''), N'(', N''), N')', N'') LIKE '%[^0-9]%');

        DELETE FROM #json_Staging WHERE [phone] IS NOT NULL AND (LEN(REPLACE(REPLACE(REPLACE(REPLACE(CAST([phone] AS NVARCHAR(200)), N'-', N''), N' ', N''), N'(', N''), N')', N'')) < 7 OR REPLACE(REPLACE(REPLACE(REPLACE(CAST([phone] AS NVARCHAR(200)), N'-', N''), N' ', N''), N'(', N''), N')', N'') LIKE '%[^0-9]%');

        -- Copy fully transformed data from Staging to target Clean table
        IF @load_type = 'FULL' OR @last_run IS NULL
        BEGIN
            TRUNCATE TABLE [data_1].[json_Clean];
            INSERT INTO [data_1].[json_Clean] ([age], [department], [email], [id], [name], [phone], [salary], etl_batch_id, etl_created_at, etl_updated_at)
            SELECT [age], [department], [email], [id], [name], [phone], [salary], etl_batch_id, GETDATE(), GETDATE() FROM #json_Staging;
        END
        ELSE
        BEGIN
            DELETE FROM [data_1].[json_Clean] WHERE [id] IN (SELECT [id] FROM #json_Staging);
            INSERT INTO [data_1].[json_Clean] ([age], [department], [email], [id], [name], [phone], [salary], etl_batch_id, etl_created_at, etl_updated_at)
            SELECT [age], [department], [email], [id], [name], [phone], [salary], etl_batch_id, GETDATE(), GETDATE() FROM #json_Staging;
        END;

        IF OBJECT_ID('tempdb..#json_Staging') IS NOT NULL DROP TABLE #json_Staging;


        -- Update process watermark
        IF @load_type = 'INCREMENTAL' OR @last_run IS NULL
        BEGIN
            MERGE INTO dbo.etl_watermark AS target
            USING (SELECT 'etl_clean_json' AS process_name) AS source
            ON target.process_name = source.process_name
            WHEN MATCHED THEN
                UPDATE SET last_run_time = GETDATE()
            WHEN NOT MATCHED THEN
                INSERT (process_name, last_run_time) VALUES (source.process_name, GETDATE());
        END
        COMMIT;

        -- Log success
        UPDATE dbo.etl_log
        SET end_time = GETDATE(), status = 'SUCCESS'
        WHERE id = @run_id;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;
        DECLARE @err VARCHAR(MAX) = ERROR_MESSAGE();
        UPDATE dbo.etl_log
        SET end_time = GETDATE(), status = 'FAILED', error_message = @err
        WHERE id = @run_id;
        THROW;
    END CATCH;
END;
GO

-- === dataset: data_1.xml === 
IF OBJECT_ID('dbo.etl_clean_xml', 'P') IS NOT NULL DROP PROCEDURE dbo.etl_clean_xml;
GO
CREATE PROCEDURE dbo.etl_clean_xml
    @load_type VARCHAR(20) = 'FULL',
    @last_run DATETIME = NULL
AS BEGIN
    SET NOCOUNT ON;
    -- Retrieve last run watermark if not provided
    IF @load_type = 'INCREMENTAL' AND @last_run IS NULL
    BEGIN
        SELECT @last_run = last_run_time FROM dbo.etl_watermark WHERE process_name = 'etl_clean_xml';
    END;

    INSERT INTO dbo.etl_log (process_name, start_time, status)
    VALUES ('etl_clean_xml', GETDATE(), 'RUNNING');
    DECLARE @run_id INT = SCOPE_IDENTITY();

    BEGIN TRY
        BEGIN TRAN;

        -- Initialize Clean Table Structure
        IF OBJECT_ID('data_1.xml_Clean', 'U') IS NULL
        BEGIN
            SELECT * INTO [data_1].[xml_Clean] FROM [data_1].[xml] WHERE 1=0;
            ALTER TABLE [data_1].[xml_Clean] ADD etl_created_at DATETIME DEFAULT GETDATE();
            ALTER TABLE [data_1].[xml_Clean] ADD etl_updated_at DATETIME DEFAULT GETDATE();
            ALTER TABLE [data_1].[xml_Clean] ADD etl_batch_id INT;
            ALTER TABLE [data_1].[xml_Clean] ADD CONSTRAINT [PK_xml_Clean] PRIMARY KEY ([id]);
            CREATE NONCLUSTERED INDEX idx_xml_Clean_id ON [data_1].[xml_Clean]([id]);
        END

        -- Create Staging Table with raw column types to preserve raw strings
        IF OBJECT_ID('tempdb..#xml_Staging') IS NOT NULL DROP TABLE #xml_Staging;
        CREATE TABLE #xml_Staging ([id] NVARCHAR(MAX) NULL, [name] NVARCHAR(MAX) NULL, [department] NVARCHAR(MAX) NULL, [phone] NVARCHAR(MAX) NULL, [email] NVARCHAR(MAX) NULL, [age] NVARCHAR(MAX) NULL, [salary] NVARCHAR(MAX) NULL, etl_batch_id INT NULL);

        -- Copy data from Raw to Staging
            INSERT INTO #xml_Staging ([id], [name], [department], [phone], [email], [age], [salary], etl_batch_id)
            SELECT [id], [name], [department], [phone], [email], [age], [salary], @run_id FROM [data_1].[xml];

        -- Single-Pass expression updates on #xml_Staging
        UPDATE #xml_Staging
        SET [age] = LOWER(LTRIM(RTRIM(CAST([age] AS NVARCHAR(MAX))))),
            [department] = LOWER(LTRIM(RTRIM(CAST([department] AS NVARCHAR(MAX))))),
            [email] = LOWER(LTRIM(RTRIM(LTRIM(RTRIM(CAST([email] AS NVARCHAR(MAX))))))),
            [name] = LOWER(LTRIM(RTRIM(CAST([name] AS NVARCHAR(MAX))))),
            [phone] = REPLACE(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(CAST([phone] AS NVARCHAR(MAX)))), N'-', N''), N' ', N''), N'(', N''), N')', N'')
        WHERE 1=1;

        -- Quarantine rows where primary key [id] is NULL to dbo.etl_rejects
        INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)
        SELECT 'etl_clean_xml', 'data_1.xml_Clean',
               (SELECT r.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
               'Primary key [id] is NULL'
        FROM #xml_Staging r
        WHERE r.[id] IS NULL;

        DELETE FROM #xml_Staging WHERE [id] IS NULL;

        -- Quarantine invalid emails from #xml_Staging.[email] to dbo.etl_rejects
        INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)
        SELECT 'etl_clean_xml', 'data_1.xml_Clean',
               (SELECT r.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
               'Column [email] with value ' + CAST(r.[email] AS NVARCHAR(MAX)) + ' is not a valid email format'
        FROM #xml_Staging r
        WHERE r.[email] IS NOT NULL AND NOT (CAST(r.[email] AS NVARCHAR(MAX)) LIKE '%_@_%._%');

        DELETE FROM #xml_Staging WHERE [email] IS NOT NULL AND NOT (CAST([email] AS NVARCHAR(MAX)) LIKE '%_@_%._%');

        -- Quarantine invalid phones from #xml_Staging.[phone] to dbo.etl_rejects
        INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)
        SELECT 'etl_clean_xml', 'data_1.xml_Clean',
               (SELECT r.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
               'Column [phone] with value ' + CAST(r.[phone] AS NVARCHAR(MAX)) + ' is not a valid phone format'
        FROM #xml_Staging r
        WHERE r.[phone] IS NOT NULL AND (LEN(REPLACE(REPLACE(REPLACE(REPLACE(CAST(r.[phone] AS NVARCHAR(200)), N'-', N''), N' ', N''), N'(', N''), N')', N'')) < 7 OR REPLACE(REPLACE(REPLACE(REPLACE(CAST(r.[phone] AS NVARCHAR(200)), N'-', N''), N' ', N''), N'(', N''), N')', N'') LIKE '%[^0-9]%');

        DELETE FROM #xml_Staging WHERE [phone] IS NOT NULL AND (LEN(REPLACE(REPLACE(REPLACE(REPLACE(CAST([phone] AS NVARCHAR(200)), N'-', N''), N' ', N''), N'(', N''), N')', N'')) < 7 OR REPLACE(REPLACE(REPLACE(REPLACE(CAST([phone] AS NVARCHAR(200)), N'-', N''), N' ', N''), N'(', N''), N')', N'') LIKE '%[^0-9]%');

        -- Copy fully transformed data from Staging to target Clean table
        IF @load_type = 'FULL' OR @last_run IS NULL
        BEGIN
            TRUNCATE TABLE [data_1].[xml_Clean];
            INSERT INTO [data_1].[xml_Clean] ([age], [department], [email], [id], [name], [phone], [salary], etl_batch_id, etl_created_at, etl_updated_at)
            SELECT [age], [department], [email], [id], [name], [phone], [salary], etl_batch_id, GETDATE(), GETDATE() FROM #xml_Staging;
        END
        ELSE
        BEGIN
            DELETE FROM [data_1].[xml_Clean] WHERE [id] IN (SELECT [id] FROM #xml_Staging);
            INSERT INTO [data_1].[xml_Clean] ([age], [department], [email], [id], [name], [phone], [salary], etl_batch_id, etl_created_at, etl_updated_at)
            SELECT [age], [department], [email], [id], [name], [phone], [salary], etl_batch_id, GETDATE(), GETDATE() FROM #xml_Staging;
        END;

        IF OBJECT_ID('tempdb..#xml_Staging') IS NOT NULL DROP TABLE #xml_Staging;


        -- Update process watermark
        IF @load_type = 'INCREMENTAL' OR @last_run IS NULL
        BEGIN
            MERGE INTO dbo.etl_watermark AS target
            USING (SELECT 'etl_clean_xml' AS process_name) AS source
            ON target.process_name = source.process_name
            WHEN MATCHED THEN
                UPDATE SET last_run_time = GETDATE()
            WHEN NOT MATCHED THEN
                INSERT (process_name, last_run_time) VALUES (source.process_name, GETDATE());
        END
        COMMIT;

        -- Log success
        UPDATE dbo.etl_log
        SET end_time = GETDATE(), status = 'SUCCESS'
        WHERE id = @run_id;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;
        DECLARE @err VARCHAR(MAX) = ERROR_MESSAGE();
        UPDATE dbo.etl_log
        SET end_time = GETDATE(), status = 'FAILED', error_message = @err
        WHERE id = @run_id;
        THROW;
    END CATCH;
END;
GO

-- ============================================================
-- Master Orchestrator Stored Procedure
-- ============================================================
IF OBJECT_ID('dbo.etl_main', 'P') IS NOT NULL DROP PROCEDURE dbo.etl_main;
GO
CREATE PROCEDURE dbo.etl_main
    @load_type VARCHAR(20) = 'FULL',
    @last_run DATETIME = NULL
AS BEGIN
    SET NOCOUNT ON;
    -- Retrieve last run watermark if not provided
    IF @load_type = 'INCREMENTAL' AND @last_run IS NULL
    BEGIN
        SELECT @last_run = last_run_time FROM dbo.etl_watermark WHERE process_name = 'etl_main';
    END;

    INSERT INTO dbo.etl_log (process_name, start_time, status)
    VALUES ('etl_main', GETDATE(), 'RUNNING');
    DECLARE @run_id INT = SCOPE_IDENTITY();

    BEGIN TRY
        EXEC dbo.etl_clean_json @load_type = @load_type, @last_run = @last_run;
        EXEC dbo.etl_clean_xml @load_type = @load_type, @last_run = @last_run;

        -- Update master process watermark
        IF @load_type = 'INCREMENTAL' OR @last_run IS NULL
        BEGIN
            MERGE INTO dbo.etl_watermark AS target
            USING (SELECT 'etl_main' AS process_name) AS source
            ON target.process_name = source.process_name
            WHEN MATCHED THEN
                UPDATE SET last_run_time = COALESCE((SELECT MAX(last_run_time) FROM dbo.etl_watermark WHERE process_name LIKE 'etl_clean_%'), GETDATE())
            WHEN NOT MATCHED THEN
                INSERT (process_name, last_run_time) VALUES (source.process_name, COALESCE((SELECT MAX(last_run_time) FROM dbo.etl_watermark WHERE process_name LIKE 'etl_clean_%'), GETDATE()));
        END

        UPDATE dbo.etl_log
        SET end_time = GETDATE(), status = 'SUCCESS'
        WHERE id = @run_id;
    END TRY
    BEGIN CATCH
        DECLARE @err VARCHAR(MAX) = ERROR_MESSAGE();
        UPDATE dbo.etl_log
        SET end_time = GETDATE(), status = 'FAILED', error_message = @err
        WHERE id = @run_id;
        THROW;
    END CATCH;
END;
GO


-- ── Staging / load order (connector manifest) ──
-- data_1.json_Clean: -- file staging required
-- data_1.xml_Clean: -- file staging required

-- Join data_1.json_Clean -> data_1.xml_Clean (one_to_one)
IF OBJECT_ID('dbo.vw_xml_Clean_Fact', 'V') IS NOT NULL DROP VIEW dbo.vw_xml_Clean_Fact;
GO
CREATE VIEW dbo.vw_xml_Clean_Fact AS
SELECT
        c.[id],
        c.[name],
        c.[department],
        c.[phone],
        c.[email],
        c.[age],
        c.[salary],
        p.[id] AS [json_Clean_id],
        p.[name] AS [json_Clean_name],
        p.[department] AS [json_Clean_department],
        p.[phone] AS [json_Clean_phone],
        p.[email] AS [json_Clean_email],
        p.[age] AS [json_Clean_age],
        p.[salary] AS [json_Clean_salary]
FROM [data_1].[xml_Clean_Clean] c
INNER JOIN [data_1].[json_Clean_Clean] p ON c.[id] = p.[id];
GO

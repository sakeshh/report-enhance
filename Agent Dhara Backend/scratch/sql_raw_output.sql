-- ETL SQL — Agent Dhara — plan_id=plan_1779786913
-- dialect=tsql — review before executing against production.

-- ⚠ 14 item(s) flagged for manual review before production run.
--   [dbo.Customers_Raw] City: 1 value(s) consisting only of punctuation/symbols
--   [dbo.Customers_Raw] CreatedDate: Mixed date formats in same column: {'YYYY-MM-DD': 17, 'MM/DD/YYYY': 1, 'DD-MM-YYYY': 2, 'YYYY/MM/DD': 2}. Standardize to ISO-8601.
--   [dbo.Customers_Raw] CreatedDate: 1 value(s) consisting only of punctuation/symbols
--   [dbo.Customers_Raw] CreatedDate: Multiple date formats detected: ISO(YYYY-MM-DD)=21, US(MM/DD/YYYY)=1
--   [dbo.Customers_Raw] CustomerName: 2 value(s) consisting only of punctuation/symbols
--   [dbo.Customers_Raw] CustomerName: 3 ALL-CAPS value(s) mixed with 42 mixed/lower-case
--   [dbo.Customers_Raw] Phone: 3 value(s) consisting only of punctuation/symbols
--   [dbo.Customers_Raw] Email: 1 value(s) consisting only of punctuation/symbols
--   [dbo.Customers_Raw] CustomerName: 1 value group(s) differ only by case/whitespace (45 raw -> 44 after normalization)
--   [dbo.Customers_Raw] Email: 1 value group(s) differ only by case/whitespace (44 raw -> 43 after normalization)
--   [dbo.Customers_Raw] City: 1 value group(s) differ only by case/whitespace (33 raw -> 32 after normalization)
--   [dbo.Customers_Raw] CreatedDate: 1 value group(s) differ only by case/whitespace (39 raw -> 38 after normalization)
--   [dbo.Customers_Raw] [Row-level]: Found 1 pair(s) of near-duplicate rows with string similarity >= 0.92
--   [dbo.Orders_Raw] [Row-level]: Found 51 pair(s) of near-duplicate rows with string similarity >= 0.92

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

IF OBJECT_ID('dbo.etl_default_values', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.etl_default_values (
        column_name VARCHAR(256) PRIMARY KEY,
        default_value VARCHAR(256) NOT NULL,
        data_type VARCHAR(50) NOT NULL
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



-- === dataset: dbo.Customers_Raw === 
IF OBJECT_ID('dbo.etl_clean_Customers_Raw', 'P') IS NOT NULL DROP PROCEDURE dbo.etl_clean_Customers_Raw;
GO
CREATE PROCEDURE dbo.etl_clean_Customers_Raw
    @load_type VARCHAR(20) = 'FULL',
    @last_run DATETIME = NULL
AS BEGIN
    SET NOCOUNT ON;
    -- Retrieve last run watermark if not provided
    IF @load_type = 'INCREMENTAL' AND @last_run IS NULL
    BEGIN
        SELECT @last_run = last_run_time FROM dbo.etl_watermark WHERE process_name = 'etl_clean_Customers_Raw';
    END;

    INSERT INTO dbo.etl_log (process_name, start_time, status)
    VALUES ('etl_clean_Customers_Raw', GETDATE(), 'RUNNING');
    DECLARE @run_id INT = SCOPE_IDENTITY();

    BEGIN TRY
        BEGIN TRAN;

        -- Initialize Clean Table Structure
        IF OBJECT_ID('dbo.Customers_Clean', 'U') IS NULL
        BEGIN
            SELECT * INTO [dbo].[Customers_Clean] FROM [dbo].[Customers_Raw] WHERE 1=0;
            ALTER TABLE [dbo].[Customers_Clean] ADD etl_created_at DATETIME DEFAULT GETDATE();
            ALTER TABLE [dbo].[Customers_Clean] ADD etl_updated_at DATETIME DEFAULT GETDATE();
            ALTER TABLE [dbo].[Customers_Clean] ADD etl_batch_id INT;
            ALTER TABLE [dbo].[Customers_Clean] ADD CONSTRAINT [PK_Customers_Raw_Clean] PRIMARY KEY ([CustomerID]);
            CREATE NONCLUSTERED INDEX idx_Customers_Raw_Clean_CreatedDate ON [dbo].[Customers_Clean]([CreatedDate]);
            CREATE NONCLUSTERED INDEX idx_Customers_Raw_Clean_CustomerID ON [dbo].[Customers_Clean]([CustomerID]);
        END

        -- Create Staging Table matching Clean structure
        IF OBJECT_ID('tempdb..#Customers_Raw_Staging') IS NOT NULL DROP TABLE #Customers_Raw_Staging;
        SELECT * INTO #Customers_Raw_Staging FROM [dbo].[Customers_Clean] WHERE 1=0;

        -- Copy data from Raw to Staging
        IF @load_type = 'FULL' OR @last_run IS NULL
        BEGIN
            ;WITH _raw_dedup AS (
                SELECT [CustomerID], [City], [CustomerName], [Email], [Phone], [CreatedDate], ROW_NUMBER() OVER (PARTITION BY [CustomerID], [Email] ORDER BY [CreatedDate] DESC) AS _rn
                FROM [dbo].[Customers_Raw]
            )
            INSERT INTO #Customers_Raw_Staging ([CustomerID], [City], [CustomerName], [Email], [Phone], [CreatedDate], etl_batch_id)
            SELECT [CustomerID], [City], [CustomerName], [Email], [Phone], [CreatedDate], @run_id FROM _raw_dedup WHERE _rn = 1;
        END
        ELSE
        BEGIN
            ;WITH _raw_dedup AS (
                SELECT [CustomerID], [City], [CustomerName], [Email], [Phone], [CreatedDate], ROW_NUMBER() OVER (PARTITION BY [CustomerID], [Email] ORDER BY [CreatedDate] DESC) AS _rn
                FROM [dbo].[Customers_Raw] WHERE [CreatedDate] > @last_run
            )
            INSERT INTO #Customers_Raw_Staging ([CustomerID], [City], [CustomerName], [Email], [Phone], [CreatedDate], etl_batch_id)
            SELECT [CustomerID], [City], [CustomerName], [Email], [Phone], [CreatedDate], @run_id FROM _raw_dedup WHERE _rn = 1;
        END

        -- Quarantine rows where primary key [CustomerID] is NULL to dbo.etl_rejects
        INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)
        SELECT 'etl_clean_Customers_Raw', 'dbo.Customers_Clean',
               (SELECT TOP 1 * FROM #Customers_Raw_Staging r2 WHERE r2.[CustomerID] IS NULL FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
               'Primary key [CustomerID] is NULL'
        FROM #Customers_Raw_Staging r
        WHERE r.[CustomerID] IS NULL;

        DELETE FROM #Customers_Raw_Staging WHERE [CustomerID] IS NULL;

        -- Quarantine invalid emails from #Customers_Raw_Staging.[Email] to dbo.etl_rejects
        INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)
        SELECT 'etl_clean_Customers_Raw', 'dbo.Customers_Clean',
               (SELECT TOP 1 * FROM #Customers_Raw_Staging r2 WHERE r2.[CustomerID] = r.[CustomerID] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
               'Column [Email] with value ' + CAST(r.[Email] AS NVARCHAR(MAX)) + ' is not a valid email format'
        FROM #Customers_Raw_Staging r
        WHERE r.[Email] IS NOT NULL AND NOT (CAST(r.[Email] AS NVARCHAR(MAX)) LIKE '%_@_%._%');

        DELETE FROM #Customers_Raw_Staging WHERE [Email] IS NOT NULL AND NOT (CAST([Email] AS NVARCHAR(MAX)) LIKE '%_@_%._%');

        -- Quarantine invalid dates from #Customers_Raw_Staging.[CreatedDate] to dbo.etl_rejects
        INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)
        SELECT 'etl_clean_Customers_Raw', 'dbo.Customers_Clean',
               (SELECT TOP 1 * FROM #Customers_Raw_Staging r2 WHERE r2.[CustomerID] = r.[CustomerID] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
               'Column [CreatedDate] with value ' + CAST(r.[CreatedDate] AS NVARCHAR(MAX)) + ' is not a valid date format'
        FROM #Customers_Raw_Staging r
        WHERE r.[CreatedDate] IS NOT NULL AND COALESCE(
            TRY_CONVERT(date, r.[CreatedDate], 120),
            TRY_CONVERT(date, r.[CreatedDate], 103),
            TRY_CONVERT(date, r.[CreatedDate], 101),
            TRY_CONVERT(date, r.[CreatedDate], 111)
        ) IS NULL;

        DELETE FROM #Customers_Raw_Staging
        WHERE [CreatedDate] IS NOT NULL AND COALESCE(
            TRY_CONVERT(date, [CreatedDate], 120),
            TRY_CONVERT(date, [CreatedDate], 103),
            TRY_CONVERT(date, [CreatedDate], 101),
            TRY_CONVERT(date, [CreatedDate], 111)
        ) IS NULL;

        -- Quarantine invalid phones from #Customers_Raw_Staging.[Phone] to dbo.etl_rejects
        INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)
        SELECT 'etl_clean_Customers_Raw', 'dbo.Customers_Clean',
               (SELECT TOP 1 * FROM #Customers_Raw_Staging r2 WHERE r2.[CustomerID] = r.[CustomerID] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
               'Column [Phone] with value ' + CAST(r.[Phone] AS NVARCHAR(MAX)) + ' is not a valid phone format'
        FROM #Customers_Raw_Staging r
        WHERE r.[Phone] IS NOT NULL AND (LEN(REPLACE(REPLACE(REPLACE(REPLACE(CAST(r.[Phone] AS NVARCHAR(200)), N'-', N''), N' ', N''), N'(', N''), N')', N'')) < 7 OR REPLACE(REPLACE(REPLACE(REPLACE(CAST(r.[Phone] AS NVARCHAR(200)), N'-', N''), N' ', N''), N'(', N''), N')', N'') LIKE '%[^0-9]%');

        DELETE FROM #Customers_Raw_Staging WHERE [Phone] IS NOT NULL AND (LEN(REPLACE(REPLACE(REPLACE(REPLACE(CAST([Phone] AS NVARCHAR(200)), N'-', N''), N' ', N''), N'(', N''), N')', N'')) < 7 OR REPLACE(REPLACE(REPLACE(REPLACE(CAST([Phone] AS NVARCHAR(200)), N'-', N''), N' ', N''), N'(', N''), N')', N'') LIKE '%[^0-9]%');

        -- Single-Pass expression updates on #Customers_Raw_Staging
        UPDATE #Customers_Raw_Staging
        SET [City] = LOWER(LTRIM(RTRIM(CAST([City] AS NVARCHAR(MAX))))),
            [CustomerName] = LOWER(LTRIM(RTRIM(CAST([CustomerName] AS NVARCHAR(MAX))))),
            [Email] = LOWER(LTRIM(RTRIM(LTRIM(RTRIM(CAST([Email] AS NVARCHAR(MAX))))))),
            [Phone] = REPLACE(REPLACE(REPLACE(REPLACE(CAST(LTRIM(RTRIM(CAST([Phone] AS NVARCHAR(MAX)))) AS NVARCHAR(200)), N'-', N''), N' ', N''), N'(', N''), N')', N''),
            [CreatedDate] = COALESCE(TRY_CONVERT(date, [CreatedDate], 120), TRY_CONVERT(date, [CreatedDate], 103), TRY_CONVERT(date, [CreatedDate], 101), TRY_CONVERT(date, [CreatedDate], 111))
        WHERE 1=1;

        -- Grouped config and null updates on #Customers_Raw_Staging
        UPDATE c
        SET c.[City] = c.[City],
            c.[CreatedDate] = c.[CreatedDate],
            c.[CustomerName] = c.[CustomerName],
            c.[Email] = c.[Email],
            c.[Phone] = c.[Phone]
        FROM #Customers_Raw_Staging c;

        -- Copy fully transformed data from Staging to target Clean table
        IF @load_type = 'FULL' OR @last_run IS NULL
        BEGIN
            TRUNCATE TABLE [dbo].[Customers_Clean];
            INSERT INTO [dbo].[Customers_Clean] ([City], [CreatedDate], [CustomerID], [CustomerName], [Email], [Phone], etl_batch_id, etl_created_at, etl_updated_at)
            SELECT [City], [CreatedDate], [CustomerID], [CustomerName], [Email], [Phone], etl_batch_id, GETDATE(), GETDATE() FROM #Customers_Raw_Staging;
        END
        ELSE
        BEGIN
            DELETE FROM [dbo].[Customers_Clean] WHERE [CustomerID] IN (SELECT [CustomerID] FROM #Customers_Raw_Staging);
            INSERT INTO [dbo].[Customers_Clean] ([City], [CreatedDate], [CustomerID], [CustomerName], [Email], [Phone], etl_batch_id, etl_created_at, etl_updated_at)
            SELECT [City], [CreatedDate], [CustomerID], [CustomerName], [Email], [Phone], etl_batch_id, GETDATE(), GETDATE() FROM #Customers_Raw_Staging;
        END;

        IF OBJECT_ID('tempdb..#Customers_Raw_Staging') IS NOT NULL DROP TABLE #Customers_Raw_Staging;


        -- Update process watermark
        IF @load_type = 'INCREMENTAL' OR @last_run IS NULL
        BEGIN
            MERGE INTO dbo.etl_watermark AS target
            USING (SELECT 'etl_clean_Customers_Raw' AS process_name) AS source
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

-- === dataset: dbo.Orders_Raw === 
IF OBJECT_ID('dbo.etl_clean_Orders_Raw', 'P') IS NOT NULL DROP PROCEDURE dbo.etl_clean_Orders_Raw;
GO
CREATE PROCEDURE dbo.etl_clean_Orders_Raw
    @load_type VARCHAR(20) = 'FULL',
    @last_run DATETIME = NULL
AS BEGIN
    SET NOCOUNT ON;
    -- Retrieve last run watermark if not provided
    IF @load_type = 'INCREMENTAL' AND @last_run IS NULL
    BEGIN
        SELECT @last_run = last_run_time FROM dbo.etl_watermark WHERE process_name = 'etl_clean_Orders_Raw';
    END;

    INSERT INTO dbo.etl_log (process_name, start_time, status)
    VALUES ('etl_clean_Orders_Raw', GETDATE(), 'RUNNING');
    DECLARE @run_id INT = SCOPE_IDENTITY();

    BEGIN TRY
        BEGIN TRAN;

        -- Initialize Clean Table Structure
        IF OBJECT_ID('dbo.Orders_Clean', 'U') IS NULL
        BEGIN
            SELECT * INTO [dbo].[Orders_Clean] FROM [dbo].[Orders_Raw] WHERE 1=0;
            ALTER TABLE [dbo].[Orders_Clean] ADD etl_created_at DATETIME DEFAULT GETDATE();
            ALTER TABLE [dbo].[Orders_Clean] ADD etl_updated_at DATETIME DEFAULT GETDATE();
            ALTER TABLE [dbo].[Orders_Clean] ADD etl_batch_id INT;
            ALTER TABLE [dbo].[Orders_Clean] ADD CONSTRAINT [PK_Orders_Raw_Clean] PRIMARY KEY ([OrderID]);
            CREATE NONCLUSTERED INDEX idx_Orders_Raw_Clean_CustomerID ON [dbo].[Orders_Clean]([CustomerID]);
            CREATE NONCLUSTERED INDEX idx_Orders_Raw_Clean_OrderDate ON [dbo].[Orders_Clean]([OrderDate]);
        END

        -- Create Staging Table matching Clean structure
        IF OBJECT_ID('tempdb..#Orders_Raw_Staging') IS NOT NULL DROP TABLE #Orders_Raw_Staging;
        SELECT * INTO #Orders_Raw_Staging FROM [dbo].[Orders_Clean] WHERE 1=0;

        -- Copy data from Raw to Staging
        IF @load_type = 'FULL' OR @last_run IS NULL
        BEGIN
            ;WITH _raw_dedup AS (
                SELECT [OrderDate], [OrderID], [OrderStatus], [OrderAmount], [CustomerID], ROW_NUMBER() OVER (PARTITION BY [OrderID], [CustomerID] ORDER BY [OrderDate] DESC) AS _rn
                FROM [dbo].[Orders_Raw]
            )
            INSERT INTO #Orders_Raw_Staging ([OrderDate], [OrderID], [OrderStatus], [OrderAmount], [CustomerID], etl_batch_id)
            SELECT [OrderDate], [OrderID], [OrderStatus], [OrderAmount], [CustomerID], @run_id FROM _raw_dedup WHERE _rn = 1;
        END
        ELSE
        BEGIN
            ;WITH _raw_dedup AS (
                SELECT [OrderDate], [OrderID], [OrderStatus], [OrderAmount], [CustomerID], ROW_NUMBER() OVER (PARTITION BY [OrderID], [CustomerID] ORDER BY [OrderDate] DESC) AS _rn
                FROM [dbo].[Orders_Raw] WHERE [OrderDate] > @last_run
            )
            INSERT INTO #Orders_Raw_Staging ([OrderDate], [OrderID], [OrderStatus], [OrderAmount], [CustomerID], etl_batch_id)
            SELECT [OrderDate], [OrderID], [OrderStatus], [OrderAmount], [CustomerID], @run_id FROM _raw_dedup WHERE _rn = 1;
        END

        -- Quarantine rows where primary key [OrderID] is NULL to dbo.etl_rejects
        INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)
        SELECT 'etl_clean_Orders_Raw', 'dbo.Orders_Clean',
               (SELECT TOP 1 * FROM #Orders_Raw_Staging r2 WHERE r2.[OrderID] IS NULL FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
               'Primary key [OrderID] is NULL'
        FROM #Orders_Raw_Staging r
        WHERE r.[OrderID] IS NULL;

        DELETE FROM #Orders_Raw_Staging WHERE [OrderID] IS NULL;

        -- Quarantine invalid dates from #Orders_Raw_Staging.[OrderDate] to dbo.etl_rejects
        INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)
        SELECT 'etl_clean_Orders_Raw', 'dbo.Orders_Clean',
               (SELECT TOP 1 * FROM #Orders_Raw_Staging r2 WHERE r2.[OrderID] = r.[OrderID] FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
               'Column [OrderDate] with value ' + CAST(r.[OrderDate] AS NVARCHAR(MAX)) + ' is not a valid date format'
        FROM #Orders_Raw_Staging r
        WHERE r.[OrderDate] IS NOT NULL AND COALESCE(
            TRY_CONVERT(date, r.[OrderDate], 120),
            TRY_CONVERT(date, r.[OrderDate], 103),
            TRY_CONVERT(date, r.[OrderDate], 101),
            TRY_CONVERT(date, r.[OrderDate], 111)
        ) IS NULL;

        DELETE FROM #Orders_Raw_Staging
        WHERE [OrderDate] IS NOT NULL AND COALESCE(
            TRY_CONVERT(date, [OrderDate], 120),
            TRY_CONVERT(date, [OrderDate], 103),
            TRY_CONVERT(date, [OrderDate], 101),
            TRY_CONVERT(date, [OrderDate], 111)
        ) IS NULL;

        -- Single-Pass expression updates on #Orders_Raw_Staging
        UPDATE #Orders_Raw_Staging
        SET [OrderStatus] = LOWER(LTRIM(RTRIM(CAST([OrderStatus] AS NVARCHAR(MAX))))),
            [OrderDate] = COALESCE(TRY_CONVERT(date, [OrderDate], 120), TRY_CONVERT(date, [OrderDate], 103), TRY_CONVERT(date, [OrderDate], 101), TRY_CONVERT(date, [OrderDate], 111))
        WHERE 1=1;

        -- Grouped config and null updates on #Orders_Raw_Staging
        UPDATE c
        SET c.[OrderDate] = c.[OrderDate],
            c.[OrderStatus] = c.[OrderStatus]
        FROM #Orders_Raw_Staging c;

        -- Copy fully transformed data from Staging to target Clean table
        IF @load_type = 'FULL' OR @last_run IS NULL
        BEGIN
            TRUNCATE TABLE [dbo].[Orders_Clean];
            INSERT INTO [dbo].[Orders_Clean] ([CustomerID], [OrderAmount], [OrderDate], [OrderID], [OrderStatus], etl_batch_id, etl_created_at, etl_updated_at)
            SELECT [CustomerID], [OrderAmount], [OrderDate], [OrderID], [OrderStatus], etl_batch_id, GETDATE(), GETDATE() FROM #Orders_Raw_Staging;
        END
        ELSE
        BEGIN
            DELETE FROM [dbo].[Orders_Clean] WHERE [OrderID] IN (SELECT [OrderID] FROM #Orders_Raw_Staging);
            INSERT INTO [dbo].[Orders_Clean] ([CustomerID], [OrderAmount], [OrderDate], [OrderID], [OrderStatus], etl_batch_id, etl_created_at, etl_updated_at)
            SELECT [CustomerID], [OrderAmount], [OrderDate], [OrderID], [OrderStatus], etl_batch_id, GETDATE(), GETDATE() FROM #Orders_Raw_Staging;
        END;

        IF OBJECT_ID('tempdb..#Orders_Raw_Staging') IS NOT NULL DROP TABLE #Orders_Raw_Staging;


        -- Update process watermark
        IF @load_type = 'INCREMENTAL' OR @last_run IS NULL
        BEGIN
            MERGE INTO dbo.etl_watermark AS target
            USING (SELECT 'etl_clean_Orders_Raw' AS process_name) AS source
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
        EXEC dbo.etl_clean_Customers_Raw @load_type = @load_type, @last_run = @last_run;
        EXEC dbo.etl_clean_Orders_Raw @load_type = @load_type, @last_run = @last_run;

        -- Update master process watermark
        IF @load_type = 'INCREMENTAL' OR @last_run IS NULL
        BEGIN
            MERGE INTO dbo.etl_watermark AS target
            USING (SELECT 'etl_main' AS process_name) AS source
            ON target.process_name = source.process_name
            WHEN MATCHED THEN
                UPDATE SET last_run_time = GETDATE()
            WHEN NOT MATCHED THEN
                INSERT (process_name, last_run_time) VALUES (source.process_name, GETDATE());
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
-- dbo.Customers_Clean: -- Source table/view: dbo.Customers_Clean
SELECT * FROM dbo.Customers_Clean;
-- dbo.Orders_Clean: -- Source table/view: dbo.Orders_Clean
SELECT * FROM dbo.Orders_Clean;

-- Join dbo.Customers_Clean -> dbo.Orders_Clean (one_to_many)
IF OBJECT_ID('dbo.vw_Orders_Clean_Fact', 'V') IS NOT NULL DROP VIEW dbo.vw_Orders_Clean_Fact;
GO
CREATE VIEW dbo.vw_Orders_Clean_Fact AS
SELECT
        c.[OrderDate],
        c.[OrderID],
        c.[OrderStatus],
        c.[OrderAmount],
        c.[CustomerID],
        p.[CustomerID] AS [Customers_Clean_CustomerID],
        p.[City],
        p.[CustomerName],
        p.[Email],
        p.[Phone],
        p.[CreatedDate]
FROM [dbo].[Orders_Clean] c
INNER JOIN [dbo].[Customers_Clean] p ON c.[CustomerID] = p.[CustomerID];
GO

-- ETL SQL — Agent Dhara — plan_id=plan_1779876372
-- dialect=tsql — review before executing against production.

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

-- Seed ETL invalid/sentinel configuration values
IF NOT EXISTS (SELECT 1 FROM dbo.etl_invalid_values WHERE column_name = 'Customers_Clean.City' AND invalid_value = '0')
    INSERT INTO dbo.etl_invalid_values (column_name, invalid_value) VALUES ('Customers_Clean.City', N'0');
IF NOT EXISTS (SELECT 1 FROM dbo.etl_invalid_values WHERE column_name = 'Customers_Clean.City' AND invalid_value = '-999')
    INSERT INTO dbo.etl_invalid_values (column_name, invalid_value) VALUES ('Customers_Clean.City', N'-999');
IF NOT EXISTS (SELECT 1 FROM dbo.etl_invalid_values WHERE column_name = 'Customers_Clean.City' AND invalid_value = '999999')
    INSERT INTO dbo.etl_invalid_values (column_name, invalid_value) VALUES ('Customers_Clean.City', N'999999');
IF NOT EXISTS (SELECT 1 FROM dbo.etl_invalid_values WHERE column_name = 'Customers_Clean.City' AND invalid_value = '9999999')
    INSERT INTO dbo.etl_invalid_values (column_name, invalid_value) VALUES ('Customers_Clean.City', N'9999999');
IF NOT EXISTS (SELECT 1 FROM dbo.etl_invalid_values WHERE column_name = 'Customers_Clean.City' AND invalid_value = '###')
    INSERT INTO dbo.etl_invalid_values (column_name, invalid_value) VALUES ('Customers_Clean.City', N'###');
IF NOT EXISTS (SELECT 1 FROM dbo.etl_invalid_values WHERE column_name = 'Customers_Clean.CreatedDate' AND invalid_value = '0')
    INSERT INTO dbo.etl_invalid_values (column_name, invalid_value) VALUES ('Customers_Clean.CreatedDate', N'0');
IF NOT EXISTS (SELECT 1 FROM dbo.etl_invalid_values WHERE column_name = 'Customers_Clean.CreatedDate' AND invalid_value = '-999')
    INSERT INTO dbo.etl_invalid_values (column_name, invalid_value) VALUES ('Customers_Clean.CreatedDate', N'-999');
IF NOT EXISTS (SELECT 1 FROM dbo.etl_invalid_values WHERE column_name = 'Customers_Clean.CreatedDate' AND invalid_value = '999999')
    INSERT INTO dbo.etl_invalid_values (column_name, invalid_value) VALUES ('Customers_Clean.CreatedDate', N'999999');
IF NOT EXISTS (SELECT 1 FROM dbo.etl_invalid_values WHERE column_name = 'Customers_Clean.CreatedDate' AND invalid_value = '9999999')
    INSERT INTO dbo.etl_invalid_values (column_name, invalid_value) VALUES ('Customers_Clean.CreatedDate', N'9999999');
IF NOT EXISTS (SELECT 1 FROM dbo.etl_invalid_values WHERE column_name = 'Customers_Clean.CreatedDate' AND invalid_value = '###')
    INSERT INTO dbo.etl_invalid_values (column_name, invalid_value) VALUES ('Customers_Clean.CreatedDate', N'###');
IF NOT EXISTS (SELECT 1 FROM dbo.etl_invalid_values WHERE column_name = 'Customers_Clean.CustomerName' AND invalid_value = '0')
    INSERT INTO dbo.etl_invalid_values (column_name, invalid_value) VALUES ('Customers_Clean.CustomerName', N'0');
IF NOT EXISTS (SELECT 1 FROM dbo.etl_invalid_values WHERE column_name = 'Customers_Clean.CustomerName' AND invalid_value = '-999')
    INSERT INTO dbo.etl_invalid_values (column_name, invalid_value) VALUES ('Customers_Clean.CustomerName', N'-999');
IF NOT EXISTS (SELECT 1 FROM dbo.etl_invalid_values WHERE column_name = 'Customers_Clean.CustomerName' AND invalid_value = '999999')
    INSERT INTO dbo.etl_invalid_values (column_name, invalid_value) VALUES ('Customers_Clean.CustomerName', N'999999');
IF NOT EXISTS (SELECT 1 FROM dbo.etl_invalid_values WHERE column_name = 'Customers_Clean.CustomerName' AND invalid_value = '9999999')
    INSERT INTO dbo.etl_invalid_values (column_name, invalid_value) VALUES ('Customers_Clean.CustomerName', N'9999999');
IF NOT EXISTS (SELECT 1 FROM dbo.etl_invalid_values WHERE column_name = 'Customers_Clean.CustomerName' AND invalid_value = '###')
    INSERT INTO dbo.etl_invalid_values (column_name, invalid_value) VALUES ('Customers_Clean.CustomerName', N'###');
IF NOT EXISTS (SELECT 1 FROM dbo.etl_invalid_values WHERE column_name = 'Customers_Clean.Email' AND invalid_value = '0')
    INSERT INTO dbo.etl_invalid_values (column_name, invalid_value) VALUES ('Customers_Clean.Email', N'0');
IF NOT EXISTS (SELECT 1 FROM dbo.etl_invalid_values WHERE column_name = 'Customers_Clean.Email' AND invalid_value = '-999')
    INSERT INTO dbo.etl_invalid_values (column_name, invalid_value) VALUES ('Customers_Clean.Email', N'-999');
IF NOT EXISTS (SELECT 1 FROM dbo.etl_invalid_values WHERE column_name = 'Customers_Clean.Email' AND invalid_value = '999999')
    INSERT INTO dbo.etl_invalid_values (column_name, invalid_value) VALUES ('Customers_Clean.Email', N'999999');
IF NOT EXISTS (SELECT 1 FROM dbo.etl_invalid_values WHERE column_name = 'Customers_Clean.Email' AND invalid_value = '9999999')
    INSERT INTO dbo.etl_invalid_values (column_name, invalid_value) VALUES ('Customers_Clean.Email', N'9999999');
IF NOT EXISTS (SELECT 1 FROM dbo.etl_invalid_values WHERE column_name = 'Customers_Clean.Email' AND invalid_value = '###')
    INSERT INTO dbo.etl_invalid_values (column_name, invalid_value) VALUES ('Customers_Clean.Email', N'###');
IF NOT EXISTS (SELECT 1 FROM dbo.etl_invalid_values WHERE column_name = 'Customers_Clean.Phone' AND invalid_value = '0')
    INSERT INTO dbo.etl_invalid_values (column_name, invalid_value) VALUES ('Customers_Clean.Phone', N'0');
IF NOT EXISTS (SELECT 1 FROM dbo.etl_invalid_values WHERE column_name = 'Customers_Clean.Phone' AND invalid_value = '-999')
    INSERT INTO dbo.etl_invalid_values (column_name, invalid_value) VALUES ('Customers_Clean.Phone', N'-999');
IF NOT EXISTS (SELECT 1 FROM dbo.etl_invalid_values WHERE column_name = 'Customers_Clean.Phone' AND invalid_value = '999999')
    INSERT INTO dbo.etl_invalid_values (column_name, invalid_value) VALUES ('Customers_Clean.Phone', N'999999');
IF NOT EXISTS (SELECT 1 FROM dbo.etl_invalid_values WHERE column_name = 'Customers_Clean.Phone' AND invalid_value = '9999999')
    INSERT INTO dbo.etl_invalid_values (column_name, invalid_value) VALUES ('Customers_Clean.Phone', N'9999999');
IF NOT EXISTS (SELECT 1 FROM dbo.etl_invalid_values WHERE column_name = 'Customers_Clean.Phone' AND invalid_value = '###')
    INSERT INTO dbo.etl_invalid_values (column_name, invalid_value) VALUES ('Customers_Clean.Phone', N'###');
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

        -- Create Staging Table with raw column types to preserve raw strings
        IF OBJECT_ID('tempdb..#Customers_Raw_Staging') IS NOT NULL DROP TABLE #Customers_Raw_Staging;
        CREATE TABLE #Customers_Raw_Staging ([Email] NVARCHAR(MAX) NULL, [CustomerName] NVARCHAR(MAX) NULL, [CreatedDate] NVARCHAR(MAX) NULL, [CustomerID] NVARCHAR(MAX) NULL, [City] NVARCHAR(MAX) NULL, [Phone] NVARCHAR(MAX) NULL, etl_batch_id INT NULL);

        -- Copy data from Raw to Staging
        IF @load_type = 'FULL' OR @last_run IS NULL
        BEGIN
            INSERT INTO #Customers_Raw_Staging ([Email], [CustomerName], [CreatedDate], [CustomerID], [City], [Phone], etl_batch_id)
            SELECT [Email], [CustomerName], [CreatedDate], [CustomerID], [City], [Phone], @run_id FROM [dbo].[Customers_Raw];
        END
        ELSE
        BEGIN
            INSERT INTO #Customers_Raw_Staging ([Email], [CustomerName], [CreatedDate], [CustomerID], [City], [Phone], etl_batch_id)
            SELECT [Email], [CustomerName], [CreatedDate], [CustomerID], [City], [Phone], @run_id FROM [dbo].[Customers_Raw] WHERE COALESCE(TRY_CONVERT(datetime, [CreatedDate], 120), TRY_CONVERT(datetime, [CreatedDate], 103), TRY_CONVERT(datetime, [CreatedDate], 101), TRY_CONVERT(datetime, [CreatedDate], 111)) > @last_run;
        END

        -- Single-Pass expression updates on #Customers_Raw_Staging
        UPDATE #Customers_Raw_Staging
        SET [City] = LOWER(LTRIM(RTRIM(CAST([City] AS NVARCHAR(MAX))))),
            [CustomerName] = LOWER(LTRIM(RTRIM(CAST([CustomerName] AS NVARCHAR(MAX))))),
            [Email] = LOWER(LTRIM(RTRIM(CAST([Email] AS NVARCHAR(MAX))))),
            [Phone] = REPLACE(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(CAST([Phone] AS NVARCHAR(MAX)))), N'-', N''), N' ', N''), N'(', N''), N')', N'')
        WHERE 1=1;

        -- Grouped config and null updates on #Customers_Raw_Staging
        UPDATE c
        SET c.[City] = CASE WHEN iv_City.invalid_value IS NOT NULL THEN NULL ELSE c.[City] END,
            c.[CreatedDate] = CASE WHEN iv_CreatedDate.invalid_value IS NOT NULL THEN NULL ELSE c.[CreatedDate] END,
            c.[CustomerName] = CASE WHEN iv_CustomerName.invalid_value IS NOT NULL THEN NULL ELSE c.[CustomerName] END,
            c.[Email] = CASE WHEN iv_Email.invalid_value IS NOT NULL THEN NULL ELSE c.[Email] END,
            c.[Phone] = CASE WHEN iv_Phone.invalid_value IS NOT NULL THEN NULL ELSE c.[Phone] END
        FROM #Customers_Raw_Staging c
        LEFT JOIN dbo.etl_invalid_values iv_City ON iv_City.column_name = 'Customers_Clean.City' AND TRY_CAST(iv_City.invalid_value AS NVARCHAR(MAX)) = c.[City]
        LEFT JOIN dbo.etl_invalid_values iv_CreatedDate ON iv_CreatedDate.column_name = 'Customers_Clean.CreatedDate' AND CAST(c.[CreatedDate] AS NVARCHAR(MAX)) = iv_CreatedDate.invalid_value
        LEFT JOIN dbo.etl_invalid_values iv_CustomerName ON iv_CustomerName.column_name = 'Customers_Clean.CustomerName' AND TRY_CAST(iv_CustomerName.invalid_value AS NVARCHAR(MAX)) = c.[CustomerName]
        LEFT JOIN dbo.etl_invalid_values iv_Email ON iv_Email.column_name = 'Customers_Clean.Email' AND TRY_CAST(iv_Email.invalid_value AS NVARCHAR(MAX)) = c.[Email]
        LEFT JOIN dbo.etl_invalid_values iv_Phone ON iv_Phone.column_name = 'Customers_Clean.Phone' AND TRY_CAST(iv_Phone.invalid_value AS NVARCHAR(MAX)) = c.[Phone]
        WHERE iv_City.invalid_value IS NOT NULL OR iv_CreatedDate.invalid_value IS NOT NULL OR iv_CustomerName.invalid_value IS NOT NULL OR iv_Email.invalid_value IS NOT NULL OR iv_Phone.invalid_value IS NOT NULL;

        -- Quarantine rows where primary key [CustomerID] is NULL to dbo.etl_rejects
        INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)
        SELECT 'etl_clean_Customers_Raw', 'dbo.Customers_Clean',
               (SELECT r.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
               'Primary key [CustomerID] is NULL'
        FROM #Customers_Raw_Staging r
        WHERE r.[CustomerID] IS NULL;

        DELETE FROM #Customers_Raw_Staging WHERE [CustomerID] IS NULL;

        -- Quarantine invalid emails from #Customers_Raw_Staging.[Email] to dbo.etl_rejects
        INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)
        SELECT 'etl_clean_Customers_Raw', 'dbo.Customers_Clean',
               (SELECT r.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
               'Column [Email] with value ' + CAST(r.[Email] AS NVARCHAR(MAX)) + ' is not a valid email format'
        FROM #Customers_Raw_Staging r
        WHERE r.[Email] IS NOT NULL AND NOT (CAST(r.[Email] AS NVARCHAR(MAX)) LIKE '%_@_%._%');

        DELETE FROM #Customers_Raw_Staging WHERE [Email] IS NOT NULL AND NOT (CAST([Email] AS NVARCHAR(MAX)) LIKE '%_@_%._%');

        -- Quarantine null dates from #Customers_Raw_Staging.[CreatedDate] to dbo.etl_rejects
        INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)
        SELECT 'etl_clean_Customers_Raw', 'dbo.Customers_Clean',
               (SELECT r.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
               'Required date column [CreatedDate] is NULL'
        FROM #Customers_Raw_Staging r
        WHERE r.[CreatedDate] IS NULL;

        DELETE FROM #Customers_Raw_Staging WHERE [CreatedDate] IS NULL;

        -- Quarantine invalid dates from #Customers_Raw_Staging.[CreatedDate] to dbo.etl_rejects
        INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)
        SELECT 'etl_clean_Customers_Raw', 'dbo.Customers_Clean',
               (SELECT r.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
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
               (SELECT r.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
               'Column [Phone] with value ' + CAST(r.[Phone] AS NVARCHAR(MAX)) + ' is not a valid phone format'
        FROM #Customers_Raw_Staging r
        WHERE r.[Phone] IS NOT NULL AND (LEN(REPLACE(REPLACE(REPLACE(REPLACE(CAST(r.[Phone] AS NVARCHAR(200)), N'-', N''), N' ', N''), N'(', N''), N')', N'')) < 7 OR REPLACE(REPLACE(REPLACE(REPLACE(CAST(r.[Phone] AS NVARCHAR(200)), N'-', N''), N' ', N''), N'(', N''), N')', N'') LIKE '%[^0-9]%');

        DELETE FROM #Customers_Raw_Staging WHERE [Phone] IS NOT NULL AND (LEN(REPLACE(REPLACE(REPLACE(REPLACE(CAST([Phone] AS NVARCHAR(200)), N'-', N''), N' ', N''), N'(', N''), N')', N'')) < 7 OR REPLACE(REPLACE(REPLACE(REPLACE(CAST([Phone] AS NVARCHAR(200)), N'-', N''), N' ', N''), N'(', N''), N')', N'') LIKE '%[^0-9]%');

        -- Post-validation date/type parsing on #Customers_Raw_Staging
        UPDATE #Customers_Raw_Staging
        SET [CreatedDate] = COALESCE(TRY_CONVERT(date, [CreatedDate], 120), TRY_CONVERT(date, [CreatedDate], 103), TRY_CONVERT(date, [CreatedDate], 101), TRY_CONVERT(date, [CreatedDate], 111))
        WHERE 1=1;

        -- Deduplicate staging table by partition key(s)
        ;WITH _staging_dedup AS (
            SELECT ROW_NUMBER() OVER (PARTITION BY LOWER(LTRIM(RTRIM(CAST([CustomerID] AS NVARCHAR(400))))) ORDER BY [CreatedDate] DESC) AS _rn
            FROM #Customers_Raw_Staging
        )
        DELETE FROM _staging_dedup WHERE _rn > 1;

        -- Copy fully transformed data from Staging to target Clean table
        IF @load_type = 'FULL' OR @last_run IS NULL
        BEGIN
            TRUNCATE TABLE [dbo].[Customers_Clean];
            INSERT INTO [dbo].[Customers_Clean] ([City], [CreatedDate], [CustomerID], [CustomerName], [Email], [Phone], etl_batch_id, etl_created_at, etl_updated_at)
            SELECT [City], [CreatedDate], TRY_CAST([CustomerID] AS BIGINT), [CustomerName], [Email], [Phone], etl_batch_id, GETDATE(), GETDATE() FROM #Customers_Raw_Staging;
        END
        ELSE
        BEGIN
            DELETE FROM [dbo].[Customers_Clean] WHERE [CustomerID] IN (SELECT [CustomerID] FROM #Customers_Raw_Staging);
            INSERT INTO [dbo].[Customers_Clean] ([City], [CreatedDate], [CustomerID], [CustomerName], [Email], [Phone], etl_batch_id, etl_created_at, etl_updated_at)
            SELECT [City], [CreatedDate], TRY_CAST([CustomerID] AS BIGINT), [CustomerName], [Email], [Phone], etl_batch_id, GETDATE(), GETDATE() FROM #Customers_Raw_Staging;
        END;

        DECLARE @max_watermark DATETIME = COALESCE((SELECT MAX(TRY_CAST([CreatedDate] AS DATETIME)) FROM #Customers_Raw_Staging), GETDATE());

        IF OBJECT_ID('tempdb..#Customers_Raw_Staging') IS NOT NULL DROP TABLE #Customers_Raw_Staging;


        -- Update process watermark
        IF @load_type = 'INCREMENTAL' OR @last_run IS NULL
        BEGIN
            MERGE INTO dbo.etl_watermark AS target
            USING (SELECT 'etl_clean_Customers_Raw' AS process_name) AS source
            ON target.process_name = source.process_name
            WHEN MATCHED THEN
                UPDATE SET last_run_time = @max_watermark
            WHEN NOT MATCHED THEN
                INSERT (process_name, last_run_time) VALUES (source.process_name, @max_watermark);
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

        -- Create Staging Table with raw column types to preserve raw strings
        IF OBJECT_ID('tempdb..#Orders_Raw_Staging') IS NOT NULL DROP TABLE #Orders_Raw_Staging;
        CREATE TABLE #Orders_Raw_Staging ([OrderDate] NVARCHAR(MAX) NULL, [OrderID] NVARCHAR(MAX) NULL, [CustomerID] NVARCHAR(MAX) NULL, [OrderStatus] NVARCHAR(MAX) NULL, [OrderAmount] NVARCHAR(MAX) NULL, etl_batch_id INT NULL);

        -- Copy data from Raw to Staging
        IF @load_type = 'FULL' OR @last_run IS NULL
        BEGIN
            INSERT INTO #Orders_Raw_Staging ([OrderDate], [OrderID], [CustomerID], [OrderStatus], [OrderAmount], etl_batch_id)
            SELECT [OrderDate], [OrderID], [CustomerID], [OrderStatus], [OrderAmount], @run_id FROM [dbo].[Orders_Raw];
        END
        ELSE
        BEGIN
            INSERT INTO #Orders_Raw_Staging ([OrderDate], [OrderID], [CustomerID], [OrderStatus], [OrderAmount], etl_batch_id)
            SELECT [OrderDate], [OrderID], [CustomerID], [OrderStatus], [OrderAmount], @run_id FROM [dbo].[Orders_Raw] WHERE COALESCE(TRY_CONVERT(datetime, [OrderDate], 120), TRY_CONVERT(datetime, [OrderDate], 103), TRY_CONVERT(datetime, [OrderDate], 101), TRY_CONVERT(datetime, [OrderDate], 111)) > @last_run;
        END

        -- Single-Pass expression updates on #Orders_Raw_Staging
        UPDATE #Orders_Raw_Staging
        SET [OrderStatus] = LOWER(LTRIM(RTRIM(CAST([OrderStatus] AS NVARCHAR(MAX)))))
        WHERE 1=1;

        -- Quarantine rows where primary key [OrderID] is NULL to dbo.etl_rejects
        INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)
        SELECT 'etl_clean_Orders_Raw', 'dbo.Orders_Clean',
               (SELECT r.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
               'Primary key [OrderID] is NULL'
        FROM #Orders_Raw_Staging r
        WHERE r.[OrderID] IS NULL;

        DELETE FROM #Orders_Raw_Staging WHERE [OrderID] IS NULL;

        -- Quarantine invalid dates from #Orders_Raw_Staging.[OrderDate] to dbo.etl_rejects
        INSERT INTO dbo.etl_rejects (process_name, table_name, row_data, error_reason)
        SELECT 'etl_clean_Orders_Raw', 'dbo.Orders_Clean',
               (SELECT r.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
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

        -- Post-validation date/type parsing on #Orders_Raw_Staging
        UPDATE #Orders_Raw_Staging
        SET [OrderDate] = COALESCE(TRY_CONVERT(date, [OrderDate], 120), TRY_CONVERT(date, [OrderDate], 103), TRY_CONVERT(date, [OrderDate], 101), TRY_CONVERT(date, [OrderDate], 111))
        WHERE 1=1;

        -- Deduplicate staging table by partition key(s)
        ;WITH _staging_dedup AS (
            SELECT ROW_NUMBER() OVER (PARTITION BY LOWER(LTRIM(RTRIM(CAST([OrderID] AS NVARCHAR(400))))) ORDER BY [OrderDate] DESC) AS _rn
            FROM #Orders_Raw_Staging
        )
        DELETE FROM _staging_dedup WHERE _rn > 1;

        -- Copy fully transformed data from Staging to target Clean table
        IF @load_type = 'FULL' OR @last_run IS NULL
        BEGIN
            TRUNCATE TABLE [dbo].[Orders_Clean];
            INSERT INTO [dbo].[Orders_Clean] ([CustomerID], [OrderAmount], [OrderDate], [OrderID], [OrderStatus], etl_batch_id, etl_created_at, etl_updated_at)
            SELECT TRY_CAST([CustomerID] AS BIGINT), [OrderAmount], [OrderDate], TRY_CAST([OrderID] AS BIGINT), [OrderStatus], etl_batch_id, GETDATE(), GETDATE() FROM #Orders_Raw_Staging;
        END
        ELSE
        BEGIN
            DELETE FROM [dbo].[Orders_Clean] WHERE [OrderID] IN (SELECT [OrderID] FROM #Orders_Raw_Staging);
            INSERT INTO [dbo].[Orders_Clean] ([CustomerID], [OrderAmount], [OrderDate], [OrderID], [OrderStatus], etl_batch_id, etl_created_at, etl_updated_at)
            SELECT TRY_CAST([CustomerID] AS BIGINT), [OrderAmount], [OrderDate], TRY_CAST([OrderID] AS BIGINT), [OrderStatus], etl_batch_id, GETDATE(), GETDATE() FROM #Orders_Raw_Staging;
        END;

        DECLARE @max_watermark DATETIME = COALESCE((SELECT MAX(TRY_CAST([OrderDate] AS DATETIME)) FROM #Orders_Raw_Staging), GETDATE());

        IF OBJECT_ID('tempdb..#Orders_Raw_Staging') IS NOT NULL DROP TABLE #Orders_Raw_Staging;


        -- Update process watermark
        IF @load_type = 'INCREMENTAL' OR @last_run IS NULL
        BEGIN
            MERGE INTO dbo.etl_watermark AS target
            USING (SELECT 'etl_clean_Orders_Raw' AS process_name) AS source
            ON target.process_name = source.process_name
            WHEN MATCHED THEN
                UPDATE SET last_run_time = @max_watermark
            WHEN NOT MATCHED THEN
                INSERT (process_name, last_run_time) VALUES (source.process_name, @max_watermark);
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
        c.[CustomerID],
        c.[OrderStatus],
        c.[OrderAmount],
        p.[Email],
        p.[CustomerName],
        p.[CreatedDate],
        p.[CustomerID] AS [Customers_Clean_CustomerID],
        p.[City],
        p.[Phone]
FROM [dbo].[Orders_Clean] c
INNER JOIN [dbo].[Customers_Clean] p ON c.[CustomerID] = p.[CustomerID];
GO

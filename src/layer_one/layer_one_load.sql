/*
The load_one stored procedure orchestrates the initial extraction phase of the ETL pipeline,
using optimized BULK INSERT operations to ingest raw CSV data from source systems into the 
Layer One staging tables while capturing detailed execution metrics in an audit log.
*/



USE DataWareHouse;
GO

CREATE OR ALTER PROCEDURE one.load_one (
    @base_file_path NVARCHAR(255) = 'D:\Legion 5\Downloads\sql-data-warehouse-project-main\datasets\'
) 
AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    DECLARE @row_count INT;
    DECLARE @file_path NVARCHAR(500);

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Executing Data Ingestion Pipeline: Layer One';
        PRINT '================================================';

        PRINT '------------------------------------------------';
        PRINT 'Phase 1: Loading CRM Tables';
        PRINT '------------------------------------------------';

        SET @start_time= GETDATE();
        PRINT '>> Truncating Table: one.crm_cust_info';
        TRUNCATE TABLE one.crm_cust_info;
        
        PRINT '>> Inserting Data Into: one.crm_cust_info';
        SET @file_path = @base_file_path + 'source_crm\cust_info.csv';
        EXEC('
            BULK INSERT one.crm_cust_info FROM ''' + @file_path + '''
            WITH ( FIRSTROW = 2, FIELDTERMINATOR = '','', FORMAT = ''CSV'', TABLOCK )
        ');
        
        SET @row_count = (SELECT COUNT(*) FROM one.crm_cust_info);
        SET @end_time = GETDATE();
        
        INSERT INTO one.etl_audit_log (procedure_name, target_table, rows_loaded, duration_seconds, status)
        VALUES ('one.load_one', 'one.crm_cust_info', @row_count, DATEDIFF(second, @start_time, @end_time), 'SUCCESS');

        PRINT '>> Rows Loaded:   ' + CAST(@row_count AS NVARCHAR);
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '------------------------------------------------';

        SET @start_time= GETDATE();
        PRINT '>> Truncating Table: one.crm_prd_info';
        TRUNCATE TABLE one.crm_prd_info;
        
        PRINT '>> Inserting Data Into: one.crm_prd_info';
        SET @file_path = @base_file_path + 'source_crm\prd_info.csv';
        EXEC('
            BULK INSERT one.crm_prd_info FROM ''' + @file_path + '''
            WITH ( FIRSTROW = 2, FIELDTERMINATOR = '','', FORMAT = ''CSV'', TABLOCK )
        ');
        
        SET @row_count = (SELECT COUNT(*) FROM one.crm_prd_info);
        SET @end_time = GETDATE();

        INSERT INTO one.etl_audit_log (procedure_name, target_table, rows_loaded, duration_seconds, status)
        VALUES ('one.load_one', 'one.crm_prd_info', @row_count, DATEDIFF(second, @start_time, @end_time), 'SUCCESS');

        PRINT '>> Rows Loaded:   ' + CAST(@row_count AS NVARCHAR);
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '------------------------------------------------';

        SET @start_time= GETDATE();
        PRINT '>> Truncating Table: one.crm_sales_details';
        TRUNCATE TABLE one.crm_sales_details;
        
        PRINT '>> Inserting Data Into: one.crm_sales_details';
        SET @file_path = @base_file_path + 'source_crm\sales_details.csv';
        EXEC('
            BULK INSERT one.crm_sales_details FROM ''' + @file_path + '''
            WITH ( FIRSTROW = 2, FIELDTERMINATOR = '','', FORMAT = ''CSV'', TABLOCK )
        ');
        
        SET @row_count = (SELECT COUNT(*) FROM one.crm_sales_details);
        SET @end_time = GETDATE();

        INSERT INTO one.etl_audit_log (procedure_name, target_table, rows_loaded, duration_seconds, status)
        VALUES ('one.load_one', 'one.crm_sales_details', @row_count, DATEDIFF(second, @start_time, @end_time), 'SUCCESS');

        PRINT '>> Rows Loaded:   ' + CAST(@row_count AS NVARCHAR);
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '------------------------------------------------';

        PRINT '------------------------------------------------';
        PRINT 'Phase 2: Loading ERP Tables';
        PRINT '------------------------------------------------';
       
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: one.erp_loc_a101';
        TRUNCATE TABLE one.erp_loc_a101;
        
        PRINT '>> Inserting Data Into: one.erp_loc_a101';
        SET @file_path = @base_file_path + 'source_erp\loc_a101.csv';
        EXEC('
            BULK INSERT one.erp_loc_a101 FROM ''' + @file_path + '''
            WITH ( FIRSTROW = 2, FIELDTERMINATOR = '','', FORMAT = ''CSV'', TABLOCK )
        ');
        
        SET @row_count = (SELECT COUNT(*) FROM one.erp_loc_a101);
        SET @end_time = GETDATE();

        INSERT INTO one.etl_audit_log (procedure_name, target_table, rows_loaded, duration_seconds, status)
        VALUES ('one.load_one', 'one.erp_loc_a101', @row_count, DATEDIFF(second, @start_time, @end_time), 'SUCCESS');

        PRINT '>> Rows Loaded:   ' + CAST(@row_count AS NVARCHAR);
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '------------------------------------------------';

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: one.erp_cust_az12';
        TRUNCATE TABLE one.erp_cust_az12;
        
        PRINT '>> Inserting Data Into: one.erp_cust_az12';
        SET @file_path = @base_file_path + 'source_erp\cust_az12.csv';
        EXEC('
            BULK INSERT one.erp_cust_az12 FROM ''' + @file_path + '''
            WITH ( FIRSTROW = 2, FIELDTERMINATOR = '','', FORMAT = ''CSV'', TABLOCK )
        ');
        
        SET @row_count = (SELECT COUNT(*) FROM one.erp_cust_az12);
        SET @end_time = GETDATE();

        INSERT INTO one.etl_audit_log (procedure_name, target_table, rows_loaded, duration_seconds, status)
        VALUES ('one.load_one', 'one.erp_cust_az12', @row_count, DATEDIFF(second, @start_time, @end_time), 'SUCCESS');

        PRINT '>> Rows Loaded:   ' + CAST(@row_count AS NVARCHAR);
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '------------------------------------------------';

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: one.erp_px_cat_g1v2';
        TRUNCATE TABLE one.erp_px_cat_g1v2;
        
        PRINT '>> Inserting Data Into: one.erp_px_cat_g1v2';
        SET @file_path = @base_file_path + 'source_erp\px_cat_g1v2.csv';
        EXEC('
            BULK INSERT one.erp_px_cat_g1v2 FROM ''' + @file_path + '''
            WITH ( FIRSTROW = 2, FIELDTERMINATOR = '','', FORMAT = ''CSV'', TABLOCK )
        ');
        
        SET @row_count = (SELECT COUNT(*) FROM one.erp_px_cat_g1v2);
        SET @end_time = GETDATE();

        INSERT INTO one.etl_audit_log (procedure_name, target_table, rows_loaded, duration_seconds, status)
        VALUES ('one.load_one', 'one.erp_px_cat_g1v2', @row_count, DATEDIFF(second, @start_time, @end_time), 'SUCCESS');

        PRINT '>> Rows Loaded:   ' + CAST(@row_count AS NVARCHAR);
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '------------------------------------------------';

        SET @batch_end_time = GETDATE();
        PRINT '================================================';
        PRINT 'Layer One Ingestion Completed Successfully';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '================================================';

    END TRY
    BEGIN CATCH
        INSERT INTO one.etl_audit_log (procedure_name, target_table, rows_loaded, duration_seconds, status, error_message)
        VALUES ('one.load_one', 'PIPELINE_FAILURE', 0, 0, 'FAILED', ERROR_MESSAGE());

        PRINT '================================================';
        PRINT 'ERROR OCCURRED DURING LAYER ONE INGESTION';
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error Line:   ' + CAST(ERROR_LINE() AS NVARCHAR);
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT '================================================';
    END CATCH
END;
GO

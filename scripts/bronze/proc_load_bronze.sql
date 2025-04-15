/*
=============================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
=============================================================
Procedure Purpose:
    This stored procedure performs a complete data load of 
    the "bronze" layer tables in the data warehouse. It:
        1. Truncates existing CRM and ERP bronze tables
        2. Loads raw CSV files from local file system
        3. Captures timing logs for each table's load
        4. Handles any exceptions and displays error messages

Table Summary:
    1. bronze.crm_cust_info      – CRM customer master data
    2. bronze.crm_prd_info       – CRM product master data
    3. bronze.crm_sales_details  – CRM transactional sales data
    4. bronze.erp_loc_a101       – ERP customer-country mapping
    5. bronze.erp_cust_az12      – ERP customer demographic data
    6. bronze.erp_px_cat_g1v2    – ERP product-category mapping

Execution Notes:
    - File paths are hardcoded and must be updated per local environment
    - Run this procedure only after creating bronze layer tables
    - Ensure PostgreSQL has access to the CSV file paths (local server access)
    - COPY command requires appropriate file privileges

Caution:
    Existing data in bronze tables will be wiped before reload.
    This procedure is meant for raw data refresh from source.
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    -- Timestamps to track execution time of table loads and entire batch
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;

    -- File paths to source CSVs (modify as per your environment)
    file_path_1 TEXT := 'C:\Users\Niraj Patil\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_crm\cust_info.csv';
    file_path_2 TEXT := 'C:\Users\Niraj Patil\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_crm\prd_info.csv';
    file_path_3 TEXT := 'C:\Users\Niraj Patil\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_crm\sales_details.csv';
    file_path_4 TEXT := 'C:\Users\Niraj Patil\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv';
    file_path_5 TEXT := 'C:\Users\Niraj Patil\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv';
    file_path_6 TEXT := 'C:\Users\Niraj Patil\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv';
BEGIN
    -- Start of procedure execution
    RAISE NOTICE '====================================================';
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '====================================================';

    batch_start_time := clock_timestamp(); -- Start total batch timer

    -- Step 1: Truncate all CRM and ERP bronze tables to remove existing data
    RAISE NOTICE '>> Truncating all the CRM & ERP Tables';
    TRUNCATE TABLE bronze.crm_cust_info;
    TRUNCATE TABLE bronze.crm_prd_info;
    TRUNCATE TABLE bronze.crm_sales_details;
    TRUNCATE TABLE bronze.erp_loc_a101;
    TRUNCATE TABLE bronze.erp_cust_az12;
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;

    -- Step 2: Begin insertion of new data
    RAISE NOTICE '>> Inserting Data in all the CRM & ERP Tables';

    BEGIN
        /*
        ----------------------------------------------------
        Load CRM Tables
        ----------------------------------------------------
        */
        RAISE NOTICE '----------------------------------------------------';
        RAISE NOTICE 'Loading CRM Tables';
        RAISE NOTICE '----------------------------------------------------';

        -- Load bronze.crm_cust_info
        start_time := clock_timestamp();
        EXECUTE format('COPY bronze.crm_cust_info FROM %L WITH (FORMAT csv, HEADER true, DELIMITER '','')', file_path_1);
        end_time := clock_timestamp();
        RAISE NOTICE '>> crm_cust_info Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

        -- Load bronze.crm_prd_info
        start_time := clock_timestamp();
        EXECUTE format('COPY bronze.crm_prd_info FROM %L WITH (FORMAT csv, HEADER true, DELIMITER '','')', file_path_2);
        end_time := clock_timestamp();
        RAISE NOTICE '>> crm_prd_info Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

        -- Load bronze.crm_sales_details
        start_time := clock_timestamp();
        EXECUTE format('COPY bronze.crm_sales_details FROM %L WITH (FORMAT csv, HEADER true, DELIMITER '','')', file_path_3);
        end_time := clock_timestamp();
        RAISE NOTICE '>> crm_sales_details Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

        /*
        ----------------------------------------------------
        Load ERP Tables
        ----------------------------------------------------
        */
        RAISE NOTICE '----------------------------------------------------';
        RAISE NOTICE 'Loading ERP Tables';
        RAISE NOTICE '----------------------------------------------------';

        -- Load bronze.erp_loc_a101
        start_time := clock_timestamp();
        EXECUTE format('COPY bronze.erp_loc_a101 FROM %L WITH (FORMAT csv, HEADER true, DELIMITER '','')', file_path_4);
        end_time := clock_timestamp();
        RAISE NOTICE '>> erp_loc_a101 Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

        -- Load bronze.erp_cust_az12
        start_time := clock_timestamp();
        EXECUTE format('COPY bronze.erp_cust_az12 FROM %L WITH (FORMAT csv, HEADER true, DELIMITER '','')', file_path_5);
        end_time := clock_timestamp();
        RAISE NOTICE '>> erp_cust_az12 Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

        -- Load bronze.erp_px_cat_g1v2
        start_time := clock_timestamp();
        EXECUTE format('COPY bronze.erp_px_cat_g1v2 FROM %L WITH (FORMAT csv, HEADER true, DELIMITER '','')', file_path_6);
        end_time := clock_timestamp();
        RAISE NOTICE '>> erp_px_cat_g1v2 Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

        -- Batch completed
        batch_end_time := clock_timestamp();
        RAISE NOTICE '====================================================';
        RAISE NOTICE 'Loading Bronze Layer is Completed';
        RAISE NOTICE '       - Total Batch Load Duration: % seconds', EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));
        RAISE NOTICE '====================================================';

    EXCEPTION
        -- Catch-all exception handler to raise error info if anything fails
        WHEN OTHERS THEN
            RAISE NOTICE '====================================================';
            RAISE NOTICE '⚠️ ERROR OCCURRED DURING LOADING BRONZE LAYER';
            RAISE NOTICE 'Error Message: %', SQLERRM;
            RAISE NOTICE '====================================================';
    END;
END;
$$;

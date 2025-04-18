/*
=============================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
=============================================================

Procedure Purpose:
    This stored procedure transforms and loads cleaned, structured 
    data from the bronze layer into the silver layer. It:
        1. Truncates existing silver layer tables
        2. Applies business logic and data transformations
        3. Deduplicates and formats raw data for analytical use
        4. Ensures standardized, high-quality records across CRM and ERP

Transformation Summary:
    CRM Domain:
        - silver.crm_cust_info:
            * Deduplicates by customer ID using latest record
            * Maps marital status and gender codes to descriptive values
        - silver.crm_prd_info:
            * Extracts category and product keys
            * Converts product line codes to readable values
            * Calculates product end date based on lead logic
        - silver.crm_sales_details:
            * Converts integer dates to valid DATE format
            * Fixes inconsistent or missing sales and price values

    ERP Domain:
        - silver.erp_cust_az12:
            * Normalizes gender labels and drops invalid birthdates
        - silver.erp_loc_a101:
            * Standardizes country codes and removes special characters in customer IDs
        - silver.erp_px_cat_g1v2:
            * Performs direct copy from bronze to silver without transformation

Execution Notes:
    - Designed for staging and prep layer before gold/business layer
    - Silver layer acts as an intermediate clean data zone
    - Run this script only after bronze layer is successfully loaded

Caution:
    This script performs TRUNCATE on silver tables, overwriting existing data.
    Intended for full reload scenarios, not incremental loads.
Uasage Example:
    CALL silver.load_silver();
*/

CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE 
	-- Timestamps to track execution time of table loads and entire batch
	start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;

BEGIN
	--Start of procedure execution
	RAISE NOTICE '====================================================';
	RAISE NOTICE 'Loading Silver Layer';
	RAISE NOTICE '====================================================';

	batch_start_time := clock_timestamp(); -- Start total batch timer

	-- Step 1: Truncate all CRM and ERP silver tables to remove existing data
    RAISE NOTICE '>> Truncating all the CRM & ERP Tables';
    TRUNCATE TABLE silver.crm_cust_info;
    TRUNCATE TABLE silver.crm_prd_info;
    TRUNCATE TABLE silver.crm_sales_details;
    TRUNCATE TABLE silver.erp_loc_a101;
    TRUNCATE TABLE silver.erp_cust_az12;
    TRUNCATE TABLE silver.erp_px_cat_g1v2;

	-- Step 2: Begin transforming & insertion of new data
	RAISE NOTICE '>> Transforming & Inserting Data in all the CRM & ERP Tables';

	BEGIN
		/*
		----------------------------------------------------
		Transform & Load CRM Tables
		----------------------------------------------------
		*/
		RAISE NOTICE '----------------------------------------------------';
        RAISE NOTICE 'Transform and Loading CRM Tables';
        RAISE NOTICE '----------------------------------------------------';

		--Transform & Load silver.crm_cust_info
		start_time := clock_timestamp();

		INSERT INTO silver.crm_cust_info(
						cst_id,
						cst_key,
						cst_firstname,
						cst_lastname,
						cst_marital_status,
						cst_gndr,
						cst_create_date
					)
					SELECT
					cst_id,
					cst_key,
					TRIM(cst_firstname) AS cst_firstname,
					TRIM(cst_lastname) AS cst_lastname,
					CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
						 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
						 ELSE 'n/a'
					END cst_marital_status,
					CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
						 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
						 ELSE 'n/a'
					END cst_gndr,
					cst_create_date
					FROM(
					SELECT
					*,
					ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
					FROM bronze.crm_cust_info
					WHERE cst_id IS NOT NULL
					)t WHERE flag_last = 1;

		end_time := clock_timestamp();
		RAISE NOTICE '>> crm_cust_info Transform & Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

		--Transform & Load silver.crm_prd_info
		start_time := clock_timestamp();

		INSERT INTO silver.crm_prd_info(
						prd_id,
						cat_id,
						prd_key,
						prd_nm,
						prd_cost,
						prd_line,
						prd_start_dt,
						prd_end_dt
					)
					SELECT 
						prd_id,
						REPLACE(SUBSTRING(prd_key, 1, 5), '-','_') AS cat_id, --Extraxt category ID
						SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,    --Extract product key
						prd_nm,
						COALESCE(prd_cost:: NUMERIC, 0) AS prd_cost,
						CASE UPPER(TRIM(prd_line)) 
							WHEN 'M' THEN 'Mountain'
							WHEN 'R' THEN 'Road'
							WHEN 'S' THEN 'Other Sales'
							WHEN 'T' THEN 'Touring'
							ELSE 'n/a'
						END AS prd_line, -- Map product line codes to descriptive values
						CAST(prd_start_dt AS DATE) AS prd_start_date, --
						CAST(
							LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL '1 day' 
							AS DATE
						) AS prd_end_dt --Calculate end date as one day before the next start date
					FROM bronze.crm_prd_info;
		
		end_time := clock_timestamp();
		RAISE NOTICE '>> crm_prd_info Transform & Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

		-- Transform & Load silver.crm_sales_details
		start_time := clock_timestamp();

		INSERT INTO silver.crm_sales_details (
						sls_ord_num,
						sls_prd_key,
						sls_cust_id,
						sls_order_dt,
						sls_ship_dt,
						sls_due_dt,
						sls_sales,
						sls_quantity,
						sls_price
					)
					SELECT
						sls_ord_num,
						sls_prd_key,
						sls_cust_id,
						-- Dates converted and stored as DATE
						CASE 
							WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL
							ELSE TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD')
						END AS sls_order_dt,
						CASE 
							WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL
							ELSE TO_DATE(sls_ship_dt::TEXT, 'YYYYMMDD')
						END AS sls_ship_dt,
						CASE 
							WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::TEXT) != 8 THEN NULL
							ELSE TO_DATE(sls_due_dt::TEXT, 'YYYYMMDD')
						END AS sls_due_dt,
						-- Sales fix
						CASE 
							WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
								THEN sls_quantity * ABS(sls_price)
							ELSE sls_sales
						END AS sls_sales,
						sls_quantity,
						-- Price fix
						CASE 
							WHEN sls_price IS NULL OR sls_price <= 0
								THEN sls_sales/NULLIF(sls_quantity, 0)
							ELSE sls_price
						END AS sls_price
					FROM bronze.crm_sales_details;

		end_time := clock_timestamp();
		RAISE NOTICE '>> crm_sales_details Transform & Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

		/*
        ----------------------------------------------------
        Load ERP Tables
        ----------------------------------------------------
        */
		RAISE NOTICE '----------------------------------------------------';
        RAISE NOTICE 'Transform & Loading ERP Tables';
        RAISE NOTICE '----------------------------------------------------';

		-- Transform & Load silver.erp_cust_az12
		start_time := clock_timestamp();

		INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
					SELECT
					CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))  -- Remove 'NAS' prefix if present
						 ELSE cid
					END cid,
					CASE WHEN bdate > CURRENT_DATE THEN NULL  -- Set future birthdates to NULL
						 ELSE bdate
					END AS bdate,
					CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
						 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE')   THEN 'Male'
						 ELSE 'n/a'
					END AS gen  -- Normalize gender values and handle unknown cases
					FROM bronze.erp_cust_az12;

		end_time := clock_timestamp();
		RAISE NOTICE '>> erp_cust_az12 Transform & Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));


		-- Transform & Load silver.erp_loc_a101
		start_time := clock_timestamp();

		INSERT INTO silver.erp_loc_a101 (cid, cntry)
					SELECT
					REPLACE(cid, '-', '') AS cid,
					CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
						 WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
						 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
						 ELSE TRIM(cntry)
					END AS cntry  -- Normalize or handle missing or blank country codes
					FROM bronze.erp_loc_a101;

		end_time := clock_timestamp();
		RAISE NOTICE '>> erp_loc_a101 Transform & Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

		-- Load bronze.erp_px_cat_g1v2
		INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
					SELECT
					id,
					cat,
					subcat,
					maintenance
					FROM bronze.erp_px_cat_g1v2;
		end_time := clock_timestamp();
		RAISE NOTICE '>> erp_px_cat_g1v2 Transform & Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

		-- Batch completed
        batch_end_time := clock_timestamp();
        RAISE NOTICE '====================================================';
        RAISE NOTICE 'Transforming & Loading Silver Layer is Completed';
        RAISE NOTICE '       - Total Batch Load Duration: % seconds', EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));
        RAISE NOTICE '====================================================';

	EXCEPTION
        -- Catch-all exception handler to raise error info if anything fails
        WHEN OTHERS THEN
            RAISE NOTICE '====================================================';
            RAISE NOTICE '⚠️ ERROR OCCURRED DURING TRANSFORM & LOADING SILVER LAYER';
            RAISE NOTICE 'Error Message: %', SQLERRM;
            RAISE NOTICE '====================================================';
    END;
END;
$$;

/*
=============================================================
Create Silver Layer Tables
=============================================================
Script Purpose:
    This script creates transformed and curated "silver" layer tables 
    in the 'datawarehouse' database under the 'silver' schema. These 
    tables represent cleaned and enriched datasets from the 
    raw bronze layer, designed for analytical use cases.

Table Summary:
    1. crm_cust_info      – Cleaned customer master data from CRM
    2. crm_prd_info       – Standardized CRM product metadata
    3. crm_sales_details  – Structured CRM transactional sales data
    4. erp_loc_a101       – Normalized ERP customer-country mappings
    5. erp_cust_az12      – Refined ERP customer demographic details
    6. erp_px_cat_g1v2    – Clean ERP product-category hierarchy

Data Characteristics:
    - Data types are standardized for consistency across tables
    - Nullable columns may still exist to reflect source data realities
    - Timestamps (`dwh_create_date`) are added for auditability

WARNING:
    This script will drop and recreate all silver layer tables.
    Executing this will permanently delete any existing silver layer data.
    Ensure that any downstream dependencies or views are handled appropriately.

Instructions:
    1. Connect to the 'datawarehouse' database before execution.
    2. Ensure Bronze layer data is ingested before running this script.
    3. Run this script to create the Silver layer for analytics and transformations.
*/

-- =============================================================
-- Step 1: Drop existing silver layer tables (if they exist)
-- =============================================================
DROP TABLE IF EXISTS silver.crm_cust_info;
DROP TABLE IF EXISTS silver.crm_prd_info;
DROP TABLE IF EXISTS silver.crm_sales_details;
DROP TABLE IF EXISTS silver.erp_loc_a101;
DROP TABLE IF EXISTS silver.erp_cust_az12;
DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;

-- =============================================================
-- Step 2: Create crm_cust_info table
-- -------------------------------------------------------------
-- Purpose: Stores customer master data from CRM
-- Use Case: Join with sales and demographic tables for analytics
-- =============================================================
CREATE TABLE silver.crm_cust_info (
    cst_id              INT,            -- Unique customer ID
    cst_key             VARCHAR(50),    -- External CRM customer key
    cst_firstname      VARCHAR(50),    -- First name of customer
    cst_lastname        VARCHAR(50),    -- Last name of customer
    cst_marital_status VARCHAR(50),    -- Marital or material status
    cst_gndr            VARCHAR(50),    -- Gender
    cst_create_date     DATE,            -- Customer creation date in CRM
	dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================
-- Step 3: Create crm_prd_info table
-- -------------------------------------------------------------
-- Purpose: Stores product metadata from CRM
-- Use Case: Product hierarchy mapping, pricing analysis
-- =============================================================
CREATE TABLE silver.crm_prd_info (
    prd_id       INT,             -- Product ID
	cat_id VARCHAR(50),
    prd_key      VARCHAR(50),     -- Product unique key (used in joins)
    prd_nm       VARCHAR(50),     -- Product name
    prd_cost     INT,     -- Product cost (consider changing to NUMERIC)
    prd_line     VARCHAR(50),     -- Product line/category
    prd_start_dt DATE,       -- Product availability start date
    prd_end_dt DATE,        -- Product end/retirement date
	dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================
-- Step 4: Create crm_sales_details table
-- -------------------------------------------------------------
-- Purpose: Stores raw transactional sales orders from CRM
-- Use Case: Revenue analysis, time-series sales reports
-- =============================================================
CREATE TABLE silver.crm_sales_details (
    sls_ord_num  VARCHAR(50),     -- Sales order number
    sls_prd_key  VARCHAR(50),     -- Product key (foreign key to product table)
    sls_cust_id  INT,             -- Customer ID (foreign key to customer table)
    sls_order_dt DATE,             -- Order date (suggest casting to DATE in transformations)
    sls_ship_dt  DATE,             -- Shipping date
    sls_due_dt   DATE,             -- Due date for delivery
    sls_sales    INT,             -- Total sales amount
    sls_quantity INT,             -- Units sold
    sls_price    INT,              -- Price per unit
	dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================
-- Step 5: Create erp_loc_a101 table
-- -------------------------------------------------------------
-- Purpose: Maps customer IDs to countries from ERP
-- Use Case: Geo-level segmentation, location-based filters
-- =============================================================
CREATE TABLE silver.erp_loc_a101 (
    cid   VARCHAR(50),     -- Customer ID (ERP reference)
    cntry VARCHAR(50),      -- Country name or ISO code
	dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP	
);

-- =============================================================
-- Step 6: Create erp_cust_az12 table
-- -------------------------------------------------------------
-- Purpose: Captures demographic info like birthdate and gender
-- Use Case: Age-based segmentation, customer profiling
-- =============================================================
CREATE TABLE silver.erp_cust_az12 (
    cid   VARCHAR(50),     -- Customer ID
    bdate DATE,            -- Birth date
    gen   VARCHAR(50),      -- Gender
	dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================
-- Step 7: Create erp_px_cat_g1v2 table
-- -------------------------------------------------------------
-- Purpose: Contains product-category hierarchy from ERP
-- Use Case: Mapping products to category and subcategory
-- =============================================================
CREATE TABLE silver.erp_px_cat_g1v2 (
    id          VARCHAR(50),     -- Unique ID for product or category
    cat         VARCHAR(50),     -- Category (e.g., Electronics)
    subcat      VARCHAR(50),     -- Subcategory (e.g., Mobiles)
    maintenance VARCHAR(50),      -- Maintenance status (e.g., Active/Retired)
	dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

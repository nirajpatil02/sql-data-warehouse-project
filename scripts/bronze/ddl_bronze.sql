/*
=============================================================
Create Bronze Layer Tables (PostgreSQL Version)
=============================================================
Script Purpose:
    This script creates raw-level "bronze" layer tables in the 'datawarehouse' database
    to store ingested CRM and ERP data. These tables serve as the landing zone
    for untransformed or lightly cleaned data.

Table Summary:
    1. crm_cust_info      – CRM customer master data
    2. crm_prd_info       – CRM product master data
    3. crm_sales_details  – CRM transactional sales data
    4. erp_loc_a101       – ERP customer-country mapping
    5. erp_cust_az12      – ERP customer demographic data
    6. erp_px_cat_g1v2    – ERP product category mapping

WARNING:
    This script will drop and recreate existing tables in the bronze schema.
    Any existing data in these tables will be permanently deleted. Ensure 
    data is backed up or persisted elsewhere before execution.

Instructions:
    1. Connect to the 'datawarehouse' database before executing.
    2. Run this script as-is to reset the bronze layer table structures.
*/

-- =============================================================
-- Step 1: Drop existing bronze layer tables (if they exist)
-- =============================================================
DROP TABLE IF EXISTS bronze.crm_cust_info;
DROP TABLE IF EXISTS bronze.crm_prd_info;
DROP TABLE IF EXISTS bronze.crm_sales_details;
DROP TABLE IF EXISTS bronze.erp_loc_a101;
DROP TABLE IF EXISTS bronze.erp_cust_az12;
DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;

-- =============================================================
-- Step 2: Create crm_cust_info table
-- -------------------------------------------------------------
-- Purpose: Stores customer master data from CRM
-- Use Case: Join with sales and demographic tables for analytics
-- =============================================================
CREATE TABLE bronze.crm_cust_info (
    cst_id              INT,            -- Unique customer ID
    cst_key             VARCHAR(50),    -- External CRM customer key
    cst_firstname      VARCHAR(50),    -- First name of customer
    cst_lastname        VARCHAR(50),    -- Last name of customer
    cst_marital_status VARCHAR(50),    -- Marital or material status
    cst_gndr            VARCHAR(50),    -- Gender
    cst_create_date     DATE            -- Customer creation date in CRM
);

-- =============================================================
-- Step 3: Create crm_prd_info table
-- -------------------------------------------------------------
-- Purpose: Stores product metadata from CRM
-- Use Case: Product hierarchy mapping, pricing analysis
-- =============================================================
CREATE TABLE bronze.crm_prd_info (
    prd_id       INT,             -- Product ID
    prd_key      VARCHAR(50),     -- Product unique key (used in joins)
    prd_nm       VARCHAR(50),     -- Product name
    prd_cost     VARCHAR(50),     -- Product cost (consider changing to NUMERIC)
    prd_line     VARCHAR(50),     -- Product line/category
    prd_start_dt TIMESTAMP,       -- Product availability start date
    prd_end_date TIMESTAMP        -- Product end/retirement date
);

-- =============================================================
-- Step 4: Create crm_sales_details table
-- -------------------------------------------------------------
-- Purpose: Stores raw transactional sales orders from CRM
-- Use Case: Revenue analysis, time-series sales reports
-- =============================================================
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num  VARCHAR(50),     -- Sales order number
    sls_prd_key  VARCHAR(50),     -- Product key (foreign key to product table)
    sls_cust_id  INT,             -- Customer ID (foreign key to customer table)
    sls_order_dt INT,             -- Order date (suggest casting to DATE in transformations)
    sls_ship_dt  INT,             -- Shipping date
    sls_due_dt   INT,             -- Due date for delivery
    sls_sales    INT,             -- Total sales amount
    sls_quantity INT,             -- Units sold
    sls_price    INT              -- Price per unit
);

-- =============================================================
-- Step 5: Create erp_loc_a101 table
-- -------------------------------------------------------------
-- Purpose: Maps customer IDs to countries from ERP
-- Use Case: Geo-level segmentation, location-based filters
-- =============================================================
CREATE TABLE bronze.erp_loc_a101 (
    cid   VARCHAR(50),     -- Customer ID (ERP reference)
    cntry VARCHAR(50)      -- Country name or ISO code
);

-- =============================================================
-- Step 6: Create erp_cust_az12 table
-- -------------------------------------------------------------
-- Purpose: Captures demographic info like birthdate and gender
-- Use Case: Age-based segmentation, customer profiling
-- =============================================================
CREATE TABLE bronze.erp_cust_az12 (
    cid   VARCHAR(50),     -- Customer ID
    bdate DATE,            -- Birth date
    gen   VARCHAR(50)      -- Gender
);

-- =============================================================
-- Step 7: Create erp_px_cat_g1v2 table
-- -------------------------------------------------------------
-- Purpose: Contains product-category hierarchy from ERP
-- Use Case: Mapping products to category and subcategory
-- =============================================================
CREATE TABLE bronze.erp_px_cat_g1v2 (
    id          VARCHAR(50),     -- Unique ID for product or category
    cat         VARCHAR(50),     -- Category (e.g., Electronics)
    subcat      VARCHAR(50),     -- Subcategory (e.g., Mobiles)
    maintenance VARCHAR(50)      -- Maintenance status (e.g., Active/Retired)
);

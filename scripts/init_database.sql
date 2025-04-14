/*
=============================================================
Create Database and Schemas (PostgreSQL Version)
=============================================================
Script Purpose:
    This script creates a new PostgreSQL database named 'datawarehouse' after 
    safely terminating active connections and dropping the existing database if it exists.
    It then creates three schemas inside the newly created database: 'bronze', 'silver', and 'gold'.

WARNING:
    Running this script will forcibly terminate active connections and drop the 'datawarehouse' database if it exists.
    All data in the database will be permanently deleted. Proceed with caution
    and ensure you have proper backups before running this script.

Instructions:
    1. Connect to the 'postgres' database before executing this script.
    2. Run steps 1 and 2 to drop and recreate the 'datawarehouse' database.
    3. Then, manually connect to the newly created 'datawarehouse' DB to run steps 3 and 4.
*/

-- Step 1: Terminate active connections to 'datawarehouse'
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = 'datawarehouse'
  AND pid <> pg_backend_pid();

-- Step 2: Drop and recreate the 'datawarehouse' database
DROP DATABASE IF EXISTS datawarehouse;
CREATE DATABASE datawarehouse;

-- Step 3: Manually switch connection to 'datawarehouse' before running the next section

-- Step 4: Create schemas in the new database
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;

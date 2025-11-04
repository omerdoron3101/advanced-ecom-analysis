/* =============================================================================
   Create Database and Schemas
   ============================================================================= 

   Script Purpose:
       Initialize the Data Warehouse environment by creating the required
       database and schemas (bronze, silver, gold) following Medallion Architecture.

   ⚠️  WARNING:
       Running this script will permanently DROP and recreate the entire 'DataWarehouse'
       database. All existing data, tables, views, and stored procedures will be lost.
	   Proceed with caution ans ensure you have proper bachups before running this script.
   ============================================================================= */

-- Drop existing database if present (optional for controlled environments)
USE master;
GO

-- 1️. Set single user mode to avoid connections locking the DB
ALTER DATABASE ADVANCED_ECOM_ANALYSIS SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
GO
-- 2️. Drop the database
DROP DATABASE ADVANCED_ECOM_ANALYSIS;
GO
PRINT 'Database ADVANCED_ECOM_ANALYSIS has been fully reset.';

-- Create and switch to new Data Warehouse
CREATE DATABASE ADVANCED_ECOM_ANALYSIS;
GO
USE ADVANCED_ECOM_ANALYSIS;
GO

-- Create Medallion Architecture schemas
CREATE SCHEMA bronze;  -- Raw data
GO
CREATE SCHEMA silver;  -- Cleansed and standardized data
GO
CREATE SCHEMA gold;    -- Business-ready analytical data

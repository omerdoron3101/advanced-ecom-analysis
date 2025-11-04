/* =============================================================================
   DDL Script: Create Bronze Tables - ADVANCED_ECOM_ANALYSIS
   ============================================================================= 

   Script Purpose:
       Create all Bronze layer tables for the ADVANCED_ECOM_ANALYSIS
       Data Warehouse. The Bronze layer ingests raw CSV datasets exactly 
       as received, without transformations, providing a staging area
       for further cleaning and canonicalization in Silver.

   Key Notes:
       - All columns are stored as NVARCHAR where appropriate to avoid 
         parsing or type conversion issues during raw ingestion.
       - Table names follow the original Olist dataset naming convention.
       - This layer is intended for raw data storage; do not perform
         transformations here.
       - Medallion Architecture principle: Bronze = raw, Silver = cleaned,
         Gold = analytical.

   ⚠️ WARNING:
       Dropping existing Bronze tables will permanently remove all
       previously ingested raw data in these tables. Use caution and
       ensure backups before running this script.

   ============================================================================= */

USE ADVANCED_ECOM_ANALYSIS;
GO

----------------------------------------------------------------------------------------------------------------------------------
-- 2. Orders table (olist_orders_dataset)
IF OBJECT_ID ('bronze.olist_orders_dataset', 'U') IS NOT NULL
DROP TABLE bronze.olist_orders_dataset;
GO

CREATE TABLE bronze.olist_orders_dataset (
	order_id NVARCHAR(50),
	customer_id NVARCHAR(50),
	order_status NVARCHAR(50),
	order_purchase_timestamp NVARCHAR(50),
	order_approved_at NVARCHAR(50),
	order_delivered_carrier_date NVARCHAR(50),
	order_delivered_customer_date NVARCHAR(50),
	order_estimated_delivery_date NVARCHAR(50)
);
GO

----------------------------------------------------------------------------------------------------------------------------------
-- 3. Products table (olist_products_dataset)
IF OBJECT_ID ('bronze.olist_products_dataset', 'U') IS NOT NULL
DROP TABLE bronze.olist_products_dataset;
GO

CREATE TABLE bronze.olist_products_dataset (
	product_id NVARCHAR(50),
	product_category_name NVARCHAR(100), -- Expanded to 100 for safety
	product_name_length INT,
	product_description_length INT,
	product_photos_qty INT,
	product_weight_g INT,
	product_length_cm INT,
	product_height_cm INT,
	product_width_cm INT
);
GO

----------------------------------------------------------------------------------------------------------------------------------
-- 4. Sellers table (olist_sellers_dataset)
IF OBJECT_ID ('bronze.olist_sellers_dataset', 'U') IS NOT NULL
DROP TABLE bronze.olist_sellers_dataset;
GO

CREATE TABLE bronze.olist_sellers_dataset (
	seller_id NVARCHAR(50),
	seller_zip_code_prefix NVARCHAR(10), -- NVARCHAR(10) to handle zip codes
	seller_city NVARCHAR(50),
	seller_state NVARCHAR(50)
);
GO

----------------------------------------------------------------------------------------------------------------------------------
-- 5. Product category translation table (product_category_name_translation)
IF OBJECT_ID ('bronze.product_category_name_translation', 'U') IS NOT NULL
DROP TABLE bronze.product_category_name_translation;
GO

CREATE TABLE bronze.product_category_name_translation (
	product_category_name NVARCHAR(100), 
	product_category_name_english NVARCHAR(100)
);
GO

----------------------------------------------------------------------------------------------------------------------------------
-- 6. Customers table (olist_customers_dataset)
IF OBJECT_ID ('bronze.olist_customers_dataset', 'U') IS NOT NULL
DROP TABLE bronze.olist_customers_dataset;
GO

CREATE TABLE bronze.olist_customers_dataset (
	customer_id NVARCHAR(50),
	customer_unique_id NVARCHAR(50),
	customer_zip_code_prefix NVARCHAR(10), -- NVARCHAR(10) to handle zip codes
	customer_city NVARCHAR(50),
	customer_state NVARCHAR(50)
);
GO

----------------------------------------------------------------------------------------------------------------------------------
-- 7. Geolocation table (olist_geolocation_dataset)
IF OBJECT_ID ('bronze.olist_geolocation_dataset', 'U') IS NOT NULL
DROP TABLE bronze.olist_geolocation_dataset;
GO

CREATE TABLE bronze.olist_geolocation_dataset (
	geolocation_zip_code_prefix NVARCHAR(10), -- NVARCHAR(10) to handle zip codes
	geolocation_lat FLOAT,
	geolocation_lng FLOAT,
	geolocation_city NVARCHAR(50),
	geolocation_state NVARCHAR(50)
);
GO

----------------------------------------------------------------------------------------------------------------------------------
-- 8. Order items table (olist_order_items_dataset)
IF OBJECT_ID ('bronze.olist_order_items_dataset', 'U') IS NOT NULL
DROP TABLE bronze.olist_order_items_dataset;
GO

CREATE TABLE bronze.olist_order_items_dataset (
	order_id NVARCHAR(50),
	order_item_id INT,
	product_id NVARCHAR(50),
	seller_id NVARCHAR(50),
	shipping_limit_date NVARCHAR(50),
	price FLOAT,
	freight_value FLOAT
);
GO

----------------------------------------------------------------------------------------------------------------------------------
-- 9. Order payments table (olist_order_payments_dataset)
IF OBJECT_ID ('bronze.olist_order_payments_dataset', 'U') IS NOT NULL
DROP TABLE bronze.olist_order_payments_dataset;
GO

CREATE TABLE bronze.olist_order_payments_dataset (
	order_id NVARCHAR(50),
	payment_sequential INT,
	payment_type NVARCHAR(50),
	payment_installments INT,
	payment_value FLOAT
);
GO

----------------------------------------------------------------------------------------------------------------------------------
-- 10. Order reviews table (olist_order_reviews_dataset)
IF OBJECT_ID ('bronze.olist_order_reviews_dataset', 'U') IS NOT NULL
DROP TABLE bronze.olist_order_reviews_dataset;
GO

CREATE TABLE bronze.olist_order_reviews_dataset (
	review_id NVARCHAR(50),
	order_id NVARCHAR(50),
	review_score INT,
	review_creation_date NVARCHAR(50),
	review_answer_timestamp NVARCHAR(50)
);
GO

----------------------------------------------------------------------------------------------------------------------------------

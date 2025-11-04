/* =============================================================================
   DDL Script: Create Silver Tables
   ============================================================================= 

   Script Purpose:
       Creates the Silver layer tables in the ADVANCED_ECOM_ANALYSIS Data Warehouse.
       The Silver layer stores CLEANSED, STANDARDIZED data with proper data types,
       primary keys, and ready for analytical processing.

   Key Notes:
       - Silver tables are derived from the Bronze layer (raw data).
       - Columns have been converted to appropriate types (e.g., DATETIME, INT, DECIMAL).
       - Primary Keys and composite keys are defined for uniqueness and integrity.
       - Fact tables reference dimension tables via foreign keys (FKs) where applicable.
       - This layer is intended for reliable, clean, and normalized analytical data.

   ⚠️ WARNING:
       Running this script will DROP and RECREATE all existing Silver tables.
       Any previously loaded data in these tables will be permanently deleted.

   Best Practices:
       - Maintain naming consistency between Bronze and Silver layers.
       - Ensure source data in Bronze is accurate before loading into Silver.
       - Use this layer as the foundation for Gold analytical models.
   ============================================================================= */

USE ADVANCED_ECOM_ANALYSIS;
GO

-- 1. dim_customers
IF OBJECT_ID ('silver.dim_customers', 'U') IS NOT NULL
DROP TABLE silver.dim_customers;
GO

CREATE TABLE silver.dim_customers (
	customer_id         VARCHAR(50)     NOT NULL PRIMARY KEY,
	customer_unique_id  VARCHAR(50)     NOT NULL,
	customer_zip_code_prefix INT        NOT NULL,
	customer_city       VARCHAR(50)     NOT NULL,
	customer_state      VARCHAR(50)     NOT NULL
);
GO

-- 2. dim_orders
IF OBJECT_ID ('silver.dim_orders', 'U') IS NOT NULL
DROP TABLE silver.dim_orders;
GO

CREATE TABLE silver.dim_orders (
	order_id                        VARCHAR(50)     NOT NULL PRIMARY KEY,
	customer_id                     VARCHAR(50)     NOT NULL,
	order_status                    VARCHAR(50)     NOT NULL,
	order_purchase_timestamp        DATETIME        NOT NULL,
	order_approved_at               DATETIME        NULL,
	order_delivered_carrier_date    DATETIME        NULL,
	order_delivered_customer_date   DATETIME        NULL,
	order_estimated_delivery_date   DATE            NULL
);
GO

-- 3. dim_product
IF OBJECT_ID ('silver.dim_product', 'U') IS NOT NULL
DROP TABLE silver.dim_product;
GO

CREATE TABLE silver.dim_product (
	product_id                  VARCHAR(50)     NOT NULL PRIMARY KEY,
	product_category_name       VARCHAR(100)    NULL,
	product_name_length         INT             NULL,
	product_description_length  INT             NULL,
	product_photos_qty          INT             NULL,
	product_weight_g            DECIMAL(10, 2)  NULL,
	product_length_cm           DECIMAL(10, 2)  NULL,
	product_height_cm           DECIMAL(10, 2)  NULL,
	product_width_cm            DECIMAL(10, 2)  NULL
);
GO

-- 4. dim_sellers
IF OBJECT_ID ('silver.dim_sellers', 'U') IS NOT NULL
DROP TABLE silver.dim_sellers;
GO

CREATE TABLE silver.dim_sellers (
	seller_id                   VARCHAR(50)     NOT NULL PRIMARY KEY,
	seller_zip_code_prefix      INT             NOT NULL,
	seller_city                 VARCHAR(50)     NOT NULL,
	seller_state                VARCHAR(50)     NOT NULL
);
GO

-- 5. dim_category_name_translation
IF OBJECT_ID ('silver.dim_category_name_translation', 'U') IS NOT NULL
DROP TABLE silver.dim_category_name_translation;
GO

CREATE TABLE silver.dim_category_name_translation (
	product_category_name           VARCHAR(100) NOT NULL PRIMARY KEY,
	product_category_name_english   VARCHAR(100) NOT NULL
);
GO

-- 6. dim_geolocations
IF OBJECT_ID ('silver.dim_geolocations', 'U') IS NOT NULL
DROP TABLE silver.dim_geolocations;
GO

CREATE TABLE silver.dim_geolocations (
	geolocation_zip_code_prefix VARCHAR(10)     NOT NULL,
	geolocation_lat             FLOAT           NOT NULL,
	geolocation_lng             FLOAT           NOT NULL,
	geolocation_city            VARCHAR(50)     NOT NULL,
	geolocation_state           VARCHAR(50)     NOT NULL,
    CONSTRAINT PK_Geolocation PRIMARY KEY (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng) 
);
GO

-- 7. fact_order_items
IF OBJECT_ID ('silver.fact_order_items', 'U') IS NOT NULL
DROP TABLE silver.fact_order_items;
GO

CREATE TABLE silver.fact_order_items (
	order_id                    VARCHAR(50)     NOT NULL,
	order_item_id               INT             NOT NULL,
	product_id                  VARCHAR(50)     NOT NULL,
	seller_id                   VARCHAR(50)     NOT NULL,
	shipping_limit_date         DATETIME        NOT NULL,
	price                       DECIMAL(10, 2)  NOT NULL,
	freight_value               DECIMAL(10, 2)  NOT NULL,
    CONSTRAINT PK_Order_Items PRIMARY KEY (order_id, order_item_id)
);
GO

-- 8. fact_payments
IF OBJECT_ID ('silver.fact_payments', 'U') IS NOT NULL
DROP TABLE silver.fact_payments;
GO

CREATE TABLE silver.fact_payments (
	order_id                    VARCHAR(50)     NOT NULL,
	payment_sequential          INT             NOT NULL,
	payment_type                VARCHAR(50)     NOT NULL,
	payment_installments        INT             NOT NULL,
	payment_value               DECIMAL(10, 2)  NOT NULL,
    CONSTRAINT PK_Order_Payments PRIMARY KEY (order_id, payment_sequential)
);
GO

-- 9. fact_reviews
IF OBJECT_ID ('silver.fact_reviews', 'U') IS NOT NULL
DROP TABLE silver.fact_reviews;
GO

CREATE TABLE silver.fact_reviews (
	review_id                   VARCHAR(50)     NOT NULL PRIMARY KEY,
	order_id                    VARCHAR(50)     NOT NULL,
	review_score                INT             NOT NULL,
	review_creation_date        DATE            NOT NULL,
	review_answer_timestamp     DATETIME        NOT NULL
);
GO

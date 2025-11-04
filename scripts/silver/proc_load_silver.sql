/* =============================================================================
   Stored Procedure: Load Silver Layer (Bronze -> Silver)
   =============================================================================

   Script Purpose:
       This stored procedure loads data from the Bronze layer into the Silver layer.
       It applies cleansing, standardization, type conversion, and deduplication
       to prepare the Silver tables for analytical processing.

   Key Notes:
       - Dimension tables are populated first, followed by fact tables.
       - Data type conversions are applied (e.g., NVARCHAR -> INT/DECIMAL/DATETIME).
       - Duplicates in geolocations and reviews are handled.
       - Invalid or missing numeric values are replaced with default placeholders.
       - Table truncation ensures fresh loads each execution.

   Usage:
       EXEC silver.load_silver;
       
   ⚠️ WARNING:
       Running this procedure will TRUNCATE all Silver tables before loading.
       Existing data will be permanently deleted.
   ============================================================================= */

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        PRINT '=============================================================================';
        PRINT 'Starting Silver Layer Load';
        PRINT '=============================================================================';

        -- 1. Customers
        PRINT '>> Loading dim_customers';
        TRUNCATE TABLE silver.dim_customers;

        INSERT INTO silver.dim_customers (
            customer_id,
            customer_unique_id,
            customer_zip_code_prefix,
            customer_city,
            customer_state
        )
        SELECT
            TRIM(customer_id),
            TRIM(customer_unique_id),
            COALESCE(TRY_CAST(customer_zip_code_prefix AS INT), -1),
            UPPER(TRIM(COALESCE(customer_city,'N/A'))),
            UPPER(TRIM(COALESCE(customer_state,'N/A')))
        FROM bronze.olist_customers_dataset
        WHERE customer_id IS NOT NULL
          AND customer_unique_id IS NOT NULL
        GROUP BY customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state;

        -- 2. Orders
        PRINT '>> Loading dim_orders';
        TRUNCATE TABLE silver.dim_orders;

        INSERT INTO silver.dim_orders (
            order_id,
            customer_id,
            order_status,
            order_purchase_timestamp,
            order_approved_at,
            order_delivered_carrier_date,
            order_delivered_customer_date,
            order_estimated_delivery_date
        )
        SELECT
            TRIM(order_id),
            TRIM(COALESCE(customer_id,'N/A')),
            TRIM(COALESCE(order_status,'N/A')),
            COALESCE(TRY_CONVERT(DATETIME, order_purchase_timestamp, 103), '1900-01-01'),
            TRY_CONVERT(DATETIME, order_approved_at, 103),
            TRY_CONVERT(DATETIME, order_delivered_carrier_date, 103),
            TRY_CONVERT(DATETIME, order_delivered_customer_date, 103),
            TRY_CONVERT(DATE, order_estimated_delivery_date, 103)
        FROM bronze.olist_orders_dataset
        WHERE order_id IS NOT NULL;

        -- 3. Products
        PRINT '>> Loading dim_product';
        TRUNCATE TABLE silver.dim_product;

        INSERT INTO silver.dim_product (
            product_id,
            product_category_name,
            product_name_length,
            product_description_length,
            product_photos_qty,
            product_weight_g,
            product_length_cm,
            product_height_cm,
            product_width_cm
        )
        SELECT
            TRIM(product_id),
            TRIM(product_category_name),
            product_name_length,
            product_description_length,
            product_photos_qty,
            CASE WHEN TRY_CAST(product_weight_g AS DECIMAL(10,2)) <= 0 THEN -1 ELSE TRY_CAST(product_weight_g AS DECIMAL(10,2)) END,
            CASE WHEN TRY_CAST(product_length_cm AS DECIMAL(10,2)) <= 0 THEN -1 ELSE TRY_CAST(product_length_cm AS DECIMAL(10,2)) END,
            CASE WHEN TRY_CAST(product_height_cm AS DECIMAL(10,2)) <= 0 THEN -1 ELSE TRY_CAST(product_height_cm AS DECIMAL(10,2)) END,
            CASE WHEN TRY_CAST(product_width_cm AS DECIMAL(10,2)) <= 0 THEN -1 ELSE TRY_CAST(product_width_cm AS DECIMAL(10,2)) END
        FROM bronze.olist_products_dataset
        WHERE product_id IS NOT NULL;

        -- 4. Sellers
        PRINT '>> Loading dim_sellers';
        TRUNCATE TABLE silver.dim_sellers;

        INSERT INTO silver.dim_sellers (
            seller_id,
            seller_zip_code_prefix,
            seller_city,
            seller_state
        )
        SELECT
            TRIM(seller_id),
            COALESCE(TRY_CAST(seller_zip_code_prefix AS INT), -1),
            TRIM(UPPER(CASE WHEN seller_city IS NULL OR seller_city LIKE '%[0-9]%' THEN 'N/A' ELSE seller_city END)),
            TRIM(UPPER(COALESCE(seller_state,'N/A')))
        FROM bronze.olist_sellers_dataset
        WHERE seller_id IS NOT NULL;

        -- 5. Category Translation
        PRINT '>> Loading dim_category_name_translation';
        TRUNCATE TABLE silver.dim_category_name_translation;

        INSERT INTO silver.dim_category_name_translation (
            product_category_name,
            product_category_name_english
        )
        SELECT
            TRIM(product_category_name),
            TRIM(product_category_name_english)
        FROM bronze.product_category_name_translation;

        -- 6. Geolocations
        PRINT '>> Loading dim_geolocations';
        TRUNCATE TABLE silver.dim_geolocations;

        WITH lat_lng_cleanup AS (
            SELECT
                TRIM(geolocation_zip_code_prefix) AS geolocation_zip_code_prefix,
                ROUND(AVG(geolocation_lat),6) AS geolocation_lat,
                ROUND(AVG(geolocation_lng),6) AS geolocation_lng
            FROM bronze.olist_geolocation_dataset
            WHERE geolocation_zip_code_prefix IS NOT NULL
              AND geolocation_lat IS NOT NULL
              AND geolocation_lng IS NOT NULL
            GROUP BY geolocation_zip_code_prefix
        ),
        geolocation_dedup AS (
            SELECT
                TRIM(gd.geolocation_zip_code_prefix) AS geolocation_zip_code_prefix,
                llc.geolocation_lat,
                llc.geolocation_lng,
                TRIM(gd.geolocation_city) AS geolocation_city,
                TRIM(gd.geolocation_state) AS geolocation_state,
                ROW_NUMBER() OVER (
                    PARTITION BY TRIM(gd.geolocation_zip_code_prefix), llc.geolocation_lat, llc.geolocation_lng
                    ORDER BY gd.geolocation_city
                ) AS rn
            FROM bronze.olist_geolocation_dataset gd
            INNER JOIN lat_lng_cleanup llc
                ON TRIM(gd.geolocation_zip_code_prefix) = llc.geolocation_zip_code_prefix
        )
        INSERT INTO silver.dim_geolocations (
            geolocation_zip_code_prefix,
            geolocation_lat,
            geolocation_lng,
            geolocation_city,
            geolocation_state
        )
        SELECT
            geolocation_zip_code_prefix,
            geolocation_lat,
            geolocation_lng,
            geolocation_city,
            geolocation_state
        FROM geolocation_dedup
        WHERE rn = 1;

        -- 7. Order Items
        PRINT '>> Loading fact_order_items';
        TRUNCATE TABLE silver.fact_order_items;

        INSERT INTO silver.fact_order_items (
            order_id,
            order_item_id,
            product_id,
            seller_id,
            shipping_limit_date,
            price,
            freight_value
        )
        SELECT
            TRIM(order_id),
            order_item_id,
            COALESCE(TRIM(product_id),'N/A'),
            COALESCE(TRIM(seller_id),'N/A'),
            COALESCE(TRY_CONVERT(DATETIME, shipping_limit_date, 103),'1900-01-01'),
            CASE WHEN price IS NULL OR price <= 0 THEN -1 ELSE price END,
            CASE WHEN freight_value IS NULL OR freight_value < 0 THEN 0 ELSE freight_value END
        FROM bronze.olist_order_items_dataset
        WHERE order_id IS NOT NULL
          AND order_item_id IS NOT NULL;

        -- 8. Payments
        PRINT '>> Loading fact_payments';
        TRUNCATE TABLE silver.fact_payments;

        INSERT INTO silver.fact_payments (
            order_id,
            payment_sequential,
            payment_type,
            payment_installments,
            payment_value
        )
        SELECT
            TRIM(order_id),
            payment_sequential,
            COALESCE(LOWER(REPLACE(TRIM(payment_type),' ','_')),'N/A'),
            COALESCE(payment_installments,-1),
            COALESCE(TRY_CAST(payment_value AS DECIMAL(10,2)),-1)
        FROM bronze.olist_order_payments_dataset
        WHERE order_id IS NOT NULL
          AND payment_sequential IS NOT NULL;

        -- 9. Reviews
        PRINT '>> Loading fact_reviews';
        TRUNCATE TABLE silver.fact_reviews;

        WITH reviews_dedup AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY review_id
                       ORDER BY review_answer_timestamp DESC
                   ) AS rn
            FROM bronze.olist_order_reviews_dataset
        )
        INSERT INTO silver.fact_reviews (
            review_id,
            order_id,
            review_score,
            review_creation_date,
            review_answer_timestamp
        )
        SELECT 
            TRIM(review_id),
            TRIM(order_id),
            COALESCE(review_score,-1),
            COALESCE(TRY_CONVERT(DATE, review_creation_date,103),'1900-01-01'),
            COALESCE(TRY_CONVERT(DATETIME, review_answer_timestamp,103),'1900-01-01')
        FROM reviews_dedup
        WHERE rn = 1;

        PRINT '=============================================================================';
        PRINT 'Silver Layer Load Completed Successfully';
        PRINT '=============================================================================';

    END TRY
    BEGIN CATCH
        PRINT '=============================================================================';
        PRINT 'ERROR during Silver Layer Load:';
        PRINT ERROR_MESSAGE();
        PRINT '=============================================================================';
    END CATCH
END
GO

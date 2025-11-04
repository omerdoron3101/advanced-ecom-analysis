/* =============================================================================
   GOLD VIEWS SCRIPT
   =============================================================================

Script Purpose:
  Create analytical Gold layer views in the Data Warehouse.
  These views consolidate and transform Silver layer data into business-ready analytical tables.
  The views include:
  1. Dimensional Views (Products, Customers, Sellers, Orders)
  2. Fact Views (Order Items, Payments, Reviews)

⚠️ WARNING:
  Running this script will ALTER or CREATE VIEWS in the 'gold' schema.
  Existing views with the same name will be replaced. Ensure this is intended.

=============================================================================== */

-- ===============================
-- 1. Dimensional Views
-- ===============================

-- dim_product_view
CREATE OR ALTER VIEW gold.dim_product_view AS
SELECT
    p.product_id,
    p.product_category_name,
    t.product_category_name_english,
    NULLIF(p.product_name_length, -1) AS product_name_length,
    NULLIF(p.product_description_length, -1) AS product_description_length,
    NULLIF(p.product_photos_qty, -1) AS product_photos_qty,
    NULLIF(p.product_weight_g, -1) AS product_weight_g,
    NULLIF(p.product_length_cm, -1) AS product_length_cm,
    NULLIF(p.product_height_cm, -1) AS product_height_cm,
    NULLIF(p.product_width_cm, -1) AS product_width_cm
FROM silver.dim_product p
LEFT JOIN silver.dim_category_name_translation t 
    ON p.product_category_name = t.product_category_name;
GO

-- dim_customers_view
CREATE OR ALTER VIEW gold.dim_customers_view AS
SELECT
    customer_id,
    customer_unique_id,
    customer_city,
    customer_state,
    NULLIF(customer_zip_code_prefix, -1) AS customer_zip_code_prefix
FROM silver.dim_customers;
GO

-- dim_sellers_view
CREATE OR ALTER VIEW gold.dim_sellers_view AS
SELECT
    seller_id,
    seller_city,
    seller_state,
    NULLIF(seller_zip_code_prefix, -1) AS seller_zip_code_prefix
FROM silver.dim_sellers;
GO

-- dim_orders_view
CREATE OR ALTER VIEW gold.dim_orders_view AS
SELECT
    order_id,
    customer_id,
    order_status,
    CASE WHEN order_purchase_timestamp <> '1900-01-01' THEN order_purchase_timestamp ELSE NULL END AS order_purchase_timestamp,
    CASE WHEN order_approved_at <> '1900-01-01' THEN order_approved_at ELSE NULL END AS order_approved_at,
    CASE WHEN order_delivered_carrier_date <> '1900-01-01' THEN order_delivered_carrier_date ELSE NULL END AS order_delivered_carrier_date,
    CASE WHEN order_delivered_customer_date <> '1900-01-01' THEN order_delivered_customer_date ELSE NULL END AS order_delivered_customer_date,
    CASE WHEN order_estimated_delivery_date <> '1900-01-01' THEN order_estimated_delivery_date ELSE NULL END AS order_estimated_delivery_date
FROM silver.dim_orders;
GO

-- ===============================
-- 2. Fact Views
-- ===============================

-- fact_order_items_view
CREATE OR ALTER VIEW gold.fact_order_items_view AS
SELECT 
    f.order_id,
    f.order_item_id,
    f.product_id,
    p.product_category_name,
    t.product_category_name_english,
    f.seller_id,
    s.seller_city,
    s.seller_state,
    o.customer_id,
    c.customer_city,
    c.customer_state,
    NULLIF(f.price, -1) AS price,
    NULLIF(f.freight_value, -1) AS freight_value,
    COALESCE(NULLIF(f.price, -1), 0) + COALESCE(NULLIF(f.freight_value, -1), 0) AS item_total_value,
    CASE 
        WHEN o.order_delivered_customer_date IS NOT NULL 
             AND o.order_delivered_customer_date <> '1900-01-01'
        THEN DATEDIFF(day, o.order_purchase_timestamp, o.order_delivered_customer_date)
        ELSE NULL
    END AS shipping_days,
    NULLIF(f.shipping_limit_date, '1900-01-01') AS shipping_limit_date
FROM silver.fact_order_items f
LEFT JOIN silver.dim_product p ON f.product_id = p.product_id
LEFT JOIN silver.dim_category_name_translation t ON p.product_category_name = t.product_category_name
LEFT JOIN silver.dim_sellers s ON f.seller_id = s.seller_id
LEFT JOIN silver.dim_orders o ON f.order_id = o.order_id
LEFT JOIN silver.dim_customers c ON o.customer_id = c.customer_id;
GO

-- fact_payments_view
CREATE OR ALTER VIEW gold.fact_payments_view AS
SELECT
    p.order_id,
    p.payment_sequential,
    p.payment_type,
    p.payment_installments,
    NULLIF(p.payment_value, -1) AS payment_value,
    o.customer_id,
    c.customer_city,
    c.customer_state
FROM silver.fact_payments p
LEFT JOIN silver.dim_orders o ON p.order_id = o.order_id
LEFT JOIN silver.dim_customers c ON o.customer_id = c.customer_id;
GO

-- fact_reviews_view
CREATE OR ALTER VIEW gold.fact_reviews_view AS
SELECT
    r.review_id,
    r.order_id,
    NULLIF(r.review_score, -1) AS review_score,
    CASE WHEN r.review_creation_date <> '1900-01-01' THEN r.review_creation_date ELSE NULL END AS review_creation_date,
    CASE WHEN r.review_answer_timestamp <> '1900-01-01' THEN r.review_answer_timestamp ELSE NULL END AS review_answer_timestamp,
    o.customer_id,
    c.customer_city,
    c.customer_state,
    f.product_id,
    s.seller_id,
    s.seller_city,
    s.seller_state
FROM silver.fact_reviews r
LEFT JOIN silver.dim_orders o ON r.order_id = o.order_id
LEFT JOIN silver.dim_customers c ON o.customer_id = c.customer_id
LEFT JOIN silver.fact_order_items f ON r.order_id = f.order_id
LEFT JOIN silver.dim_sellers s ON f.seller_id = s.seller_id;
GO

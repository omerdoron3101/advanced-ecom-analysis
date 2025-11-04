/* =============================================================================
   Seller Performance Analysis
   =============================================================================
   Script Purpose:
      Analyze seller performance by product category using Gold layer views. 
      The queries compute key metrics such as revenue, orders, average price,
      shipping speed, and customer satisfaction, and classify sellers into tiers 
      for revenue, volume, shipping, and reviews.

      Key Metrics:
         1. Products sold per category
         2. Total orders per seller
         3. Average product price per seller
         4. Average shipping days
         5. Average customer review score
         6. Tiers for revenue, volume, shipping, and review satisfaction

      Notes:
         - Uses Gold layer views: dim_sellers_view, dim_product_view, dim_orders_view,
           fact_order_items_view, fact_reviews_view.
         - Shipping and review tiers are based on average performance thresholds.
         - The queries are read-only and do not modify any data.
   ============================================================================= */

-- ===============================
-- 1. Seller Insights by Product Category
-- ===============================

WITH seller_reviews AS (
    SELECT 
        f.seller_id,
        f.product_id,
        AVG(r.review_score) AS avg_review_score
    FROM gold.fact_reviews_view AS r
    LEFT JOIN gold.fact_order_items_view AS f 
        ON r.order_id = f.order_id
    GROUP BY f.seller_id, f.product_id
),

seller_products AS (
    SELECT 
        f.seller_id,
        f.product_id,
        AVG(f.shipping_days) AS avg_shipping_days,
        AVG(f.price) AS avg_price,
        COUNT(DISTINCT f.order_id) AS order_count
    FROM gold.fact_order_items_view AS f
    WHERE f.shipping_days IS NOT NULL
    GROUP BY f.seller_id, f.product_id
)

SELECT 
    p.product_category_name_english,
    s.seller_id,
    se.seller_city,
    se.seller_state,
    COUNT(DISTINCT sp.product_id) AS products_sold,
    SUM(sp.order_count) AS total_orders,
    ROUND(AVG(sp.avg_price), 2) AS avg_product_price,
    ROUND(AVG(sp.avg_shipping_days), 2) AS avg_shipping_days,
    ROUND(AVG(sr.avg_review_score), 2) AS avg_review_score,
    
    -- Shipping tier based on average shipping days
    CASE 
        WHEN ROUND(AVG(sp.avg_shipping_days), 2) <= 7 THEN 'Fast Shipping'
        WHEN ROUND(AVG(sp.avg_shipping_days), 2) BETWEEN 8 AND 10 THEN 'Moderate'
        ELSE 'Slow'
    END AS shipping_tier,
    
    -- Customer satisfaction tier based on average review score
    CASE 
        WHEN ROUND(AVG(sr.avg_review_score), 2) >= 4.5 THEN 'Excellent'
        WHEN ROUND(AVG(sr.avg_review_score), 2) BETWEEN 3.5 AND 4.4 THEN 'Good'
        ELSE 'Poor'
    END AS satisfaction_tier
FROM gold.dim_product_view AS p
LEFT JOIN seller_products AS sp 
    ON p.product_id = sp.product_id
LEFT JOIN seller_reviews AS sr 
    ON p.product_id = sr.product_id AND sp.seller_id = sr.seller_id
LEFT JOIN gold.dim_sellers_view AS se 
    ON sp.seller_id = se.seller_id
LEFT JOIN gold.fact_order_items_view AS s 
    ON p.product_id = s.product_id AND s.seller_id = sp.seller_id
GROUP BY 
    p.product_category_name_english,
    s.seller_id,
    se.seller_city,
    se.seller_state
ORDER BY 
    p.product_category_name_english,
    avg_review_score DESC;

-- ===============================
-- 2. Overall Seller Metrics and Tier Classification
-- ===============================

WITH seller_metrics AS (
    SELECT
        s.seller_id,
        s.seller_city,
        s.seller_state,
        COUNT(DISTINCT f.order_id) AS total_orders,
        SUM(COALESCE(f.price,0) + COALESCE(f.freight_value,0)) AS total_revenue,
        AVG(DATEDIFF(day, o.order_purchase_timestamp, o.order_delivered_customer_date)) AS avg_shipping_days,
        AVG(r.review_score) AS avg_review_score
    FROM gold.dim_sellers_view AS s
    LEFT JOIN gold.fact_order_items_view AS f 
        ON s.seller_id = f.seller_id
    LEFT JOIN gold.dim_orders_view AS o 
        ON f.order_id = o.order_id
    LEFT JOIN gold.fact_reviews_view AS r 
        ON f.order_id = r.order_id
    GROUP BY s.seller_id, s.seller_city, s.seller_state
)

SELECT *,
    -- Revenue Tier
    CASE 
        WHEN total_revenue >= (SELECT AVG(total_revenue)*1.5 FROM seller_metrics) THEN 'High Revenue'
        WHEN total_revenue >= (SELECT AVG(total_revenue) FROM seller_metrics) THEN 'Moderate Revenue'
        ELSE 'Low Revenue' 
    END AS revenue_tier,
    
    -- Volume Tier
    CASE 
        WHEN total_orders >= (SELECT AVG(total_orders)*1.5 FROM seller_metrics) THEN 'High Volume'
        WHEN total_orders >= (SELECT AVG(total_orders) FROM seller_metrics) THEN 'Moderate Volume'
        ELSE 'Low Volume' 
    END AS volume_tier,
    
    -- Shipping Tier
    CASE 
        WHEN avg_shipping_days <= 5 THEN 'Fast'
        WHEN avg_shipping_days <= 10 THEN 'Moderate'
        ELSE 'Slow' 
    END AS shipping_tier,
    
    -- Review Tier
    CASE 
        WHEN avg_review_score >= 4.5 THEN 'Excellent'
        WHEN avg_review_score >= 3.5 THEN 'Good'
        ELSE 'Poor' 
    END AS review_tier
FROM seller_metrics
ORDER BY total_revenue DESC;

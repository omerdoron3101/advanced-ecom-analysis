/* =============================================================================
   Dimensional Geo Insights
   =============================================================================
   Script Purpose:
      Generate dimensional insights on product category and geographic performance 
      using Gold layer views. The script aggregates sales, revenue, shipping, 
      and customer satisfaction metrics at multiple levels:
         1. Product Category Performance
         2. Geographic Analysis by City/State
         3. City Performance Metrics
         4. City-Category Performance Metrics

      Key Metrics:
         - Average review scores per product and category
         - Average shipping days
         - Average product price
         - Total orders and revenue
         - Rankings and performance tiers (Revenue, Satisfaction, Shipping)

      Notes:
         - Uses Gold layer views: dim_product_view, fact_order_items_view, 
           fact_payments_view, fact_reviews_view.
         - Queries are read-only.
         - Tiers are calculated based on relative averages across the dataset.
   ============================================================================= */

-- 1. Product Category Performance
WITH score_by_product AS (
    SELECT 
        product_id,
        AVG(review_score) AS product_avg_score
    FROM gold.fact_reviews_view 
    GROUP BY product_id
),
products_details AS (
    SELECT 
        product_id,
        AVG(shipping_days) AS avg_shipping_days,
        AVG(price) AS avg_product_price,
        COUNT(order_id) AS product_orders
    FROM gold.fact_order_items_view
    WHERE shipping_days IS NOT NULL
    GROUP BY product_id
)
SELECT 
    p.product_category_name_english,
    COUNT(p.product_id) AS product_count,
    ROUND(AVG(s.product_avg_score), 2) AS category_avg_score,
    ROUND(AVG(pd.avg_shipping_days), 2) AS category_avg_shipping_days,
    CASE 
        WHEN ROUND(AVG(pd.avg_shipping_days), 2) <= 7 THEN 'Fast Shipping'
        WHEN ROUND(AVG(pd.avg_shipping_days), 2) BETWEEN 8 AND 10 THEN 'Moderate Shipping'
        ELSE 'Slow Shipping'
    END AS shipping_performance_tier,
    ROUND(AVG(pd.avg_product_price), 2) AS category_avg_product_price,
    CASE 
        WHEN ROUND(AVG(pd.avg_product_price), 2) <= 70 THEN 'Cheap'
        WHEN ROUND(AVG(pd.avg_product_price), 2) BETWEEN 71 AND 110 THEN 'Moderate'
        ELSE 'Expensive'
    END AS price_tier,
    SUM(pd.product_orders) AS category_order_count,
    ROUND(CAST(SUM(pd.product_orders) AS FLOAT) / COUNT(p.product_id), 2) AS orders_per_product
FROM gold.dim_product_view AS p
LEFT JOIN score_by_product AS s
    ON p.product_id = s.product_id
LEFT JOIN products_details AS pd
    ON p.product_id = pd.product_id
GROUP BY p.product_category_name_english
ORDER BY category_avg_score DESC;

-- 2. Geographic Analysis (City/State level)
WITH geo_analysis AS (
    SELECT 
        oi.customer_city,
        oi.customer_state,
        COUNT(DISTINCT oi.order_id) AS total_orders,
        COUNT(DISTINCT oi.product_id) AS total_products,
        COUNT(DISTINCT p.product_category_name_english) AS distinct_categories,
        ROUND(SUM(pay.payment_value), 2) AS total_revenue,
        ROUND(AVG(pay.payment_value), 2) AS avg_order_value,
        ROUND(AVG(r.review_score), 2) AS avg_review_score
    FROM gold.fact_order_items_view AS oi
    LEFT JOIN gold.fact_payments_view AS pay
        ON oi.order_id = pay.order_id
    LEFT JOIN gold.fact_reviews_view AS r
        ON oi.order_id = r.order_id
    LEFT JOIN gold.dim_product_view AS p
        ON oi.product_id = p.product_id
    GROUP BY oi.customer_city, oi.customer_state
)
SELECT *
FROM geo_analysis
ORDER BY total_revenue DESC;

-- 3. City Performance Metrics
WITH city_performance AS (
    SELECT 
        f.customer_city,
        f.customer_state,
        COUNT(DISTINCT f.order_id) AS total_orders,
        COUNT(DISTINCT f.product_id) AS total_products,
        COUNT(DISTINCT p.product_category_name_english) AS distinct_categories,
        ROUND(SUM(fp.payment_value), 2) AS total_revenue,
        ROUND(AVG(fp.payment_value), 2) AS avg_order_value,
        ROUND(AVG(fr.review_score), 2) AS avg_review_score,
        ROUND(AVG(f.shipping_days), 2) AS avg_shipping_days
    FROM gold.fact_order_items_view AS f
    LEFT JOIN gold.fact_payments_view AS fp ON f.order_id = fp.order_id
    LEFT JOIN gold.fact_reviews_view AS fr ON f.order_id = fr.order_id
    LEFT JOIN gold.dim_product_view AS p ON f.product_id = p.product_id
    WHERE f.customer_city IS NOT NULL
    GROUP BY f.customer_city, f.customer_state
),
ranked AS (
    SELECT *,
        RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank,
        RANK() OVER (ORDER BY avg_review_score DESC) AS satisfaction_rank,
        RANK() OVER (ORDER BY avg_shipping_days ASC) AS shipping_efficiency_rank,
        CASE 
            WHEN total_revenue >= (SELECT AVG(total_revenue) * 1.5 FROM city_performance) THEN 'High Revenue'
            WHEN total_revenue >= (SELECT AVG(total_revenue) FROM city_performance) THEN 'Moderate Revenue'
            ELSE 'Low Revenue'
        END AS revenue_tier,
        CASE 
            WHEN avg_review_score >= (SELECT AVG(avg_review_score) * 1.1 FROM city_performance) THEN 'Excellent Satisfaction'
            WHEN avg_review_score >= (SELECT AVG(avg_review_score) FROM city_performance) THEN 'Good Satisfaction'
            ELSE 'Low Satisfaction'
        END AS satisfaction_tier,
        CASE 
            WHEN avg_shipping_days <= (SELECT AVG(avg_shipping_days) * 0.9 FROM city_performance) THEN 'Fast Shipping'
            WHEN avg_shipping_days <= (SELECT AVG(avg_shipping_days) FROM city_performance) THEN 'Moderate Shipping'
            ELSE 'Slow Shipping'
        END AS shipping_tier
    FROM city_performance
)
SELECT TOP 10 *
FROM ranked
ORDER BY total_revenue DESC;

-- 4. City-Category Performance Metrics
WITH city_category_stats AS (
    SELECT 
        f.customer_city,
        f.customer_state,
        p.product_category_name_english,
        COUNT(DISTINCT f.order_id) AS total_orders,
        COUNT(DISTINCT f.product_id) AS unique_products,
        COUNT(DISTINCT p.product_category_name_english) AS distinct_categories,
        ROUND(SUM(fp.payment_value), 2) AS total_revenue,
        ROUND(AVG(fp.payment_value), 2) AS avg_order_value,
        ROUND(AVG(fr.review_score), 2) AS avg_review_score,
        ROUND(AVG(f.shipping_days), 2) AS avg_shipping_days
    FROM gold.fact_order_items_view AS f
    LEFT JOIN gold.fact_payments_view AS fp ON f.order_id = fp.order_id
    LEFT JOIN gold.fact_reviews_view AS fr ON f.order_id = fr.order_id
    LEFT JOIN gold.dim_product_view AS p ON f.product_id = p.product_id
    WHERE f.customer_city IS NOT NULL 
      AND p.product_category_name_english IS NOT NULL
    GROUP BY f.customer_city, f.customer_state, p.product_category_name_english
),
ranked_city_categories AS (
    SELECT *,
        RANK() OVER (PARTITION BY customer_city ORDER BY total_revenue DESC) AS category_revenue_rank,
        RANK() OVER (PARTITION BY customer_city ORDER BY avg_review_score DESC) AS category_satisfaction_rank
    FROM city_category_stats
),
performance_tiers AS (
    SELECT *,
        CASE 
            WHEN total_revenue >= (SELECT AVG(total_revenue) * 1.5 FROM city_category_stats) THEN 'High Revenue'
            WHEN total_revenue >= (SELECT AVG(total_revenue) FROM city_category_stats) THEN 'Moderate Revenue'
            ELSE 'Low Revenue'
        END AS revenue_tier,
        CASE 
            WHEN avg_review_score >= (SELECT AVG(avg_review_score) * 1.1 FROM city_category_stats) THEN 'Excellent Satisfaction'
            WHEN avg_review_score >= (SELECT AVG(avg_review_score) FROM city_category_stats) THEN 'Good Satisfaction'
            ELSE 'Low Satisfaction'
        END AS satisfaction_tier,
        CASE 
            WHEN avg_shipping_days <= (SELECT AVG(avg_shipping_days) * 0.9 FROM city_category_stats) THEN 'Fast Shipping'
            WHEN avg_shipping_days <= (SELECT AVG(avg_shipping_days) FROM city_category_stats) THEN 'Moderate Shipping'
            ELSE 'Slow Shipping'
        END AS shipping_tier
    FROM ranked_city_categories
)
SELECT *
FROM performance_tiers
WHERE category_revenue_rank = 1
ORDER BY total_revenue DESC;

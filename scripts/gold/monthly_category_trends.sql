/* =============================================================================
   Monthly Category Trends
   =============================================================================
   Script Purpose:
      Calculate monthly financial and operational metrics per product category
      using Gold layer views, and compute month-over-month growth trends.

      Key Metrics:
         1. Total Orders per category per month
         2. Total Revenue per category per month (price + freight)
         3. Average Review Score per category per month
         4. Month-over-Month Revenue Growth Percentage
         5. Month-over-Month Orders Growth Percentage

      Notes:
         - Uses Gold layer views: fact_order_items_view, dim_orders_view, dim_product_view, fact_reviews_view.
         - Growth percentages are calculated using LAG window functions.
         - The query is read-only and does not modify any data.
   ============================================================================= */

WITH monthly_metrics AS (
    SELECT 
        DATEPART(YEAR, o.order_purchase_timestamp) AS year,
        DATEPART(MONTH, o.order_purchase_timestamp) AS month,
        p.product_category_name,
        p.product_category_name_english,
        COUNT(DISTINCT f.order_id) AS total_orders,
        SUM(COALESCE(f.price,0) + COALESCE(f.freight_value,0)) AS total_revenue,
        AVG(r.review_score) AS avg_review_score
    FROM gold.fact_order_items_view AS f
    LEFT JOIN gold.dim_orders_view AS o 
        ON f.order_id = o.order_id
    LEFT JOIN gold.dim_product_view AS p 
        ON f.product_id = p.product_id
    LEFT JOIN gold.fact_reviews_view AS r 
        ON f.order_id = r.order_id
    WHERE o.order_purchase_timestamp IS NOT NULL
    GROUP BY 
        DATEPART(YEAR, o.order_purchase_timestamp),
        DATEPART(MONTH, o.order_purchase_timestamp),
        p.product_category_name,
        p.product_category_name_english
),

monthly_trends AS (
    SELECT *,
        -- Previous month revenue for growth calculation
        LAG(total_revenue) OVER (PARTITION BY product_category_name_english ORDER BY year, month) AS prev_month_revenue,

        -- Month-over-Month Revenue Growth Percentage
        CASE 
            WHEN LAG(total_revenue) OVER (PARTITION BY product_category_name_english ORDER BY year, month) IS NULL THEN NULL
            ELSE (total_revenue - LAG(total_revenue) OVER (PARTITION BY product_category_name_english ORDER BY year, month)) 
                 / LAG(total_revenue) OVER (PARTITION BY product_category_name_english ORDER BY year, month) * 100
        END AS revenue_growth_pct,

        -- Previous month orders for growth calculation
        LAG(total_orders) OVER (PARTITION BY product_category_name_english ORDER BY year, month) AS prev_month_orders,

        -- Month-over-Month Orders Growth Percentage
        CASE 
            WHEN LAG(total_orders) OVER (PARTITION BY product_category_name_english ORDER BY year, month) IS NULL THEN NULL
            ELSE (total_orders - LAG(total_orders) OVER (PARTITION BY product_category_name_english ORDER BY year, month)) 
                 / LAG(total_orders) OVER (PARTITION BY product_category_name_english ORDER BY year, month) * 100
        END AS orders_growth_pct
    FROM monthly_metrics
)

-- Final output: monthly metrics and growth trends per category
SELECT *
FROM monthly_trends
ORDER BY 
    product_category_name_english,
    product_category_name,
    year,
    month;

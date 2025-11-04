/* =============================================================================
   Monthly Category Alerts
   =============================================================================
   Script Purpose:
      Identify potential issues in monthly product category performance 
      using Gold layer views. The query calculates total revenue and average 
      shipping days per category per month and generates alerts for:
         1. Revenue drop compared to previous month
         2. Rolling average shipping time exceeding threshold (slow shipping)

      Key Metrics:
         - Total Revenue per category per month
         - Previous Month Revenue
         - Revenue Difference
         - Revenue Alert (if revenue decreased)
         - Rolling Average Shipping Days (3-month window)
         - Shipping Alert (if rolling average shipping > 10 days)

      Notes:
         - Uses Gold layer views: fact_order_items_view, dim_orders_view, dim_product_view.
         - The query is read-only and does not modify any data.
         - Alerts are generated only for categories with potential issues.
   ============================================================================= */

WITH monthly_category AS (
    SELECT
        DATEPART(YEAR, o.order_purchase_timestamp) AS year,
        DATEPART(MONTH, o.order_purchase_timestamp) AS month,
        p.product_category_name_english,
        SUM(COALESCE(f.price,0) + COALESCE(f.freight_value,0)) AS total_revenue,
        AVG(DATEDIFF(day, o.order_purchase_timestamp, o.order_delivered_customer_date)) AS avg_shipping_days
    FROM gold.fact_order_items_view AS f
    LEFT JOIN gold.dim_orders_view AS o 
        ON f.order_id = o.order_id
    LEFT JOIN gold.dim_product_view AS p 
        ON f.product_id = p.product_id
    WHERE o.order_purchase_timestamp IS NOT NULL
    GROUP BY 
        DATEPART(YEAR, o.order_purchase_timestamp),
        DATEPART(MONTH, o.order_purchase_timestamp),
        p.product_category_name_english
),

category_trends AS (
    SELECT *,
        -- Previous month revenue for comparison
        LAG(total_revenue) OVER (PARTITION BY product_category_name_english ORDER BY year, month) AS prev_month_revenue,

        -- Revenue difference compared to previous month
        total_revenue - LAG(total_revenue) OVER (PARTITION BY product_category_name_english ORDER BY year, month) AS revenue_diff,

        -- Revenue alert if revenue decreased
        CASE
            WHEN total_revenue - LAG(total_revenue) OVER (PARTITION BY product_category_name_english ORDER BY year, month) < 0
            THEN 'Revenue Drop!'
            ELSE NULL
        END AS revenue_alert,

        -- Rolling 3-month average of shipping days
        AVG(avg_shipping_days) OVER (
            PARTITION BY product_category_name_english 
            ORDER BY year 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS rolling_avg_shipping,

        -- Shipping alert if rolling average shipping exceeds 10 days
        CASE 
            WHEN AVG(avg_shipping_days) OVER (
                    PARTITION BY product_category_name_english 
                    ORDER BY year 
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                 ) > 10
            THEN 'Slow Shipping Alert!'
            ELSE NULL
        END AS shipping_alert
    FROM monthly_category
)

-- Output only categories with alerts
SELECT
    year,
    month,
    product_category_name_english,
    total_revenue,
    prev_month_revenue,
    revenue_diff,
    revenue_alert,
    rolling_avg_shipping,
    shipping_alert
FROM category_trends
WHERE revenue_alert IS NOT NULL OR shipping_alert IS NOT NULL
ORDER BY year, month, product_category_name_english;

/* =============================================================================
   Seller Monthly Alerts
   =============================================================================
   Script Purpose:
      Identify potential issues in monthly seller performance using Gold layer 
      views. The query calculates total revenue and average shipping days per 
      seller per month, and generates alerts for:
         1. Revenue drop compared to previous month
         2. Rolling average shipping time exceeding threshold (slow shipping)

      Key Metrics:
         - Total Revenue per seller per month
         - Previous Month Revenue
         - Revenue Difference
         - Revenue Alert (if revenue decreased)
         - Rolling Average Shipping Days (3-month window)
         - Shipping Alert (if rolling average shipping > 10 days)

      Notes:
         - Uses Gold layer views: dim_sellers_view, dim_orders_view, fact_order_items_view.
         - Alerts are generated only for sellers with potential issues.
         - The query is read-only and does not modify any data.
   ============================================================================= */

WITH seller_monthly AS (
    SELECT
        s.seller_id,
        s.seller_city,
        s.seller_state,
        DATEPART(YEAR, o.order_purchase_timestamp) AS year,
        DATEPART(MONTH, o.order_purchase_timestamp) AS month,
        COUNT(DISTINCT f.order_id) AS total_orders,
        SUM(COALESCE(f.price,0) + COALESCE(f.freight_value,0)) AS total_revenue,
        AVG(DATEDIFF(day, o.order_purchase_timestamp, o.order_delivered_customer_date)) AS avg_shipping_days
    FROM gold.dim_sellers_view AS s
    LEFT JOIN gold.fact_order_items_view AS f 
        ON s.seller_id = f.seller_id
    LEFT JOIN gold.dim_orders_view AS o 
        ON f.order_id = o.order_id
    WHERE o.order_purchase_timestamp IS NOT NULL
    GROUP BY 
        s.seller_id, 
        s.seller_city, 
        s.seller_state,
        DATEPART(YEAR, o.order_purchase_timestamp),
        DATEPART(MONTH, o.order_purchase_timestamp)
),

seller_trends AS (
    SELECT *,
        -- Previous month revenue for comparison
        LAG(total_revenue) OVER (PARTITION BY seller_id ORDER BY year, month) AS prev_month_revenue,

        -- Revenue difference compared to previous month
        total_revenue - LAG(total_revenue) OVER (PARTITION BY seller_id ORDER BY year, month) AS revenue_diff,

        -- Revenue alert if revenue decreased
        CASE
            WHEN total_revenue - LAG(total_revenue) OVER (PARTITION BY seller_id ORDER BY year, month) < 0
            THEN 'Revenue Drop!'
            ELSE NULL
        END AS revenue_alert,

        -- Rolling 3-month average of shipping days
        AVG(avg_shipping_days) OVER (
            PARTITION BY seller_id 
            ORDER BY year 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS rolling_avg_shipping,

        -- Shipping alert if rolling average shipping exceeds 10 days
        CASE 
            WHEN AVG(avg_shipping_days) OVER (
                    PARTITION BY seller_id 
                    ORDER BY year 
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                 ) > 10
            THEN 'Slow Shipping Alert!'
            ELSE NULL
        END AS shipping_alert
    FROM seller_monthly
)

-- Output only sellers with alerts
SELECT
    seller_id,
    seller_city,
    seller_state,
    year,
    month,
    total_revenue,
    prev_month_revenue,
    revenue_diff,
    revenue_alert,
    rolling_avg_shipping,
    shipping_alert
FROM seller_trends
WHERE revenue_alert IS NOT NULL OR shipping_alert IS NOT NULL
ORDER BY seller_id, year, month;

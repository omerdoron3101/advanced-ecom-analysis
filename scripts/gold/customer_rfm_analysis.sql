/* =============================================================================
   Customer Rfm Analysis
   =============================================================================
   Script Purpose:
      Compute RFM (Recency, Frequency, Monetary) metrics and tiers 
      for each customer using the Gold layer views. This query aggregates customer 
      purchases and payments to produce actionable analytical metrics for marketing
      and segmentation purposes.

      Key Metrics:
         1. Recency (days since last purchase)
         2. Frequency (total number of orders)
         3. Monetary (total amount spent)
         4. Customer Lifetime (days between first and last purchase)
         5. RFM Tiers (High / Medium / Low based on thresholds)

      Notes:
         - Uses Gold layer views: dim_customers_view, dim_orders_view, fact_payments_view.
         - This query is read-only and does not modify any data.
   ============================================================================= */

WITH customer_rfm AS (
    SELECT 
        c.customer_id,
        MIN(o.order_purchase_timestamp) AS first_purchase,
        MAX(o.order_purchase_timestamp) AS last_purchase,
        COUNT(DISTINCT o.order_id) AS total_orders,
        SUM(fp.payment_value) AS total_spent
    FROM gold.dim_customers_view AS c
    LEFT JOIN gold.dim_orders_view AS o 
        ON c.customer_id = o.customer_id
    LEFT JOIN gold.fact_payments_view AS fp 
        ON o.order_id = fp.order_id
    GROUP BY c.customer_id
)

SELECT 
    customer_id,
    
    -- Recency: Days since last purchase
    DATEDIFF(DAY, last_purchase, GETDATE()) AS recency_days,
    
    -- Frequency: Total number of orders
    total_orders AS frequency,
    
    -- Monetary: Total amount spent
    total_spent AS monetary,
    
    -- Customer Lifetime: Days between first and last purchase
    DATEDIFF(DAY, first_purchase, last_purchase) AS customer_lifetime_days,
    
    -- Monetary Tier based on total_spent thresholds
    CASE 
        WHEN total_spent >= 1000 THEN 'High'
        WHEN total_spent >= 500 THEN 'Medium'
        ELSE 'Low'
    END AS monetary_tier,
    
    -- Frequency Tier based on total_orders thresholds
    CASE 
        WHEN total_orders >= 20 THEN 'High'
        WHEN total_orders >= 10 THEN 'Medium'
        ELSE 'Low'
    END AS frequency_tier,
    
    -- Recency Tier based on days since last purchase
    CASE 
        WHEN DATEDIFF(DAY, last_purchase, GETDATE()) <= 30 THEN 'High'
        WHEN DATEDIFF(DAY, last_purchase, GETDATE()) <= 90 THEN 'Medium'
        ELSE 'Low'
    END AS recency_tier
    
FROM customer_rfm
ORDER BY total_spent DESC;

/* =============================================================================
   Category Finance Analysis
   =============================================================================
   Script Purpose:
      Perform financial and volume analysis by product category using Gold layer
      views. The query calculates total orders, total revenue, and average price
      per category, and classifies categories into revenue and volume tiers 
      for strategic analysis.

      Key Metrics:
         1. Total Orders per category
         2. Total Revenue per category (price + freight)
         3. Average Price per category
         4. Revenue Tier (High, Moderate, Low)
         5. Volume Tier (High, Moderate, Low)

      Notes:
         - Uses Gold layer views: fact_order_items_view, dim_product_view.
         - Revenue and volume tiers are relative to the average across all categories.
         - The query is read-only and does not modify any data.
   ============================================================================= */

WITH category_finance AS (
    SELECT 
        p.product_category_name_english,
        COUNT(f.order_id) AS total_orders,
        SUM(f.price + f.freight_value) AS total_revenue,
        ROUND(AVG(f.price), 2) AS avg_price
    FROM gold.fact_order_items_view AS f
    LEFT JOIN gold.dim_product_view AS p 
        ON f.product_id = p.product_id
    WHERE f.price IS NOT NULL
    GROUP BY p.product_category_name_english
),

category_analysis AS (
    SELECT *,
        -- Revenue Tier based on relative total revenue
        CASE 
            WHEN total_revenue >= (SELECT AVG(total_revenue) * 1.5 FROM category_finance) THEN 'High Revenue'
            WHEN total_revenue >= (SELECT AVG(total_revenue) FROM category_finance) THEN 'Moderate Revenue'
            ELSE 'Low Revenue'
        END AS revenue_tier,
        
        -- Volume Tier based on relative total orders
        CASE 
            WHEN total_orders >= (SELECT AVG(total_orders) * 1.5 FROM category_finance) THEN 'High Volume'
            WHEN total_orders >= (SELECT AVG(total_orders) FROM category_finance) THEN 'Moderate Volume'
            ELSE 'Low Volume'
        END AS volume_tier
    FROM category_finance
)

-- Full analysis output
SELECT *
FROM category_analysis
ORDER BY total_revenue DESC;

-- Focused analysis: Low Revenue but High or Moderate Volume categories
SELECT *
FROM category_analysis
WHERE revenue_tier = 'Low Revenue'
  AND (volume_tier = 'High Volume' OR volume_tier = 'Moderate Volume')
ORDER BY total_orders DESC;

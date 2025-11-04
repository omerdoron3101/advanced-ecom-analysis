# Advanced Ecom Analysis Project

This project is an advanced, end-to-end SQL Server data pipeline built on the **Medallion Architecture (Bronze → Silver → Gold)**, designed to process and analyze the **Olist Brazilian E-Commerce dataset**.

This project focuses on building a scalable and reliable data ecosystem - from raw CSV ingestion to fully cleaned and analytics-ready Gold models. It ensures data quality, consistency, and business-ready insights for BI and reporting.

**A comprehensive and production-oriented data architecture that turns raw e-commerce data into actionable insights.**


---



## 🛠️ Project Overview

This project implements a full **ETL and analytical pipeline** for the *Brazilian E‑Commerce (Olist)* dataset using **SQL Server** and the **Medallion Architecture** (Bronze → Silver → Gold).

The goal is to create a reliable, maintainable, and production-grade data flow that ensures clean, consistent, and analytical-ready data for insights and BI analysis.

---

## 🎯 Goals

* Automate ingestion of raw CSV data into the Bronze layer.
* Apply robust data cleaning and type enforcement in Silver.
* Design analytical Gold models (Star Schema) optimized for BI and reporting.
* Preserve strict separation between ingestion, transformation, and analytics.

---

## 🏗️ Architecture

```
CSV (Raw Data)
   ↓
Bronze (Raw Ingestion)
   ↓
Silver (Cleaned & Canonicalized Data)
   ↓
Gold (Analytical Models & Aggregations)
```

All layers are built entirely in **SQL Server**, using stored procedures, DDL scripts, and transformation queries.

---

## 📦 Data Sources

* `orders.csv` - order details
* `order_items.csv` - line items for each order
* `customers.csv` - customer details
* `products.csv` - product metadata
* `sellers.csv` - seller data
* `order_payments.csv` - payment transactions
* `order_reviews.csv` - customer reviews
* `geolocation.csv` - location data 


---

## 🧩 Detailed Layer Design

### Bronze - Raw Ingestion Layer

* Stores raw data exactly as received.
* Columns stored as `NVARCHAR` to prevent parsing failures.

**Example:**

```sql
CREATE TABLE bronze.olist_orders_dataset (
	order_id NVARCHAR(50),
	customer_id NVARCHAR(50),
	order_status NVARCHAR(50),
	order_purchase_timestamp NVARCHAR(50),
	order_approved_at NVARCHAR(50),
	order_delivered_carrier_date NVARCHAR(50),
	order_delivered_customer_date NVARCHAR(50),
	order_estimated_delivery_date NVARCHAR(50)
);
```

### Silver - Cleaning & Canonicalization Layer

* Converts data to accurate types.
* Cleans whitespace, case, and invalid characters.
* Normalizes keys and date formats with `TRY_CONVERT`.
* Creates consistent business keys.

**Example:**

```sql
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        PRINT '=============================================================================';
        PRINT 'Starting Silver Layer Load';
        PRINT '=============================================================================';

        -- =============================
        -- 1️. Customers
        -- =============================
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

...

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
```

### Gold - Analytical Layer

* Built using **Star Schema** principles.
* Includes high-level analytical views for business insights.
* Dimension tables (`Dim_`) and Fact table (`Fact_`).
* Optimized for BI performance and visualization.
  
<br>
<img width="1000" height="610" alt="Gold Layer Overview" src="https://github.com/user-attachments/assets/5a22b8b9-cf9b-47d2-88a9-c07dc8c6ec6f" />
<br>
  
**Proposed Tables:**

* `fact_sales` - item-level transactions.
* `dim_product`, `dim_customer`, `dim_seller`, `dim_date`.
  

**Example:**

```sql
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
    LEFT JOIN gold.dim_orders_view AS o ON f.order_id = o.order_id
    LEFT JOIN gold.dim_product_view AS p ON f.product_id = p.product_id
    LEFT JOIN gold.fact_reviews_view AS r ON f.order_id = r.order_id
    WHERE o.order_purchase_timestamp IS NOT NULL
    GROUP BY DATEPART(YEAR, o.order_purchase_timestamp),
             DATEPART(MONTH, o.order_purchase_timestamp),
			 p.product_category_name,
             p.product_category_name_english
),

monthly_trends AS (
    SELECT *,
        LAG(total_revenue) OVER (PARTITION BY product_category_name_english ORDER BY year, month) AS prev_month_revenue,
        CASE 
            WHEN LAG(total_revenue) OVER (PARTITION BY product_category_name_english ORDER BY year, month) IS NULL THEN NULL
            ELSE (total_revenue - LAG(total_revenue) OVER (PARTITION BY product_category_name_english ORDER BY year, month)) 
                 / LAG(total_revenue) OVER (PARTITION BY product_category_name_english ORDER BY year, month) * 100
        END AS revenue_growth_pct,
        LAG(total_orders) OVER (PARTITION BY product_category_name_english ORDER BY year, month) AS prev_month_orders,
        CASE 
            WHEN LAG(total_orders) OVER (PARTITION BY product_category_name_english ORDER BY year, month) IS NULL THEN NULL
            ELSE (total_orders - LAG(total_orders) OVER (PARTITION BY product_category_name_english ORDER BY year, month)) 
                 / LAG(total_orders) OVER (PARTITION BY product_category_name_english ORDER BY year, month) * 100
        END AS orders_growth_pct
    FROM monthly_metrics
)

SELECT *
FROM monthly_trends
ORDER BY product_category_name_english,
		 product_category_name,
		 year,
		 month;

```

---

## ✅ Data Quality & Idempotency

* Uses `TRY_CONVERT` to handle malformed values.
* Validates nulls, unique keys, and record counts.
* Supports reload (`Replace` / `Append`) in Bronze.
* Uses `MERGE` in Silver and Gold for deterministic refresh.

---

## 🛠️ Engineering Decisions & Rationale

1. **Medallion separation:** ensures traceability and easy debugging.
2. **NVARCHAR storage in Bronze:** avoids ingestion failures.
3. **`TRY_CONVERT` usage:** maintains pipeline stability.
4. **Parameterized ingestion:** enables flexible data paths.
5. **`MERGE` operations in Gold:** allow incremental updates.

---

## 🌟 About Me

👋 Hi! I'm Omer Doron
I’m a student of Information Systems specializing in Digital Innovation.
I’m passionate about transforming raw information into meaningful insights.

I created this project as part of my learning journey in data warehousing and analytics, and as a showcase of my technical and analytical skills.

🔗 [Connect with me on LinkedIn](https://www.linkedin.com/in/omer-doron-a070732b1/)


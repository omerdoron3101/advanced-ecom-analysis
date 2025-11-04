# Gold Layer - Advanced E-Commerce Analytics

This document provides an **in-depth explanation of the Gold layer**, including the design rationale, query logic, and potential insights for business analysis.

The Gold layer is built on top of the **Medallion Architecture (Bronze → Silver → Gold)**, and is meant to **deliver analytics-ready datasets** optimized for BI, reporting, and decision-making.

---

## 🎯 Purpose of the Gold Layer

The Gold layer serves several purposes:

* **Analytical readiness:** Transform cleaned and canonicalized Silver tables into high-level dimensional and fact models.
* **Business insight:** Pre-aggregate key metrics like revenue, orders, shipping performance, and customer satisfaction.
* **Star Schema:** Provides fact tables (`Fact_`) and dimension tables (`Dim_`) optimized for reporting and BI visualization.
* **Scalability:** Supports incremental updates using deterministic `MERGE` statements and aggregation logic.
* **Insight generation:** Enables RFM analysis, trend monitoring, and performance ranking across products, sellers, and geographies.

---

## 🗂️ Key Gold Models

### 1️⃣ Dimensional Views

Gold **dimension views** are designed to enrich Silver tables with business-relevant attributes:

* **`dim_customers_view`** - includes `customer_id`, `unique_id`, zip code, city, state, and derived metrics.
* **`dim_products_view`** - includes `product_id`, category, price, dimensions, and other product metadata.
* **`dim_sellers_view`** - contains seller metadata like city, state, and other attributes.
* **`dim_orders_view`** - enriched with order status, purchase/approval/delivery timestamps, and calculated delivery times.

**Rationale:**

* Dimensions provide **context for fact tables**.
* Calculations like shipping times, delivery delays, and trimming of text fields ensure **data quality and consistency**.

---

### 2️⃣ Fact Tables

Fact tables capture **event-level metrics**, aggregated or transactional:

* **`fact_order_items_view`** - combines order items, product IDs, seller IDs, prices, and shipping information.
* **`fact_payments_view`** - transactional payments linked to order IDs.
* **`fact_reviews_view`** - includes review scores, comments, and timestamps for customer feedback.

**Logic Implemented:**

* Aggregates revenue as `price + freight_value`.
* Calculates **shipping duration**: `DATEDIFF(day, purchase, delivery)`.
* Links dimensions for enriched reporting (products, customers, sellers).
* Precomputes metrics like average review score, total revenue, and order counts.

**Why This Approach:**

* Centralizes all metrics for analytical queries.
* Reduces complexity in downstream reporting (no need to join raw tables repeatedly).
* Supports **trend analysis** by precomputing month/year aggregates.

---

## 📊 Analytics Queries & Insights

The Gold layer includes queries for **various business insights**:

### 1️⃣ Customer RFM Analysis

**Insights:**
* Identify top customers and churn risk.
* Segment for marketing campaigns or loyalty programs.
* Understand spending patterns over time.

### 2️⃣ Product Category Performance
Aggregates revenue, orders, and average price by product category.

**Insights:**
* Which categories generate the most revenue vs. volume.
* Identify underperforming but popular categories for strategy adjustments.
* Track average price and shipping times to optimize product selection.

### 3️⃣ Monthly Trends & Growth
Precompute monthly revenue, orders, and review metrics.

**Calculate month-over-month growth:**
```sql
revenue_growth_pct = (total_revenue - prev_month_revenue) / prev_month_revenue * 100
orders_growth_pct = (total_orders - prev_month_orders) / prev_month_orders * 100
```

**Insights:**
* Detect seasonality trends.
* Flag declining categories early.
* Support sales forecasting and marketing planning.

### 4️⃣ Seller Performance
**Aggregates seller-level metrics:**
* Total revenue and orders.
* Average shipping days.
* Average review scores.
* Tiers for revenue, volume, shipping speed, and satisfaction.

**Insights:**
* Identify top-performing sellers.
* Flag sellers with slow delivery or low satisfaction.
* Inform logistics optimization and seller engagement strategies.

**Example Query Snippet:**
```sql
CASE WHEN avg_shipping_days <= 5 THEN 'Fast'
     WHEN avg_shipping_days <= 10 THEN 'Moderate'
     ELSE 'Slow' END AS shipping_tier,
CASE WHEN avg_review_score >= 4.5 THEN 'Excellent'
     WHEN avg_review_score >= 3.5 THEN 'Good'
     ELSE 'Poor' END AS review_tier
```

### 5️⃣ City & Geographical Analysis
* Aggregates orders, revenue, and customer satisfaction by city and state.
* City-level rankings for revenue, satisfaction, and shipping efficiency.
* Includes category-level insights (top category per city).

**Insights:**
* Understand geographical demand distribution.
* Prioritize shipping infrastructure and marketing efforts by city.
* Optimize city-specific promotions based on top-performing categories.

### 6️⃣ Shipping & Review Insights
* Shipping speed calculated as `avg_shipping_days`.
* Review scores used to create satisfaction tiers:
* `Excellent` (≥4.5), `Good` (3.5–4.4), `Poor` (<3.5)
* Rolling averages used to detect slow shipping alerts

**Insights:**
* Monitor seller performance continuously.
* Flag potential customer dissatisfaction early.
* Support operational KPIs for logistics.

---

## 💡 Key Design Decisions
**1. Precomputed aggregates:** reduce runtime on BI dashboards.  
**2. Tier-based segmentation:** simplifies analysis for marketing and operations.  
**3. RFM & lifetime calculations:** allow customer segmentation and retention analysis.  
**4. Consistent dimension linking:** ensures facts are always connected to context.  
**5. Rolling averages and trends:** support operational alerts and forecasting.  
**6. Error handling with `TRY_CONVERT`:** ensures robustness against dirty Bronze data.  

---

## 🖼️ Gold Layer Overview
<br>
<img width="1000" height="610" alt="Gold Layer Overview" src="https://github.com/user-attachments/assets/5a22b8b9-cf9b-47d2-88a9-c07dc8c6ec6f" />
<br>

---

## ✅ Summary of Insights
* **Customers:** Segment by RFM and lifetime.
* **Products:** Identify top-selling, high-margin, and underperforming categories.
* **Sellers:** Monitor revenue, shipping efficiency, and satisfaction.
* **Cities:** Rank by revenue, satisfaction, and shipping speed.
* **Trends:** Detect seasonal patterns, growth, and declines.
* **Operational KPIs:** Rolling shipping averages and alerts for proactive decisions.
* **Overall:** The Gold layer transforms cleaned transactional data into actionable business insights, ready for BI dashboards, executive reports, and predictive analysis.

---

---

## 🔗 Next Steps
* Integrate with Power BI / Tableau for visualization.
* Use Gold metrics for customer retention and loyalty programs.
* Monitor product and seller performance monthly.
* Feed insights into marketing, logistics, and pricing decisions.

---

## 🌟 About Me

👋 Hi! I'm Omer Doron
I’m a student of Information Systems specializing in Digital Innovation.
I’m passionate about transforming raw information into meaningful insights.

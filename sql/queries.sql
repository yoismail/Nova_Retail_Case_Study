-- ==================================================
-- BUSINESS INSIGHT QUERIES
-- Nova Retail Case Study
-- ==================================================

-- 1. SALES PERFORMANCE
-- 1.1 Total Business Overview
SELECT
  COUNT(DISTINCT order_id)          AS total_orders,
  SUM(payment_value)                AS total_revenue,
  ROUND(AVG(payment_value), 2)       AS avg_order_value,
  SUM(freight_value)                 AS total_shipping_cost,
  ROUND(SUM(freight_value) / SUM(payment_value) * 100, 2) 
      AS shipping_cost_pct_of_revenue
FROM retail_gold.fact_sales;

-- 1.2 Monthly Sales Trend with Growth Rate
WITH monthly AS (
  SELECT
    order_year_month,
    SUM(total_revenue) AS monthly_revenue
  FROM retail_gold.sales_by_month_state
  GROUP BY order_year_month
  ORDER BY order_year_month
)
SELECT
  order_year_month,
  monthly_revenue,
  LAG(monthly_revenue) OVER (ORDER BY order_year_month) AS prev_month_revenue,
  ROUND(
    (monthly_revenue - LAG(monthly_revenue) OVER (ORDER BY order_year_month)) 
    / LAG(monthly_revenue) OVER (ORDER BY order_year_month) * 100, 
  2) AS growth_percent
FROM monthly;

-- 1.3 Best & Worst Performing States
SELECT
  customer_state,
  SUM(total_orders)  AS total_orders,
  SUM(total_revenue) AS total_revenue,
  ROUND(SUM(total_revenue) / SUM(SUM(total_revenue)) OVER () * 100, 2) 
      AS revenue_share_pct
FROM retail_gold.sales_summary
GROUP BY customer_state
ORDER BY total_revenue DESC;

-- 2. PRODUCT & CATEGORY INSIGHTS

-- 2.1 Top 10 Best-Selling Categories
SELECT
  product_category_name,
  SUM(total_orders)  AS total_orders,
  SUM(total_revenue) AS total_revenue,
  ROUND(AVG(avg_order_value), 2) AS avg_order_value
FROM retail_gold.sales_summary
GROUP BY product_category_name
ORDER BY total_revenue DESC
LIMIT 10;

-- 2.2 Categories with Low Average Order Value
SELECT
  product_category_name,
  ROUND(AVG(avg_order_value), 2) AS avg_order_value,
  SUM(total_orders) AS total_orders
FROM retail_gold.sales_summary
GROUP BY product_category_name
HAVING AVG(avg_order_value) < (SELECT AVG(payment_value) FROM retail_gold.fact_sales)
ORDER BY avg_order_value ASC;

-- 2.3 Top Category per State
WITH ranked AS (
  SELECT
    customer_state,
    product_category_name,
    total_revenue,
    RANK() OVER (PARTITION BY customer_state ORDER BY total_revenue DESC) AS rnk
  FROM retail_gold.sales_summary
)
SELECT customer_state, product_category_name, total_revenue
FROM ranked
WHERE rnk = 1;

-- 3. PAYMENT BEHAVIOUR

-- 3.1 Popularity of Payment Methods
SELECT
  payment_type,
  COUNT(DISTINCT order_id) AS usage_count,
  ROUND(COUNT(*) / SUM(COUNT(*)) OVER () * 100, 2) AS usage_percent,
  ROUND(AVG(payment_value), 2) AS avg_order_value
FROM retail_gold.fact_sales
GROUP BY payment_type
ORDER BY usage_count DESC;

-- 3.2 Preferred Payment Method per State
WITH ranked AS (
  SELECT
    customer_state,
    payment_type,
    COUNT(*) AS total_uses,
    RANK() OVER (PARTITION BY customer_state ORDER BY COUNT(*) DESC) AS rnk
  FROM retail_gold.fact_sales
  GROUP BY customer_state, payment_type
)
SELECT customer_state, payment_type, total_uses
FROM ranked
WHERE rnk = 1;

-- 4. TIME & SEASONALITY

-- 4.1 Sales by Day of Week
SELECT
  order_weekday,
  CASE order_weekday
    WHEN 1 THEN 'Sunday'
    WHEN 2 THEN 'Monday'
    WHEN 3 THEN 'Tuesday'
    WHEN 4 THEN 'Wednesday'
    WHEN 5 THEN 'Thursday'
    WHEN 6 THEN 'Friday'
    WHEN 7 THEN 'Saturday'
  END AS day_name,
  COUNT(DISTINCT order_id) AS total_orders,
  SUM(payment_value) AS total_revenue
FROM retail_gold.fact_sales
GROUP BY order_weekday
ORDER BY order_weekday;


-- 5. LOGISTICS & PROFITABILITY

-- 5.1 Top 10 Highest Value Orders
SELECT
  order_id,
  SUM(payment_value) AS order_total
FROM retail_gold.fact_sales
GROUP BY order_id
ORDER BY order_total DESC
LIMIT 10;

-- =========================
-- 1. Total sales revenue
SELECT SUM(total_revenue) AS total_revenue
FROM retail_gold.sales_summary;

-- 2. Sales trend over time
SELECT order_year_month, SUM(total_revenue) AS monthly_revenue
FROM retail_gold.sales_by_month_state
GROUP BY order_year_month
ORDER BY order_year_month;
-- =========================



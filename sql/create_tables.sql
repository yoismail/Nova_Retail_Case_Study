CREATE SCHEMA IF NOT EXISTS retail_gold;

DROP TABLE IF EXISTS retail_gold.fact_sales;
DROP TABLE IF EXISTS retail_gold.sales_summary;
DROP TABLE IF EXISTS retail_gold.sales_by_month_state;

-- ================================
-- TABLE 1: retail_gold.fact_sales
-- Detailed transaction-level sales data, denormalized for easy querying
-- ================================

CREATE TABLE IF NOT EXISTS retail_gold.fact_sales (
    order_id                VARCHAR(255) NOT NULL,
    customer_id             VARCHAR(255),
    product_id              VARCHAR(255),
    seller_id               VARCHAR(255),
    customer_state          VARCHAR(100),
    payment_value           NUMERIC(10,2),
    payment_type            VARCHAR(100),
    product_category_name   VARCHAR(255),
    freight_value           NUMERIC(10,2),
    
    -- DATE COLUMNS
    order_purchase_timestamp TIMESTAMP,
    order_purchase_date     DATE,
    order_year              INTEGER,
    order_month             INTEGER,
    order_day               INTEGER,
    order_year_month        VARCHAR(7),
    order_weekday           INTEGER,


    PRIMARY KEY (order_id, product_id)
);

-- INDEXES FOR FAST QUERYING
CREATE INDEX IF NOT EXISTS idx_fact_sales_date  ON retail_gold.fact_sales (order_purchase_date);
CREATE INDEX IF NOT EXISTS idx_fact_sales_year  ON retail_gold.fact_sales (order_year);
CREATE INDEX IF NOT EXISTS idx_fact_sales_ym    ON retail_gold.fact_sales (order_year_month);
CREATE INDEX IF NOT EXISTS idx_fact_sales_state ON retail_gold.fact_sales (customer_state);
CREATE INDEX IF NOT EXISTS idx_fact_sales_payment ON retail_gold.fact_sales (payment_type);

-- ================================
-- TABLE 2: retail_gold.sales_summary
-- Aggregated KPIs: by State + Product Category
-- ================================

CREATE TABLE IF NOT EXISTS retail_gold.sales_summary (
    customer_state          VARCHAR(100) NOT NULL,
    product_category_name   VARCHAR(255) NOT NULL,
    total_orders            INTEGER,
    total_revenue           NUMERIC(12,2),
    avg_order_value         NUMERIC(10,2),

    -- Primary Key: unique grouping combination
    PRIMARY KEY (customer_state, product_category_name)
);

-- INDEXES — OPTIMISED FOR REPORTS & DASHBOARDS
CREATE INDEX IF NOT EXISTS idx_summary_state 
    ON retail_gold.sales_summary (customer_state);

CREATE INDEX IF NOT EXISTS idx_summary_category 
    ON retail_gold.sales_summary (product_category_name);

CREATE INDEX IF NOT EXISTS idx_summary_revenue 
    ON retail_gold.sales_summary (total_revenue DESC);

-- ================================
-- TABLE 3: retail_gold.sales_by_month_state
-- Aggregated KPIs: by Month + State
-- ================================

CREATE TABLE IF NOT EXISTS retail_gold.sales_by_month_state (
    order_year_month        VARCHAR(7) NOT NULL,
    customer_state          VARCHAR(100) NOT NULL,
    total_orders            INTEGER,
    total_revenue           NUMERIC(12,2),
    avg_order_value         NUMERIC(10,2),

    PRIMARY KEY (order_year_month, customer_state)
);

CREATE INDEX IF NOT EXISTS idx_sales_ym_state 
    ON retail_gold.sales_by_month_state (order_year_month);

-- --------------------------
-- HOW TO RUN:
-- psql -U postgres -d nova_retail_db -f sql/create_tables.sql
-- --------------------------
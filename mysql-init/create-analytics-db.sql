-- Create analytics database for Metabase
CREATE DATABASE IF NOT EXISTS analytics;

-- Create metabase user if needed
CREATE USER IF NOT EXISTS 'metabase'@'%' IDENTIFIED BY 'metabase123';
GRANT ALL PRIVILEGES ON analytics.* TO 'metabase'@'%';
GRANT ALL PRIVILEGES ON metabase.* TO 'metabase'@'%';

-- Grant privileges to root for analytics
GRANT ALL PRIVILEGES ON analytics.* TO 'root'@'%';

FLUSH PRIVILEGES;

USE analytics;

-- Daily aggregates table
CREATE TABLE IF NOT EXISTS sales_daily_aggregates (
    transaction_date DATE,
    product_name VARCHAR(100),
    product_category VARCHAR(50),
    region VARCHAR(50),
    total_quantity_sold INT,
    total_revenue DECIMAL(15,2),
    avg_unit_price DECIMAL(10,2),
    avg_discount_percent DECIMAL(5,2),
    transaction_count INT,
    max_transaction_amount DECIMAL(10,2),
    min_transaction_amount DECIMAL(10,2),
    revenue_rank INT,
    report_date DATE,
    processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_date (transaction_date),
    INDEX idx_product (product_name),
    INDEX idx_region (region)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Monthly aggregates table
CREATE TABLE IF NOT EXISTS sales_monthly_aggregates (
    year INT,
    month INT,
    month_name VARCHAR(20),
    product_name VARCHAR(100),
    product_category VARCHAR(50),
    total_quantity_sold INT,
    total_revenue DECIMAL(15,2),
    avg_unit_price DECIMAL(10,2),
    transaction_count INT,
    unique_customers_approx INT,
    revenue_rounded DECIMAL(15,2),
    processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_year_month (year, month),
    INDEX idx_product (product_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Product analytics table
CREATE TABLE IF NOT EXISTS sales_product_analytics (
    product_name VARCHAR(100) PRIMARY KEY,
    product_category VARCHAR(50),
    lifetime_revenue DECIMAL(15,2),
    lifetime_quantity_sold INT,
    avg_selling_price DECIMAL(10,2),
    avg_discount DECIMAL(5,2),
    total_transactions INT,
    total_customers_approx INT,
    lifetime_revenue_rounded DECIMAL(15,2),
    revenue_rank INT,
    market_share_percent DECIMAL(5,2),
    processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_category (product_category),
    INDEX idx_revenue_rank (revenue_rank)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
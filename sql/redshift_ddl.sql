-- ====================================================================
-- Amazon AuroraStream - Amazon Redshift Serverless DDL Architecture
-- Implements optimal Distribution Styles (KEY/ALL/EVEN) and Sort Keys
-- for High-Performance Distributed Analytical Queries (MPP Architecture)
-- ====================================================================

CREATE SCHEMA IF NOT EXISTS aurorastream_dw;
SET search_path TO aurorastream_dw;

-- 
-- 1. Date Dimension (DISTSTYLE ALL - replicated to every compute node)
-- 
CREATE TABLE IF NOT EXISTS dim_date (
    date_key            INT NOT NULL ENCODE AZ64,
    event_date          DATE NOT NULL ENCODE AZ64,
    year                SMALLINT NOT NULL ENCODE AZ64,
    quarter             SMALLINT NOT NULL ENCODE AZ64,
    month               SMALLINT NOT NULL ENCODE AZ64,
    month_name          VARCHAR(20) NOT NULL ENCODE ZSTD,
    day_of_week         SMALLINT NOT NULL ENCODE AZ64,
    day_name            VARCHAR(15) NOT NULL ENCODE ZSTD,
    is_weekend          BOOLEAN NOT NULL ENCODE ZSTD,
    is_prime_day_event  BOOLEAN NOT NULL ENCODE ZSTD,
    is_diwali_sale_event BOOLEAN NOT NULL ENCODE ZSTD,
    PRIMARY KEY (date_key)
)
DISTSTYLE ALL
COMPOUND SORTKEY (year, month, date_key);

-- 
-- 2. Customer Dimension (SCD Type 2)
-- 
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_sk         VARCHAR(64) NOT NULL ENCODE ZSTD,
    customer_id         VARCHAR(32) NOT NULL ENCODE ZSTD,
    geo_city            VARCHAR(64) ENCODE ZSTD,
    device_type         VARCHAR(32) ENCODE ZSTD,
    valid_from          TIMESTAMP NOT NULL ENCODE AZ64,
    valid_to            TIMESTAMP ENCODE AZ64,
    is_current          BOOLEAN NOT NULL ENCODE ZSTD,
    PRIMARY KEY (customer_sk)
)
DISTSTYLE KEY
DISTKEY (customer_id)
COMPOUND SORTKEY (customer_id, is_current);

-- 
-- 3. Product Dimension
-- 
CREATE TABLE IF NOT EXISTS dim_products (
    product_id          VARCHAR(32) NOT NULL ENCODE ZSTD,
    category            VARCHAR(64) NOT NULL ENCODE ZSTD,
    price_tier          VARCHAR(32) NOT NULL ENCODE ZSTD,
    PRIMARY KEY (product_id)
)
DISTSTYLE ALL
SORTKEY (category, product_id);

-- 
-- 4. Fact Table: Order Transactions
-- 
CREATE TABLE IF NOT EXISTS fact_orders (
    transaction_id      VARCHAR(64) NOT NULL ENCODE ZSTD,
    order_id            VARCHAR(32) NOT NULL ENCODE ZSTD,
    customer_id         VARCHAR(32) NOT NULL ENCODE ZSTD,
    product_id          VARCHAR(32) NOT NULL ENCODE ZSTD,
    date_key            INT NOT NULL ENCODE AZ64,
    event_timestamp_utc TIMESTAMP NOT NULL ENCODE AZ64,
    order_status        VARCHAR(32) NOT NULL ENCODE ZSTD,
    payment_method      VARCHAR(32) NOT NULL ENCODE ZSTD,
    quantity            INT NOT NULL ENCODE AZ64,
    unit_price          NUMERIC(10, 2) NOT NULL ENCODE AZ64,
    discount_pct        NUMERIC(5, 2) NOT NULL ENCODE AZ64,
    subtotal_amount     NUMERIC(12, 2) NOT NULL ENCODE AZ64,
    discount_amount     NUMERIC(12, 2) NOT NULL ENCODE AZ64,
    estimated_gross_margin NUMERIC(12, 2) NOT NULL ENCODE AZ64,
    is_prime_eligible   BOOLEAN NOT NULL ENCODE ZSTD,
    PRIMARY KEY (transaction_id),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (product_id) REFERENCES dim_products(product_id)
)
DISTSTYLE KEY
DISTKEY (customer_id)
COMPOUND SORTKEY (date_key, order_status, event_timestamp_utc);

-- 
-- 5. Fact Table: Clickstream
-- 
CREATE TABLE IF NOT EXISTS fact_clickstream (
    event_id            VARCHAR(64) NOT NULL ENCODE ZSTD,
    session_id          VARCHAR(32) NOT NULL ENCODE ZSTD,
    customer_id         VARCHAR(32) NOT NULL ENCODE ZSTD,
    product_id          VARCHAR(32) NOT NULL ENCODE ZSTD,
    date_key            INT NOT NULL ENCODE AZ64,
    event_type          VARCHAR(32) NOT NULL ENCODE ZSTD,
    dwell_time_seconds  INT ENCODE AZ64,
    referral_url        VARCHAR(128) ENCODE ZSTD,
    device_type         VARCHAR(32) ENCODE ZSTD,
    PRIMARY KEY (event_id)
)
DISTSTYLE KEY
DISTKEY (customer_id)
COMPOUND SORTKEY (date_key, session_id);

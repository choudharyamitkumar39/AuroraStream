-- ====================================================================
-- Amazon AuroraStream - Enterprise Analytical SQL Suite (Amazon Redshift)
-- 10 Senior-Level Analytical Queries Demonstrating Window Functions,
-- CTEs, Sliding Averages, Funnels, and Fraud Velocity Rules
-- ====================================================================

SET search_path TO aurorastream_dw;

--  1. Hourly GMV Velocity & Spike Detection (Sliding Window Z-Score) 
WITH hourly_sales AS (
    SELECT
        DATE_TRUNC('hour', event_timestamp_utc) AS sales_hour,
        SUM(subtotal_amount) AS hourly_gmv,
        COUNT(DISTINCT order_id) AS order_count
    FROM fact_orders
    WHERE order_status != 'CANCELLED'
    GROUP BY 1
),
stats AS (
    SELECT
        sales_hour,
        hourly_gmv,
        order_count,
        AVG(hourly_gmv) OVER (ORDER BY sales_hour ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING) AS rolling_24h_avg,
        STDDEV(hourly_gmv) OVER (ORDER BY sales_hour ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING) AS rolling_24h_stddev
    FROM hourly_sales
)
SELECT
    sales_hour,
    hourly_gmv,
    order_count,
    ROUND(rolling_24h_avg, 2) AS rolling_avg,
    ROUND((hourly_gmv - rolling_24h_avg) / NULLIF(rolling_24h_stddev, 0), 2) AS z_score_spike_indicator
FROM stats
ORDER BY sales_hour DESC;

--  2. Customer RFM Segmentation using NTILE Window Functions 
WITH rfm_raw AS (
    SELECT
        customer_id,
        DATEDIFF('day', MAX(event_timestamp_utc), GETDATE()) AS recency_days,
        COUNT(DISTINCT order_id) AS frequency,
        SUM(subtotal_amount) AS monetary
    FROM fact_orders
    WHERE order_status != 'CANCELLED'
    GROUP BY customer_id
),
rfm_scored AS (
    SELECT
        customer_id,
        recency_days,
        frequency,
        monetary,
        NTILE(5) OVER (ORDER BY recency_days DESC) AS r_score,
        NTILE(5) OVER (ORDER BY frequency ASC) AS f_score,
        NTILE(5) OVER (ORDER BY monetary ASC) AS m_score
    FROM rfm_raw
)
SELECT
    customer_id,
    recency_days,
    frequency,
    monetary,
    CASE
        WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
        WHEN r_score >= 3 AND f_score >= 3                   THEN 'Loyal Customers'
        WHEN r_score <= 2 AND f_score >= 3                   THEN 'At-Risk High Spenders'
        WHEN r_score <= 2 AND f_score <= 2                   THEN 'Hibernating / Lost'
        ELSE 'Potential Growth'
    END AS customer_segment
FROM rfm_scored;

--  3. Real-Time Fraud & Abuse Detection: Rapid Order Velocity Rule 
-- Detects customers placing > 3 orders within a 60-second sliding window
WITH order_velocities AS (
    SELECT
        customer_id,
        order_id,
        event_timestamp_utc,
        COUNT(order_id) OVER(
            PARTITION BY customer_id
            ORDER BY event_timestamp_utc
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS recent_order_count,
        DATEDIFF('second', LAG(event_timestamp_utc, 2) OVER(
            PARTITION BY customer_id ORDER BY event_timestamp_utc
        ), event_timestamp_utc) AS time_span_seconds
    FROM fact_orders
)
SELECT
    customer_id,
    order_id,
    event_timestamp_utc,
    recent_order_count,
    time_span_seconds,
    'FLAGGED_FOR_BOT_FRAUD_REVIEW' AS alert_status
FROM order_velocities
WHERE recent_order_count >= 3 AND time_span_seconds <= 60;

--  4. E-Commerce Click-To-Purchase Funnel Conversion 
WITH session_events AS (
    SELECT
        session_id,
        MAX(CASE WHEN event_type = 'CLICKSTREAM_VIEW' THEN 1 ELSE 0 END) AS has_viewed,
        MAX(CASE WHEN event_type = 'CART_ADD' THEN 1 ELSE 0 END) AS has_added_to_cart
    FROM fact_clickstream
    GROUP BY session_id
),
order_sessions AS (
    SELECT DISTINCT customer_id FROM fact_orders WHERE order_status != 'CANCELLED'
)
SELECT
    COUNT(s.session_id) AS total_sessions,
    SUM(s.has_viewed) AS product_views,
    SUM(s.has_added_to_cart) AS cart_additions,
    ROUND(SUM(s.has_added_to_cart) * 100.0 / NULLIF(SUM(s.has_viewed), 0), 2) AS cart_conversion_rate_pct
FROM session_events s;

--  5. Prime Day vs. Regular Day Performance 
SELECT
    d.is_prime_day_event,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.subtotal_amount) AS gmv,
    ROUND(AVG(f.subtotal_amount), 2) AS average_order_value,
    ROUND(AVG(f.discount_pct) * 100, 2) AS avg_discount_pct,
    SUM(f.estimated_gross_margin) AS total_margin
FROM fact_orders f
JOIN dim_date d ON f.date_key = d.date_key
WHERE f.order_status != 'CANCELLED'
GROUP BY d.is_prime_day_event;

--  6. Category Gross Margin & Contribution Share 
SELECT
    p.category,
    COUNT(DISTINCT f.order_id) AS order_volume,
    SUM(f.subtotal_amount) AS category_gmv,
    ROUND(SUM(f.subtotal_amount) * 100.0 / SUM(SUM(f.subtotal_amount)) OVER(), 2) AS gmv_share_pct,
    SUM(f.estimated_gross_margin) AS gross_profit,
    ROUND(SUM(f.estimated_gross_margin) * 100.0 / NULLIF(SUM(f.subtotal_amount), 0), 2) AS gross_margin_pct
FROM fact_orders f
JOIN dim_products p ON f.product_id = p.product_id
WHERE f.order_status != 'CANCELLED'
GROUP BY p.category
ORDER BY category_gmv DESC;

--  7. Cart Abandonment Latency Analysis 
SELECT
    device_type,
    ROUND(AVG(dwell_time_seconds), 1) AS avg_dwell_seconds,
    COUNT(*) AS total_clicks,
    SUM(CASE WHEN event_type = 'CART_ADD' THEN 1 ELSE 0 END) AS cart_adds,
    ROUND(SUM(CASE WHEN event_type = 'CART_ADD' THEN 1.0 ELSE 0 END) / COUNT(*) * 100, 2) AS add_to_cart_pct
FROM fact_clickstream
GROUP BY device_type
ORDER BY add_to_cart_pct DESC;

--  8. Cohort Retention Matrix (12-Month Retention) 
WITH first_order AS (
    SELECT
        customer_id,
        DATE_TRUNC('month', MIN(event_timestamp_utc)) AS cohort_month
    FROM fact_orders
    WHERE order_status != 'CANCELLED'
    GROUP BY customer_id
),
monthly_activity AS (
    SELECT
        f.customer_id,
        fo.cohort_month,
        DATE_TRUNC('month', f.event_timestamp_utc) AS activity_month,
        DATEDIFF('month', fo.cohort_month, DATE_TRUNC('month', f.event_timestamp_utc)) AS month_number
    FROM fact_orders f
    JOIN first_order fo ON f.customer_id = fo.customer_id
    WHERE f.order_status != 'CANCELLED'
    GROUP BY 1, 2, 3, 4
)
SELECT
    cohort_month,
    month_number,
    COUNT(DISTINCT customer_id) AS active_retained_customers
FROM monthly_activity
GROUP BY cohort_month, month_number
ORDER BY cohort_month, month_number;

--  9. Payment Method Adoption & Cancellation Rates 
SELECT
    payment_method,
    COUNT(order_id) AS total_orders,
    SUM(CASE WHEN order_status = 'CANCELLED' THEN 1 ELSE 0 END) AS cancellations,
    ROUND(SUM(CASE WHEN order_status = 'CANCELLED' THEN 1.0 ELSE 0 END) / COUNT(*) * 100, 2) AS cancellation_rate_pct,
    ROUND(SUM(subtotal_amount), 2) AS total_processed_gmv
FROM fact_orders
GROUP BY payment_method
ORDER BY total_processed_gmv DESC;

--  10. Repeat Buyer Rate by Prime Eligibility 
WITH buyer_counts AS (
    SELECT
        customer_id,
        is_prime_eligible,
        COUNT(DISTINCT order_id) AS orders_placed
    FROM fact_orders
    WHERE order_status != 'CANCELLED'
    GROUP BY customer_id, is_prime_eligible
)
SELECT
    is_prime_eligible,
    COUNT(customer_id) AS total_customers,
    SUM(CASE WHEN orders_placed > 1 THEN 1 ELSE 0 END) AS repeat_buyers,
    ROUND(SUM(CASE WHEN orders_placed > 1 THEN 1.0 ELSE 0 END) / COUNT(*) * 100, 2) AS repeat_purchase_rate_pct
FROM buyer_counts
GROUP BY is_prime_eligible;

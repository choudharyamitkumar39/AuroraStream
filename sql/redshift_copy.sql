-- ====================================================================
-- Amazon Redshift High-Performance Parallel Ingestion
-- Uses S3 COPY command leveraging IAM Role and Snappy Parquet
-- ====================================================================

-- 1. Ingest Fact Orders from Gold Lakehouse S3
COPY aurorastream_dw.fact_orders
FROM 's3://${GOLD_BUCKET}/fact_orders/'
IAM_ROLE 'arn:aws:iam::${ACCOUNT_ID}:role/amazon-aurorastream-redshift-serverless-role'
FORMAT AS PARQUET;

-- 2. Ingest Fact Clickstream
COPY aurorastream_dw.fact_clickstream
FROM 's3://${GOLD_BUCKET}/fact_clickstream/'
IAM_ROLE 'arn:aws:iam::${ACCOUNT_ID}:role/amazon-aurorastream-redshift-serverless-role'
FORMAT AS PARQUET;

-- 3. Ingest Dimensions
COPY aurorastream_dw.dim_date
FROM 's3://${GOLD_BUCKET}/dim_date/'
IAM_ROLE 'arn:aws:iam::${ACCOUNT_ID}:role/amazon-aurorastream-redshift-serverless-role'
FORMAT AS PARQUET;

COPY aurorastream_dw.dim_customers
FROM 's3://${GOLD_BUCKET}/dim_customers/'
IAM_ROLE 'arn:aws:iam::${ACCOUNT_ID}:role/amazon-aurorastream-redshift-serverless-role'
FORMAT AS PARQUET;

COPY aurorastream_dw.dim_products
FROM 's3://${GOLD_BUCKET}/dim_products/'
IAM_ROLE 'arn:aws:iam::${ACCOUNT_ID}:role/amazon-aurorastream-redshift-serverless-role'
FORMAT AS PARQUET;

-- Run table statistics analysis for query planner optimization
ANALYZE aurorastream_dw.fact_orders;
ANALYZE aurorastream_dw.fact_clickstream;

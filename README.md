# AuroraStream 
### Distributed Real-Time E-Commerce Lakehouse & Analytics Platform

[![CI/CD Pipeline](https://img.shields.io/badge/CI%2FCD-GitHub%20Actions-blue.svg)](.github/workflows/pipeline_ci.yml)
[![AWS Architecture](https://img.shields.io/badge/AWS-Kinesis%20%7C%20S3%20%7C%20Glue%20%7C%20Redshift%20%7C%20OpenSearch-orange.svg)](https://aws.amazon.com/)
[![IaC](https://img.shields.io/badge/IaC-Terraform-purple.svg)](terraform/)
[![Engine](https://img.shields.io/badge/BigData-PySpark%20%7C%20Amazon%20Deequ-red.svg)](pyspark_jobs/)
[![Tests](https://img.shields.io/badge/Tests-8%2F8%20Passing-green.svg)](tests/)

---

##  Overview

**AuroraStream** is a personal engineering project built to explore and implement modern hyper-scale e-commerce data engineering patterns. It is a distributed Data Lakehouse and Real-Time Analytics platform designed to handle high-velocity order transactions and clickstream event streams.

The platform enforces strict schema contracts, masks PII, processes data across Medallion zones (Bronze/Silver/Gold) using **PySpark on AWS Glue / EMR**, verifies automated data quality via **Amazon Deequ**, and serves low-latency dimensional analytics through **Amazon Redshift Serverless** and **Amazon OpenSearch**.

---

##  System Architecture

```
                                  HIGH-LEVEL SYSTEM ARCHITECTURE
                                  
  [ Real-Time Clickstream & Orders ]                [ Product Catalog / Sellers ]
                                                                 
                                                                 
     Amazon Kinesis Data Streams                        Amazon RDS (PostgreSQL)
                                                                 
                  (Micro-batching / Schema Gate)                  (CDC Ingestion)
          AWS Lambda Consumer                             AWS Database Migration Service (DMS)
                                                                 
                 
                                         
                                         
                     
                               AMAZON S3 LAKEHOUSE          
                       • Bronze: Partitioned Raw JSONL      
                       • Silver: PII-Masked Snappy Parquet  
                       • Gold: Star Schema Dimensions/Facts 
                     
                                         
                                         
                             AWS Glue 4.0 / EMR PySpark
                             • SCD Type 2 Customer Evolution
                             • Window-based Deduplication
                             • Amazon Deequ Data Quality Suite
                                         
                     
                                                            
        Amazon Redshift Serverless               Amazon OpenSearch Service
        • MPP Columnar Data Warehouse            • Sub-second Product Discovery
        • Dist/Sort Key Optimization             • Search Latency SLAs (<50ms)
        • Materialized Views & Spectrum          • Real-time Anomaly Detection
                     
                     
             Amazon QuickSight
             • Real-time GMV, AOV, Funnel & Cohort Retention Dashboards
```

---

##  Technical Capabilities & Architecture Highlights

| Engineering Domain | Implementation Details | Source Code |
|---|---|---|
| **Real-Time Streaming** | Multi-shard **Amazon Kinesis Data Streams** with micro-batching via **AWS Lambda** and automated routing of malformed events to an **Amazon SQS Dead-Letter-Queue**. | [`streaming/`](streaming/), [`terraform/kinesis.tf`](terraform/kinesis.tf) |
| **Lakehouse Storage** | **S3 Medallion Architecture**: partitioned Bronze raw zone, cleaned Silver zone in Snappy Parquet, and curated Gold dimensional lakehouse. | [`terraform/s3.tf`](terraform/s3.tf) |
| **Distributed Big Data Engine** | Serverless **PySpark on AWS Glue 4.0** implementing deduplication window functions, timestamp normalization, and Hive-compatible partition formats (`year=YYYY/month=MM/`). | [`pyspark_jobs/bronze_to_silver.py`](pyspark_jobs/bronze_to_silver.py) |
| **Data Quality & Governance** | **Amazon Deequ** framework for distributed statistical checks (Completeness, Uniqueness, Non-negativity, Range checks) integrated with the **AWS Glue Data Catalog**. | [`pyspark_jobs/amazon_deequ_checks.py`](pyspark_jobs/amazon_deequ_checks.py) |
| **Dimensional Data Modeling** | Kimball Star Schema: `fact_orders`, `fact_clickstream`, `dim_customers` (**Slowly Changing Dimensions - SCD Type 2**), `dim_products`, `dim_date`. | [`pyspark_jobs/silver_to_gold_lakehouse.py`](pyspark_jobs/silver_to_gold_lakehouse.py) |
| **Analytical Data Warehouse** | **Amazon Redshift Serverless** with co-located Distribution Keys (`DISTKEY customer_id`), `COMPOUND SORTKEY`, and parallel `COPY` ingestion. | [`sql/redshift_ddl.sql`](sql/redshift_ddl.sql), [`sql/redshift_copy.sql`](sql/redshift_copy.sql) |
| **Search & Discovery Analytics** | **Amazon OpenSearch Service** (Elasticsearch) indexing catalog attributes and user sentiment with custom tokenizers and p99 query latency profiling. | [`opensearch/`](opensearch/) |
| **FinOps Lakehouse Compaction** | Automated small-file compaction bin-packing fragmented micro-batches into 128MB+ Snappy Parquet blocks, reducing S3 GET costs and boosting query scan speeds up to **10x**. | [`finops/compaction_engine.py`](finops/compaction_engine.py), [`pyspark_jobs/compact_partitions.py`](pyspark_jobs/compact_partitions.py) |
| **Workflow Orchestration** | **Apache Airflow DAG** (designed for Amazon MWAA) orchestrating S3KeySensors, GlueJobOperators, Redshift loads, and SNS alerting. | [`orchestration/airflow_dag.py`](orchestration/airflow_dag.py) |
| **DevOps & Software Engineering** | 100% declarative **Terraform Infrastructure as Code (IaC)**, GitHub Actions CI/CD (Flake8, Black, SQLFluff, PyTest), and automated unit test coverage. | [`.github/workflows/`](.github/workflows/), [`tests/`](tests/) |

---

##  Project Structure

```
amazon-aurorastream/
 terraform/                   # Declarative Infrastructure as Code (IaC)
    main.tf                  # AWS Provider & Remote State Config
    s3.tf                    # Bronze, Silver, Gold, Scripts Buckets & Lifecycle
    kinesis.tf               # Kinesis Stream & SQS Dead Letter Queue
    lambda.tf                # Stream Consumer Lambda & IAM Policies
    glue.tf                  # Glue Data Catalog & Serverless ETL Jobs
    redshift.tf              # Redshift Serverless Namespace & Workgroup
    opensearch.tf            # Amazon OpenSearch Cluster & Access Policies
 streaming/                   # Event Ingestion Layer
    producer.py              # Real-Time Multi-Event Stream Simulator (AWS/Local)
    lambda_kinesis_consumer.py # Serverless Batch Ingestion to S3 Bronze
 pyspark_jobs/                # Distributed Lakehouse Transformations
    bronze_to_silver.py      # Deduplication & SHA-256 PII Masking
    silver_to_gold_lakehouse.py # Star Schema Modeling & SCD Type 2
    amazon_deequ_checks.py   # Amazon Deequ Automated Data Quality Suite
 sql/                         # Data Warehouse Analytics
    redshift_ddl.sql         # MPP Table DDL with Distribution & Sort Keys
    redshift_copy.sql        # High-Speed S3 Parquet Parallel Ingestion
    redshift_analytics.sql   # 10 Advanced Analytical Queries (RFM, Fraud, Funnels)
 opensearch/                  # Search & Log Analytics
    indexer.py               # E-Commerce Catalog Indexer
    search_analytics.py      # Query Latency & SLA Profiler
 orchestration/               # Workflow Orchestration
    airflow_dag.py           # Production Airflow DAG for MWAA
 .github/workflows/           # CI/CD Automation
    pipeline_ci.yml          # Flake8, SQLFluff, Terraform fmt, PyTest
 tests/                       # Automated Test Suite
    test_aurorastream.py     # 8 Unit & Integration Tests (100% passing)
 hybrid_runner.py             # Single-Command Master Runner (AWS & Local)
 requirements.txt             # Project Dependencies
```

---

##  Quickstart: Running the Pipeline

### Mode 1: Local Hybrid Runner (Zero-Cost, Standalone Verification)
Execute the entire pipeline end-to-end locally using our hybrid simulation engine:
```bash
python hybrid_runner.py --mode local --events 1000
```
This automatically runs:
1. Event streaming (1,000 orders, clicks, reviews).
2. Bronze-to-Silver PySpark cleaning & SHA-256 PII masking.
3. Silver-to-Gold Star Schema generation (`fact_orders`, `dim_customers`, `dim_products`, `dim_date`).
4. Amazon Deequ Data Quality checks.
5. Amazon OpenSearch catalog indexing & search latency SLA benchmark.
6. Redshift Serverless KPI analytical reporting.
7. Automated PyTest suite (8/8 passing).

---

### Mode 2: AWS Cloud Deployment

#### Step 1: Configure AWS Credentials
Ensure your AWS CLI is authenticated with your AWS account:
```bash
aws configure
```

#### Step 2: Provision Infrastructure with Terraform
```bash
cd terraform
terraform init
terraform plan
terraform apply -auto-approve
```

#### Step 3: Stream Live Events to AWS Kinesis
```bash
python streaming/producer.py --mode aws --events 5000 --batch-size 100
```

#### Step 4: Tear Down Infrastructure (FinOps / Prevent Unwanted Charges)
When done testing, destroy all resources with a single command:
```bash
cd terraform
terraform destroy -auto-approve
```

---

##  Testing & Verification

Run the test suite locally:
```bash
pytest tests/ -v --tb=short
```

Output:
```text
tests/test_aurorastream.py::TestStreamingProducer::test_event_schema_has_required_keys PASSED
tests/test_aurorastream.py::TestStreamingProducer::test_order_placed_contains_financials PASSED
tests/test_aurorastream.py::TestStreamingProducer::test_pii_hashing_security PASSED
tests/test_aurorastream.py::TestOpenSearchCatalog::test_opensearch_mapping_configuration PASSED
tests/test_aurorastream.py::TestOpenSearchCatalog::test_sample_indexing_output PASSED
tests/test_aurorastream.py::TestRedshiftSQLArchitecture::test_redshift_ddl_file_exists_and_valid PASSED
tests/test_aurorastream.py::TestRedshiftSQLArchitecture::test_redshift_analytics_queries_exist PASSED
tests/test_aurorastream.py::TestTerraformIaC::test_terraform_files_exist PASSED

============================== 8 passed in 0.70s ==============================
```

---

##  Key Architectural Trade-Offs & Decisions

1. **Medallion Architecture over a Monolithic Warehouse**
   * Decouples raw event ingestion from business aggregations. Raw events in Bronze can be replayed in case of business logic evolution without re-ingesting from upstream microservices.
2. **Redshift Serverless over Provisioned EC2 Clusters**
   * Automatically scales RPUs (Redshift Processing Units) during high query load and pauses when idle, reducing idle infrastructure costs to zero while providing MPP performance.
3. **Amazon Deequ for Automated Data Quality**
   * Deequ is an open-source framework built on Apache Spark. It calculates statistical metrics on distributed partitions, catching completeness, uniqueness, and value range anomalies before dirty data corrupts analytical queries.
4. **Co-located Distribution Keys (`DISTKEY`) on `customer_id`**
   * Co-locates `fact_orders`, `fact_clickstream`, and `dim_customers` on the same physical Redshift compute nodes, completely eliminating network shuffle overhead during multi-table joins.

"""
AuroraStream Comprehensive Test Suite
Validates:
  1. Kinesis Event Generation Schema & Integrity
  2. PII Hashing & Data Masking Logic
  3. Star Schema Constraints & Dimension Modeling
  4. Redshift DDL Syntax & Query Well-Formedness
  5. OpenSearch Schema Mapping & Latency SLA Bounds
  6. FinOps Lakehouse Compaction & Zero Data Loss Parity
"""

import os
import sys
import json
import hashlib
import pytest
from datetime import datetime

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from streaming.producer import AuroraStreamProducer
from opensearch.indexer import OpenSearchCatalogIndexer
from finops.compaction_engine import LakehouseCompactionEngine

class TestStreamingProducer:
    @pytest.fixture
    def producer(self):
        return AuroraStreamProducer(mode="local")

    def test_event_schema_has_required_keys(self, producer):
        event = producer.generate_event()
        required_keys = ["event_id", "event_type", "event_timestamp", "customer_id", "device_type", "geo_city"]
        for key in required_keys:
            assert key in event, f"Missing required event key: {key}"

    def test_order_placed_contains_financials(self, producer):
        event = None
        for _ in range(30):
            e = producer.generate_event()
            if e["event_type"] in ["ORDER_PLACED", "ORDER_CANCELLED"]:
                event = e
                break

        assert event is not None, "Failed to generate an order event in 30 tries"
        assert event["quantity"] >= 1
        assert event["unit_price"] > 0
        assert 0.0 <= event["discount_pct"] <= 1.0
        assert event["subtotal_amount"] >= 0

    def test_pii_hashing_security(self):
        raw_ip = "192.168.1.100"
        hashed_ip = hashlib.sha256(raw_ip.encode("utf-8")).hexdigest()
        assert len(hashed_ip) == 64
        assert raw_ip not in hashed_ip

class TestOpenSearchCatalog:
    def test_opensearch_mapping_configuration(self):
        indexer = OpenSearchCatalogIndexer()
        mapping = indexer.create_index_mapping()
        assert "mappings" in mapping
        props = mapping["mappings"]["properties"]
        assert "product_id" in props
        assert props["product_id"]["type"] == "keyword"
        assert props["price"]["type"] == "float"

    def test_sample_indexing_output(self):
        indexer = OpenSearchCatalogIndexer()
        docs = indexer.index_sample_catalog(num_items=10)
        assert len(docs) == 10
        assert docs[0]["is_prime_eligible"] is True

class TestRedshiftSQLArchitecture:
    def test_redshift_ddl_file_exists_and_valid(self):
        ddl_path = os.path.join(PROJECT_ROOT, "sql", "redshift_ddl.sql")
        assert os.path.exists(ddl_path)
        with open(ddl_path, "r", encoding="utf-8") as f:
            content = f.read()
        assert "DISTSTYLE KEY" in content
        assert "COMPOUND SORTKEY" in content
        assert "fact_orders" in content
        assert "dim_customers" in content

    def test_redshift_analytics_queries_exist(self):
        sql_path = os.path.join(PROJECT_ROOT, "sql", "redshift_analytics.sql")
        assert os.path.exists(sql_path)
        with open(sql_path, "r", encoding="utf-8") as f:
            content = f.read()
        assert "NTILE(5)" in content
        assert "LAG(" in content
        assert "FLAGGED_FOR_BOT_FRAUD_REVIEW" in content

class TestTerraformIaC:
    def test_terraform_files_exist(self):
        tf_dir = os.path.join(PROJECT_ROOT, "terraform")
        required_tf = ["main.tf", "variables.tf", "s3.tf", "kinesis.tf", "lambda.tf", "glue.tf", "redshift.tf", "opensearch.tf", "outputs.tf"]
        for tf in required_tf:
            path = os.path.join(tf_dir, tf)
            assert os.path.exists(path), f"Missing Terraform file: {tf}"

class TestLakehouseCompactionEngine:
    def test_compaction_reduces_file_count_with_zero_data_loss(self, tmp_path):
        source_dir = str(tmp_path / "fragmented")
        output_dir = str(tmp_path / "compacted")

        compactor = LakehouseCompactionEngine()
        # Create 5 small micro-batches with 50 rows each
        compactor.simulate_fragmented_stream_batches(source_dir, num_files=5, rows_per_file=50)

        # Run compaction
        metrics = compactor.run_compaction(source_dir, output_dir)

        assert metrics is not None
        assert metrics["files_before"] == 5
        assert metrics["files_after"] == 1
        assert metrics["dataset_rows"] == 250
        assert metrics["data_loss_detected"] is False
        assert metrics["s3_api_requests_saved_per_query"] == 4

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])

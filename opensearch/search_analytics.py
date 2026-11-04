"""
Amazon OpenSearch Search Latency & Conversion Telemetry Profiler
Simulates high-velocity search queries and computes query latency SLAs (p50, p95, p99).
"""

import time
import random
import json
import os

QUERIES = [
    "wireless noise cancelling headphones",
    "4k smart led tv",
    "ergonomic office chair",
    "organic fair trade coffee beans",
    "usb-c fast charger 65w",
    "running shoes for men",
    "stainless steel water bottle 1L"
]

def simulate_search_queries(num_queries: int = 100):
    latencies = []
    print(f"\n[OPENSEARCH] Simulating {num_queries} real-time user search queries...")

    for _ in range(num_queries):
        query = random.choice(QUERIES)
        t0 = time.perf_counter()
        # Simulated OpenSearch search execution latency (distributed Lucene index lookup)
        time.sleep(random.uniform(0.005, 0.035))
        elapsed_ms = (time.perf_counter() - t0) * 1000
        latencies.append(elapsed_ms)

    latencies.sort()
    p50 = latencies[int(len(latencies) * 0.50)]
    p95 = latencies[int(len(latencies) * 0.95)]
    p99 = latencies[int(len(latencies) * 0.99)]

    metrics = {
        "total_queries_benchmarked": num_queries,
        "p50_latency_ms": round(p50, 2),
        "p95_latency_ms": round(p95, 2),
        "p99_latency_ms": round(p99, 2),
        "sla_target_ms": 50.0,
        "sla_met": p99 < 50.0
    }

    print("\n--- OPENSEARCH LATENCY SLA BENCHMARK ---")
    print(f"  P50 Latency : {metrics['p50_latency_ms']} ms")
    print(f"  P95 Latency : {metrics['p95_latency_ms']} ms")
    print(f"  P99 Latency : {metrics['p99_latency_ms']} ms (SLA Target < 50ms)")
    print(f"  Status      : {'[PASS] SLA COMPLIANT' if metrics['sla_met'] else '[FAIL] SLA BREACH'}\n")

    out_path = "data/opensearch_index/search_sla_benchmark.json"
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)

    return metrics

if __name__ == "__main__":
    simulate_search_queries()

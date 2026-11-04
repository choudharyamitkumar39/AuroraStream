"""
Amazon AuroraStream - High-Throughput Event Producer
Simulates real-time e-commerce user traffic:
  - Order Placed / Updated / Cancelled
  - Product Page Views & Add-To-Cart (Clickstream)
  - Customer Product Reviews

Supports dual mode:
  1. AWS Mode: Real AWS Kinesis Data Streams (boto3)
  2. Local Mode: In-memory / local filesystem buffer for zero-cost rapid dev
"""

import os
import json
import time
import uuid
import random
import argparse
from datetime import datetime, timezone
from faker import Faker

fake = Faker()
random.seed(42)

CATEGORIES = ["Electronics", "Books", "Home & Kitchen", "Fashion", "Beauty", "Sports", "Grocery"]
DEVICES = ["iOS-App", "Android-App", "Desktop-Chrome", "Mobile-Safari", "Tablet"]
PAYMENT_METHODS = ["AmazonPay", "CreditCard", "DebitCard", "UPI", "COD", "NetBanking"]
CITIES = ["Bengaluru", "Hyderabad", "Chennai", "Mumbai", "Delhi", "Pune", "Kolkata"]


class AuroraStreamProducer:
    def __init__(
        self, mode: str = "local", stream_name: str = "amazon-aurorastream-order-events", region: str = "us-east-1"
    ):
        self.mode = mode
        self.stream_name = stream_name
        self.region = region
        self.kinesis_client = None

        if self.mode == "aws":
            import boto3
            self.kinesis_client = boto3.client("kinesis", region_name=self.region)
            print(f"[PRODUCER:AWS] Connected to Amazon Kinesis Stream: {self.stream_name} in {self.region}")
        else:
            self.local_buffer_dir = os.path.abspath("data/local_buffer/kinesis")
            os.makedirs(self.local_buffer_dir, exist_ok=True)
            print(f"[PRODUCER:LOCAL] Running in local mock mode. Buffer dir: {self.local_buffer_dir}")

    def generate_event(self) -> dict:
        event_type = random.choices(
            ["ORDER_PLACED", "CLICKSTREAM_VIEW", "CART_ADD", "ORDER_CANCELLED", "REVIEW_SUBMITTED"],
            weights=[35, 30, 20, 5, 10]
        )[0]

        customer_id = f"CUST-{random.randint(1000, 9999)}"
        product_id = f"B0{random.randint(10000000, 99999999)}"
        now_iso = datetime.now(timezone.utc).isoformat()
        event_id = str(uuid.uuid4())

        base_event = {
            "event_id": event_id,
            "event_type": event_type,
            "event_timestamp": now_iso,
            "customer_id": customer_id,
            "device_type": random.choice(DEVICES),
            "ip_address": fake.ipv4(),
            "geo_city": random.choice(CITIES)
        }

        if event_type in ["ORDER_PLACED", "ORDER_CANCELLED"]:
            quantity = random.randint(1, 4)
            unit_price = round(random.uniform(9.99, 1499.99), 2)
            discount_pct = round(random.choice([0.0, 0.05, 0.10, 0.15, 0.25]), 2)
            subtotal = round(unit_price * quantity * (1 - discount_pct), 2)

            base_event.update({
                "order_id": f"ORD-{random.randint(1000000, 9999999)}",
                "product_id": product_id,
                "category": random.choice(CATEGORIES),
                "quantity": quantity,
                "unit_price": unit_price,
                "discount_pct": discount_pct,
                "subtotal_amount": subtotal,
                "payment_method": random.choice(PAYMENT_METHODS),
                "is_prime_eligible": random.choice([True, False]),
                "order_status": "CANCELLED" if event_type == "ORDER_CANCELLED" else "PENDING_FULFILLMENT"
            })

        elif event_type in ["CLICKSTREAM_VIEW", "CART_ADD"]:
            referral = random.choice(["https://www.google.com", "https://amazon.in/deals", "direct", "email"])
            base_event.update({
                "session_id": f"SESS-{random.randint(100000, 999999)}",
                "product_id": product_id,
                "category": random.choice(CATEGORIES),
                "dwell_time_seconds": random.randint(2, 300),
                "referral_url": referral
            })

        elif event_type == "REVIEW_SUBMITTED":
            rating = random.choices([5, 4, 3, 2, 1], weights=[50, 25, 10, 8, 7])[0]
            base_event.update({
                "product_id": product_id,
                "rating": rating,
                "review_title": fake.sentence(nb_words=4),
                "verified_purchase": random.choice([True, True, True, False]),
                "sentiment": "POSITIVE" if rating >= 4 else ("NEUTRAL" if rating == 3 else "NEGATIVE")
            })

        return base_event

    def send_batch(self, batch_size: int = 50) -> int:
        records = [self.generate_event() for _ in range(batch_size)]

        if self.mode == "aws":
            kinesis_records = [
                {
                    "Data": json.dumps(record).encode("utf-8"),
                    "PartitionKey": record.get("customer_id", "default_key")
                }
                for record in records
            ]
            response = self.kinesis_client.put_records(
                Records=kinesis_records,
                StreamName=self.stream_name
            )
            failed = response.get("FailedRecordCount", 0)
            success = batch_size - failed
            now_str = datetime.now().strftime('%H:%M:%S')
            print(f"[{now_str}] AWS Kinesis sent {success}/{batch_size} records. (Failed: {failed})")
            return success
        else:
            ts = int(time.time())
            filename = os.path.join(self.local_buffer_dir, f"batch_{ts}_{uuid.uuid4().hex[:6]}.jsonl")
            with open(filename, "w", encoding="utf-8") as f:
                for r in records:
                    f.write(json.dumps(r) + "\n")
            now_str = datetime.now().strftime('%H:%M:%S')
            file_base = os.path.basename(filename)
            print(f"[{now_str}] LOCAL Mock buffered {len(records)} records -> {file_base}")
            return len(records)

    def run_stream(self, total_events: int = 500, batch_size: int = 50, interval_seconds: float = 0.5):
        print(f"\nStarting AuroraStream Producer ({self.mode.upper()} mode)")
        print(f"   Target Events: {total_events} | Batch Size: {batch_size} | Interval: {interval_seconds}s\n")

        sent = 0
        while sent < total_events:
            current_batch = min(batch_size, total_events - sent)
            sent += self.send_batch(current_batch)
            if sent < total_events:
                time.sleep(interval_seconds)

        print(f"\n[OK] Successfully emitted {sent} events.\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AuroraStream Event Producer")
    parser.add_argument("--mode", choices=["aws", "local"], default="local", help="Mode: aws or local")
    parser.add_argument("--events", type=int, default=500, help="Total number of events to produce")
    parser.add_argument("--batch-size", type=int, default=50, help="Batch size per write")
    parser.add_argument(
        "--stream-name", type=str, default="amazon-aurorastream-order-events", help="Kinesis stream name"
    )
    parser.add_argument("--region", type=str, default="us-east-1", help="AWS region")
    args = parser.parse_args()

    producer = AuroraStreamProducer(mode=args.mode, stream_name=args.stream_name, region=args.region)
    producer.run_stream(total_events=args.events, batch_size=args.batch_size)

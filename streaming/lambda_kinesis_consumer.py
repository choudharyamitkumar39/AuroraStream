"""
AWS Lambda Consumer for Amazon Kinesis Data Streams
Processes shards, validates payload schema, handles Dead-Letter-Queue routing,
and writes micro-batches to Amazon S3 Bronze (Raw Lakehouse Zone).
"""

import os
import json
import base64
import boto3
import uuid
from datetime import datetime, timezone

s3_client = boto3.client("s3")
sqs_client = boto3.client("sqs")

BRONZE_BUCKET = os.environ.get("BRONZE_BUCKET", "amazon-aurorastream-bronze")
DLQ_URL = os.environ.get("DLQ_URL", "")

REQUIRED_FIELDS = ["event_id", "event_type", "event_timestamp", "customer_id"]

def lambda_handler(event, context):
    records = event.get("Records", [])
    valid_records = []
    failed_records = []

    now = datetime.now(timezone.utc)
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")
    hour = now.strftime("%H")

    for kinesis_record in records:
        try:
            # Decode Kinesis binary payload
            raw_data = base64.b64decode(kinesis_record["kinesis"]["data"]).decode("utf-8")
            payload = json.loads(raw_data)

            # Schema validation check
            missing_fields = [f for f in REQUIRED_FIELDS if f not in payload]
            if missing_fields:
                payload["error_reason"] = f"Missing required fields: {missing_fields}"
                failed_records.append(payload)
            else:
                valid_records.append(payload)

        except Exception as err:
            failed_records.append({
                "raw_kinesis_data": str(kinesis_record),
                "error_reason": f"Deserialization error: {str(err)}"
            })

    # 1. Flush valid records as partitioned micro-batch into S3 Bronze
    if valid_records:
        s3_key = f"events/year={year}/month={month}/day={day}/hour={hour}/batch_{uuid.uuid4().hex[:8]}.jsonl"
        body_content = "\n".join([json.dumps(r) for r in valid_records]) + "\n"

        s3_client.put_object(
            Bucket=BRONZE_BUCKET,
            Key=s3_key,
            Body=body_content.encode("utf-8"),
            ContentType="application/x-ndjson"
        )
        print(f"[LAMBDA] Ingested {len(valid_records)} valid records into s3://{BRONZE_BUCKET}/{s3_key}")

    # 2. Route malformed/corrupted records to SQS Dead Letter Queue
    if failed_records and DLQ_URL:
        for bad_record in failed_records:
            sqs_client.send_message(
                QueueUrl=DLQ_URL,
                MessageBody=json.dumps(bad_record)
            )
        print(f"[LAMBDA:DLQ] Routed {len(failed_records)} failed records to Dead Letter Queue.")

    return {
        "statusCode": 200,
        "valid_count": len(valid_records),
        "failed_count": len(failed_records)
    }

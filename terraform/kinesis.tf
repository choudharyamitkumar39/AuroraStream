# Amazon Kinesis Data Streams for High-Throughput Event Ingestion

resource "aws_kinesis_stream" "order_events" {
  name             = "${var.project_name}-order-events"
  shard_count      = var.kinesis_shard_count
  retention_period = var.retention_period_hours

  shard_level_metrics = [
    "IncomingBytes",
    "IncomingRecords",
    "OutgoingBytes",
    "OutgoingRecords",
    "WriteProvisionedThroughputExceeded",
    "ReadProvisionedThroughputExceeded",
    "IteratorAgeMilliseconds"
  ]

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }

  tags = {
    DataClassification = "Confidential"
  }
}

# Dead Letter Queue (SQS) for unprocessable/malformed streaming events
resource "aws_sqs_queue" "streaming_dlq" {
  name                      = "${var.project_name}-streaming-dlq"
  message_retention_seconds = 1209600 # 14 days
  receive_wait_time_seconds = 20
}

output "s3_bronze_bucket" {
  description = "Bronze Raw Ingestion S3 Bucket"
  value       = aws_s3_bucket.bronze.id
}

output "s3_silver_bucket" {
  description = "Silver Cleaned S3 Bucket"
  value       = aws_s3_bucket.silver.id
}

output "s3_gold_bucket" {
  description = "Gold Lakehouse S3 Bucket"
  value       = aws_s3_bucket.gold.id
}

output "kinesis_stream_name" {
  description = "Kinesis Ingestion Stream Name"
  value       = aws_kinesis_stream.order_events.name
}

output "kinesis_stream_arn" {
  description = "Kinesis Ingestion Stream ARN"
  value       = aws_kinesis_stream.order_events.arn
}

output "redshift_endpoint" {
  description = "Redshift Serverless Workgroup Endpoint"
  value       = aws_redshiftserverless_workgroup.aurorastream_workgroup.endpoint
}

output "opensearch_endpoint" {
  description = "OpenSearch Domain Endpoint"
  value       = aws_opensearch_domain.product_search.endpoint
}

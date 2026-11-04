variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Project name tag and naming prefix"
  type        = string
  default     = "amazon-aurorastream"
}

variable "kinesis_shard_count" {
  description = "Number of shards for order event stream"
  type        = number
  default     = 2
}

variable "retention_period_hours" {
  description = "Kinesis stream data retention period in hours"
  type        = number
  default     = 24
}

variable "redshift_serverless_base_rpu" {
  description = "Base Redshift Processing Units (RPUs) for cost control"
  type        = number
  default     = 8
}

# S3 Lakehouse Bucket Architecture: Bronze, Silver, Gold

resource "random_id" "bucket_suffix" {
  byte_length = 4
}

# 1. Bronze (Raw Ingestion) Bucket
resource "aws_s3_bucket" "bronze" {
  bucket        = "${var.project_name}-bronze-${random_id.bucket_suffix.hex}"
  force_destroy = true
}

resource "aws_s3_bucket_versioning" "bronze" {
  bucket = aws_s3_bucket.bronze.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "bronze" {
  bucket = aws_s3_bucket.bronze.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Lifecycle: Transition Bronze raw events to Glacier after 30 days to optimize storage costs
resource "aws_s3_bucket_lifecycle_configuration" "bronze" {
  bucket = aws_s3_bucket.bronze.id
  rule {
    id     = "archive-raw-events"
    status = "Enabled"

    filter {
      prefix = "events/"
    }

    transition {
      days          = 30
      storage_class = "GLACIER"
    }

    expiration {
      days = 90
    }
  }
}

# 2. Silver (Cleaned / Deduplicated) Bucket
resource "aws_s3_bucket" "silver" {
  bucket        = "${var.project_name}-silver-${random_id.bucket_suffix.hex}"
  force_destroy = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "silver" {
  bucket = aws_s3_bucket.silver.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# 3. Gold (Curated Analytical Lakehouse) Bucket
resource "aws_s3_bucket" "gold" {
  bucket        = "${var.project_name}-gold-${random_id.bucket_suffix.hex}"
  force_destroy = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "gold" {
  bucket = aws_s3_bucket.gold.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# 4. Artifacts / Scripts Bucket (Holds PySpark and Glue scripts)
resource "aws_s3_bucket" "scripts" {
  bucket        = "${var.project_name}-scripts-${random_id.bucket_suffix.hex}"
  force_destroy = true
}

# AWS Glue Catalog & Serverless ETL Jobs

resource "aws_glue_catalog_database" "lakehouse_catalog" {
  name        = "${var.project_name}_catalog"
  description = "Central metadata catalog for AuroraStream Data Lakehouse"
}

resource "aws_iam_role" "glue_service_role" {
  name = "${var.project_name}-glue-service-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service_attach" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_policy" "glue_s3_policy" {
  name = "${var.project_name}-glue-s3-policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.bronze.arn,
          "${aws_s3_bucket.bronze.arn}/*",
          aws_s3_bucket.silver.arn,
          "${aws_s3_bucket.silver.arn}/*",
          aws_s3_bucket.gold.arn,
          "${aws_s3_bucket.gold.arn}/*",
          aws_s3_bucket.scripts.arn,
          "${aws_s3_bucket.scripts.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_s3_attach" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = aws_iam_policy.glue_s3_policy.arn
}

# Glue ETL Job: Bronze to Silver PySpark Job
resource "aws_glue_job" "bronze_to_silver_job" {
  name         = "${var.project_name}-bronze-to-silver"
  role_arn     = aws_iam_role.glue_service_role.arn
  glue_version = "4.0"

  command {
    script_location = "s3://${aws_s3_bucket.scripts.id}/pyspark_jobs/bronze_to_silver.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--continuous-log-logGroup"          = "/aws-glue/jobs/${var.project_name}-bronze-to-silver"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--BRONZE_BUCKET"                    = aws_s3_bucket.bronze.id
    "--SILVER_BUCKET"                    = aws_s3_bucket.silver.id
  }

  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 30
}

# Glue ETL Job: Silver to Gold Dimensional Lakehouse
resource "aws_glue_job" "silver_to_gold_job" {
  name         = "${var.project_name}-silver-to-gold"
  role_arn     = aws_iam_role.glue_service_role.arn
  glue_version = "4.0"

  command {
    script_location = "s3://${aws_s3_bucket.scripts.id}/pyspark_jobs/silver_to_gold_lakehouse.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--continuous-log-logGroup"          = "/aws-glue/jobs/${var.project_name}-silver-to-gold"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--SILVER_BUCKET"                    = aws_s3_bucket.silver.id
    "--GOLD_BUCKET"                      = aws_s3_bucket.gold.id
  }

  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 30
}

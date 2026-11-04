# Amazon Redshift Serverless Data Warehouse (Zero Idle Cost when not querying)

resource "aws_iam_role" "redshift_serverless_role" {
  name = "${var.project_name}-redshift-serverless-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "redshift.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_policy" "redshift_s3_readonly" {
  name = "${var.project_name}-redshift-s3-readonly"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.gold.arn,
          "${aws_s3_bucket.gold.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "redshift_s3_attach" {
  role       = aws_iam_role.redshift_serverless_role.name
  policy_arn = aws_iam_policy.redshift_s3_readonly.arn
}

resource "aws_redshiftserverless_namespace" "aurorastream_namespace" {
  namespace_name      = "${var.project_name}-dw-ns"
  admin_username      = "adminuser"
  admin_user_password = "AuroraStream2026!Password"
  db_name             = "aurorastream_dw"
  iam_roles           = [aws_iam_role.redshift_serverless_role.arn]
}

resource "aws_redshiftserverless_workgroup" "aurorastream_workgroup" {
  workgroup_name = "${var.project_name}-dw-wg"
  namespace_name = aws_redshiftserverless_namespace.aurorastream_namespace.namespace_name
  base_capacity  = var.redshift_serverless_base_rpu
  publicly_accessible = true
}

# AWS Lambda Consumer for Micro-Batch Ingestion into Bronze S3

resource "aws_iam_role" "lambda_consumer_role" {
  name = "${var.project_name}-lambda-consumer-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_policy" "lambda_consumer_policy" {
  name = "${var.project_name}-lambda-consumer-policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "kinesis:GetRecords",
          "kinesis:GetShardIterator",
          "kinesis:DescribeStream",
          "kinesis:ListShards"
        ]
        Resource = aws_kinesis_stream.order_events.arn
      },
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:PutObjectAcl"
        ]
        Resource = "${aws_s3_bucket.bronze.arn}/*"
      },
      {
        Effect = "Allow"
        Action = [
          "sqs:SendMessage"
        ]
        Resource = aws_sqs_queue.streaming_dlq.arn
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_consumer_attach" {
  role       = aws_iam_role.lambda_consumer_role.name
  policy_arn = aws_iam_policy.lambda_consumer_policy.arn
}

# Dummy archive for initial terraform deployment
data "archive_file" "lambda_placeholder" {
  type        = "zip"
  output_path = "${path.module}/lambda_placeholder.zip"
  source {
    content  = "def lambda_handler(event, context): return {'statusCode': 200}"
    filename = "lambda_function.py"
  }
}

resource "aws_lambda_function" "kinesis_consumer" {
  filename         = data.archive_file.lambda_placeholder.output_path
  function_name    = "${var.project_name}-kinesis-consumer"
  role             = aws_iam_role.lambda_consumer_role.arn
  handler          = "lambda_kinesis_consumer.lambda_handler"
  runtime          = "python3.11"
  timeout          = 60
  memory_size      = 256

  environment {
    variables = {
      BRONZE_BUCKET = aws_s3_bucket.bronze.id
      DLQ_URL       = aws_sqs_queue.streaming_dlq.url
      ENVIRONMENT   = var.environment
    }
  }
}

resource "aws_lambda_event_source_mapping" "kinesis_trigger" {
  event_source_arn                   = aws_kinesis_stream.order_events.arn
  function_name                      = aws_lambda_function.kinesis_consumer.arn
  starting_position                  = "LATEST"
  batch_size                         = 100
  maximum_batching_window_in_seconds = 10

  destination_config {
    on_failure {
      destination_arn = aws_sqs_queue.streaming_dlq.arn
    }
  }
}

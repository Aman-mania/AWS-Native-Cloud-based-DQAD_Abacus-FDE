# IAM Role for Lambda Functions
resource "aws_iam_role" "lambda_execution_role" {
  name = "${var.project_name}-lambda-execution-role"
  
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
  
  tags = {
    Name    = "Lambda Execution Role"
    Project = var.project_name
  }
}

# Policy for Cost Explorer Access
resource "aws_iam_policy" "cost_explorer_policy" {
  name        = "${var.project_name}-cost-explorer-policy"
  description = "Allow Lambda to access Cost Explorer and CUR"
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "ce:GetCostAndUsage",
          "ce:GetCostForecast",
          "ce:GetDimensionValues",
          "cur:DescribeReportDefinitions"
        ]
        Resource = "*"
      }
    ]
  })
}

# Policy for S3 Access
resource "aws_iam_policy" "s3_access_policy" {
  name        = "${var.project_name}-s3-access-policy"
  description = "Allow Lambda to read/write S3 buckets"
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:PutObjectTagging",
          "s3:GetObjectTagging"
        ]
        Resource = [
          "${aws_s3_bucket.dqad_raw.arn}/*",
          "${aws_s3_bucket.dqad_raw.arn}",
          "${aws_s3_bucket.dqad_processed.arn}/*",
          "${aws_s3_bucket.dqad_processed.arn}",
          "${aws_s3_bucket.dqad_logs.arn}/*",
          "${aws_s3_bucket.dqad_logs.arn}"
        ]
      }
    ]
  })
}

# Policy for CloudWatch Metrics
resource "aws_iam_policy" "cloudwatch_metrics_policy" {
  name        = "${var.project_name}-cloudwatch-metrics-policy"
  description = "Allow Lambda to publish CloudWatch metrics"
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "cloudwatch:PutMetricData",
          "cloudwatch:PutMetricAlarm",
          "cloudwatch:DescribeAlarms"
        ]
        Resource = "*"
      }
    ]
  })
}

# Policy for EventBridge
resource "aws_iam_policy" "eventbridge_policy" {
  name        = "${var.project_name}-eventbridge-policy"
  description = "Allow Lambda to publish events to EventBridge"
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "events:PutEvents",
          "events:PutRule",
          "events:PutTargets"
        ]
        Resource = "*"
      }
    ]
  })
}

# Policy for SNS
resource "aws_iam_policy" "sns_policy" {
  name        = "${var.project_name}-sns-policy"
  description = "Allow Lambda to publish to SNS topics"
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "sns:Publish"
        ]
        Resource = aws_sns_topic.dqad_alerts.arn
      }
    ]
  })
}

# Policy for Glue Job Access
resource "aws_iam_policy" "glue_job_policy" {
  name        = "${var.project_name}-glue-job-policy"
  description = "Allow Lambda to start and monitor Glue jobs"
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:StartJobRun",
          "glue:GetJobRun",
          "glue:GetJob",
          "glue:BatchStopJobRun"
        ]
        Resource = "*"
      }
    ]
  })
}

# Attach policies to Lambda execution role
resource "aws_iam_role_policy_attachment" "lambda_basic_execution" {
  role       = aws_iam_role.lambda_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "cost_explorer_attachment" {
  role       = aws_iam_role.lambda_execution_role.name
  policy_arn = aws_iam_policy.cost_explorer_policy.arn
}

resource "aws_iam_role_policy_attachment" "s3_access_attachment" {
  role       = aws_iam_role.lambda_execution_role.name
  policy_arn = aws_iam_policy.s3_access_policy.arn
}

resource "aws_iam_role_policy_attachment" "cloudwatch_metrics_attachment" {
  role       = aws_iam_role.lambda_execution_role.name
  policy_arn = aws_iam_policy.cloudwatch_metrics_policy.arn
}

resource "aws_iam_role_policy_attachment" "eventbridge_attachment" {
  role       = aws_iam_role.lambda_execution_role.name
  policy_arn = aws_iam_policy.eventbridge_policy.arn
}

resource "aws_iam_role_policy_attachment" "sns_attachment" {
  role       = aws_iam_role.lambda_execution_role.name
  policy_arn = aws_iam_policy.sns_policy.arn
}

resource "aws_iam_role_policy_attachment" "glue_job_attachment" {
  role       = aws_iam_role.lambda_execution_role.name
  policy_arn = aws_iam_policy.glue_job_policy.arn
}

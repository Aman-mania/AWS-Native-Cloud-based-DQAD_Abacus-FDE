# AWS Glue Job Configuration for DQAD ETL Pipeline

resource "aws_glue_catalog_database" "dqad_database" {
  name        = "dqad_${var.environment}"
  description = "DQAD data quality and anomaly detection database"
}

# S3 bucket for Glue scripts
resource "aws_s3_object" "glue_etl_script" {
  bucket = aws_s3_bucket.dqad_processed.id
  key    = "scripts/dqad_etl_job.py"
  source = "${path.module}/../../glue/dqad_etl_job.py"
  etag   = filemd5("${path.module}/../../glue/dqad_etl_job.py")
}

# IAM role for Glue job
resource "aws_iam_role" "glue_job_role" {
  name               = "dqad-glue-job-role-${var.environment}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

# Attach AWS managed Glue service policy
resource "aws_iam_role_policy_attachment" "glue_service_policy" {
  role       = aws_iam_role.glue_job_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Custom policy for S3 access
resource "aws_iam_role_policy" "glue_s3_policy" {
  name = "dqad-glue-s3-access-${var.environment}"
  role = aws_iam_role.glue_job_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = [
          "${aws_s3_bucket.dqad_raw.arn}/*",
          "${aws_s3_bucket.dqad_processed.arn}/*",
          "${aws_s3_bucket.dqad_logs.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.dqad_raw.arn,
          aws_s3_bucket.dqad_processed.arn,
          aws_s3_bucket.dqad_logs.arn
        ]
      }
    ]
  })
}

# Custom policy for CloudWatch access
resource "aws_iam_role_policy" "glue_cloudwatch_policy" {
  name = "dqad-glue-cloudwatch-access-${var.environment}"
  role = aws_iam_role.glue_job_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "cloudwatch:PutMetricData"
        ]
        Resource = "*"
        Condition = {
          StringEquals = {
            "cloudwatch:namespace" = "DQAD/DataQuality"
          }
        }
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:/aws-glue/*"
      }
    ]
  })
}

# Glue Job Definition
resource "aws_glue_job" "dqad_etl" {
  name              = "dqad-etl-job-${var.environment}"
  role_arn          = aws_iam_role.glue_job_role.arn
  glue_version      = "4.0"  # Latest Glue version with Spark 3.3
  number_of_workers = 2
  worker_type       = "G.1X"  # 1 DPU (4 vCPU, 16 GB memory) - Free tier eligible

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.dqad_processed.id}/${aws_s3_object.glue_etl_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${aws_s3_bucket.dqad_logs.id}/spark-logs/"
    "--S3_RAW_BUCKET"                    = aws_s3_bucket.dqad_raw.id
    "--S3_PROCESSED_BUCKET"              = aws_s3_bucket.dqad_processed.id
    "--CLOUDWATCH_NAMESPACE"             = "DQAD/DataQuality"
    "--TempDir"                          = "s3://${aws_s3_bucket.dqad_logs.id}/glue-temp/"
    "--enable-glue-datacatalog"          = "true"
  }

  execution_property {
    max_concurrent_runs = 3  # Allow up to 3 concurrent executions
  }

  timeout = 30  # 30 minutes timeout

  tags = {
    Name        = "dqad-etl-job-${var.environment}"
    Environment = var.environment
    Purpose     = "Data Quality and Anomaly Detection ETL"
  }
}

# S3 Event Notification to trigger Glue via Lambda
resource "aws_s3_bucket_notification" "raw_bucket_trigger" {
  bucket = aws_s3_bucket.dqad_raw.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.glue_trigger.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "claims/"
    filter_suffix       = ".csv"
  }

  depends_on = [aws_lambda_permission.allow_s3_invoke_glue_trigger]
}

# Lambda function to trigger Glue job from S3 event
resource "aws_lambda_function" "glue_trigger" {
  filename         = "${path.module}/../../lambda/glue_trigger/deployment.zip"
  function_name    = "dqad-glue-trigger-${var.environment}"
  role             = aws_iam_role.glue_trigger_lambda_role.arn
  handler          = "app.lambda_handler"
  source_code_hash = filebase64sha256("${path.module}/../../lambda/glue_trigger/deployment.zip")
  runtime          = "python3.11"
  timeout          = 60
  memory_size      = 128

  environment {
    variables = {
      GLUE_JOB_NAME        = aws_glue_job.dqad_etl.name
      S3_RAW_BUCKET        = aws_s3_bucket.dqad_raw.id
      S3_PROCESSED_BUCKET  = aws_s3_bucket.dqad_processed.id
      CLOUDWATCH_NAMESPACE = "DQAD/DataQuality"
    }
  }

  tags = {
    Name        = "dqad-glue-trigger-${var.environment}"
    Environment = var.environment
  }
}

# IAM role for Glue trigger Lambda
resource "aws_iam_role" "glue_trigger_lambda_role" {
  name = "dqad-glue-trigger-lambda-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

# Lambda execution policy
resource "aws_iam_role_policy_attachment" "glue_trigger_lambda_basic" {
  role       = aws_iam_role.glue_trigger_lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# Custom policy for starting Glue jobs
resource "aws_iam_role_policy" "glue_trigger_lambda_glue_policy" {
  name = "dqad-glue-trigger-start-job-${var.environment}"
  role = aws_iam_role.glue_trigger_lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:StartJobRun",
          "glue:GetJobRun",
          "glue:GetJob"
        ]
        Resource = aws_glue_job.dqad_etl.arn
      }
    ]
  })
}

# Permission for S3 to invoke Lambda
resource "aws_lambda_permission" "allow_s3_invoke_glue_trigger" {
  statement_id  = "AllowS3InvokeGlueTrigger"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.glue_trigger.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.dqad_raw.arn
}

# CloudWatch Log Group for Glue job
resource "aws_cloudwatch_log_group" "glue_job_logs" {
  name              = "/aws-glue/jobs/${aws_glue_job.dqad_etl.name}"
  retention_in_days = 7  # Free tier: 5GB storage, this keeps logs for 1 week
}

# Outputs
output "glue_job_name" {
  description = "Name of the Glue ETL job"
  value       = aws_glue_job.dqad_etl.name
}

output "glue_job_arn" {
  description = "ARN of the Glue ETL job"
  value       = aws_glue_job.dqad_etl.arn
}

output "glue_trigger_lambda_arn" {
  description = "ARN of the Lambda function that triggers Glue jobs"
  value       = aws_lambda_function.glue_trigger.arn
}

output "glue_database_name" {
  description = "Name of the Glue catalog database"
  value       = aws_glue_catalog_database.dqad_database.name
}

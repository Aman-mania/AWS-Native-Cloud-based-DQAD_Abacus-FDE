terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# S3 Buckets for DQAD pipeline
resource "aws_s3_bucket" "dqad_raw" {
  bucket = "${var.project_name}-raw-${var.environment}"
  
  tags = {
    Name        = "DQAD Raw Claims Data"
    Environment = var.environment
    Project     = var.project_name
  }
}

resource "aws_s3_bucket" "dqad_processed" {
  bucket = "${var.project_name}-processed-${var.environment}"
  
  tags = {
    Name        = "DQAD Processed Delta Tables"
    Environment = var.environment
    Project     = var.project_name
  }
}

resource "aws_s3_bucket" "dqad_logs" {
  bucket = "${var.project_name}-logs-${var.environment}"
  
  tags = {
    Name        = "DQAD Application Logs"
    Environment = var.environment
    Project     = var.project_name
  }
}

# Enable versioning for processed data
resource "aws_s3_bucket_versioning" "processed_versioning" {
  bucket = aws_s3_bucket.dqad_processed.id
  
  versioning_configuration {
    status = "Enabled"
  }
}

# Server-side encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "dqad_encryption" {
  for_each = {
    raw       = aws_s3_bucket.dqad_raw.id
    processed = aws_s3_bucket.dqad_processed.id
    logs      = aws_s3_bucket.dqad_logs.id
  }
  
  bucket = each.value
  
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Block public access
resource "aws_s3_bucket_public_access_block" "dqad_buckets" {
  for_each = {
    raw       = aws_s3_bucket.dqad_raw.id
    processed = aws_s3_bucket.dqad_processed.id
    logs      = aws_s3_bucket.dqad_logs.id
  }
  
  bucket = each.value
  
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# CloudWatch Log Groups
resource "aws_cloudwatch_log_group" "cost_collector_logs" {
  name              = "/aws/lambda/${var.project_name}-cost-collector"
  retention_in_days = 7
  
  tags = {
    Name    = "Cost Collector Lambda Logs"
    Project = var.project_name
  }
}

resource "aws_cloudwatch_log_group" "orchestrator_logs" {
  name              = "/aws/lambda/${var.project_name}-orchestrator"
  retention_in_days = 7
  
  tags = {
    Name    = "Orchestrator Lambda Logs"
    Project = var.project_name
  }
}

# SNS Topic for Alerts
resource "aws_sns_topic" "dqad_alerts" {
  name = "${var.project_name}-alerts-${var.environment}"
  
  tags = {
    Name    = "DQAD Alerts Topic"
    Project = var.project_name
  }
}

resource "aws_sns_topic_subscription" "email_alerts" {
  topic_arn = aws_sns_topic.dqad_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# EventBridge Rule for Cost Spike Detection
resource "aws_cloudwatch_event_rule" "cost_spike_rule" {
  name        = "${var.project_name}-cost-spike"
  description = "Trigger when cost spike is detected"
  
  event_pattern = jsonencode({
    source      = ["custom.dqad"]
    detail-type = ["Cost Spike Detected"]
  })
  
  tags = {
    Name    = "Cost Spike Detection Rule"
    Project = var.project_name
  }
}

# EventBridge Rule for Anomaly Detection
resource "aws_cloudwatch_event_rule" "anomaly_spike_rule" {
  name        = "${var.project_name}-anomaly-spike"
  description = "Trigger when anomaly count spikes"
  
  event_pattern = jsonencode({
    source      = ["custom.dqad"]
    detail-type = ["Anomaly Spike Detected"]
  })
  
  tags = {
    Name    = "Anomaly Spike Detection Rule"
    Project = var.project_name
  }
}

# Lambda function for Cost Collection
resource "aws_lambda_function" "cost_collector" {
  filename         = "${path.module}/../../lambda/cost_collector/deployment.zip"
  function_name    = "${var.project_name}-cost-collector"
  role            = aws_iam_role.lambda_execution_role.arn
  handler         = "app.lambda_handler"
  source_code_hash = fileexists("${path.module}/../../lambda/cost_collector/deployment.zip") ? filebase64sha256("${path.module}/../../lambda/cost_collector/deployment.zip") : null
  runtime         = "python3.11"
  timeout         = 60
  memory_size     = 128  # Optimized for free tier
  
  environment {
    variables = {
      CLOUDWATCH_NAMESPACE = "DQAD/Cost"
      PROJECT_NAME        = var.project_name
      ENVIRONMENT         = var.environment
      COST_THRESHOLD_USD  = var.cost_threshold_usd
      SNS_TOPIC_ARN       = aws_sns_topic.dqad_alerts.arn
    }
  }
  
  tags = {
    Name    = "Cost Collector Lambda"
    Project = var.project_name
  }
}

# CloudWatch Event Rule to trigger cost collector every 6 hours (free-tier optimized)
resource "aws_cloudwatch_event_rule" "cost_collector_schedule" {
  name                = "${var.project_name}-cost-collector-schedule"
  description         = "Trigger cost collector Lambda every 6 hours"
  schedule_expression = "rate(6 hours)"
  
  tags = {
    Name    = "Cost Collector Schedule"
    Project = var.project_name
  }
}

resource "aws_cloudwatch_event_target" "cost_collector_target" {
  rule      = aws_cloudwatch_event_rule.cost_collector_schedule.name
  target_id = "CostCollectorLambda"
  arn       = aws_lambda_function.cost_collector.arn
}

resource "aws_lambda_permission" "allow_eventbridge_cost_collector" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.cost_collector.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.cost_collector_schedule.arn
}

# CloudWatch Alarm for Cost Metrics (moved to cloudwatch_alarms.tf for organization)
resource "aws_cloudwatch_metric_alarm" "daily_cost_alarm" {
  alarm_name          = "${var.project_name}-daily-cost-spike"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "DailyCost"
  namespace           = "DQAD/Cost"
  period              = 3600
  statistic           = "Maximum"
  threshold           = var.cost_threshold_usd
  alarm_description   = "Triggers when daily cost exceeds threshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.dqad_alerts.arn]
  
  tags = {
    Name    = "Daily Cost Alarm"
    Project = var.project_name
  }
}

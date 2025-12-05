# Lambda function for Orchestration (Self-Heal Actions)
resource "aws_lambda_function" "orchestrator" {
  filename         = "${path.module}/../../lambda/orchestrator/deployment.zip"
  function_name    = "${var.project_name}-orchestrator"
  role            = aws_iam_role.lambda_execution_role.arn
  handler         = "app.lambda_handler"
  source_code_hash = fileexists("${path.module}/../../lambda/orchestrator/deployment.zip") ? filebase64sha256("${path.module}/../../lambda/orchestrator/deployment.zip") : null
  runtime         = "python3.11"
  timeout         = 300
  memory_size     = 256  # Optimized for free tier
  
  environment {
    variables = {
      PROJECT_NAME           = var.project_name
      ENVIRONMENT            = var.environment
      S3_RAW_BUCKET         = aws_s3_bucket.dqad_raw.id
      S3_PROCESSED_BUCKET   = aws_s3_bucket.dqad_processed.id
      S3_LOGS_BUCKET        = aws_s3_bucket.dqad_logs.id
      SNS_TOPIC_ARN         = aws_sns_topic.dqad_alerts.arn
      GLUE_JOB_NAME         = aws_glue_job.dqad_etl.name
    }
  }
  
  tags = {
    Name    = "Orchestrator Lambda"
    Project = var.project_name
  }
}

resource "aws_lambda_permission" "allow_eventbridge_orchestrator" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.orchestrator.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.cost_spike_rule.arn
}

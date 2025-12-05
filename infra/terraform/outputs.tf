output "s3_raw_bucket" {
  description = "S3 bucket for raw claims data"
  value       = aws_s3_bucket.dqad_raw.id
}

output "s3_processed_bucket" {
  description = "S3 bucket for processed Delta tables"
  value       = aws_s3_bucket.dqad_processed.id
}

output "s3_logs_bucket" {
  description = "S3 bucket for application logs"
  value       = aws_s3_bucket.dqad_logs.id
}

output "lambda_cost_collector_arn" {
  description = "ARN of cost collector Lambda function"
  value       = aws_lambda_function.cost_collector.arn
}

output "lambda_orchestrator_arn" {
  description = "ARN of orchestrator Lambda function"
  value       = aws_lambda_function.orchestrator.arn
}

output "sns_topic_arn" {
  description = "ARN of SNS alerts topic"
  value       = aws_sns_topic.dqad_alerts.arn
}

output "lambda_execution_role_arn" {
  description = "ARN of Lambda execution role"
  value       = aws_iam_role.lambda_execution_role.arn
}

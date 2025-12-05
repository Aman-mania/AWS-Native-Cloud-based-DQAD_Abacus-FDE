variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name used for resource naming"
  type        = string
  default     = "dqad"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "alert_email" {
  description = "Email address for SNS alerts"
  type        = string
}

variable "cost_threshold_usd" {
  description = "Daily cost threshold in USD for alerts (free-tier safe: use 2.0)"
  type        = number
  default     = 2.0
}

variable "anomaly_threshold" {
  description = "Number of anomalies to trigger alert"
  type        = number
  default     = 100
}

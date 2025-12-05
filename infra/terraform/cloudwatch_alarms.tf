# CloudWatch Alarms for Anomaly Detection and Cost Monitoring

# Anomaly Count Alarm - Triggers when too many data quality issues detected
resource "aws_cloudwatch_metric_alarm" "anomaly_count_alarm" {
  alarm_name          = "${var.project_name}-anomaly-spike"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "AnomalyCount"
  namespace           = "DQAD/Quality"
  period              = 3600
  statistic           = "Sum"
  threshold           = var.anomaly_threshold
  alarm_description   = "Triggers when anomaly count exceeds threshold"
  treat_missing_data  = "notBreaching"
  
  alarm_actions = [aws_sns_topic.dqad_alerts.arn]
  
  tags = {
    Name    = "Anomaly Count Alarm"
    Project = var.project_name
  }
}

# Data Quality Score Alarm - Triggers when DQ score drops too low
resource "aws_cloudwatch_metric_alarm" "dq_score_alarm" {
  alarm_name          = "${var.project_name}-dq-score-low"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = 2
  metric_name         = "DataQualityScore"
  namespace           = "DQAD/Quality"
  period              = 3600
  statistic           = "Average"
  threshold           = 85.0  # DQ score below 85% triggers alarm
  alarm_description   = "Triggers when data quality score drops below 85%"
  treat_missing_data  = "notBreaching"
  
  alarm_actions = [aws_sns_topic.dqad_alerts.arn]
  
  tags = {
    Name    = "DQ Score Alarm"
    Project = var.project_name
  }
}

# EventBridge rule to trigger orchestrator on anomaly alarm
resource "aws_cloudwatch_event_rule" "anomaly_alarm_rule" {
  name        = "${var.project_name}-anomaly-alarm-trigger"
  description = "Trigger orchestrator when anomaly alarm changes state"
  
  event_pattern = jsonencode({
    source      = ["aws.cloudwatch"]
    detail-type = ["CloudWatch Alarm State Change"]
    detail = {
      alarmName = [aws_cloudwatch_metric_alarm.anomaly_count_alarm.alarm_name]
      state = {
        value = ["ALARM"]
      }
    }
  })
  
  tags = {
    Name    = "Anomaly Alarm Trigger"
    Project = var.project_name
  }
}

resource "aws_cloudwatch_event_target" "anomaly_alarm_target" {
  rule      = aws_cloudwatch_event_rule.anomaly_alarm_rule.name
  target_id = "OrchestratorLambda"
  arn       = aws_lambda_function.orchestrator.arn
  
  input_transformer {
    input_paths = {
      alarmName = "$.detail.alarmName"
      newState  = "$.detail.state.value"
      reason    = "$.detail.state.reason"
      timestamp = "$.time"
    }
    
    input_template = <<EOF
{
  "event_type": "anomaly_spike",
  "alarm_name": <alarmName>,
  "state": <newState>,
  "reason": <reason>,
  "timestamp": <timestamp>,
  "action": "handle_anomaly_spike"
}
EOF
  }
}

resource "aws_lambda_permission" "allow_eventbridge_anomaly_alarm" {
  statement_id  = "AllowExecutionFromAnomalyAlarm"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.orchestrator.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.anomaly_alarm_rule.arn
}

# EventBridge rule to trigger orchestrator on cost alarm
resource "aws_cloudwatch_event_rule" "cost_alarm_rule" {
  name        = "${var.project_name}-cost-alarm-trigger"
  description = "Trigger orchestrator when cost alarm changes state"
  
  event_pattern = jsonencode({
    source      = ["aws.cloudwatch"]
    detail-type = ["CloudWatch Alarm State Change"]
    detail = {
      alarmName = [aws_cloudwatch_metric_alarm.daily_cost_alarm.alarm_name]
      state = {
        value = ["ALARM"]
      }
    }
  })
  
  tags = {
    Name    = "Cost Alarm Trigger"
    Project = var.project_name
  }
}

resource "aws_cloudwatch_event_target" "cost_alarm_target" {
  rule      = aws_cloudwatch_event_rule.cost_alarm_rule.name
  target_id = "OrchestratorLambda"
  arn       = aws_lambda_function.orchestrator.arn
  
  input_transformer {
    input_paths = {
      alarmName = "$.detail.alarmName"
      newState  = "$.detail.state.value"
      reason    = "$.detail.state.reason"
      timestamp = "$.time"
    }
    
    input_template = <<EOF
{
  "event_type": "cost_spike",
  "alarm_name": <alarmName>,
  "state": <newState>,
  "reason": <reason>,
  "timestamp": <timestamp>,
  "action": "handle_cost_spike"
}
EOF
  }
}

resource "aws_lambda_permission" "allow_eventbridge_cost_alarm" {
  statement_id  = "AllowExecutionFromCostAlarm"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.orchestrator.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.cost_alarm_rule.arn
}

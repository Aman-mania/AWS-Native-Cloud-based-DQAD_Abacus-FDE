# API Gateway for Manual Self-Healing Triggers from Dashboard

# API Gateway REST API
resource "aws_api_gateway_rest_api" "dqad_api" {
  name        = "${var.project_name}-api"
  description = "DQAD API for manual self-healing triggers"
  
  endpoint_configuration {
    types = ["REGIONAL"]
  }
  
  tags = {
    Name    = "DQAD API"
    Project = var.project_name
  }
}

# /trigger resource
resource "aws_api_gateway_resource" "trigger" {
  rest_api_id = aws_api_gateway_rest_api.dqad_api.id
  parent_id   = aws_api_gateway_rest_api.dqad_api.root_resource_id
  path_part   = "trigger"
}

# POST /trigger method
resource "aws_api_gateway_method" "trigger_post" {
  rest_api_id   = aws_api_gateway_rest_api.dqad_api.id
  resource_id   = aws_api_gateway_resource.trigger.id
  http_method   = "POST"
  authorization = "NONE"  # For demo - in production use IAM or API Key
}

# Enable CORS for POST
resource "aws_api_gateway_method_response" "trigger_post_200" {
  rest_api_id = aws_api_gateway_rest_api.dqad_api.id
  resource_id = aws_api_gateway_resource.trigger.id
  http_method = aws_api_gateway_method.trigger_post.http_method
  status_code = "200"
  
  response_parameters = {
    "method.response.header.Access-Control-Allow-Origin" = true
  }
}

# OPTIONS method for CORS preflight
resource "aws_api_gateway_method" "trigger_options" {
  rest_api_id   = aws_api_gateway_rest_api.dqad_api.id
  resource_id   = aws_api_gateway_resource.trigger.id
  http_method   = "OPTIONS"
  authorization = "NONE"
}

resource "aws_api_gateway_integration" "trigger_options" {
  rest_api_id = aws_api_gateway_rest_api.dqad_api.id
  resource_id = aws_api_gateway_resource.trigger.id
  http_method = aws_api_gateway_method.trigger_options.http_method
  type        = "MOCK"
  
  request_templates = {
    "application/json" = "{\"statusCode\": 200}"
  }
}

resource "aws_api_gateway_method_response" "trigger_options_200" {
  rest_api_id = aws_api_gateway_rest_api.dqad_api.id
  resource_id = aws_api_gateway_resource.trigger.id
  http_method = aws_api_gateway_method.trigger_options.http_method
  status_code = "200"
  
  response_parameters = {
    "method.response.header.Access-Control-Allow-Headers" = true
    "method.response.header.Access-Control-Allow-Methods" = true
    "method.response.header.Access-Control-Allow-Origin"  = true
  }
}

resource "aws_api_gateway_integration_response" "trigger_options" {
  rest_api_id = aws_api_gateway_rest_api.dqad_api.id
  resource_id = aws_api_gateway_resource.trigger.id
  http_method = aws_api_gateway_method.trigger_options.http_method
  status_code = aws_api_gateway_method_response.trigger_options_200.status_code
  
  response_parameters = {
    "method.response.header.Access-Control-Allow-Headers" = "'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token'"
    "method.response.header.Access-Control-Allow-Methods" = "'POST,OPTIONS'"
    "method.response.header.Access-Control-Allow-Origin"  = "'*'"
  }
}

# Lambda integration for POST
resource "aws_api_gateway_integration" "trigger_lambda" {
  rest_api_id             = aws_api_gateway_rest_api.dqad_api.id
  resource_id             = aws_api_gateway_resource.trigger.id
  http_method             = aws_api_gateway_method.trigger_post.http_method
  integration_http_method = "POST"
  type                    = "AWS_PROXY"
  uri                     = aws_lambda_function.orchestrator.invoke_arn
}

resource "aws_api_gateway_integration_response" "trigger_lambda" {
  rest_api_id = aws_api_gateway_rest_api.dqad_api.id
  resource_id = aws_api_gateway_resource.trigger.id
  http_method = aws_api_gateway_method.trigger_post.http_method
  status_code = aws_api_gateway_method_response.trigger_post_200.status_code
  
  response_parameters = {
    "method.response.header.Access-Control-Allow-Origin" = "'*'"
  }
  
  depends_on = [aws_api_gateway_integration.trigger_lambda]
}

# Deployment
resource "aws_api_gateway_deployment" "dqad_api_deployment" {
  rest_api_id = aws_api_gateway_rest_api.dqad_api.id
  
  depends_on = [
    aws_api_gateway_integration.trigger_lambda,
    aws_api_gateway_integration.trigger_options
  ]
  
  lifecycle {
    create_before_destroy = true
  }
}

# Stage
resource "aws_api_gateway_stage" "dqad_api_stage" {
  deployment_id = aws_api_gateway_deployment.dqad_api_deployment.id
  rest_api_id   = aws_api_gateway_rest_api.dqad_api.id
  stage_name    = var.environment
  
  tags = {
    Name    = "DQAD API Stage"
    Project = var.project_name
  }
}

# Lambda permission for API Gateway
resource "aws_lambda_permission" "allow_api_gateway" {
  statement_id  = "AllowExecutionFromAPIGateway"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.orchestrator.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_api_gateway_rest_api.dqad_api.execution_arn}/*/*"
}

# Output API endpoint
output "api_gateway_url" {
  description = "API Gateway endpoint URL for manual triggers"
  value       = "${aws_api_gateway_stage.dqad_api_stage.invoke_url}/trigger"
}

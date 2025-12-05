"""
AWS Cost Explorer Lambda - Collects cost metrics and pushes to CloudWatch
"""

import json
import os
from datetime import datetime, timedelta
from typing import Dict, List
import boto3
from botocore.exceptions import ClientError

# Initialize AWS clients
ce_client = boto3.client('ce')  # Cost Explorer
cw_client = boto3.client('cloudwatch')
events_client = boto3.client('events')
sns_client = boto3.client('sns')

# Configuration from environment variables
CLOUDWATCH_NAMESPACE = os.getenv('CLOUDWATCH_NAMESPACE', 'DQAD/Cost')
PROJECT_NAME = os.getenv('PROJECT_NAME', 'dqad')
ENVIRONMENT = os.getenv('ENVIRONMENT', 'dev')
COST_THRESHOLD_USD = float(os.getenv('COST_THRESHOLD_USD', '50.0'))
SNS_TOPIC_ARN = os.getenv('SNS_TOPIC_ARN', '')


def get_cost_and_usage(start_date: str, end_date: str) -> Dict:
    """
    Get cost and usage data from AWS Cost Explorer
    """
    try:
        response = ce_client.get_cost_and_usage(
            TimePeriod={
                'Start': start_date,
                'End': end_date
            },
            Granularity='DAILY',
            Metrics=['UnblendedCost', 'UsageQuantity'],
            GroupBy=[
                {
                    'Type': 'DIMENSION',
                    'Key': 'SERVICE'
                }
            ],
            Filter={
                'Tags': {
                    'Key': 'Project',
                    'Values': [PROJECT_NAME]
                }
            }
        )
        return response
    except ClientError as e:
        print(f"Error fetching cost data: {e}")
        # Fallback: get overall account costs
        response = ce_client.get_cost_and_usage(
            TimePeriod={
                'Start': start_date,
                'End': end_date
            },
            Granularity='DAILY',
            Metrics=['UnblendedCost']
        )
        return response


def get_cost_forecast(start_date: str, end_date: str) -> Dict:
    """
    Get cost forecast from AWS Cost Explorer
    """
    try:
        response = ce_client.get_cost_forecast(
            TimePeriod={
                'Start': start_date,
                'End': end_date
            },
            Metric='UNBLENDED_COST',
            Granularity='MONTHLY',
            Filter={
                'Tags': {
                    'Key': 'Project',
                    'Values': [PROJECT_NAME]
                }
            }
        )
        return response
    except ClientError as e:
        print(f"Error fetching forecast: {e}")
        return None


def parse_cost_data(cost_response: Dict) -> Dict:
    """
    Parse Cost Explorer response and calculate metrics
    """
    if 'ResultsByTime' not in cost_response or not cost_response['ResultsByTime']:
        return {
            'total_cost': 0.0,
            'service_costs': {},
            'date': datetime.now().strftime('%Y-%m-%d')
        }
    
    results = cost_response['ResultsByTime'][0]
    total_cost = 0.0
    service_costs = {}
    
    # Calculate total cost
    if 'Total' in results:
        total_cost = float(results['Total'].get('UnblendedCost', {}).get('Amount', 0.0))
    
    # Parse service-level costs
    if 'Groups' in results:
        for group in results['Groups']:
            service = group['Keys'][0]
            amount = float(group['Metrics']['UnblendedCost']['Amount'])
            service_costs[service] = amount
    
    return {
        'total_cost': total_cost,
        'service_costs': service_costs,
        'date': results['TimePeriod']['Start']
    }


def push_cost_metrics_to_cloudwatch(cost_data: Dict):
    """
    Push cost metrics to CloudWatch
    """
    timestamp = datetime.now()
    
    metric_data = [
        {
            'MetricName': 'DailyCost',
            'Value': cost_data['total_cost'],
            'Unit': 'None',
            'Timestamp': timestamp,
            'Dimensions': [
                {'Name': 'Project', 'Value': PROJECT_NAME},
                {'Name': 'Environment', 'Value': ENVIRONMENT}
            ]
        }
    ]
    
    # Add service-specific metrics
    for service, cost in cost_data['service_costs'].items():
        metric_data.append({
            'MetricName': 'ServiceCost',
            'Value': cost,
            'Unit': 'None',
            'Timestamp': timestamp,
            'Dimensions': [
                {'Name': 'Project', 'Value': PROJECT_NAME},
                {'Name': 'Service', 'Value': service}
            ]
        })
    
    try:
        cw_client.put_metric_data(
            Namespace=CLOUDWATCH_NAMESPACE,
            MetricData=metric_data
        )
        print(f"✓ Pushed {len(metric_data)} metrics to CloudWatch")
    except ClientError as e:
        print(f"Error pushing metrics: {e}")


def check_cost_threshold(cost_data: Dict) -> bool:
    """
    Check if cost exceeds threshold and trigger alert
    """
    if cost_data['total_cost'] > COST_THRESHOLD_USD:
        print(f"⚠ Cost threshold exceeded: ${cost_data['total_cost']:.2f} > ${COST_THRESHOLD_USD:.2f}")
        
        # Trigger EventBridge event
        try:
            event_detail = {
                'current_cost': cost_data['total_cost'],
                'threshold': COST_THRESHOLD_USD,
                'service_breakdown': cost_data['service_costs'],
                'date': cost_data['date'],
                'timestamp': datetime.now().isoformat()
            }
            
            events_client.put_events(
                Entries=[
                    {
                        'Source': 'custom.dqad',
                        'DetailType': 'Cost Spike Detected',
                        'Detail': json.dumps(event_detail)
                    }
                ]
            )
            print("✓ Cost spike event triggered")
            
            # Send SNS notification
            if SNS_TOPIC_ARN:
                sns_client.publish(
                    TopicArn=SNS_TOPIC_ARN,
                    Subject=f'DQAD Alert: Cost Threshold Exceeded',
                    Message=json.dumps(event_detail, indent=2)
                )
                print("✓ SNS notification sent")
            
        except ClientError as e:
            print(f"Error triggering events: {e}")
        
        return True
    
    return False


def lambda_handler(event, context):
    """
    Main Lambda handler - collects costs and pushes to CloudWatch
    """
    print(f"Cost Collector Lambda triggered at {datetime.now()}")
    print(f"Event: {json.dumps(event, default=str)}")
    
    # Handle different invocation types
    action = event.get('action', 'collect_costs')
    
    if action == 'check_cost_status':
        # Quick cost check for Step Functions
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)
        
        cost_response = get_cost_and_usage(
            start_date=yesterday.strftime('%Y-%m-%d'),
            end_date=today.strftime('%Y-%m-%d')
        )
        cost_data = parse_cost_data(cost_response)
        
        status = 'high' if cost_data['total_cost'] > COST_THRESHOLD_USD else 'normal'
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'status': status,
                'current_cost': cost_data['total_cost'],
                'threshold': COST_THRESHOLD_USD
            })
        }
    
    # Default: Collect and report costs
    try:
        # Get yesterday's costs
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)
        
        # Get cost and usage
        print(f"Fetching costs from {yesterday} to {today}")
        cost_response = get_cost_and_usage(
            start_date=yesterday.strftime('%Y-%m-%d'),
            end_date=today.strftime('%Y-%m-%d')
        )
        
        # Parse cost data
        cost_data = parse_cost_data(cost_response)
        print(f"Total daily cost: ${cost_data['total_cost']:.2f}")
        
        # Get forecast (optional)
        try:
            forecast_start = today.strftime('%Y-%m-%d')
            forecast_end = (today + timedelta(days=30)).strftime('%Y-%m-%d')
            forecast_response = get_cost_forecast(forecast_start, forecast_end)
            
            if forecast_response and 'Total' in forecast_response:
                forecast_amount = float(forecast_response['Total']['Amount'])
                print(f"30-day forecast: ${forecast_amount:.2f}")
        except Exception as e:
            print(f"Forecast unavailable: {e}")
        
        # Push metrics to CloudWatch
        push_cost_metrics_to_cloudwatch(cost_data)
        
        # Check threshold
        threshold_exceeded = check_cost_threshold(cost_data)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Cost collection completed',
                'total_cost': cost_data['total_cost'],
                'threshold_exceeded': threshold_exceeded,
                'service_costs': cost_data['service_costs']
            })
        }
        
    except Exception as e:
        print(f"Error in lambda_handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }


# For local testing
if __name__ == "__main__":
    test_event = {'action': 'collect_costs'}
    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=2))

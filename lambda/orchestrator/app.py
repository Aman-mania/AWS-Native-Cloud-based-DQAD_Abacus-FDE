"""
Orchestrator Lambda - Handles self-healing actions with structured logging
Actions: restart Glue job, quarantine data, send alerts
Supports both CloudWatch alarm triggers and manual API Gateway triggers
"""

import json
import os
import logging
from datetime import datetime
from typing import Dict, Optional
import boto3
from botocore.exceptions import ClientError

# Configure structured JSON logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class JSONFormatter(logging.Formatter):
    """Custom JSON formatter for structured logging"""
    def format(self, record):
        log_obj = {
            'timestamp': datetime.utcnow().isoformat(),
            'level': record.levelname,
            'message': record.getMessage(),
            'function': record.funcName,
            'line': record.lineno
        }
        if hasattr(record, 'event_data'):
            log_obj['event_data'] = record.event_data
        return json.dumps(log_obj)

# Set JSON formatter
handler = logging.StreamHandler()
handler.setFormatter(JSONFormatter())
logger.handlers = [handler]

# Initialize AWS clients
s3_client = boto3.client('s3')
sns_client = boto3.client('sns')
glue_client = boto3.client('glue')

# Configuration
PROJECT_NAME = os.getenv('PROJECT_NAME', 'dqad')
ENVIRONMENT = os.getenv('ENVIRONMENT', 'dev')
S3_RAW_BUCKET = os.getenv('S3_RAW_BUCKET', '')
S3_PROCESSED_BUCKET = os.getenv('S3_PROCESSED_BUCKET', '')
S3_LOGS_BUCKET = os.getenv('S3_LOGS_BUCKET', '')
SNS_TOPIC_ARN = os.getenv('SNS_TOPIC_ARN', '')
GLUE_JOB_NAME = os.getenv('GLUE_JOB_NAME', '')


def quarantine_data(bucket: str, prefix: str = "claims/") -> Dict:
    """
    Move suspicious data files to quarantine location (logs bucket)
    Tags files instead of deleting to preserve audit trail
    """
    if not S3_LOGS_BUCKET:
        logger.warning("No logs bucket configured for quarantine")
        return {'status': 'skipped', 'reason': 'no_logs_bucket'}
    
    quarantined_files = []
    
    try:
        logger.info(f"Quarantining data from s3://{bucket}/{prefix}")
        
        # List files in source bucket
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        
        if 'Contents' not in response:
            logger.info("No files found to quarantine")
            return {'status': 'success', 'quarantined_count': 0, 'files': []}
        
        # Move files to quarantine location
        for obj in response['Contents']:
            source_key = obj['Key']
            
            # Skip directories
            if source_key.endswith('/'):
                continue
            
            # Create quarantine key with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            quarantine_key = f"quarantine/{timestamp}/{source_key}"
            
            # Copy to logs bucket
            copy_source = {'Bucket': bucket, 'Key': source_key}
            s3_client.copy_object(
                CopySource=copy_source,
                Bucket=S3_LOGS_BUCKET,
                Key=quarantine_key
            )
            
            # Tag original file as quarantined (don't delete yet)
            s3_client.put_object_tagging(
                Bucket=bucket,
                Key=source_key,
                Tagging={'TagSet': [{'Key': 'Status', 'Value': 'Quarantined'}]}
            )
            
            quarantined_files.append(source_key)
            logger.info(f"Quarantined: {source_key} -> s3://{S3_LOGS_BUCKET}/{quarantine_key}")
        
        logger.info(f"Quarantined {len(quarantined_files)} files", 
                   extra={'event_data': {'count': len(quarantined_files)}})
        
        return {
            'status': 'success',
            'quarantined_count': len(quarantined_files),
            'files': quarantined_files
        }
        
    except ClientError as e:
        logger.error(f"Error quarantining data: {str(e)}", extra={'event_data': {'error': str(e)}})
        return {
            'status': 'error',
            'error': str(e),
            'quarantined_count': len(quarantined_files)
        }


def restart_glue_job(job_name: str) -> Dict:
    """
    Restart AWS Glue job with graceful fallback
    """
    if not job_name:
        logger.warning("No Glue job name configured")
        return {'status': 'skipped', 'reason': 'no_job_name'}
    
    try:
        glue_client = boto3.client('glue')
        
        logger.info(f"Starting Glue job: {job_name}")
        
        # Start job run with default parameters
        response = glue_client.start_job_run(
            JobName=job_name,
            Arguments={
                '--S3_RAW_BUCKET': S3_RAW_BUCKET,
                '--S3_PROCESSED_BUCKET': S3_PROCESSED_BUCKET,
                '--S3_INPUT_KEY': 'claims/',  # Reprocess all claims
                '--CLOUDWATCH_NAMESPACE': 'DQAD/DataQuality'
            }
        )
        
        job_run_id = response['JobRunId']
        logger.info(f"Glue job started successfully", 
                   extra={'event_data': {'job_name': job_name, 'job_run_id': job_run_id}})
        
        return {
            'status': 'success',
            'job_name': job_name,
            'job_run_id': job_run_id,
            'action': 'glue_job_started'
        }
        
    except Exception as e:
        logger.error(f"Error starting Glue job: {str(e)}", extra={'event_data': {'error': str(e)}})
        return {
            'status': 'error',
            'error': str(e)
        }


def restart_glue_job(job_name: str) -> Dict:
    """
    Restart AWS Glue ETL job
    """
    if not job_name:
        logger.warning("No Glue job name configured")
        return {'status': 'skipped', 'reason': 'no_job_name'}
    
    try:
        logger.info(f"Starting Glue job: {job_name}")
        
        # Start job run with default parameters
        response = glue_client.start_job_run(
            JobName=job_name,
            Arguments={
                '--S3_RAW_BUCKET': S3_RAW_BUCKET,
                '--S3_PROCESSED_BUCKET': S3_PROCESSED_BUCKET,
                '--S3_INPUT_KEY': 'claims/',  # Reprocess all claims
                '--CLOUDWATCH_NAMESPACE': 'DQAD/DataQuality'
            }
        )
        
        job_run_id = response['JobRunId']
        logger.info(f"Glue job started successfully", 
                   extra={'event_data': {'job_name': job_name, 'job_run_id': job_run_id}})
        
        return {
            'status': 'success',
            'job_name': job_name,
            'job_run_id': job_run_id,
            'action': 'glue_job_started'
        }
        
    except Exception as e:
        logger.error(f"Error starting Glue job: {str(e)}", extra={'event_data': {'error': str(e)}})
        return {
            'status': 'error',
            'error': str(e)
        }
        
    except Exception as e:
        logger.error(f"Error restarting job: {str(e)}", extra={'event_data': {'error': str(e)}})
        return {
            'status': 'error',
            'error': str(e)
        }


def send_notification(subject: str, message: Dict):
    """Send SNS notification with structured data"""
    if not SNS_TOPIC_ARN:
        logger.warning("No SNS topic configured")
        return
    
    try:
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject,
            Message=json.dumps(message, indent=2, default=str)
        )
        logger.info(f"Notification sent: {subject}")
    except ClientError as e:
        logger.error(f"Error sending notification: {str(e)}")


def log_remediation_action(action: str, result: Dict):
    """Log remediation action to S3 for audit trail"""
    if not S3_LOGS_BUCKET:
        return
    
    try:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_key = f"remediation/{timestamp}_{action}.json"
        
        log_data = {
            'timestamp': datetime.now().isoformat(),
            'action': action,
            'result': result,
            'environment': ENVIRONMENT
        }
        
        s3_client.put_object(
            Bucket=S3_LOGS_BUCKET,
            Key=log_key,
            Body=json.dumps(log_data, indent=2, default=str),
            ContentType='application/json'
        )
        
        logger.info(f"Remediation logged to s3://{S3_LOGS_BUCKET}/{log_key}")
    except ClientError as e:
        logger.error(f"Error logging remediation: {str(e)}")


def lambda_handler(event, context):
    """
    Main Lambda handler - orchestrates self-healing actions
    Supports both CloudWatch alarm triggers and manual API Gateway triggers
    """
    logger.info("Orchestrator Lambda triggered", 
               extra={'event_data': {'event_type': event.get('event_type', event.get('action', 'unknown'))}})
    
    # Handle API Gateway invocation
    if 'httpMethod' in event:
        try:
            body = json.loads(event.get('body', '{}'))
            action = body.get('action', '')
        except json.JSONDecodeError:
            return {
                'statusCode': 400,
                'headers': {'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({'error': 'Invalid JSON in request body'})
            }
    else:
        # Handle EventBridge/CloudWatch alarm trigger
        action = event.get('action', event.get('event_type', ''))
    
    results = {
        'timestamp': datetime.now().isoformat(),
        'action': action,
        'success': False
    }
    
    try:
        # Handle different action types
        if action in ['quarantine_data', 'handle_anomaly_spike']:
            logger.info("Action: Quarantine Data")
            quarantine_result = quarantine_data(S3_RAW_BUCKET)
            results['quarantine_result'] = quarantine_result
            results['success'] = quarantine_result.get('status') == 'success'
            
            log_remediation_action('quarantine_data', quarantine_result)
            
            send_notification(
                'DQAD: Data Quarantined',
                {
                    'action': 'quarantine_data',
                    'quarantined_count': quarantine_result.get('quarantined_count', 0),
                    'result': quarantine_result
                }
            )
        
        elif action in ['restart_glue_job', 'restart_etl']:
            logger.info("Action: Restart Glue ETL Job")
            
            restart_result = restart_glue_job(GLUE_JOB_NAME)
            
            results['restart_result'] = restart_result
            results['success'] = restart_result.get('status') == 'success'
            
            log_remediation_action('restart_job', restart_result)
            
            send_notification(
                'DQAD: ETL Job Restarted',
                {
                    'action': 'restart_job',
                    'result': restart_result
                }
            )
        
        elif action in ['handle_cost_spike', 'cost_spike']:
            logger.info("Action: Handle Cost Spike")
            
            # Log the cost spike event
            cost_event = {
                'alarm_name': event.get('alarm_name', 'unknown'),
                'reason': event.get('reason', 'Cost threshold exceeded'),
                'timestamp': event.get('timestamp', datetime.now().isoformat())
            }
            
            log_remediation_action('cost_spike_detected', cost_event)
            
            send_notification(
                'DQAD: Cost Spike Detected',
                {
                    'action': 'cost_spike_alert',
                    'event': cost_event,
                    'recommendation': 'Review CloudWatch metrics and consider scaling down resources'
                }
            )
            
            results['success'] = True
            results['cost_event'] = cost_event
        
        else:
            logger.warning(f"Unknown action: {action}")
            results['error'] = f'Unknown action: {action}'
        
        # Return response
        status_code = 200 if results['success'] else 400
        
        response = {
            'statusCode': status_code,
            'body': json.dumps(results, default=str)
        }
        
        # Add CORS headers if from API Gateway
        if 'httpMethod' in event:
            response['headers'] = {'Access-Control-Allow-Origin': '*'}
        
        return response
        
    except Exception as e:
        logger.error(f"Error in orchestrator: {str(e)}", 
                    extra={'event_data': {'error': str(e), 'action': action}})
        results['error'] = str(e)
        
        response = {
            'statusCode': 500,
            'body': json.dumps(results, default=str)
        }
        
        if 'httpMethod' in event:
            response['headers'] = {'Access-Control-Allow-Origin': '*'}
        
        return response

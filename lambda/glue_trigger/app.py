"""
Lambda Function: Glue Job Trigger
Triggered by S3 ObjectCreated events to start AWS Glue ETL jobs

Event Flow:
S3 ObjectCreated:* → Lambda → StartGlueJob → CloudWatch Metrics
"""

import json
import boto3
import os
from datetime import datetime

# Initialize AWS clients
glue_client = boto3.client('glue')
cloudwatch = boto3.client('cloudwatch')

# Environment variables
GLUE_JOB_NAME = os.environ['GLUE_JOB_NAME']
S3_RAW_BUCKET = os.environ['S3_RAW_BUCKET']
S3_PROCESSED_BUCKET = os.environ['S3_PROCESSED_BUCKET']
CLOUDWATCH_NAMESPACE = os.environ['CLOUDWATCH_NAMESPACE']


def lambda_handler(event, context):
    """
    Handle S3 ObjectCreated events and trigger Glue ETL job
    """
    
    print(f"Received event: {json.dumps(event)}")
    
    try:
        # Parse S3 event
        for record in event['Records']:
            if record['eventName'].startswith('ObjectCreated'):
                bucket = record['s3']['bucket']['name']
                key = record['s3']['object']['key']
                size = record['s3']['object']['size']
                
                print(f"Processing S3 event: bucket={bucket}, key={key}, size={size} bytes")
                
                # Start Glue job
                response = start_glue_job(bucket, key)
                
                # Log success
                log_event = {
                    "timestamp": datetime.now().isoformat(),
                    "event_type": "glue_job_triggered",
                    "source_bucket": bucket,
                    "source_key": key,
                    "file_size_bytes": size,
                    "glue_job_name": GLUE_JOB_NAME,
                    "glue_job_run_id": response['JobRunId']
                }
                print(f"SUCCESS: {json.dumps(log_event)}")
                
                # Push CloudWatch metric
                push_trigger_metric(bucket, key, response['JobRunId'])
                
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Glue job triggered successfully',
                'job_run_id': response['JobRunId']
            })
        }
        
    except Exception as e:
        error_log = {
            "timestamp": datetime.now().isoformat(),
            "event_type": "glue_trigger_error",
            "error": str(e),
            "event": event
        }
        print(f"ERROR: {json.dumps(error_log)}")
        raise


def start_glue_job(bucket, key):
    """
    Start AWS Glue ETL job with S3 file parameters
    """
    
    print(f"Starting Glue job: {GLUE_JOB_NAME}")
    
    # Job arguments
    job_args = {
        '--S3_RAW_BUCKET': S3_RAW_BUCKET,
        '--S3_PROCESSED_BUCKET': S3_PROCESSED_BUCKET,
        '--S3_INPUT_KEY': key,
        '--CLOUDWATCH_NAMESPACE': CLOUDWATCH_NAMESPACE
    }
    
    # Start job run
    response = glue_client.start_job_run(
        JobName=GLUE_JOB_NAME,
        Arguments=job_args
    )
    
    print(f"Glue job started successfully. JobRunId: {response['JobRunId']}")
    
    return response


def push_trigger_metric(bucket, key, job_run_id):
    """
    Push CloudWatch metric for Glue job trigger
    """
    
    try:
        cloudwatch.put_metric_data(
            Namespace=CLOUDWATCH_NAMESPACE,
            MetricData=[
                {
                    'MetricName': 'GlueJobTriggered',
                    'Value': 1,
                    'Unit': 'Count',
                    'Timestamp': datetime.now(),
                    'Dimensions': [
                        {'Name': 'SourceBucket', 'Value': bucket},
                        {'Name': 'JobName', 'Value': GLUE_JOB_NAME}
                    ]
                }
            ]
        )
        print("CloudWatch metric pushed: GlueJobTriggered")
        
    except Exception as e:
        print(f"Warning: Failed to push CloudWatch metric: {str(e)}")

"""Quick script to check CloudWatch metrics"""
import boto3
from datetime import datetime, timedelta

cw = boto3.client('cloudwatch', region_name='us-east-1')

# Check metrics from last 7 days
end_time = datetime.now()
start_time = end_time - timedelta(days=7)

print("Checking CloudWatch metrics...")
print(f"Time range: {start_time} to {end_time}")
print("=" * 60)

# Check TotalRecords with aggregated dimension
response = cw.get_metric_statistics(
    Namespace='DQAD/DataQuality',
    MetricName='TotalRecords',
    Dimensions=[{'Name': 'SourceFile', 'Value': 'claims/'}],
    StartTime=start_time,
    EndTime=end_time,
    Period=3600,
    Statistics=['Maximum']
)

print(f"\nTotalRecords (claims/ dimension): {len(response['Datapoints'])} datapoints")
for dp in sorted(response['Datapoints'], key=lambda x: x['Timestamp'], reverse=True)[:3]:
    print(f"  {dp['Timestamp']}: {dp['Maximum']}")

# Check for individual file metrics
response2 = cw.list_metrics(
    Namespace='DQAD/DataQuality',
    MetricName='TotalRecords'
)

print(f"\nAll TotalRecords metrics found: {len(response2['Metrics'])}")
for metric in response2['Metrics'][:5]:
    dims = {d['Name']: d['Value'] for d in metric['Dimensions']}
    print(f"  Dimensions: {dims}")

# Check AnomalyCount
response3 = cw.get_metric_statistics(
    Namespace='DQAD/DataQuality',
    MetricName='AnomalyCount',
    Dimensions=[{'Name': 'SourceFile', 'Value': 'claims/'}],
    StartTime=start_time,
    EndTime=end_time,
    Period=3600,
    Statistics=['Maximum']
)

print(f"\nAnomalyCount (claims/ dimension): {len(response3['Datapoints'])} datapoints")
for dp in sorted(response3['Datapoints'], key=lambda x: x['Timestamp'], reverse=True)[:3]:
    print(f"  {dp['Timestamp']}: {dp['Maximum']}")

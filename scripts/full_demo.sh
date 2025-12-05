#!/bin/bash
# Full Demo Script for DQAD Platform (Linux/Mac)

set -e  # Exit on error

echo "=============================================="
echo "DQAD Platform - Full Demonstration"
echo "=============================================="
echo ""

# Step 1: Generate Claims Data
echo "STEP 1/9: Generating synthetic claims data..."
cd ../data
python3 generate_payer_data.py
cd ../scripts
echo "✓ Generated claims data"
echo ""

# Step 2: Upload to S3
echo "STEP 2/9: Uploading to S3..."
aws s3 cp ../raw_data/ s3://dqad-raw-dev/claims/ --recursive --exclude "*" --include "*.csv"
echo "✓ Uploaded to S3"
echo ""

# Step 3: Trigger Glue Job
echo "STEP 3/9: Triggering Glue ETL job..."
LATEST_FILE=$(ls -t ../raw_data/*.csv | head -1 | xargs -n 1 basename)
RUN_ID=$(aws glue start-job-run \
  --job-name dqad-etl-job-dev \
  --arguments "{\"--S3_INPUT_KEY\":\"claims/$LATEST_FILE\"}" \
  --query 'JobRunId' \
  --output text)
echo "✓ Glue job started: $RUN_ID"
echo ""

# Step 4: Monitor Execution
echo "STEP 4/9: Monitoring Glue job execution..."
while true; do
  STATUS=$(aws glue get-job-run \
    --job-name dqad-etl-job-dev \
    --run-id "$RUN_ID" \
    --query 'JobRun.JobRunState' \
    --output text)
  
  if [ "$STATUS" == "SUCCEEDED" ]; then
    echo "✓ Glue job completed successfully"
    break
  elif [ "$STATUS" == "FAILED" ] || [ "$STATUS" == "STOPPED" ]; then
    echo "✗ Glue job failed with status: $STATUS"
    exit 1
  else
    echo "  Status: $STATUS - waiting..."
    sleep 10
  fi
done
echo ""

# Step 5: Verify Outputs
echo "STEP 5/9: Verifying 3-tier outputs..."
echo "Gold tier (clean data):"
aws s3 ls s3://dqad-processed-dev/gold/ --recursive --human-readable | tail -5
echo ""
echo "Silver tier (DQ failures):"
aws s3 ls s3://dqad-processed-dev/silver/ --recursive --human-readable | tail -5
echo ""
echo "Quarantine tier (outliers):"
aws s3 ls s3://dqad-processed-dev/quarantine/ --recursive --human-readable | tail -5
echo ""

# Step 6: Check CloudWatch Metrics
echo "STEP 6/9: Checking CloudWatch metrics..."
aws cloudwatch get-metric-statistics \
  --namespace DQAD/DataQuality \
  --metric-name DataQualityScore \
  --dimensions Name=Environment,Value=dev \
  --start-time "$(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S)" \
  --end-time "$(date -u +%Y-%m-%dT%H:%M:%S)" \
  --period 3600 \
  --statistics Average \
  --query 'Datapoints[0].Average' \
  --output text
echo "✓ Metrics published"
echo ""

# Step 7: Check Alarms
echo "STEP 7/9: Checking CloudWatch alarms..."
aws cloudwatch describe-alarms \
  --alarm-names dqad-anomaly-spike-dev dqad-dq-score-low-dev dqad-cost-spike-dev \
  --query 'MetricAlarms[*].[AlarmName,StateValue]' \
  --output table
echo ""

# Step 8: Test Self-Healing (optional)
echo "STEP 8/9: Testing self-healing orchestrator..."
echo "  (Skipped - requires alarm trigger)"
echo ""

# Step 9: View Logs
echo "STEP 9/9: Retrieving recent logs..."
aws logs tail /aws-glue/jobs/output --since 5m --format short | tail -20
echo ""

echo "=============================================="
echo "✓ DEMO COMPLETED SUCCESSFULLY"
echo "=============================================="
echo ""
echo "Next steps:"
echo "  1. Launch dashboard: cd ../dashboard && streamlit run app.py"
echo "  2. View S3 data: aws s3 ls s3://dqad-processed-dev/ --recursive"
echo "  3. Query Athena (if configured)"
echo ""

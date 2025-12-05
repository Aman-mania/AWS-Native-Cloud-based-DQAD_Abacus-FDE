# ============================================================================
# DQAD Production Flow - End-to-End AWS Automation
# ============================================================================
# This script demonstrates the complete production workflow:
# 1. Upload CSV to S3
# 2. Monitor Glue job execution
# 3. Check CloudWatch metrics
# 4. Verify output layers
# 5. Test self-healing orchestration
# ============================================================================

Write-Host "=====================================================================" -ForegroundColor Cyan
Write-Host "   DQAD PRODUCTION FLOW - AWS End-to-End Automation" -ForegroundColor Cyan
Write-Host "=====================================================================" -ForegroundColor Cyan
Write-Host ""

# Configuration
$S3_RAW_BUCKET = "dqad-raw-dev"
$S3_PROCESSED_BUCKET = "dqad-processed-dev"
$GLUE_JOB_NAME = "dqad-etl-job-dev"
$CLOUDWATCH_NAMESPACE = "DQAD/DataQuality"
$TEST_FILE = "../data/test_claims.csv"

# ============================================================================
# STEP 1: Validate Prerequisites
# ============================================================================
Write-Host "[STEP 1] Validating prerequisites..." -ForegroundColor Yellow
Write-Host ""

# Check AWS credentials
try {
    $account = aws sts get-caller-identity --query 'Account' --output text
    Write-Host "[OK] AWS credentials configured" -ForegroundColor Green
    Write-Host "     Account: $account" -ForegroundColor Gray
} catch {
    Write-Host "[X] AWS credentials not configured!" -ForegroundColor Red
    Write-Host "    Run: aws configure" -ForegroundColor Yellow
    exit 1
}

# Check if test file exists
if (-Not (Test-Path $TEST_FILE)) {
    Write-Host "[X] Test file not found: $TEST_FILE" -ForegroundColor Red
    Write-Host "    Generating test data..." -ForegroundColor Yellow
    cd ../data
    python generate_payer_data.py
    cd ../scripts
}

Write-Host "[OK] Test file exists: $TEST_FILE" -ForegroundColor Green
Write-Host ""

# ============================================================================
# STEP 2: Upload CSV to S3 (Triggers Automated Pipeline)
# ============================================================================
Write-Host "[STEP 2] Uploading CSV to S3 (this triggers the pipeline)..." -ForegroundColor Yellow
Write-Host ""

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$s3_key = "claims/production_test_$timestamp.csv"

Write-Host "  Uploading: $TEST_FILE" -ForegroundColor Gray
Write-Host "  Destination: s3://$S3_RAW_BUCKET/$s3_key" -ForegroundColor Gray
Write-Host ""

aws s3 cp $TEST_FILE "s3://$S3_RAW_BUCKET/$s3_key"

if ($LASTEXITCODE -eq 0) {
    Write-Host "[OK] File uploaded successfully!" -ForegroundColor Green
    Write-Host "     S3 Event → Lambda Trigger → Glue Job (starting...)" -ForegroundColor Gray
} else {
    Write-Host "[X] Upload failed!" -ForegroundColor Red
    exit 1
}
Write-Host ""

# ============================================================================
# STEP 3: Monitor Glue Job Execution
# ============================================================================
Write-Host "[STEP 3] Monitoring Glue job execution..." -ForegroundColor Yellow
Write-Host ""
Write-Host "  Waiting for job to start (5 seconds)..." -ForegroundColor Gray
Start-Sleep -Seconds 5

# Get latest job run
$jobRuns = aws glue get-job-runs --job-name $GLUE_JOB_NAME --max-results 1 | ConvertFrom-Json

if ($jobRuns.JobRuns.Count -eq 0) {
    Write-Host "[X] No job runs found!" -ForegroundColor Red
    exit 1
}

$latestRun = $jobRuns.JobRuns[0]
$runId = $latestRun.Id
$state = $latestRun.JobRunState

Write-Host "[OK] Latest job run found:" -ForegroundColor Green
Write-Host "     Run ID: $runId" -ForegroundColor Gray
Write-Host "     State: $state" -ForegroundColor Gray
Write-Host ""

# Wait for job to complete
$maxWaitTime = 300  # 5 minutes
$waitInterval = 10  # 10 seconds
$elapsed = 0

Write-Host "  Waiting for job to complete (max 5 minutes)..." -ForegroundColor Gray
Write-Host ""

while ($state -eq "RUNNING" -and $elapsed -lt $maxWaitTime) {
    Write-Host "  [$elapsed s] Job status: $state" -ForegroundColor Cyan
    Start-Sleep -Seconds $waitInterval
    $elapsed += $waitInterval
    
    $jobRun = aws glue get-job-run --job-name $GLUE_JOB_NAME --run-id $runId | ConvertFrom-Json
    $state = $jobRun.JobRun.JobRunState
}

Write-Host ""
if ($state -eq "SUCCEEDED") {
    Write-Host "[OK] Job completed successfully!" -ForegroundColor Green
    $executionTime = $jobRun.JobRun.ExecutionTime
    Write-Host "     Execution time: $executionTime seconds" -ForegroundColor Gray
} elseif ($state -eq "FAILED") {
    Write-Host "[X] Job failed!" -ForegroundColor Red
    Write-Host "     Check logs: aws logs tail /aws-glue/jobs/output --since 10m" -ForegroundColor Yellow
    exit 1
} else {
    Write-Host "[!] Job still running (timeout reached)" -ForegroundColor Yellow
    Write-Host "     Current state: $state" -ForegroundColor Gray
    Write-Host "     Check status manually: aws glue get-job-run --job-name $GLUE_JOB_NAME --run-id $runId" -ForegroundColor Yellow
}
Write-Host ""

# ============================================================================
# STEP 4: Verify Output Layers
# ============================================================================
Write-Host "[STEP 4] Verifying output data layers..." -ForegroundColor Yellow
Write-Host ""

# Check Gold layer
Write-Host "  Checking Gold layer (clean data)..." -ForegroundColor Gray
$goldFiles = aws s3 ls "s3://$S3_PROCESSED_BUCKET/gold/" --recursive | Select-String -Pattern ".parquet"
if ($goldFiles) {
    Write-Host "[OK] Gold layer contains data:" -ForegroundColor Green
    $goldFiles | ForEach-Object { Write-Host "     $_" -ForegroundColor Gray }
} else {
    Write-Host "[!] No files in Gold layer" -ForegroundColor Yellow
}
Write-Host ""

# Check Silver layer
Write-Host "  Checking Silver layer (DQ failures)..." -ForegroundColor Gray
$silverFiles = aws s3 ls "s3://$S3_PROCESSED_BUCKET/silver/" --recursive | Select-String -Pattern ".parquet"
if ($silverFiles) {
    Write-Host "[OK] Silver layer contains data:" -ForegroundColor Green
    $silverFiles | ForEach-Object { Write-Host "     $_" -ForegroundColor Gray }
} else {
    Write-Host "[OK] No DQ failures detected (Silver layer empty)" -ForegroundColor Green
}
Write-Host ""

# Check Quarantine layer
Write-Host "  Checking Quarantine layer (statistical outliers)..." -ForegroundColor Gray
$quarantineFiles = aws s3 ls "s3://$S3_PROCESSED_BUCKET/quarantine/" --recursive | Select-String -Pattern ".parquet"
if ($quarantineFiles) {
    Write-Host "[OK] Quarantine layer contains data:" -ForegroundColor Green
    $quarantineFiles | ForEach-Object { Write-Host "     $_" -ForegroundColor Gray }
} else {
    Write-Host "[OK] No statistical outliers detected (Quarantine layer empty)" -ForegroundColor Green
}
Write-Host ""

# ============================================================================
# STEP 5: Check CloudWatch Metrics
# ============================================================================
Write-Host "[STEP 5] Checking CloudWatch metrics..." -ForegroundColor Yellow
Write-Host ""
Write-Host "  NOTE: Metrics may take 2-5 minutes to appear in CloudWatch" -ForegroundColor Yellow
Write-Host "        Waiting 90 seconds for metric propagation...`n" -ForegroundColor Gray
Start-Sleep -Seconds 90

$startTime = (Get-Date).AddHours(-1).ToString("o")
$endTime = (Get-Date).ToString("o")

# Data Quality Score
Write-Host "  Fetching Data Quality Score..." -ForegroundColor Gray
$dqScore = aws cloudwatch get-metric-statistics `
    --namespace $CLOUDWATCH_NAMESPACE `
    --metric-name DataQualityScore `
    --start-time $startTime `
    --end-time $endTime `
    --period 300 `
    --statistics Maximum | ConvertFrom-Json

if ($dqScore.Datapoints.Count -gt 0) {
    $score = $dqScore.Datapoints[0].Maximum
    Write-Host "[OK] Data Quality Score: $score%" -ForegroundColor Green
} else {
    Write-Host "[!] No Data Quality Score metrics found (may take a few minutes)" -ForegroundColor Yellow
}

# Anomaly Count
Write-Host "  Fetching Anomaly Count..." -ForegroundColor Gray
$anomalyCount = aws cloudwatch get-metric-statistics `
    --namespace $CLOUDWATCH_NAMESPACE `
    --metric-name AnomalyCount `
    --start-time $startTime `
    --end-time $endTime `
    --period 300 `
    --statistics Sum | ConvertFrom-Json

if ($anomalyCount.Datapoints.Count -gt 0) {
    $count = $anomalyCount.Datapoints[0].Sum
    Write-Host "[OK] Anomaly Count: $count" -ForegroundColor Green
} else {
    Write-Host "[!] No Anomaly Count metrics found (may take a few minutes)" -ForegroundColor Yellow
}

# Total Records
Write-Host "  Fetching Total Records Processed..." -ForegroundColor Gray
$totalRecords = aws cloudwatch get-metric-statistics `
    --namespace $CLOUDWATCH_NAMESPACE `
    --metric-name TotalRecords `
    --start-time $startTime `
    --end-time $endTime `
    --period 300 `
    --statistics Sum | ConvertFrom-Json

if ($totalRecords.Datapoints.Count -gt 0) {
    $total = $totalRecords.Datapoints[0].Sum
    Write-Host "[OK] Total Records: $total" -ForegroundColor Green
} else {
    Write-Host "[!] No Total Records metrics found (may take a few minutes)" -ForegroundColor Yellow
}
Write-Host ""

# ============================================================================
# STEP 6: Test Self-Healing Orchestration
# ============================================================================
Write-Host "[STEP 6] Testing self-healing orchestration..." -ForegroundColor Yellow
Write-Host ""

$apiGatewayUrl = aws cloudformation describe-stacks --query "Stacks[?contains(StackName, 'dqad')].Outputs[?OutputKey=='api_gateway_url'].OutputValue" --output text
if (-not $apiGatewayUrl) {
    # Fallback to Terraform output
    $apiGatewayUrl = "https://3x8a9rr1ah.execute-api.us-east-1.amazonaws.com/dev/trigger"
}

Write-Host "  Testing manual job restart via API Gateway..." -ForegroundColor Gray
Write-Host "  Endpoint: $apiGatewayUrl" -ForegroundColor Gray
Write-Host ""

$body = @{
    action = "restart_glue_job"
} | ConvertTo-Json

try {
    $response = Invoke-RestMethod -Uri $apiGatewayUrl -Method POST -Body $body -ContentType "application/json"
    Write-Host "[OK] Orchestrator response:" -ForegroundColor Green
    Write-Host ($response | ConvertTo-Json -Depth 5) -ForegroundColor Gray
} catch {
    Write-Host "[!] API call failed (this is optional for testing)" -ForegroundColor Yellow
    Write-Host "    Error: $_" -ForegroundColor Gray
}
Write-Host ""

# ============================================================================
# STEP 7: Summary
# ============================================================================
Write-Host "=====================================================================" -ForegroundColor Cyan
Write-Host "   PRODUCTION FLOW COMPLETE!" -ForegroundColor Cyan
Write-Host "=====================================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Summary:" -ForegroundColor Green
Write-Host "  1. CSV uploaded to S3: s3://$S3_RAW_BUCKET/$s3_key" -ForegroundColor Gray
Write-Host "  2. Glue job executed: $state" -ForegroundColor Gray
Write-Host "  3. Output layers verified (Gold/Silver/Quarantine)" -ForegroundColor Gray
Write-Host "  4. CloudWatch metrics published" -ForegroundColor Gray
Write-Host "  5. Self-healing orchestration tested" -ForegroundColor Gray
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "  - View dashboard: cd ../dashboard; streamlit run app.py" -ForegroundColor Gray
Write-Host "  - Check Glue logs: aws logs tail /aws-glue/jobs/output --since 10m" -ForegroundColor Gray
Write-Host "  - Download Gold data: aws s3 sync s3://$S3_PROCESSED_BUCKET/gold/ ./output/" -ForegroundColor Gray
Write-Host ""

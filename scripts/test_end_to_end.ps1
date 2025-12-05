# End-to-End DQAD Platform Test Script
# This script validates the complete workflow from data generation to self-healing

param(
    [string]$AwsProfile = "default",
    [string]$AwsRegion = "us-east-1",
    [string]$ProjectName = "dqad",
    [string]$Environment = "dev"
)

$ErrorActionPreference = "Stop"

Write-Host "======================================" -ForegroundColor Cyan
Write-Host "DQAD End-to-End Test Suite" -ForegroundColor Cyan
Write-Host "======================================" -ForegroundColor Cyan
Write-Host ""

# Configuration
$env:AWS_PROFILE = $AwsProfile
$env:AWS_DEFAULT_REGION = $AwsRegion

$S3_RAW_BUCKET = "$ProjectName-raw-$Environment"
$S3_PROCESSED_BUCKET = "$ProjectName-processed-$Environment"
$S3_LOGS_BUCKET = "$ProjectName-logs-$Environment"
$COST_COLLECTOR_LAMBDA = "$ProjectName-cost-collector"
$ORCHESTRATOR_LAMBDA = "$ProjectName-orchestrator"
$ALARM_NAME = "$ProjectName-daily-cost-spike"

$TestResults = @{
    DataGeneration = $false
    S3Upload = $false
    LambdaCostCollector = $false
    CloudWatchMetrics = $false
    AlarmTrigger = $false
    OrchestratorExecution = $false
    SNSNotification = $false
    DeltaLakeValidation = $false
}

# Step 1: Generate Synthetic Data
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Yellow
Write-Host "Step 1: Generating Synthetic Claims Data" -ForegroundColor Yellow
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Yellow

try {
    Push-Location "$PSScriptRoot\..\data"
    
    if (-not (Test-Path "raw_data")) {
        New-Item -ItemType Directory -Path "raw_data" -Force | Out-Null
    }
    
    Write-Host "Running data generator..." -ForegroundColor Gray
    python generate_payer_data.py
    
    $csvFiles = Get-ChildItem "raw_data\*.csv"
    if ($csvFiles.Count -ge 3) {
        Write-Host "✓ Generated $($csvFiles.Count) CSV files" -ForegroundColor Green
        $totalLines = ($csvFiles | ForEach-Object { (Get-Content $_.FullName).Count - 1 } | Measure-Object -Sum).Sum
        Write-Host "  Total claims: $totalLines" -ForegroundColor Gray
        $TestResults.DataGeneration = $true
    } else {
        throw "Expected at least 3 CSV files, found $($csvFiles.Count)"
    }
} catch {
    Write-Host "✗ Data generation failed: $_" -ForegroundColor Red
} finally {
    Pop-Location
}

Write-Host ""

# Step 2: Upload to S3
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Yellow
Write-Host "Step 2: Uploading Claims to S3" -ForegroundColor Yellow
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Yellow

try {
    # Check if bucket exists
    $bucketExists = aws s3 ls "s3://$S3_RAW_BUCKET" 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "S3 bucket $S3_RAW_BUCKET does not exist. Run terraform apply first."
    }
    
    Write-Host "Syncing data to s3://$S3_RAW_BUCKET/claims/" -ForegroundColor Gray
    aws s3 sync "$PSScriptRoot\..\data\raw_data\" "s3://$S3_RAW_BUCKET/claims/" --delete
    
    if ($LASTEXITCODE -eq 0) {
        $s3Objects = aws s3 ls "s3://$S3_RAW_BUCKET/claims/" | Measure-Object
        Write-Host "✓ Uploaded to S3 ($($s3Objects.Count) objects)" -ForegroundColor Green
        $TestResults.S3Upload = $true
    }
} catch {
    Write-Host "✗ S3 upload failed: $_" -ForegroundColor Red
}

Write-Host ""

# Step 3: Invoke Cost Collector Lambda
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Yellow
Write-Host "Step 3: Testing Cost Collector Lambda" -ForegroundColor Yellow
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Yellow

try {
    Write-Host "Invoking $COST_COLLECTOR_LAMBDA..." -ForegroundColor Gray
    
    $response = aws lambda invoke `
        --function-name $COST_COLLECTOR_LAMBDA `
        --payload '{}' `
        --cli-binary-format raw-in-base64-out `
        response_cost_collector.json 2>&1
    
    if ($LASTEXITCODE -eq 0) {
        $result = Get-Content "response_cost_collector.json" | ConvertFrom-Json
        Write-Host "✓ Lambda executed successfully" -ForegroundColor Green
        Write-Host "  Status: $($result.statusCode)" -ForegroundColor Gray
        Write-Host "  Daily Cost: `$$($result.daily_cost)" -ForegroundColor Gray
        Write-Host "  Forecast: `$$($result.forecast)" -ForegroundColor Gray
        $TestResults.LambdaCostCollector = $true
        
        Remove-Item "response_cost_collector.json" -ErrorAction SilentlyContinue
    }
} catch {
    Write-Host "✗ Lambda invocation failed: $_" -ForegroundColor Red
}

Write-Host ""

# Step 4: Verify CloudWatch Metrics
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Yellow
Write-Host "Step 4: Verifying CloudWatch Metrics" -ForegroundColor Yellow
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Yellow

try {
    Start-Sleep -Seconds 5  # Wait for metrics to propagate
    
    $endTime = (Get-Date).ToUniversalTime()
    $startTime = $endTime.AddHours(-1)
    
    Write-Host "Checking DQAD/Cost/DailyCost metric..." -ForegroundColor Gray
    
    $metrics = aws cloudwatch get-metric-statistics `
        --namespace "DQAD/Cost" `
        --metric-name "DailyCost" `
        --start-time $startTime.ToString("yyyy-MM-ddTHH:mm:ss") `
        --end-time $endTime.ToString("yyyy-MM-ddTHH:mm:ss") `
        --period 3600 `
        --statistics Maximum `
        --output json | ConvertFrom-Json
    
    if ($metrics.Datapoints.Count -gt 0) {
        $latestValue = ($metrics.Datapoints | Sort-Object Timestamp -Descending | Select-Object -First 1).Maximum
        Write-Host "✓ CloudWatch metric exists" -ForegroundColor Green
        Write-Host "  Latest value: `$$latestValue" -ForegroundColor Gray
        $TestResults.CloudWatchMetrics = $true
    } else {
        Write-Host "⚠ No metric data points found (may need to wait longer)" -ForegroundColor Yellow
    }
} catch {
    Write-Host "✗ CloudWatch metrics check failed: $_" -ForegroundColor Red
}

Write-Host ""

# Step 5: Test Alarm Trigger
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Yellow
Write-Host "Step 5: Testing Alarm Trigger" -ForegroundColor Yellow
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Yellow

try {
    Write-Host "Setting alarm to ALARM state manually..." -ForegroundColor Gray
    
    aws cloudwatch set-alarm-state `
        --alarm-name $ALARM_NAME `
        --state-value ALARM `
        --state-reason "End-to-end test - simulated cost spike"
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✓ Alarm triggered successfully" -ForegroundColor Green
        $TestResults.AlarmTrigger = $true
        
        Write-Host "Waiting 10 seconds for EventBridge to process..." -ForegroundColor Gray
        Start-Sleep -Seconds 10
    }
} catch {
    Write-Host "✗ Alarm trigger failed: $_" -ForegroundColor Red
}

Write-Host ""

# Step 6: Verify Orchestrator Execution
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Yellow
Write-Host "Step 6: Verifying Orchestrator Lambda" -ForegroundColor Yellow
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Yellow

try {
    Write-Host "Checking recent Lambda invocations..." -ForegroundColor Gray
    
    # Get CloudWatch logs
    $logGroupName = "/aws/lambda/$ORCHESTRATOR_LAMBDA"
    
    $streams = aws logs describe-log-streams `
        --log-group-name $logGroupName `
        --order-by LastEventTime `
        --descending `
        --max-items 1 `
        --output json | ConvertFrom-Json
    
    if ($streams.logStreams.Count -gt 0) {
        $latestStream = $streams.logStreams[0].logStreamName
        
        Write-Host "Fetching logs from stream: $latestStream" -ForegroundColor Gray
        
        $logs = aws logs get-log-events `
            --log-group-name $logGroupName `
            --log-stream-name $latestStream `
            --limit 50 `
            --output json | ConvertFrom-Json
        
        $recentLogs = $logs.events | Where-Object { $_.timestamp -gt ((Get-Date).AddMinutes(-5).ToFileTimeUtc() / 10000) }
        
        if ($recentLogs.Count -gt 0) {
            Write-Host "✓ Orchestrator executed recently" -ForegroundColor Green
            Write-Host "  Recent log entries: $($recentLogs.Count)" -ForegroundColor Gray
            $TestResults.OrchestratorExecution = $true
        } else {
            Write-Host "⚠ No recent orchestrator executions found" -ForegroundColor Yellow
        }
    }
} catch {
    Write-Host "✗ Orchestrator verification failed: $_" -ForegroundColor Red
}

Write-Host ""

# Step 7: Check SNS Notifications
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Yellow
Write-Host "Step 7: Checking SNS Notification Status" -ForegroundColor Yellow
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Yellow

try {
    $topicArn = aws sns list-topics --output json | ConvertFrom-Json | 
                Select-Object -ExpandProperty Topics | 
                Where-Object { $_.TopicArn -like "*$ProjectName-alerts*" } | 
                Select-Object -First 1 -ExpandProperty TopicArn
    
    if ($topicArn) {
        $subscriptions = aws sns list-subscriptions-by-topic --topic-arn $topicArn --output json | ConvertFrom-Json
        
        $confirmedSubs = $subscriptions.Subscriptions | Where-Object { $_.SubscriptionArn -ne "PendingConfirmation" }
        
        if ($confirmedSubs.Count -gt 0) {
            Write-Host "✓ SNS topic configured with $($confirmedSubs.Count) confirmed subscription(s)" -ForegroundColor Green
            Write-Host "  ⚠ Check your email for alert notifications" -ForegroundColor Yellow
            $TestResults.SNSNotification = $true
        } else {
            Write-Host "⚠ SNS subscriptions pending confirmation" -ForegroundColor Yellow
        }
    }
} catch {
    Write-Host "✗ SNS check failed: $_" -ForegroundColor Red
}

Write-Host ""

# Step 8: Validate Delta Lake Output (optional - requires Databricks)
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Yellow
Write-Host "Step 8: Validating Delta Lake Output" -ForegroundColor Yellow
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Yellow

try {
    $deltaPath = "s3://$S3_PROCESSED_BUCKET/delta/claims/"
    $deltaExists = aws s3 ls $deltaPath 2>&1
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✓ Delta Lake path exists" -ForegroundColor Green
        Write-Host "  Path: $deltaPath" -ForegroundColor Gray
        $TestResults.DeltaLakeValidation = $true
    } else {
        Write-Host "⚠ Delta Lake not yet created (run Databricks ETL notebook)" -ForegroundColor Yellow
    }
} catch {
    Write-Host "⚠ Delta Lake validation skipped (run ETL first)" -ForegroundColor Yellow
}

Write-Host ""

# Final Report
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Cyan
Write-Host "TEST RESULTS SUMMARY" -ForegroundColor Cyan
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Cyan
Write-Host ""

$passedTests = ($TestResults.Values | Where-Object { $_ -eq $true }).Count
$totalTests = $TestResults.Count

foreach ($test in $TestResults.GetEnumerator() | Sort-Object Name) {
    $status = if ($test.Value) { "PASS" } else { "FAIL" }
    $symbol = if ($test.Value) { "[OK]" } else { "[X]" }
    $color = if ($test.Value) { "Green" } else { "Red" }
    Write-Host "  $($test.Key.PadRight(25)) : " -NoNewline
    Write-Host "$symbol $status" -ForegroundColor $color
}

Write-Host ""
Write-Host "Overall: $passedTests / $totalTests tests passed" -ForegroundColor $(if ($passedTests -eq $totalTests) { "Green" } else { "Yellow" })

if ($passedTests -ge 5) {
    Write-Host ""
    Write-Host "✓ Core functionality validated!" -ForegroundColor Green
    Write-Host "  System is ready for demo." -ForegroundColor Green
} else {
    Write-Host ""
    Write-Host "⚠ Some tests failed. Review errors above." -ForegroundColor Yellow
}

Write-Host ""
Write-Host "Next Steps:" -ForegroundColor Cyan
Write-Host "  1. Check your email for SNS alert" -ForegroundColor Gray
Write-Host "  2. Run Databricks ETL notebook to process claims" -ForegroundColor Gray
Write-Host "  3. Launch dashboard: cd dashboard; streamlit run app.py" -ForegroundColor Gray
Write-Host "  4. Reset alarm: aws cloudwatch set-alarm-state --alarm-name $ALARM_NAME --state-value OK --state-reason 'Test complete'" -ForegroundColor Gray
Write-Host ""

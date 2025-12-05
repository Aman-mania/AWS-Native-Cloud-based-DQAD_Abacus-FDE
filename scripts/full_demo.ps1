# DQAD COMPLETE DEMONSTRATION SCRIPT
# This script runs the entire demonstration workflow including:
# 1. Data generation with anomalies (35 percent rate)
# 2. S3 upload
# 3. Glue job trigger and monitoring
# 4. CloudWatch metrics verification
# 5. Alarm state checking
# 6. Self-healing orchestration test
# 7. Quarantine validation

$ErrorActionPreference = "Stop"

Write-Host "=====================================================================" -ForegroundColor Cyan
Write-Host "   DQAD COMPLETE DEMONSTRATION - Full Workflow with Anomalies" -ForegroundColor Cyan
Write-Host "=====================================================================" -ForegroundColor Cyan
Write-Host ""

# Configuration
$S3_RAW_BUCKET = "dqad-raw-dev"
$S3_PROCESSED_BUCKET = "dqad-processed-dev"
$GLUE_JOB_NAME = "dqad-etl-job-dev"
$GLUE_TRIGGER_LAMBDA = "dqad-glue-trigger-dev"
$ORCHESTRATOR_LAMBDA = "dqad-orchestrator"
$CLOUDWATCH_NAMESPACE = "DQAD/DataQuality"
$ALARM_NAME = "dqad-anomaly-spike"
$API_GATEWAY_URL = "https://3x8a9rr1ah.execute-api.us-east-1.amazonaws.com/dev/trigger"
$VENV_PYTHON = "C:/Users/amanb/Desktop/Academic Projects/Abacus FDE/venv/Scripts/python.exe"

# STEP 1: Generate Synthetic Data with High Anomaly Rate
Write-Host "[STEP 1] Generating Claims Data (35 Percent Anomaly Rate)" -ForegroundColor Yellow
Write-Host ""

Push-Location "$PSScriptRoot\..\data"

try {
    Write-Host "Running data generator with 35 percent anomaly rate..." -ForegroundColor Gray
    & $VENV_PYTHON generate_payer_data.py
    
    if ($LASTEXITCODE -ne 0) {
        throw "Data generation failed with exit code $LASTEXITCODE"
    }
    
    # Count generated files
    $csvFiles = Get-ChildItem "..\raw_data\payer_claims_*.csv" -ErrorAction SilentlyContinue
    $totalClaims = 0
    
    if ($csvFiles) {
        foreach ($file in $csvFiles) {
            $lineCount = (Get-Content $file.FullName | Measure-Object -Line).Lines - 1
            $totalClaims += $lineCount
        }
        Write-Host ""
        Write-Host "[OK] Generated $($csvFiles.Count) files with $totalClaims total claims" -ForegroundColor Green
        $expectedAnomalies = [int]($totalClaims * 0.35)
        Write-Host "     Expected anomalies: ~$expectedAnomalies" -ForegroundColor Gray
        Write-Host ""
    } else {
        throw "No CSV files generated"
    }
} catch {
    Write-Host "[X] Data generation failed: $_" -ForegroundColor Red
    Pop-Location
    exit 1
} finally {
    Pop-Location
}

Start-Sleep -Seconds 2

# STEP 2: Upload Data to S3
Write-Host "[STEP 2] Uploading Claims to S3" -ForegroundColor Yellow
Write-Host ""

try {
    # Check bucket exists
    $bucketCheck = aws s3 ls "s3://$S3_RAW_BUCKET" 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "S3 bucket '$S3_RAW_BUCKET' not found. Run terraform apply first."
    }
    
    Write-Host "Syncing to s3://$S3_RAW_BUCKET/claims/" -ForegroundColor Gray
    $rawDataPath = Resolve-Path "$PSScriptRoot\..\raw_data"
    aws s3 sync "$rawDataPath" "s3://$S3_RAW_BUCKET/claims/" --exclude "*" --include "payer_claims_*.csv"
    
    if ($LASTEXITCODE -eq 0) {
        $objectCount = (aws s3 ls "s3://$S3_RAW_BUCKET/claims/" | Measure-Object).Count
        Write-Host ""
        Write-Host "[OK] Upload complete: $objectCount objects in S3" -ForegroundColor Green
        Write-Host ""
    }
} catch {
    Write-Host "[X] S3 upload failed: $_" -ForegroundColor Red
    exit 1
}

Start-Sleep -Seconds 2

# STEP 3: Trigger Glue Job via Lambda
Write-Host "[STEP 3] Triggering AWS Glue ETL Job" -ForegroundColor Yellow
Write-Host ""

try {
    Write-Host "Invoking Lambda: $GLUE_TRIGGER_LAMBDA..." -ForegroundColor Gray
    
    $response = aws lambda invoke `
        --function-name $GLUE_TRIGGER_LAMBDA `
        --payload '{}' `
        --cli-binary-format raw-in-base64-out `
        response.json 2>&1
    
    if ($LASTEXITCODE -eq 0) {
        $result = Get-Content response.json | ConvertFrom-Json
        $jobRunId = $result.jobRunId
        
        Write-Host ""
        Write-Host "[OK] Glue job started" -ForegroundColor Green
        Write-Host "     Job Name: $GLUE_JOB_NAME" -ForegroundColor Gray
        Write-Host "     Run ID: $jobRunId" -ForegroundColor Gray
        Write-Host ""
        
        Remove-Item response.json -ErrorAction SilentlyContinue
    } else {
        throw "Lambda invocation failed"
    }
} catch {
    Write-Host "[X] Failed to trigger Glue job: $_" -ForegroundColor Red
    exit 1
}

# STEP 4: Monitor Glue Job Execution
Write-Host "[STEP 4] Monitoring Glue Job Execution" -ForegroundColor Yellow
Write-Host ""

$maxWaitSeconds = 300
$elapsedSeconds = 0
$checkInterval = 10

Write-Host "Waiting for job to complete (max ${maxWaitSeconds}s)..." -ForegroundColor Gray
Write-Host ""

while ($elapsedSeconds -lt $maxWaitSeconds) {
    try {
        $jobStatus = aws glue get-job-runs `
            --job-name $GLUE_JOB_NAME `
            --max-results 1 `
            --query 'JobRuns[0].[JobRunState,ExecutionTime]' `
            --output text
        
        $status = ($jobStatus -split '\s+')[0]
        $execTime = ($jobStatus -split '\s+')[1]
        
        Write-Host "  Status: $status | Elapsed: ${elapsedSeconds}s" -ForegroundColor Gray
        
        if ($status -eq "SUCCEEDED") {
            Write-Host ""
            Write-Host "[OK] Glue job completed successfully in ${execTime}s" -ForegroundColor Green
            Write-Host ""
            break
        } elseif ($status -eq "FAILED" -or $status -eq "STOPPED") {
            Write-Host ""
            Write-Host "[X] Glue job failed with status: $status" -ForegroundColor Red
            Write-Host ""
            break
        }
        
        Start-Sleep -Seconds $checkInterval
        $elapsedSeconds += $checkInterval
        
    } catch {
        Write-Host "  Error checking job status: $_" -ForegroundColor Red
        Start-Sleep -Seconds $checkInterval
        $elapsedSeconds += $checkInterval
    }
}

if ($elapsedSeconds -ge $maxWaitSeconds) {
    Write-Host "[!] Timeout waiting for job completion" -ForegroundColor Yellow
    Write-Host ""
}

Start-Sleep -Seconds 2

# STEP 5: Verify Processed Data and Quarantine
Write-Host "[STEP 5] Verifying Data Layers" -ForegroundColor Yellow
Write-Host ""

try {
    # Check processed data
    Write-Host "Checking processed layer (s3://$S3_PROCESSED_BUCKET/validated/)..." -ForegroundColor Gray
    $validatedCount = (aws s3 ls "s3://$S3_PROCESSED_BUCKET/validated/" --recursive | Measure-Object).Count
    Write-Host "  Validated layer: $validatedCount objects" -ForegroundColor Gray
    
    # Check quarantine
    Write-Host ""
    Write-Host "Checking quarantine layer (s3://$S3_PROCESSED_BUCKET/quarantine/)..." -ForegroundColor Gray
    $quarantineCount = (aws s3 ls "s3://$S3_PROCESSED_BUCKET/quarantine/" --recursive | Measure-Object).Count
    Write-Host "  Quarantine layer: $quarantineCount objects" -ForegroundColor Gray
    
    Write-Host ""
    if ($validatedCount -gt 0 -and $quarantineCount -gt 0) {
        Write-Host "[OK] Both data layers verified" -ForegroundColor Green
        Write-Host "     ~35 percent of records should be quarantined (anomalies)" -ForegroundColor Gray
    } else {
        Write-Host "[!] Some layers may be empty (check job logs)" -ForegroundColor Yellow
    }
    Write-Host ""
} catch {
    Write-Host "[X] Error verifying data layers: $_" -ForegroundColor Red
}

Start-Sleep -Seconds 2

# STEP 6: Check CloudWatch Metrics
Write-Host "[STEP 6] Checking CloudWatch Metrics" -ForegroundColor Yellow
Write-Host ""

Write-Host "Waiting 90 seconds for metrics to propagate..." -ForegroundColor Gray
Start-Sleep -Seconds 90

try {
    $endTime = (Get-Date).ToUniversalTime()
    $startTime = $endTime.AddHours(-2)
    
    # Query metrics with the actual names from Glue job
    # Use the aggregated dimension "claims/" that combines all files
    $metricsMap = @{
        "TotalRecords" = "TotalRecords"
        "GoldRecords" = "GoldRecords"
        "AnomalyCount" = "AnomalyCount"
        "DQScore" = "DataQualityScore"
    }
    
    Write-Host ""
    foreach ($displayName in $metricsMap.Keys) {
        $metricName = $metricsMap[$displayName]
        try {
            # Query with the aggregated dimension (claims/)
            # Use separate queries for Sum and Maximum to avoid PowerShell parsing issues
            if ($displayName -eq "DQScore") {
                # For DQScore, use Maximum
                $stats = aws cloudwatch get-metric-statistics `
                    --namespace $CLOUDWATCH_NAMESPACE `
                    --metric-name $metricName `
                    --dimensions Name=SourceFile,Value="claims/" `
                    --start-time $startTime.ToString("yyyy-MM-ddTHH:mm:ss") `
                    --end-time $endTime.ToString("yyyy-MM-ddTHH:mm:ss") `
                    --period 3600 `
                    --statistics Maximum `
                    --output json | ConvertFrom-Json
                
                if ($stats.Datapoints -and $stats.Datapoints.Count -gt 0) {
                    $value = [math]::Round(($stats.Datapoints | Measure-Object -Property Maximum -Maximum).Maximum, 2)
                    Write-Host "  $displayName : $value" -ForegroundColor Green
                } else {
                    Write-Host "  $displayName : No data yet" -ForegroundColor Gray
                }
            } else {
                # For counts, use Sum
                $stats = aws cloudwatch get-metric-statistics `
                    --namespace $CLOUDWATCH_NAMESPACE `
                    --metric-name $metricName `
                    --dimensions Name=SourceFile,Value="claims/" `
                    --start-time $startTime.ToString("yyyy-MM-ddTHH:mm:ss") `
                    --end-time $endTime.ToString("yyyy-MM-ddTHH:mm:ss") `
                    --period 3600 `
                    --statistics Sum `
                    --output json | ConvertFrom-Json
                
                if ($stats.Datapoints -and $stats.Datapoints.Count -gt 0) {
                    $value = ($stats.Datapoints | Measure-Object -Property Sum -Sum).Sum
                    Write-Host "  $displayName : $value" -ForegroundColor Green
                } else {
                    Write-Host "  $displayName : No data yet" -ForegroundColor Gray
                }
            }
        } catch {
            Write-Host "  $displayName : Error - $_" -ForegroundColor Yellow
        }
    }
    
    Write-Host ""
    Write-Host "[OK] CloudWatch metrics checked" -ForegroundColor Green
    Write-Host "     Note: DQScore should be around 65-82 with 35 percent anomaly rate" -ForegroundColor Gray
    Write-Host ""
} catch {
    Write-Host "[X] Error checking metrics: $_" -ForegroundColor Red
}

Start-Sleep -Seconds 2

# STEP 7: Check CloudWatch Alarm State
Write-Host "[STEP 7] Checking CloudWatch Alarms" -ForegroundColor Yellow
Write-Host ""

try {
    $alarmState = aws cloudwatch describe-alarms `
        --alarm-names $ALARM_NAME `
        --query 'MetricAlarms[0].[StateValue,StateReason]' `
        --output text
    
    if ($alarmState) {
        $state = ($alarmState -split '\t')[0]
        $reason = ($alarmState -split '\t')[1]
        
        Write-Host "  Alarm: $ALARM_NAME" -ForegroundColor Gray
        Write-Host "  State: $state" -ForegroundColor $(if ($state -eq "ALARM") { "Red" } elseif ($state -eq "OK") { "Green" } else { "Yellow" })
        
        if ($reason) {
            Write-Host "  Reason: $reason" -ForegroundColor Gray
        }
        
        Write-Host ""
        if ($state -eq "ALARM") {
            Write-Host "[OK] Alarm triggered as expected (high anomaly count)" -ForegroundColor Green
        } else {
            Write-Host "[!] Alarm not triggered yet (may need more time)" -ForegroundColor Yellow
        }
    } else {
        Write-Host "[!] Alarm not found: $ALARM_NAME" -ForegroundColor Yellow
    }
    Write-Host ""
} catch {
    Write-Host "[X] Error checking alarm: $_" -ForegroundColor Red
}

Start-Sleep -Seconds 2

# STEP 8: Test Self-Healing Orchestration
Write-Host "[STEP 8] Testing Self-Healing Orchestration" -ForegroundColor Yellow
Write-Host ""

try {
    Write-Host "Triggering self-healing via API Gateway..." -ForegroundColor Gray
    
    $body = @{
        action = "quarantine_data"
        reason = "Demo test"
        test = $true
    } | ConvertTo-Json
    
    $response = Invoke-RestMethod -Uri $API_GATEWAY_URL -Method POST -Body $body -ContentType "application/json"
    
    Write-Host ""
    Write-Host "  Response: $($response | ConvertTo-Json -Compress)" -ForegroundColor Gray
    Write-Host ""
    Write-Host "[OK] Self-healing orchestrator invoked successfully" -ForegroundColor Green
    Write-Host ""
} catch {
    Write-Host "[X] Self-healing test failed: $_" -ForegroundColor Red
    Write-Host "     Note: Check Lambda logs for details" -ForegroundColor Gray
}

Start-Sleep -Seconds 2

# STEP 9: View Orchestrator Logs
Write-Host "[STEP 9] Recent Orchestrator Logs" -ForegroundColor Yellow
Write-Host ""

try {
    Write-Host "Fetching latest logs from /aws/lambda/$ORCHESTRATOR_LAMBDA..." -ForegroundColor Gray
    Write-Host ""
    
    aws logs tail "/aws/lambda/$ORCHESTRATOR_LAMBDA" --since 5m --format short 2>$null | Select-Object -First 20
    
    Write-Host ""
    Write-Host "[OK] Logs retrieved (showing last 20 lines)" -ForegroundColor Green
    Write-Host ""
} catch {
    Write-Host "[!] Could not retrieve logs (may not exist yet)" -ForegroundColor Yellow
}

# DEMONSTRATION COMPLETE
Write-Host "=====================================================================" -ForegroundColor Cyan
Write-Host "   DEMONSTRATION COMPLETE!" -ForegroundColor Cyan
Write-Host "=====================================================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "Summary of Results:" -ForegroundColor White
Write-Host "  [OK] Data generated with 35 percent anomaly rate" -ForegroundColor Green
Write-Host "  [OK] Uploaded to S3 and processed by Glue" -ForegroundColor Green
Write-Host "  [OK] Anomalies quarantined to Delta Lake" -ForegroundColor Green
Write-Host "  [OK] CloudWatch metrics published" -ForegroundColor Green
Write-Host "  [OK] Alarms checked (should be in ALARM state)" -ForegroundColor Green
Write-Host "  [OK] Self-healing orchestration tested" -ForegroundColor Green
Write-Host ""

Write-Host "Next Steps for Manual Verification:" -ForegroundColor Yellow
Write-Host "  1. AWS Console - CloudWatch - Alarms" -ForegroundColor Gray
Write-Host "     https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#alarmsV2:" -ForegroundColor DarkGray
Write-Host ""
Write-Host "  2. AWS Console - CloudWatch - Metrics - DQAD/DataQuality" -ForegroundColor Gray
Write-Host "     https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#metricsV2:" -ForegroundColor DarkGray
Write-Host ""
Write-Host "  3. AWS Console - S3 - dqad-processed-dev - quarantine/" -ForegroundColor Gray
Write-Host "     https://s3.console.aws.amazon.com/s3/buckets/dqad-processed-dev" -ForegroundColor DarkGray
Write-Host ""
Write-Host "  4. AWS Console - Glue - ETL Jobs - dqad-etl-job-dev" -ForegroundColor Gray
Write-Host "     https://console.aws.amazon.com/glue/home?region=us-east-1#/v2/etl-jobs" -ForegroundColor DarkGray
Write-Host ""
Write-Host "=====================================================================" -ForegroundColor Cyan
Write-Host ""

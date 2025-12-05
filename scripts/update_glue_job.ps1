# Update Glue ETL job with latest code
# This uploads the updated dqad_etl_job.py to S3 so Glue uses the new version

$ErrorActionPreference = "Stop"

$S3_BUCKET = "dqad-processed-dev"
$GLUE_SCRIPT_PATH = "..\glue\dqad_etl_job.py"
$S3_KEY = "scripts/dqad_etl_job.py"

Write-Host "==============================================" -ForegroundColor Cyan
Write-Host "  DQAD - Update Glue Job Script" -ForegroundColor Cyan
Write-Host "==============================================" -ForegroundColor Cyan
Write-Host ""

# Upload updated script to S3
Write-Host "[STEP 1] Uploading updated Glue script to S3..." -ForegroundColor Yellow
Write-Host "  Source: $GLUE_SCRIPT_PATH" -ForegroundColor Gray
Write-Host "  Target: s3://$S3_BUCKET/$S3_KEY" -ForegroundColor Gray

try {
    aws s3 cp $GLUE_SCRIPT_PATH "s3://$S3_BUCKET/$S3_KEY"
    Write-Host "[OK] Script uploaded successfully" -ForegroundColor Green
    Write-Host ""
} catch {
    Write-Host "[X] Upload failed: $_" -ForegroundColor Red
    exit 1
}

# Verify upload
Write-Host "[STEP 2] Verifying upload..." -ForegroundColor Yellow
try {
    $result = aws s3 ls "s3://$S3_BUCKET/$S3_KEY"
    if ($result) {
        Write-Host "[OK] Script verified in S3" -ForegroundColor Green
        Write-Host "  $result" -ForegroundColor Gray
    }
} catch {
    Write-Host "[X] Verification failed: $_" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "==============================================" -ForegroundColor Green
Write-Host "  Glue job script updated successfully!" -ForegroundColor Green
Write-Host "  Next Glue run will use the new code" -ForegroundColor Green
Write-Host "==============================================" -ForegroundColor Green
Write-Host ""
Write-Host "Changes in this update:" -ForegroundColor Cyan
Write-Host "  • Publishes individual file metrics (e.g., claims/file.csv)" -ForegroundColor White
Write-Host "  • ALSO publishes aggregated metrics (e.g., claims/)" -ForegroundColor White
Write-Host "  • Both dashboards and scripts can now query aggregated data" -ForegroundColor White
Write-Host ""

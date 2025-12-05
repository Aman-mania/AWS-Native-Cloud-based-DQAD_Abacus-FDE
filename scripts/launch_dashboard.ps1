# Launch DQAD Dashboard with Mock Mode
# Use this when AWS is not configured

Write-Host "=== DQAD Dashboard Launcher ===" -ForegroundColor Cyan
Write-Host ""

# Set mock mode
$env:DQAD_MOCK_MODE = "true"
$env:AWS_DEFAULT_REGION = "us-east-1"

Write-Host "✓ Mock mode enabled" -ForegroundColor Green
Write-Host "✓ AWS region set to us-east-1" -ForegroundColor Green
Write-Host ""
Write-Host "Launching Streamlit dashboard..." -ForegroundColor Yellow
Write-Host "Dashboard will display sample data since AWS is not configured." -ForegroundColor Gray
Write-Host ""

# Change to dashboard directory
$dashboardPath = Join-Path $PSScriptRoot "..\dashboard"
Set-Location $dashboardPath

# Launch Streamlit
streamlit run app.py

# ============================================================================
# DQAD Mock Flow - Local Dashboard Testing
# ============================================================================
# This script demonstrates the mock mode workflow for local development:
# 1. Set mock mode environment variables
# 2. Launch Streamlit dashboard with simulated data
# 3. No AWS resources used, no costs incurred
# ============================================================================

Write-Host "=====================================================================" -ForegroundColor Cyan
Write-Host "   DQAD MOCK FLOW - Local Dashboard Testing" -ForegroundColor Cyan
Write-Host "=====================================================================" -ForegroundColor Cyan
Write-Host ""

# ============================================================================
# STEP 1: Validate Prerequisites
# ============================================================================
Write-Host "[STEP 1] Validating prerequisites..." -ForegroundColor Yellow
Write-Host ""

# Check Python
try {
    $pythonVersion = python --version 2>&1
    Write-Host "[OK] Python installed: $pythonVersion" -ForegroundColor Green
} catch {
    Write-Host "[X] Python not found!" -ForegroundColor Red
    Write-Host "    Install from: https://www.python.org/downloads/" -ForegroundColor Yellow
    exit 1
}

# Check if dashboard directory exists
if (-Not (Test-Path "../dashboard/app.py")) {
    Write-Host "[X] Dashboard not found!" -ForegroundColor Red
    Write-Host "    Expected: ../dashboard/app.py" -ForegroundColor Yellow
    exit 1
}

Write-Host "[OK] Dashboard found: ../dashboard/app.py" -ForegroundColor Green
Write-Host ""

# ============================================================================
# STEP 2: Install Dashboard Dependencies
# ============================================================================
Write-Host "[STEP 2] Checking dashboard dependencies..." -ForegroundColor Yellow
Write-Host ""

cd ../dashboard

# Check Python version for compatibility
$pythonVersion = python --version 2>&1
if ($pythonVersion -match "Python 3\.13") {
    Write-Host "  Detected Python 3.13 - using compatible package versions..." -ForegroundColor Gray
    Write-Host "  Installing: streamlit pandas plotly boto3..." -ForegroundColor Gray
    
    # Install with compatible versions for Python 3.13
    pip install streamlit --quiet
    pip install "pandas>=2.0.0" --quiet
    pip install plotly --quiet
    pip install boto3 --quiet
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "[OK] Dependencies installed successfully" -ForegroundColor Green
    } else {
        Write-Host "[!] Some dependencies may have warnings (continuing anyway)" -ForegroundColor Yellow
    }
} elseif (Test-Path "requirements.txt") {
    Write-Host "  Installing dependencies from requirements.txt..." -ForegroundColor Gray
    pip install -r requirements.txt --quiet
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "[OK] Dependencies installed" -ForegroundColor Green
    } else {
        Write-Host "[!] Some dependencies may have failed, trying core packages..." -ForegroundColor Yellow
        pip install streamlit pandas plotly boto3 --quiet
    }
} else {
    Write-Host "  Installing core dependencies..." -ForegroundColor Gray
    pip install streamlit pandas plotly boto3 --quiet
    Write-Host "[OK] Core dependencies installed" -ForegroundColor Green
}
Write-Host ""

# ============================================================================
# STEP 3: Configure Mock Mode Environment
# ============================================================================
Write-Host "[STEP 3] Configuring mock mode environment..." -ForegroundColor Yellow
Write-Host ""

# Set environment variables
$env:DQAD_MOCK_MODE = "true"
$env:AWS_DEFAULT_REGION = "us-east-1"

Write-Host "[OK] Environment configured:" -ForegroundColor Green
Write-Host "     DQAD_MOCK_MODE = $env:DQAD_MOCK_MODE" -ForegroundColor Gray
Write-Host "     AWS_DEFAULT_REGION = $env:AWS_DEFAULT_REGION" -ForegroundColor Gray
Write-Host ""

Write-Host "NOTE: Mock mode enabled - no AWS credentials required!" -ForegroundColor Cyan
Write-Host "      All data will be simulated/fake." -ForegroundColor Cyan
Write-Host ""

# ============================================================================
# STEP 4: Launch Dashboard
# ============================================================================
Write-Host "[STEP 4] Launching Streamlit dashboard..." -ForegroundColor Yellow
Write-Host ""

Write-Host "  Dashboard features in mock mode:" -ForegroundColor Gray
Write-Host "    - Cost metrics (fake: ~$0.45/day)" -ForegroundColor Gray
Write-Host "    - Anomaly count trends (random 50-150)" -ForegroundColor Gray
Write-Host "    - Data quality score (random 80-100%)" -ForegroundColor Gray
Write-Host "    - Anomaly type breakdown (pie chart)" -ForegroundColor Gray
Write-Host "    - Latest anomalies table (sample data)" -ForegroundColor Gray
Write-Host "    - Orchestrator logs (sample entries)" -ForegroundColor Gray
Write-Host "    - Action buttons (simulated)" -ForegroundColor Gray
Write-Host ""

Write-Host "  Starting Streamlit server..." -ForegroundColor Gray
Write-Host "  Browser will open automatically at: http://localhost:8501" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Press Ctrl+C to stop the dashboard" -ForegroundColor Yellow
Write-Host ""

# Launch Streamlit - use python -m streamlit for better compatibility
try {
    python -m streamlit run app.py
} catch {
    Write-Host ""
    Write-Host "[X] Failed to start Streamlit" -ForegroundColor Red
    Write-Host "    Trying alternative launch method..." -ForegroundColor Yellow
    streamlit run app.py
}

# Clean up environment after exit
$env:DQAD_MOCK_MODE = $null
$env:AWS_DEFAULT_REGION = $null

cd ../scripts
Write-Host ""
Write-Host "=====================================================================" -ForegroundColor Cyan
Write-Host "   Dashboard stopped" -ForegroundColor Cyan
Write-Host "=====================================================================" -ForegroundColor Cyan

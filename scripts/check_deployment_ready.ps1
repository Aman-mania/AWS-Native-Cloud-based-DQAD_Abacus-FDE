# Deployment Readiness Checker for DQAD Platform

Write-Host "==============================================" -ForegroundColor Cyan
Write-Host "DQAD Platform - Deployment Readiness Check" -ForegroundColor Cyan
Write-Host "==============================================" -ForegroundColor Cyan
Write-Host ""

$allGood = $true

# Check 1: Git Repository
Write-Host "Checking Git repository..." -ForegroundColor Yellow
if (Test-Path ".git") {
    Write-Host "[PASS] Git repository initialized" -ForegroundColor Green
    
    # Check remote
    $remote = git remote get-url origin 2>$null
    if ($remote) {
        Write-Host "[PASS] Remote repository: $remote" -ForegroundColor Green
    } else {
        Write-Host "[FAIL] No remote repository configured" -ForegroundColor Red
        Write-Host "  Run: git remote add origin https://github.com/Aman-mania/Abacus-FDE.git" -ForegroundColor Yellow
        $allGood = $false
    }
} else {
    Write-Host "[FAIL] Not a git repository" -ForegroundColor Red
    $allGood = $false
}
Write-Host ""

# Check 2: Required Files
Write-Host "Checking required files..." -ForegroundColor Yellow
$requiredFiles = @(
    "dashboard/app.py",
    ".streamlit/config.toml",
    ".streamlit/secrets.toml.example",
    ".github/workflows/deploy-pipeline.yml",
    "QUICK_DEPLOY.md"
)

foreach ($file in $requiredFiles) {
    if (Test-Path $file) {
        Write-Host "[PASS] $file" -ForegroundColor Green
    } else {
        Write-Host "[FAIL] Missing: $file" -ForegroundColor Red
        $allGood = $false
    }
}
Write-Host ""

# Check 3: AWS CLI
Write-Host "Checking AWS CLI..." -ForegroundColor Yellow
$awsVersion = aws --version 2>$null
if ($awsVersion) {
    Write-Host "[PASS] AWS CLI installed: $awsVersion" -ForegroundColor Green
    
    # Check AWS credentials
    $identity = aws sts get-caller-identity 2>$null | ConvertFrom-Json
    if ($identity) {
        Write-Host "[PASS] AWS credentials configured" -ForegroundColor Green
        Write-Host "  Account: $($identity.Account)" -ForegroundColor Gray
        Write-Host "  User: $($identity.Arn)" -ForegroundColor Gray
    } else {
        Write-Host "[FAIL] AWS credentials not configured" -ForegroundColor Red
        Write-Host "  Run: aws configure" -ForegroundColor Yellow
        $allGood = $false
    }
} else {
    Write-Host "[FAIL] AWS CLI not installed" -ForegroundColor Red
    $allGood = $false
}
Write-Host ""

# Check 4: Python Dependencies
Write-Host "Checking Python environment..." -ForegroundColor Yellow
$pythonVersion = python --version 2>$null
if ($pythonVersion) {
    Write-Host "[PASS] Python installed: $pythonVersion" -ForegroundColor Green
    
    # Check if virtual environment is activated
    if ($env:VIRTUAL_ENV) {
        Write-Host "[PASS] Virtual environment activated" -ForegroundColor Green
    } else {
        Write-Host "[WARN] Virtual environment not activated" -ForegroundColor Yellow
        Write-Host "  Run: .\venv\Scripts\Activate.ps1" -ForegroundColor Yellow
    }
} else {
    Write-Host "[FAIL] Python not installed" -ForegroundColor Red
    $allGood = $false
}
Write-Host ""

# Check 5: Streamlit
Write-Host "Checking Streamlit..." -ForegroundColor Yellow
$streamlitVersion = streamlit --version 2>$null
if ($streamlitVersion) {
    Write-Host "[PASS] Streamlit installed" -ForegroundColor Green
} else {
    Write-Host "[FAIL] Streamlit not installed" -ForegroundColor Red
    Write-Host "  Run: pip install -r dashboard/requirements.txt" -ForegroundColor Yellow
    $allGood = $false
}
Write-Host ""

# Summary
Write-Host "==============================================" -ForegroundColor Cyan
if ($allGood) {
    Write-Host "[SUCCESS] ALL CHECKS PASSED - READY TO DEPLOY!" -ForegroundColor Green
    Write-Host ""
    Write-Host "Next steps:" -ForegroundColor Cyan
    Write-Host "1. Push to GitHub:" -ForegroundColor White
    Write-Host "   git add ." -ForegroundColor Gray
    Write-Host "   git commit -m 'Add deployment configurations'" -ForegroundColor Gray
    Write-Host "   git push origin main" -ForegroundColor Gray
    Write-Host ""
    Write-Host "2. Deploy dashboard:" -ForegroundColor White
    Write-Host "   Visit: https://share.streamlit.io/" -ForegroundColor Gray
    Write-Host ""
    Write-Host "3. Setup GitHub Actions:" -ForegroundColor White
    Write-Host "   Visit: https://github.com/Aman-mania/Abacus-FDE/settings/secrets/actions" -ForegroundColor Gray
    Write-Host ""
    Write-Host "See QUICK_DEPLOY.md for detailed instructions." -ForegroundColor Cyan
} else {
    Write-Host "[FAILED] SOME CHECKS FAILED - FIX ISSUES ABOVE" -ForegroundColor Red
    Write-Host ""
    Write-Host "Review the errors above and fix them before deploying." -ForegroundColor Yellow
}
Write-Host "==============================================" -ForegroundColor Cyan

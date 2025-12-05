# Package Lambda Functions with Dependencies
# Creates deployment.zip files for Lambda functions that require external libraries

param(
    [string]$FunctionName = "orchestrator"
)

$ErrorActionPreference = "Stop"

Write-Host "=====================================================================`n" -ForegroundColor Cyan
Write-Host "   Lambda Packaging Script - $FunctionName" -ForegroundColor Cyan
Write-Host "`n=====================================================================" -ForegroundColor Cyan

$LambdaDir = "..\lambda\$FunctionName"
$DeploymentZip = "$LambdaDir\deployment.zip"
$TempDir = "$LambdaDir\package"

# Check if function directory exists
if (-not (Test-Path $LambdaDir)) {
    Write-Host "[ERROR] Lambda function directory not found: $LambdaDir" -ForegroundColor Red
    exit 1
}

# Check if requirements.txt exists
if (-not (Test-Path "$LambdaDir\requirements.txt")) {
    Write-Host "[SKIP] No requirements.txt found for $FunctionName" -ForegroundColor Yellow
    Write-Host "       Creating simple deployment package without dependencies..." -ForegroundColor Yellow
    
    # Simple zip for functions without dependencies
    Remove-Item -Path $DeploymentZip -ErrorAction SilentlyContinue
    Compress-Archive -Path "$LambdaDir\app.py" -DestinationPath $DeploymentZip -Force
    
    Write-Host "[OK] Deployment package created: deployment.zip" -ForegroundColor Green
    exit 0
}

Write-Host "[STEP 1] Cleaning previous build..." -ForegroundColor Cyan
Remove-Item -Path $TempDir -Recurse -ErrorAction SilentlyContinue
Remove-Item -Path $DeploymentZip -ErrorAction SilentlyContinue
New-Item -Path $TempDir -ItemType Directory -Force | Out-Null
Write-Host "[OK] Clean build directory created`n" -ForegroundColor Green

Write-Host "[STEP 2] Installing dependencies..." -ForegroundColor Cyan
Write-Host "  Target: $TempDir`n" -ForegroundColor Gray

# Install dependencies to package directory
pip install -r "$LambdaDir\requirements.txt" -t $TempDir --quiet --disable-pip-version-check

if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Failed to install dependencies" -ForegroundColor Red
    exit 1
}

Write-Host "[OK] Dependencies installed`n" -ForegroundColor Green

Write-Host "[STEP 3] Copying Lambda code..." -ForegroundColor Cyan
Copy-Item -Path "$LambdaDir\app.py" -Destination $TempDir -Force
Write-Host "[OK] Lambda function code copied`n" -ForegroundColor Green

Write-Host "[STEP 4] Creating deployment package..." -ForegroundColor Cyan

# Change to temp directory to create clean zip
Push-Location $TempDir

# Create zip with all contents
Compress-Archive -Path * -DestinationPath ..\deployment.zip -Force

Pop-Location

$ZipSize = (Get-Item $DeploymentZip).Length / 1MB
Write-Host "[OK] Deployment package created: deployment.zip ($([math]::Round($ZipSize, 2)) MB)`n" -ForegroundColor Green

Write-Host "[STEP 5] Cleaning up..." -ForegroundColor Cyan
Remove-Item -Path $TempDir -Recurse -Force
Write-Host "[OK] Temporary files removed`n" -ForegroundColor Green

Write-Host "=====================================================================" -ForegroundColor Cyan
Write-Host "   PACKAGING COMPLETE!" -ForegroundColor Green
Write-Host "=====================================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Deployment package: $DeploymentZip" -ForegroundColor White
Write-Host "Size: $([math]::Round($ZipSize, 2)) MB" -ForegroundColor White
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "  1. cd ..\infra\terraform" -ForegroundColor Gray
Write-Host "  2. terraform apply" -ForegroundColor Gray
Write-Host ""

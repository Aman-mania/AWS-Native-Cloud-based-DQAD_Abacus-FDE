Write-Host "`n=== DQAD DEPENDENCY INSTALLER STARTED ===`n"

# -------------------------------
# 0. Ensure project directory exists
# -------------------------------
$projectRoot = "C:\Users\amanb\Desktop\Academic Projects\Abacus FDE"
if (-Not (Test-Path $projectRoot)) {
    Write-Host "Project directory not found: $projectRoot"
    exit
}
Set-Location $projectRoot

# -------------------------------
# 1. Check Python installation
# -------------------------------
Write-Host "Checking Python installation..."
$pythonExists = (Get-Command python -ErrorAction SilentlyContinue)

if (-Not $pythonExists) {
    Write-Host "Python is not installed. Install from https://www.python.org/downloads/"
    exit
} else {
    Write-Host "Python found."
}

# -------------------------------
# 2. Create virtual environment
# -------------------------------
$venvPath = "$projectRoot\venv"

if (-Not (Test-Path $venvPath)) {
    Write-Host "Creating virtual environment..."
    python -m venv $venvPath
} else {
    Write-Host "Virtual environment already exists."
}

Write-Host "Activating virtual environment..."
& "$venvPath\Scripts\Activate.ps1"

# -------------------------------
# 3. Install Python dependencies
# -------------------------------
Write-Host "`nInstalling Python dependencies..."

$requirements = @(
    "streamlit",
    "pyspark",
    "pandas",
    "numpy",
    "boto3",
    "plotly",
    "watchdog",
    "requests"
)

foreach ($pkg in $requirements) {
    Write-Host "Installing $pkg..."
    pip install $pkg --quiet
}

Write-Host "Python dependencies installed."

# -------------------------------
# 4. Check AWS CLI
# -------------------------------
Write-Host "`nChecking AWS CLI..."
$awsExists = (Get-Command aws -ErrorAction SilentlyContinue)

if (-Not $awsExists) {
    Write-Host "AWS CLI not found. Installing..."

    $installer = "$env:TEMP\aws_installer.msi"
    Invoke-WebRequest -Uri "https://awscli.amazonaws.com/AWSCLIV2.msi" -OutFile $installer

    Start-Process msiexec.exe -ArgumentList "/i `"$installer`" /qn" -Wait

    Write-Host "AWS CLI installed."
} else {
    Write-Host "AWS CLI found."
}

# -------------------------------
# 5. Check Terraform
# -------------------------------
Write-Host "`nChecking Terraform..."
$terraformExists = (Get-Command terraform -ErrorAction SilentlyContinue)

if (-Not $terraformExists) {
    Write-Host "Terraform not found. Installing..."

    $zipUrl = "https://releases.hashicorp.com/terraform/1.5.7/terraform_1.5.7_windows_amd64.zip"
    $zipPath = "$env:TEMP\terraform.zip"
    $installPath = "$env:LOCALAPPDATA\Terraform"

    Invoke-WebRequest -Uri $zipUrl -OutFile $zipPath

    if (-Not (Test-Path $installPath)) {
        New-Item -ItemType Directory -Path $installPath | Out-Null
    }

    Expand-Archive $zipPath -DestinationPath $installPath -Force

    # Update PATH safely
    $currentPath = [Environment]::GetEnvironmentVariable("Path", "User")
    if ($currentPath -notlike "*$installPath*") {
        $newPath = "$currentPath;$installPath"
        [Environment]::SetEnvironmentVariable("Path", $newPath, "User")
    }

    Write-Host "Terraform installed."
} else {
    Write-Host "Terraform found."
}

# -------------------------------
# 6. Validation summary
# -------------------------------
Write-Host "`nValidating installations..."

python --version
pip --version
aws --version
terraform --version
streamlit --version

Write-Host "`n=== ALL DEPENDENCIES INSTALLED SUCCESSFULLY ===`n"

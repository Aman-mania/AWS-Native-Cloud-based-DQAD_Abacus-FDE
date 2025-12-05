# DQAD Platform - Data Quality Anomaly Detection

**Automated healthcare claims validation with real-time monitoring and self-healing capabilities**

## Overview

DQAD is a serverless AWS-native platform that validates healthcare payer claims through 50+ data quality rules and statistical anomaly detection. It processes 100K+ claims in under 60 seconds while maintaining operational costs under $1/month.

### Key Features

- **50+ DQ Validation Rules**: Null checks, format validation, business logic, temporal validation
- **Statistical Anomaly Detection**: Z-score based outlier detection grouped by procedure type (CPT codes)
- **3-Tier Data Architecture**: Gold (clean), Silver (DQ failures), Quarantine (statistical outliers)
- **Real-Time Monitoring**: CloudWatch metrics with automated alarms
- **Self-Healing**: Lambda orchestrator for automated remediation
- **Cost-Optimized**: $0.60/month running costs (250x cheaper than industry standard)

### Architecture
<img width="2816" height="1536" alt="Gemini_Generated_Image_ujdo6vujdo6vujdo" src="https://github.com/user-attachments/assets/3ede2478-0379-4f55-a798-a7d59de30a4b" />


## Prerequisites

### Required
- **AWS Account** with free-tier eligible resources
- **AWS CLI** configured with credentials (`aws configure`)
- **Terraform** >= 1.0
- **Python** >= 3.9
- **PowerShell** (for Windows) or Bash (for Linux/Mac)

### Optional
- **Docker** (for containerized development)
- **Git** (for version control)

---

## Installation

### 1. Clone Repository

```bash
git clone https://github.com/Aman-mania/Abacus-FDE.git
cd Abacus-FDE
```

### 2. Install Python Dependencies

**Windows (PowerShell):**
```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r dashboard/requirements.txt
pip install -r data/requirements.txt
pip install -r lambda/orchestrator/requirements.txt
pip install -r lambda/cost_collector/requirements.txt
```

**Linux/Mac:**
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r dashboard/requirements.txt
pip install -r data/requirements.txt
pip install -r lambda/orchestrator/requirements.txt
pip install -r lambda/cost_collector/requirements.txt
```

### 3. Configure Terraform Variables

```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars
```

**Edit `terraform.tfvars`:**
```hcl
aws_region  = "us-east-1"
environment = "dev"
project_name = "dqad"
alert_email = "your-email@example.com"  # REQUIRED: Change this!
cost_threshold_usd = 2.0
anomaly_threshold  = 100
```

### 4. Deploy Infrastructure

```bash
# Initialize Terraform
terraform init

# Preview changes
terraform plan

# Deploy (creates 76 AWS resources)
terraform apply
```

**Expected Output:**
```
Apply complete! Resources: 76 added, 0 changed, 0 destroyed.

Outputs:
api_endpoint = "https://xxxxxxxxxx.execute-api.us-east-1.amazonaws.com/prod"
glue_job_name = "dqad-etl-job-dev"
raw_bucket = "dqad-raw-dev"
processed_bucket = "dqad-processed-dev"
```

⚠️ **IMPORTANT**: Check your email and confirm the SNS subscription before proceeding.

---

## Running the Platform

### Option 1: Full Automated Demo (Recommended)

**Windows:**
```powershell
cd scripts
.\full_demo.ps1
```

**Linux/Mac:**
```bash
cd scripts
chmod +x full_demo.sh
./full_demo.sh
```

**What it does:**
1. Generates 8,000+ synthetic claims with 35% anomaly rate
2. Uploads to S3 raw bucket
3. Triggers Glue ETL job
4. Monitors execution progress
5. Validates 3-tier output (Gold/Silver/Quarantine)
6. Checks CloudWatch metrics
7. Verifies alarm states
8. Tests self-healing orchestration

**Expected Runtime:** 3-5 minutes

---

### Option 2: Manual Step-by-Step Execution

#### Step 1: Generate Synthetic Claims Data

```bash
cd data
python generate_payer_data.py
```

**Output:** Creates 3 CSV files in `raw_data/` with ~1,000 claims each

#### Step 2: Upload to S3

```bash
aws s3 cp raw_data/ s3://dqad-raw-dev/claims/ --recursive --exclude "*" --include "*.csv"
```

#### Step 3: Trigger Glue Job

**Via AWS CLI:**
```bash
aws glue start-job-run --job-name dqad-etl-job-dev \
  --arguments '{
    "--S3_INPUT_KEY":"claims/payer_claims_YYYYMMDD_HHMMSS.csv"
  }'
```

**Via API Gateway:**
```bash
curl -X POST https://YOUR_API_ENDPOINT/prod/trigger \
  -H "Content-Type: application/json" \
  -d '{"action": "trigger_glue"}'
```

#### Step 4: Monitor Execution

```bash
# Check job status
aws glue get-job-run --job-name dqad-etl-job-dev --run-id jr_XXXXX

# View CloudWatch logs
aws logs tail /aws-glue/jobs/output --follow --since 5m
```

#### Step 5: Verify Outputs

```bash
# Check Gold tier (clean data)
aws s3 ls s3://dqad-processed-dev/gold/ --recursive

# Check Silver tier (DQ failures)
aws s3 ls s3://dqad-processed-dev/silver/ --recursive

# Check Quarantine tier (statistical outliers)
aws s3 ls s3://dqad-processed-dev/quarantine/ --recursive
```

#### Step 6: Query CloudWatch Metrics

```bash
aws cloudwatch get-metric-statistics \
  --namespace DQAD/DataQuality \
  --metric-name DataQualityScore \
  --dimensions Name=Environment,Value=dev \
  --start-time 2025-12-05T00:00:00Z \
  --end-time 2025-12-05T23:59:59Z \
  --period 3600 \
  --statistics Average
```

---

### Option 3: Real-Time Dashboard

```bash
cd dashboard
streamlit run app.py
```

**Access:** http://localhost:8501

**Dashboard Features:**
- Real-time data quality metrics
- Anomaly count trends (7-day window)
- AWS cost tracking
- Alert status monitoring

---

## Configuration Options

### Adjust Anomaly Rate for Testing

**Edit `data/generate_payer_data.py`:**
```python
generator = PayerClaimGenerator(anomaly_rate=0.35)  # 35% anomalies
```

- **Low (10-15%)**: Normal production simulation
- **High (30-40%)**: Alarm testing (triggers CloudWatch alarms)

### Modify DQ Thresholds

**Edit `glue/dqad_etl_job.py`:**
```python
DQ_THRESHOLDS = {
    "min_claim_amount": 0,
    "max_claim_amount": 1000000,
    "max_days_to_submission": 365,
    "npi_length": 10
}
```

### Update CloudWatch Alarm Thresholds

**Edit `infra/terraform/terraform.tfvars`:**
```hcl
anomaly_threshold  = 100    # Trigger when anomalies > 100
cost_threshold_usd = 2.0    # Trigger when daily cost > $2
```

Then redeploy:
```bash
cd infra/terraform
terraform apply
```

---

## Troubleshooting

### Issue: Glue Job Fails with "AccessDenied"

**Solution:** Update IAM permissions in `infra/terraform/glue.tf`:
```hcl
Resource = [
  "${aws_s3_bucket.dqad_raw.arn}/*",
  "${aws_s3_bucket.dqad_processed.arn}/*",
  "${aws_s3_bucket.dqad_logs.arn}/*"  # Ensure this exists
]
```

### Issue: Dashboard Shows "No Data"

**Solution:** 
1. Verify Glue job completed successfully
2. Check CloudWatch metrics exist:
   ```bash
   aws cloudwatch list-metrics --namespace DQAD/DataQuality
   ```
3. Ensure dashboard queries correct dimension (file-level vs aggregated)

### Issue: No Email Alerts Received

**Solution:**
1. Confirm SNS subscription in email
2. Check spam folder
3. Verify `alert_email` in `terraform.tfvars`
4. Redeploy: `terraform apply`

### Issue: Terraform "Resource Already Exists"

**Solution:**
```bash
# Import existing resource
terraform import aws_s3_bucket.dqad_raw dqad-raw-dev

# Or destroy and recreate
terraform destroy
terraform apply
```

---

## Cost Breakdown

| Component | Monthly Cost | Notes |
|-----------|--------------|-------|
| S3 Storage | $0.15 | ~10GB with Parquet compression |
| Glue Job | $0.40 | 2 G.1X workers, 1 run/day |
| CloudWatch | $0.03 | 7 metrics, batch publishing |
| Lambda | $0.02 | 128MB, <1s execution |
| **TOTAL** | **$0.60** | Free-tier eligible |

**Scaling:** 1M claims/day ≈ $15/month (10 G.2X workers)

## Technical Highlights

### Data Quality Rules (50+)

**Categories:**
1. **Null Checks** (5): member_id, provider_npi, cpt_code, icd10_code, claim_amount
2. **Format Validation** (3): NPI (10 digits), ZIP (5 or 9 digits), Gender (M/F/U)
3. **Amount Logic** (2): Negative amounts, excessive amounts (>$1M)
4. **Date Logic** (3): Future dates, late submission, submission before service
5. **Status Validation** (1): Valid claim statuses (PAID/DENIED/PENDING)

### Statistical Anomaly Detection

**Method:** Z-Score grouped by CPT procedure code

```python
z_score = (claim_amount - avg_amount_per_cpt) / stddev_amount
is_outlier = abs(z_score) > 3  # 99.7% confidence
```

**Example:**
- CPT 99213 (Office Visit): Avg $185, StdDev $42
- Claim Amount: $920
- Z-Score: 17.5 → **QUARANTINED** (fraud indicator)

### Self-Healing Actions

1. **Quarantine Data**: Moves suspicious files to isolation
2. **Restart Job**: Retries failed Glue jobs (transient errors)
3. **Send Alert**: SNS notifications to on-call team

---

## API Reference

### Trigger Glue Job

```bash
POST https://YOUR_API_ENDPOINT/prod/trigger
Content-Type: application/json

{
  "action": "trigger_glue",
  "input_key": "claims/payer_claims_20251205.csv"
}
```

### Trigger Self-Healing

```bash
POST https://YOUR_API_ENDPOINT/prod/trigger
Content-Type: application/json

{
  "action": "quarantine",
  "source_file": "claims/payer_claims_20251205.csv"
}
```

---

## Contributing

1. Fork the repository
2. Create feature branch: `git checkout -b feature/amazing-feature`
3. Commit changes: `git commit -m 'Add amazing feature'`
4. Push to branch: `git push origin feature/amazing-feature`
5. Open Pull Request

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## Cleanup

**To destroy all AWS resources:**

```bash
cd infra/terraform
terraform destroy
```

⚠️ **Warning:** This will permanently delete all data in S3 buckets. Backup if needed.

---

## Support

For issues or questions:
- Open a GitHub Issue (Screenshot)
- Email: amanbiswakarma.ak@gmail.com
- College_mail: 112215015@cse.iiitp.ac.in

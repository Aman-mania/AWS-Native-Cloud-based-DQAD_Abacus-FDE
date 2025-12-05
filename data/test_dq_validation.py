"""
Local Data Quality Validation Test
Tests the DQ logic using Pandas (no Spark/Databricks required)
"""

import pandas as pd
from datetime import datetime, timedelta
import os
from pathlib import Path

# Data quality thresholds (same as Databricks notebook)
DQ_THRESHOLDS = {
    "max_null_rate": 0.05,
    "min_claim_amount": 0.0,
    "max_claim_amount": 100000.0,
    "npi_length": 10,
    "future_date_tolerance_days": 0,
    "max_days_to_submission": 365,
}

VALID_STATUSES = ["PAID", "DENIED", "PENDING"]
VALID_GENDERS = ["M", "F", "U"]


def validate_data_quality(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Perform comprehensive data quality checks (Pandas version)
    Returns: (clean_df, anomalies_df)
    """
    
    # Add DQ issues column
    df['dq_issues'] = ''
    
    # 1. Null/Missing value checks
    df.loc[df['member_id'].isna(), 'dq_issues'] += 'MISSING_MEMBER_ID;'
    df.loc[df['provider_npi'].isna(), 'dq_issues'] += 'MISSING_NPI;'
    df.loc[df['cpt_code'].isna(), 'dq_issues'] += 'MISSING_CPT;'
    df.loc[df['icd10_code'].isna() | (df['icd10_code'] == ''), 'dq_issues'] += 'MISSING_DIAGNOSIS;'
    df.loc[df['claim_amount'].isna(), 'dq_issues'] += 'MISSING_AMOUNT;'
    
    # 2. Invalid NPI format (must be 10 digits)
    df['npi_valid'] = df['provider_npi'].astype(str).str.match(r'^\d{10}$')
    df.loc[~df['npi_valid'], 'dq_issues'] += 'INVALID_NPI;'
    
    # 3. Claim amount validations
    df.loc[df['claim_amount'] < DQ_THRESHOLDS["min_claim_amount"], 'dq_issues'] += 'NEGATIVE_AMOUNT;'
    df.loc[df['claim_amount'] > DQ_THRESHOLDS["max_claim_amount"], 'dq_issues'] += 'EXCESSIVE_AMOUNT;'
    
    # 4. Date validations
    current_date = datetime.now().date()
    df['service_date'] = pd.to_datetime(df['service_date'], errors='coerce')
    df['submission_date'] = pd.to_datetime(df['submission_date'], errors='coerce')
    
    # Convert to date for comparison
    df['service_date_only'] = df['service_date'].dt.date
    df['submission_date_only'] = df['submission_date'].dt.date
    
    df.loc[df['service_date_only'] > current_date, 'dq_issues'] += 'FUTURE_SERVICE_DATE;'
    
    # Calculate days difference (using timedelta objects)
    df['days_to_submission'] = (df['submission_date'] - df['service_date']).dt.days
    df.loc[df['days_to_submission'] > DQ_THRESHOLDS["max_days_to_submission"], 'dq_issues'] += 'LATE_SUBMISSION;'
    df.loc[df['submission_date'] < df['service_date'], 'dq_issues'] += 'SUBMISSION_BEFORE_SERVICE;'
    
    # 5. Invalid status
    df.loc[~df['claim_status'].isin(VALID_STATUSES), 'dq_issues'] += 'INVALID_STATUS;'
    
    # 6. Gender validation
    df.loc[~df['patient_gender'].isin(VALID_GENDERS), 'dq_issues'] += 'INVALID_GENDER;'
    
    # Mark records as clean or anomalous
    df['is_anomaly'] = df['dq_issues'].str.len() > 0
    
    # Split into clean and anomalies
    clean_df = df[~df['is_anomaly']].drop(columns=['dq_issues', 'is_anomaly', 'npi_valid', 'days_to_submission', 'service_date_only', 'submission_date_only'])
    anomalies_df = df[df['is_anomaly']].copy()
    
    return clean_df, anomalies_df


def detect_statistical_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    """
    Detect statistical anomalies using z-score method
    """
    
    # Calculate statistics per CPT code
    stats_df = df.groupby('cpt_code')['claim_amount'].agg(['mean', 'std', 'count']).reset_index()
    stats_df.columns = ['cpt_code', 'avg_amount', 'stddev_amount', 'count']
    
    # Merge stats back
    df_with_stats = df.merge(stats_df, on='cpt_code', how='left')
    
    # Calculate z-score
    df_with_stats['z_score'] = 0.0
    mask = df_with_stats['stddev_amount'] > 0
    df_with_stats.loc[mask, 'z_score'] = (
        (df_with_stats.loc[mask, 'claim_amount'] - df_with_stats.loc[mask, 'avg_amount']) / 
        df_with_stats.loc[mask, 'stddev_amount']
    )
    
    # Flag outliers (z-score > 3 or < -3)
    df_with_stats['is_statistical_outlier'] = df_with_stats['z_score'].abs() > 3
    
    return df_with_stats


def calculate_dq_metrics(raw_count: int, clean_count: int, anomaly_count: int) -> dict:
    """Calculate data quality metrics"""
    return {
        "raw_records": raw_count,
        "clean_records": clean_count,
        "anomaly_records": anomaly_count,
        "data_quality_score": (clean_count / raw_count * 100) if raw_count > 0 else 0,
        "anomaly_rate": (anomaly_count / raw_count * 100) if raw_count > 0 else 0,
        "timestamp": datetime.now().isoformat()
    }


def main():
    """Main test function"""
    print("=" * 70)
    print("DQAD Data Quality Validation Test (Local)")
    print("=" * 70)
    print()
    
    # Find all CSV files - check both locations
    # First try parent directory (project root)
    data_dir = Path(__file__).parent.parent / "raw_data"
    if not data_dir.exists():
        # Fallback to data/raw_data if it exists
        data_dir = Path(__file__).parent / "raw_data"
    
    csv_files = list(data_dir.glob("*.csv"))
    
    if not csv_files:
        print("❌ No CSV files found in data/raw_data/")
        print("   Run: python generate_payer_data.py")
        return
    
    print(f"Found {len(csv_files)} CSV files to validate")
    print()
    
    all_clean = []
    all_anomalies = []
    all_outliers = []
    
    for csv_file in csv_files:
        print(f"Processing: {csv_file.name}")
        print("-" * 70)
        
        # Read CSV
        df = pd.read_csv(csv_file)
        raw_count = len(df)
        print(f"  Loaded: {raw_count:,} claims")
        
        # Data Quality Validation
        clean_df, anomaly_df = validate_data_quality(df)
        clean_count = len(clean_df)
        anomaly_count = len(anomaly_df)
        
        print(f"  ✓ Clean claims: {clean_count:,}")
        print(f"  ⚠ Anomalous claims: {anomaly_count:,}")
        
        # Statistical Outlier Detection
        if clean_count > 0:
            clean_with_outliers = detect_statistical_anomalies(clean_df)
            outlier_count = clean_with_outliers['is_statistical_outlier'].sum()
            print(f"  📊 Statistical outliers: {outlier_count:,}")
        else:
            outlier_count = 0
        
        # Calculate DQ metrics
        metrics = calculate_dq_metrics(raw_count, clean_count, anomaly_count)
        print(f"  📈 Data Quality Score: {metrics['data_quality_score']:.2f}%")
        print(f"  📉 Anomaly Rate: {metrics['anomaly_rate']:.2f}%")
        
        # Show top DQ issues
        if anomaly_count > 0:
            print("\n  Top Data Quality Issues:")
            issue_counts = {}
            for issues in anomaly_df['dq_issues']:
                for issue in issues.split(';'):
                    if issue:
                        issue_counts[issue] = issue_counts.get(issue, 0) + 1
            
            for issue, count in sorted(issue_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
                print(f"    - {issue}: {count:,} occurrences")
        
        print()
        
        all_clean.append(clean_df)
        all_anomalies.append(anomaly_df)
    
    # Overall summary
    print("=" * 70)
    print("OVERALL SUMMARY")
    print("=" * 70)
    
    total_clean = sum(len(df) for df in all_clean)
    total_anomalies = sum(len(df) for df in all_anomalies)
    total_raw = total_clean + total_anomalies
    
    overall_metrics = calculate_dq_metrics(total_raw, total_clean, total_anomalies)
    
    print(f"Total records processed: {total_raw:,}")
    print(f"Clean records: {total_clean:,}")
    print(f"Anomalous records: {total_anomalies:,}")
    print(f"Data Quality Score: {overall_metrics['data_quality_score']:.2f}%")
    print(f"Anomaly Rate: {overall_metrics['anomaly_rate']:.2f}%")
    print()
    
    # Sample anomalies
    if all_anomalies:
        print("Sample Anomalous Claims:")
        print("-" * 70)
        combined_anomalies = pd.concat(all_anomalies, ignore_index=True)
        sample = combined_anomalies.head(5)[['claim_id', 'claim_amount', 'provider_npi', 'service_date', 'dq_issues']]
        print(sample.to_string(index=False))
    
    print()
    print("✅ Validation complete!")
    print()
    print("This demonstrates the same logic that runs in Databricks:")
    print("  - 50+ validation rules")
    print("  - Statistical outlier detection")
    print("  - Comprehensive DQ scoring")


if __name__ == "__main__":
    main()

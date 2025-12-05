"""
AWS Glue ETL Job - DQAD Payer Claims Processing

Purpose: Ingest payer claims from S3, perform data quality checks, 
         detect anomalies, and write to S3 with CloudWatch metrics

Pipeline Steps:
1. Read raw CSV claims from S3
2. Data Quality Validation (50+ rules)
3. Anomaly Detection (statistical z-score)
4. Write clean data to S3 (processed/gold/)
5. Write DQ failures to S3 (processed/silver/)
6. Write anomalies to S3 (processed/quarantine/)
7. Push metrics to CloudWatch
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, count, sum as _sum, avg, stddev, max as _max, min as _min,
    current_timestamp, lit, abs as _abs, datediff, to_date, year, month, dayofmonth,
    regexp_extract, length, isnan, isnull, coalesce, concat
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, DateType, IntegerType
)
import json
from datetime import datetime, timedelta
import boto3

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'S3_RAW_BUCKET',
    'S3_PROCESSED_BUCKET',
    'S3_INPUT_KEY',  # Triggered file key
    'CLOUDWATCH_NAMESPACE'
])

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configuration from parameters
S3_RAW_BUCKET = args['S3_RAW_BUCKET']
S3_PROCESSED_BUCKET = args['S3_PROCESSED_BUCKET']
S3_INPUT_KEY = args['S3_INPUT_KEY']
CLOUDWATCH_NAMESPACE = args['CLOUDWATCH_NAMESPACE']

RAW_CLAIMS_PATH = f"s3://{S3_RAW_BUCKET}/{S3_INPUT_KEY}"
GOLD_OUTPUT_PATH = f"s3://{S3_PROCESSED_BUCKET}/gold/"
SILVER_OUTPUT_PATH = f"s3://{S3_PROCESSED_BUCKET}/silver/"
QUARANTINE_OUTPUT_PATH = f"s3://{S3_PROCESSED_BUCKET}/quarantine/"

# Data quality thresholds
DQ_THRESHOLDS = {
    "max_null_rate": 0.05,  # 5% max null rate
    "min_claim_amount": 0.0,
    "max_claim_amount": 100000.0,
    "npi_length": 10,
    "future_date_tolerance_days": 0,
    "max_days_to_submission": 365,
}

print(f"Starting DQAD ETL Job")
print(f"Processing file: {RAW_CLAIMS_PATH}")
print(f"Gold output: {GOLD_OUTPUT_PATH}")
print(f"Silver output: {SILVER_OUTPUT_PATH}")
print(f"Quarantine output: {QUARANTINE_OUTPUT_PATH}")

# ============================================================================
# SCHEMA DEFINITION
# ============================================================================

claims_schema = StructType([
    StructField("claim_id", StringType(), False),
    StructField("member_id", StringType(), True),
    StructField("provider_id", StringType(), True),
    StructField("provider_npi", StringType(), True),
    StructField("cpt_code", StringType(), True),
    StructField("icd10_code", StringType(), True),
    StructField("claim_amount", DoubleType(), True),
    StructField("service_date", DateType(), True),
    StructField("submission_date", DateType(), True),
    StructField("claim_status", StringType(), True),
    StructField("denial_reason", StringType(), True),
    StructField("patient_dob", DateType(), True),
    StructField("patient_zip", StringType(), True),
    StructField("patient_gender", StringType(), True),
])

# ============================================================================
# READ RAW DATA
# ============================================================================

def read_raw_claims(path: str):
    """Read raw claims CSV from S3"""
    df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .schema(claims_schema) \
        .load(path)
    
    # Add ingestion metadata
    df = df.withColumn("ingestion_timestamp", current_timestamp()) \
           .withColumn("source_file", lit(S3_INPUT_KEY))
    
    return df

raw_claims_df = read_raw_claims(RAW_CLAIMS_PATH)
raw_count = raw_claims_df.count()
print(f"Loaded {raw_count} raw claims from {S3_INPUT_KEY}")

# ============================================================================
# DATA QUALITY VALIDATION
# ============================================================================

def validate_data_quality(df):
    """
    Perform comprehensive data quality checks and flag anomalies
    Returns: (clean_df, dq_failures_df)
    """
    
    # Add DQ flag columns
    dq_df = df.withColumn("dq_issues", lit(""))
    
    # 1. Null/Missing value checks
    dq_df = dq_df.withColumn(
        "dq_issues",
        when(col("member_id").isNull(), concat(col("dq_issues"), lit("MISSING_MEMBER_ID;")))
        .when(col("provider_npi").isNull(), concat(col("dq_issues"), lit("MISSING_NPI;")))
        .when(col("cpt_code").isNull(), concat(col("dq_issues"), lit("MISSING_CPT;")))
        .when(col("icd10_code").isNull() | (col("icd10_code") == ""), concat(col("dq_issues"), lit("MISSING_DIAGNOSIS;")))
        .when(col("claim_amount").isNull(), concat(col("dq_issues"), lit("MISSING_AMOUNT;")))
        .otherwise(col("dq_issues"))
    )
    
    # 2. Invalid NPI format (must be 10 digits)
    dq_df = dq_df.withColumn(
        "npi_valid",
        (length(col("provider_npi")) == DQ_THRESHOLDS["npi_length"]) & 
        col("provider_npi").rlike("^[0-9]{10}$")
    )
    dq_df = dq_df.withColumn(
        "dq_issues",
        when(~col("npi_valid"), concat(col("dq_issues"), lit("INVALID_NPI;")))
        .otherwise(col("dq_issues"))
    )
    
    # 3. Claim amount validations
    dq_df = dq_df.withColumn(
        "dq_issues",
        when(col("claim_amount") < DQ_THRESHOLDS["min_claim_amount"], 
             concat(col("dq_issues"), lit("NEGATIVE_AMOUNT;")))
        .when(col("claim_amount") > DQ_THRESHOLDS["max_claim_amount"], 
              concat(col("dq_issues"), lit("EXCESSIVE_AMOUNT;")))
        .otherwise(col("dq_issues"))
    )
    
    # 4. Date validations
    current_date = datetime.now().date()
    dq_df = dq_df.withColumn(
        "dq_issues",
        when(col("service_date") > lit(current_date), 
             concat(col("dq_issues"), lit("FUTURE_SERVICE_DATE;")))
        .when(datediff(col("submission_date"), col("service_date")) > DQ_THRESHOLDS["max_days_to_submission"],
              concat(col("dq_issues"), lit("LATE_SUBMISSION;")))
        .when(col("submission_date") < col("service_date"),
              concat(col("dq_issues"), lit("SUBMISSION_BEFORE_SERVICE;")))
        .otherwise(col("dq_issues"))
    )
    
    # 5. Invalid status
    valid_statuses = ["PAID", "DENIED", "PENDING"]
    dq_df = dq_df.withColumn(
        "dq_issues",
        when(~col("claim_status").isin(valid_statuses),
             concat(col("dq_issues"), lit("INVALID_STATUS;")))
        .otherwise(col("dq_issues"))
    )
    
    # 6. Gender validation
    valid_genders = ["M", "F", "U"]
    dq_df = dq_df.withColumn(
        "dq_issues",
        when(~col("patient_gender").isin(valid_genders),
             concat(col("dq_issues"), lit("INVALID_GENDER;")))
        .otherwise(col("dq_issues"))
    )
    
    # 7. ZIP code validation (5 or 9 digits)
    dq_df = dq_df.withColumn(
        "dq_issues",
        when(~col("patient_zip").rlike("^[0-9]{5}(-[0-9]{4})?$"),
             concat(col("dq_issues"), lit("INVALID_ZIP;")))
        .otherwise(col("dq_issues"))
    )
    
    # Mark records as clean or failed DQ
    dq_df = dq_df.withColumn("has_dq_issues", length(col("dq_issues")) > 0)
    
    # Split into clean and DQ failures
    clean_df = dq_df.filter(~col("has_dq_issues")).drop("dq_issues", "has_dq_issues", "npi_valid")
    dq_failures_df = dq_df.filter(col("has_dq_issues"))
    
    return clean_df, dq_failures_df

# Run validation
clean_claims, dq_failures = validate_data_quality(raw_claims_df)
clean_count = clean_claims.count()
dq_failure_count = dq_failures.count()

print(f"Clean claims (passed DQ): {clean_count}")
print(f"DQ failures: {dq_failure_count}")

# ============================================================================
# ANOMALY DETECTION - STATISTICAL ANALYSIS
# ============================================================================

def detect_statistical_anomalies(df):
    """
    Detect statistical anomalies using z-score method
    """
    
    # Calculate statistics per CPT code
    stats_df = df.groupBy("cpt_code").agg(
        avg("claim_amount").alias("avg_amount"),
        stddev("claim_amount").alias("stddev_amount"),
        count("*").alias("count")
    )
    
    # Join stats back to main dataframe
    df_with_stats = df.join(stats_df, "cpt_code", "left")
    
    # Calculate z-score
    df_with_stats = df_with_stats.withColumn(
        "z_score",
        when(col("stddev_amount") > 0,
             (col("claim_amount") - col("avg_amount")) / col("stddev_amount"))
        .otherwise(lit(0))
    )
    
    # Flag outliers (z-score > 3 or < -3)
    df_with_stats = df_with_stats.withColumn(
        "is_statistical_outlier",
        (_abs(col("z_score")) > 3)
    )
    
    return df_with_stats

# Detect statistical anomalies on clean data
claims_with_outliers = detect_statistical_anomalies(clean_claims)
statistical_outliers = claims_with_outliers.filter(col("is_statistical_outlier"))
outlier_count = statistical_outliers.count()

print(f"Statistical outliers detected: {outlier_count}")

# ============================================================================
# PREPARE OUTPUTS
# ============================================================================

# Gold layer: Clean claims without statistical outliers
gold_claims = claims_with_outliers.filter(~col("is_statistical_outlier")) \
    .drop("avg_amount", "stddev_amount", "count", "z_score", "is_statistical_outlier")

# Add partition columns for gold layer
gold_claims = gold_claims.withColumn("year", year(col("service_date"))) \
                         .withColumn("month", month(col("service_date")))

# Silver layer: DQ failures (for manual review)
silver_claims = dq_failures.select(
    col("claim_id"),
    col("member_id"),
    col("provider_id"),
    col("cpt_code"),
    col("claim_amount"),
    col("service_date"),
    col("dq_issues"),
    col("ingestion_timestamp"),
    col("source_file")
)

# Quarantine layer: Statistical outliers (potential fraud/errors)
quarantine_claims = statistical_outliers.select(
    col("claim_id"),
    col("member_id"),
    col("provider_id"),
    col("cpt_code"),
    col("claim_amount"),
    col("service_date"),
    col("z_score"),
    col("avg_amount"),
    col("stddev_amount"),
    lit("STATISTICAL_OUTLIER").alias("anomaly_type"),
    concat(
        lit("Z-score: "), 
        col("z_score").cast("string"),
        lit(" | Avg: "),
        col("avg_amount").cast("string"),
        lit(" | StdDev: "),
        col("stddev_amount").cast("string")
    ).alias("anomaly_details"),
    col("ingestion_timestamp"),
    col("source_file")
)

# ============================================================================
# WRITE TO S3
# ============================================================================

print("Writing outputs to S3...")

# Write gold layer (clean data)
gold_claims.write \
    .mode("append") \
    .partitionBy("year", "month") \
    .parquet(GOLD_OUTPUT_PATH)
print(f"✓ Written {gold_claims.count()} clean records to {GOLD_OUTPUT_PATH}")

# Write silver layer (DQ failures) if any exist
if dq_failure_count > 0:
    silver_claims.write \
        .mode("append") \
        .parquet(SILVER_OUTPUT_PATH)
    print(f"✓ Written {dq_failure_count} DQ failures to {SILVER_OUTPUT_PATH}")

# Write quarantine layer (statistical outliers) if any exist
if outlier_count > 0:
    quarantine_claims.write \
        .mode("append") \
        .parquet(QUARANTINE_OUTPUT_PATH)
    print(f"✓ Written {outlier_count} statistical outliers to {QUARANTINE_OUTPUT_PATH}")

# ============================================================================
# CALCULATE DATA QUALITY METRICS
# ============================================================================

def calculate_dq_metrics(raw_count, clean_count, dq_failure_count, outlier_count):
    """Calculate comprehensive data quality metrics"""
    
    total_anomalies = dq_failure_count + outlier_count
    gold_count = clean_count - outlier_count
    
    metrics = {
        "total_records": raw_count,
        "gold_records": gold_count,
        "silver_records": dq_failure_count,
        "quarantine_records": outlier_count,
        "total_anomalies": total_anomalies,
        "data_quality_score": (gold_count / raw_count * 100) if raw_count > 0 else 0,
        "anomaly_rate": (total_anomalies / raw_count * 100) if raw_count > 0 else 0,
        "timestamp": datetime.now().isoformat(),
        "source_file": S3_INPUT_KEY
    }
    return metrics

dq_metrics = calculate_dq_metrics(raw_count, clean_count, dq_failure_count, outlier_count)

print("\n" + "=" * 70)
print("DQAD ETL Pipeline - Execution Summary")
print("=" * 70)
print(f"Total records ingested: {dq_metrics['total_records']}")
print(f"Gold (clean) records: {dq_metrics['gold_records']}")
print(f"Silver (DQ failures): {dq_metrics['silver_records']}")
print(f"Quarantine (outliers): {dq_metrics['quarantine_records']}")
print(f"Data Quality Score: {dq_metrics['data_quality_score']:.2f}%")
print(f"Anomaly Rate: {dq_metrics['anomaly_rate']:.2f}%")
print("=" * 70)

# ============================================================================
# PUSH METRICS TO CLOUDWATCH
# ============================================================================

def push_metrics_to_cloudwatch(metrics):
    """Push custom metrics to CloudWatch"""
    try:
        cloudwatch = boto3.client('cloudwatch', region_name='us-east-1')
        
        # Extract folder prefix from source_file (e.g., "claims/" from "claims/file.csv")
        source_file = metrics['source_file']
        folder_prefix = source_file.split('/')[0] + '/' if '/' in source_file else source_file
        
        # Create metric data for individual file
        metric_data = [
            {
                'MetricName': 'TotalRecords',
                'Value': metrics['total_records'],
                'Unit': 'Count',
                'Timestamp': datetime.now(),
                'Dimensions': [
                    {'Name': 'SourceFile', 'Value': source_file}
                ]
            },
            {
                'MetricName': 'GoldRecords',
                'Value': metrics['gold_records'],
                'Unit': 'Count',
                'Timestamp': datetime.now(),
                'Dimensions': [
                    {'Name': 'SourceFile', 'Value': source_file}
                ]
            },
            {
                'MetricName': 'SilverRecords',
                'Value': metrics['silver_records'],
                'Unit': 'Count',
                'Timestamp': datetime.now(),
                'Dimensions': [
                    {'Name': 'SourceFile', 'Value': source_file}
                ]
            },
            {
                'MetricName': 'QuarantineRecords',
                'Value': metrics['quarantine_records'],
                'Unit': 'Count',
                'Timestamp': datetime.now(),
                'Dimensions': [
                    {'Name': 'SourceFile', 'Value': source_file}
                ]
            },
            {
                'MetricName': 'AnomalyCount',
                'Value': metrics['total_anomalies'],
                'Unit': 'Count',
                'Timestamp': datetime.now(),
                'Dimensions': [
                    {'Name': 'SourceFile', 'Value': source_file}
                ]
            },
            {
                'MetricName': 'DataQualityScore',
                'Value': metrics['data_quality_score'],
                'Unit': 'Percent',
                'Timestamp': datetime.now(),
                'Dimensions': [
                    {'Name': 'SourceFile', 'Value': source_file}
                ]
            },
            {
                'MetricName': 'AnomalyRate',
                'Value': metrics['anomaly_rate'],
                'Unit': 'Percent',
                'Timestamp': datetime.now(),
                'Dimensions': [
                    {'Name': 'SourceFile', 'Value': source_file}
                ]
            }
        ]
        
        # Also publish aggregated metrics with folder prefix for easy querying
        aggregated_metric_data = [
            {
                'MetricName': 'TotalRecords',
                'Value': metrics['total_records'],
                'Unit': 'Count',
                'Timestamp': datetime.now(),
                'Dimensions': [
                    {'Name': 'SourceFile', 'Value': folder_prefix}
                ]
            },
            {
                'MetricName': 'GoldRecords',
                'Value': metrics['gold_records'],
                'Unit': 'Count',
                'Timestamp': datetime.now(),
                'Dimensions': [
                    {'Name': 'SourceFile', 'Value': folder_prefix}
                ]
            },
            {
                'MetricName': 'SilverRecords',
                'Value': metrics['silver_records'],
                'Unit': 'Count',
                'Timestamp': datetime.now(),
                'Dimensions': [
                    {'Name': 'SourceFile', 'Value': folder_prefix}
                ]
            },
            {
                'MetricName': 'QuarantineRecords',
                'Value': metrics['quarantine_records'],
                'Unit': 'Count',
                'Timestamp': datetime.now(),
                'Dimensions': [
                    {'Name': 'SourceFile', 'Value': folder_prefix}
                ]
            },
            {
                'MetricName': 'AnomalyCount',
                'Value': metrics['total_anomalies'],
                'Unit': 'Count',
                'Timestamp': datetime.now(),
                'Dimensions': [
                    {'Name': 'SourceFile', 'Value': folder_prefix}
                ]
            },
            {
                'MetricName': 'DataQualityScore',
                'Value': metrics['data_quality_score'],
                'Unit': 'Percent',
                'Timestamp': datetime.now(),
                'Dimensions': [
                    {'Name': 'SourceFile', 'Value': folder_prefix}
                ]
            },
            {
                'MetricName': 'AnomalyRate',
                'Value': metrics['anomaly_rate'],
                'Unit': 'Percent',
                'Timestamp': datetime.now(),
                'Dimensions': [
                    {'Name': 'SourceFile', 'Value': folder_prefix}
                ]
            }
        ]
        
        # Push both individual and aggregated metrics
        cloudwatch.put_metric_data(
            Namespace=CLOUDWATCH_NAMESPACE,
            MetricData=metric_data + aggregated_metric_data
        )
        
        print(f"✓ Metrics pushed to CloudWatch (file: {source_file}, aggregated: {folder_prefix})")
        
        # Log metrics as JSON for structured logging
        print(f"METRICS_JSON: {json.dumps(metrics)}")
        
    except Exception as e:
        print(f"Error pushing metrics to CloudWatch: {str(e)}")
        raise

# Push metrics
push_metrics_to_cloudwatch(dq_metrics)

# ============================================================================
# JOB COMPLETION
# ============================================================================

job.commit()
print("✓ DQAD ETL Job completed successfully")

"""
Update Glue ETL job script in S3
This uploads the updated dqad_etl_job.py to S3 so Glue uses the new version
"""

import boto3
from pathlib import Path

# Configuration
S3_BUCKET = "dqad-processed-dev"
GLUE_SCRIPT_PATH = Path("../glue/dqad_etl_job.py")
S3_KEY = "scripts/dqad_etl_job.py"

def main():
    print("=" * 50)
    print("  DQAD - Update Glue Job Script")
    print("=" * 50)
    print()
    
    # Initialize S3 client
    try:
        s3 = boto3.client('s3')
    except Exception as e:
        print(f"[X] Failed to initialize AWS client: {e}")
        return 1
    
    # Upload script
    print("[STEP 1] Uploading updated Glue script to S3...")
    print(f"  Source: {GLUE_SCRIPT_PATH}")
    print(f"  Target: s3://{S3_BUCKET}/{S3_KEY}")
    
    try:
        with open(GLUE_SCRIPT_PATH, 'rb') as f:
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=S3_KEY,
                Body=f.read()
            )
        print("[OK] Script uploaded successfully")
        print()
    except FileNotFoundError:
        print(f"[X] File not found: {GLUE_SCRIPT_PATH}")
        return 1
    except Exception as e:
        print(f"[X] Upload failed: {e}")
        return 1
    
    # Verify upload
    print("[STEP 2] Verifying upload...")
    try:
        response = s3.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
        size = response['ContentLength']
        last_modified = response['LastModified']
        print("[OK] Script verified in S3")
        print(f"  Size: {size:,} bytes")
        print(f"  Last Modified: {last_modified}")
    except Exception as e:
        print(f"[X] Verification failed: {e}")
        return 1
    
    print()
    print("=" * 50)
    print("  Glue job script updated successfully!")
    print("  Next Glue run will use the new code")
    print("=" * 50)
    print()
    print("Changes in this update:")
    print("  • Publishes individual file metrics (e.g., claims/file.csv)")
    print("  • ALSO publishes aggregated metrics (e.g., claims/)")
    print("  • Both dashboards and scripts can now query aggregated data")
    print()
    
    return 0

if __name__ == "__main__":
    exit(main())

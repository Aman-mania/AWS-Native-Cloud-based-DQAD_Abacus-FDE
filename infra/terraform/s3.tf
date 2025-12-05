# S3 Bucket Lifecycle policies for cost optimization
resource "aws_s3_bucket_lifecycle_configuration" "raw_data_lifecycle" {
  bucket = aws_s3_bucket.dqad_raw.id
  
  rule {
    id     = "archive-old-raw-data"
    status = "Enabled"
    
    filter {}
    
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
    
    expiration {
      days = 365
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "logs_lifecycle" {
  bucket = aws_s3_bucket.dqad_logs.id
  
  rule {
    id     = "expire-old-logs"
    status = "Enabled"
    
    filter {}
    
    expiration {
      days = 30
    }
  }
}

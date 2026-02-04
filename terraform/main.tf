terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "us-east-1"
}

# S3 bucket for NWM streamflow data
resource "aws_s3_bucket" "nwm_streamflow" {
  bucket = "nwm-streamflow-data"

  tags = {
    Name        = "NWM Streamflow Data"
    Project     = "streamflow-viz"
    Environment = "production"
  }
}

# Enable versioning for safety
resource "aws_s3_bucket_versioning" "nwm_streamflow" {
  bucket = aws_s3_bucket.nwm_streamflow.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Public access block - allow public reads for the JSON
resource "aws_s3_bucket_public_access_block" "nwm_streamflow" {
  bucket = aws_s3_bucket.nwm_streamflow.id

  block_public_acls       = false
  block_public_policy     = false
  ignore_public_acls      = false
  restrict_public_buckets = false
}

# Bucket policy for public read access
resource "aws_s3_bucket_policy" "nwm_streamflow_public_read" {
  bucket = aws_s3_bucket.nwm_streamflow.id

  depends_on = [aws_s3_bucket_public_access_block.nwm_streamflow]

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "PublicReadGetObject"
        Effect    = "Allow"
        Principal = "*"
        Action    = "s3:GetObject"
        Resource  = "${aws_s3_bucket.nwm_streamflow.arn}/live/*"
      }
    ]
  })
}

# CORS configuration for browser access
resource "aws_s3_bucket_cors_configuration" "nwm_streamflow" {
  bucket = aws_s3_bucket.nwm_streamflow.id

  cors_rule {
    allowed_headers = ["*"]
    allowed_methods = ["GET", "HEAD"]
    allowed_origins = ["*"]
    expose_headers  = ["ETag"]
    max_age_seconds = 3000
  }
}

output "bucket_name" {
  value = aws_s3_bucket.nwm_streamflow.bucket
}

output "bucket_region" {
  value = "us-east-1"
}

output "live_data_url" {
  value = "https://${aws_s3_bucket.nwm_streamflow.bucket}.s3.us-east-1.amazonaws.com/live/current_velocity.json"
}

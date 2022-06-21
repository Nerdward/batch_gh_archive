terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 3.0"
    }
  }
}

provider "aws" {
  shared_credentials_file = "/home/azureuser/.aws/credentials"
  region                  = "us-east-1"
}

# Data Lake Bucket
resource "aws_s3_bucket" "data-lake-bucket" {
  bucket = var.s3-bucket

  versioning {
    enabled = true
  }

  lifecycle_rule {
    enabled = true

    expiration {
      days = 30
    }
  }
}
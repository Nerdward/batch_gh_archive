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

resource "aws_redshift_cluster" "data-warehouse" {
  cluster_identifier = var.redshift_name
  database_name      = "dev"
  master_username    = var.username
  master_password    = var.password
  node_type          = "dc2.large"
  cluster_type       = "single-node"
}

# IAM role for EMR Service
resource "aws_iam_role" "iam_emr_service_role" {
  name = var.EMR_ROLE

  assume_role_policy = <<EOF
{
  "Version": "2008-10-17",
  "Statement": [
    {
      "Sid": "",
      "Effect": "Allow",
      "Principal": {
        "Service": "elasticmapreduce.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
}

# IAM Role for EC2 Instance Profile
resource "aws_iam_role" "iam_emr_profile_role" {
  name = var.EMR_EC2_ROLE

  assume_role_policy = <<EOF
{
  "Version": "2008-10-17",
  "Statement": [
    {
      "Sid": "",
      "Effect": "Allow",
      "Principal": {
        "Service": "ec2.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
}
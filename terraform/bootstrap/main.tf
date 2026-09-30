# Bootstrap stack - creates the S3 bucket + DynamoDB lock table that the
# REAL stack (environments/dev) stores its remote state in. This one deliberately
# keeps its own state LOCAL (no backend block) - it can't depend on the
# remote backend it's creating, that's the standard chicken-and-egg fix.
#
# Run this once, ever, before the first `terraform init` in environments/dev.
# After this applies successfully, you generally never touch this folder
# again.

terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.region
}

variable "region" {
  type    = string
  default = "us-west-1"
}

variable "state_bucket_name" {
  description = "Must be globally unique. Defaults to healthcare-metrics-tfstate-<account-id>."
  type        = string
  default     = null
}

variable "lock_table_name" {
  type    = string
  default = "healthcare-metrics-tfstate-lock"
}

data "aws_caller_identity" "current" {}

locals {
  state_bucket_name = coalesce(var.state_bucket_name, "healthcare-metrics-tfstate-${data.aws_caller_identity.current.account_id}")
}

resource "aws_s3_bucket" "tfstate" {
  bucket = local.state_bucket_name
}

resource "aws_s3_bucket_versioning" "tfstate" {
  bucket = aws_s3_bucket.tfstate.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "tfstate" {
  bucket = aws_s3_bucket.tfstate.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "tfstate" {
  bucket                  = aws_s3_bucket.tfstate.id
  block_public_acls       = true
  ignore_public_acls      = true
  block_public_policy     = true
  restrict_public_buckets = true
}

resource "aws_dynamodb_table" "tflock" {
  name         = var.lock_table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "LockID"

  attribute {
    name = "LockID"
    type = "S"
  }
}

output "state_bucket_name" {
  value = aws_s3_bucket.tfstate.id
}

output "lock_table_name" {
  value = aws_dynamodb_table.tflock.name
}

output "region" {
  value = var.region
}

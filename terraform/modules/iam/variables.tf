variable "role_name" {
  type    = string
  default = "HealthcareMetricsGlueRole"
}

variable "region" {
  type = string
}

variable "account_id" {
  type = string
}

variable "bucket_arns" {
  description = "The 3 zone bucket ARNs (bronze/silver/gold), for the S3 read/write policy."
  type        = list(string)
}

variable "glue_database_name" {
  type = string
}

variable "dynamodb_table_arn" {
  type = string
}

variable "drive_secret_name" {
  description = "Secrets Manager secret NAME (not ARN) - the policy scopes to name-*, matching how Secrets Manager suffixes a random 6 characters onto every secret ARN."
  type        = string
}

variable "resource_name_prefix" {
  description = "Every Glue job/crawler/workflow this role is allowed to touch must start with this prefix - keeps the role scoped instead of account-wide."
  type        = string
  default     = "HealthcareMetrics"
}

variable "tags" {
  type    = map(string)
  default = {}
}

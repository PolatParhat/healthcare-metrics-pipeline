variable "role_arn" {
  type = string
}

variable "region" {
  type = string
}

variable "bronze_bucket" {
  type = string
}

variable "silver_bucket" {
  type = string
}

variable "gold_bucket" {
  type = string
}

variable "drive_folder_id" {
  description = "Google Drive folder ID the ingestion job watches (the long ID in the folder's URL after /folders/). Not a secret - just a resource pointer - so it's a plain variable, not stored in Secrets Manager."
  type        = string
}

variable "drive_secret_name" {
  type = string
}

variable "dynamodb_table_name" {
  type = string
}

variable "glue_scripts_dir" {
  description = <<-EOT
    Local directory holding the 4 Glue job .py files (ingest_pbj_data.py,
    HealthcareMetricsValidateBronze.py, HealthcareMetricsTransformSilver.py,
    HealthcareMetricsGoldETL.py). Point this at wherever your project keeps
    them, e.g. aws/scripts if terraform/ sits at your repo root next to it.
  EOT
  type = string
}

variable "resource_name_prefix" {
  type    = string
  default = "HealthcareMetrics"
}

variable "tags" {
  type    = map(string)
  default = {}
}

variable "region" {
  type    = string
  default = "us-west-1"
}

variable "project" {
  type    = string
  default = "HealthcareMetrics"
}

variable "environment" {
  type    = string
  default = "dev"
}

# ---------------------------------------------------------------------------
# Values you must set yourself in terraform.tfvars (see terraform.tfvars.example)
# ---------------------------------------------------------------------------

variable "drive_folder_id" {
  description = "Google Drive folder ID the ingestion job watches. Not secret, just a resource pointer."
  type        = string
}

variable "alert_email" {
  description = "Where SNS sends job-failure / pipeline-finished emails. AWS emails a confirmation link here after apply."
  type        = string
}

variable "glue_scripts_dir" {
  description = "Local path to the folder holding the 4 Glue job .py files."
  type        = string
}

# ---------------------------------------------------------------------------
# Sensible defaults - override only if you want different names
# ---------------------------------------------------------------------------

variable "glue_database_name" {
  type    = string
  default = "healthcare_metrics"
}

variable "dynamodb_table_name" {
  type    = string
  default = "HealthcareMetricsSyncState"
}

variable "drive_secret_name" {
  type    = string
  default = "healthcare-metrics/google-drive-creds"
}

variable "sns_topic_name" {
  type    = string
  default = "HealthcareMetricsAlerts"
}

variable "workflow_start_schedule" {
  description = "Cron expression to auto-start the pipeline (e.g. monthly). Leave null for ON_DEMAND-only, same as the original CLI build."
  type        = string
  default     = null
}

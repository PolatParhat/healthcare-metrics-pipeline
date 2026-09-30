variable "topic_name" {
  type    = string
  default = "HealthcareMetricsAlerts"
}

variable "alert_email" {
  description = "Email to subscribe to the SNS topic. AWS sends a confirmation link to this address - you must click it before alerts actually deliver."
  type        = string
}

variable "region" {
  type = string
}

variable "account_id" {
  type = string
}

variable "resource_name_prefix" {
  type    = string
  default = "HealthcareMetrics"
}

# The success rule is scoped to exactly this job name, as a simple
# "pipeline finished" ping - matches the CLI build's HealthcareMetricsGoldETLSuccessAlert.
variable "success_job_name" {
  type    = string
  default = "HealthcareMetricsGoldETL"
}

variable "tags" {
  type    = map(string)
  default = {}
}

variable "workflow_name" {
  type    = string
  default = "HealthcareMetricsPipeline"
}

variable "resource_name_prefix" {
  type    = string
  default = "HealthcareMetrics"
}

variable "job_names" {
  description = "Map from job_names output of the glue_jobs module."
  type = object({
    ingestion        = string
    validate_bronze  = string
    transform_silver = string
    gold_etl         = string
  })
}

variable "crawler_names" {
  description = "Map from crawler_names output of the glue_crawlers module."
  type = object({
    bronze = string
    silver = string
    gold   = string
  })
}

variable "start_schedule" {
  description = <<-EOT
    Optional cron expression (e.g. "cron(0 6 1 * ? *)" for 6am UTC on the 1st
    of every month, matching how often CMS actually publishes new PBJ
    files). Leave null to keep the pipeline ON_DEMAND-only, exactly like the
    original CLI build - you trigger it yourself with
    `aws glue start-workflow-run`.
  EOT
  type    = string
  default = null
}

variable "tags" {
  type    = map(string)
  default = {}
}

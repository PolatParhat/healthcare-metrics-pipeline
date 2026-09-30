locals {
  account_id            = data.aws_caller_identity.current.account_id
  resource_name_prefix  = var.project # "HealthcareMetrics"

  bucket_names = {
    bronze = "healthcare-metrics-bronze-${local.account_id}"
    silver = "healthcare-metrics-silver-${local.account_id}"
    gold   = "healthcare-metrics-gold-${local.account_id}"
  }

  common_tags = {
    Project     = var.project
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

# Composition root - wires the 9 modules together in the same dependency
# order the original CLI build created resources in
# (PIPELINE_BUILD_GUIDE.md Steps 1-13). Terraform figures out the real
# create order itself from these references; the module blocks are just
# ordered to read the same way top to bottom.

module "s3_data_lake" {
  source       = "../../modules/s3_data_lake"
  bucket_names = local.bucket_names
  tags         = local.common_tags
}

module "glue_catalog" {
  source        = "../../modules/glue_catalog"
  database_name = var.glue_database_name
}

module "dynamodb" {
  source     = "../../modules/dynamodb"
  table_name = var.dynamodb_table_name
  tags       = local.common_tags
}

module "secrets" {
  source      = "../../modules/secrets"
  secret_name = var.drive_secret_name
  tags        = local.common_tags
}

module "iam" {
  source                = "../../modules/iam"
  role_name             = "${local.resource_name_prefix}GlueRole"
  region                = var.region
  account_id            = local.account_id
  bucket_arns           = values(module.s3_data_lake.bucket_arns)
  glue_database_name    = module.glue_catalog.database_name
  dynamodb_table_arn    = module.dynamodb.table_arn
  drive_secret_name     = module.secrets.secret_name
  resource_name_prefix  = local.resource_name_prefix
  tags                  = local.common_tags
}

module "alerting" {
  source                = "../../modules/alerting"
  topic_name            = var.sns_topic_name
  alert_email           = var.alert_email
  region                = var.region
  account_id            = local.account_id
  resource_name_prefix  = local.resource_name_prefix
  success_job_name      = "${local.resource_name_prefix}GoldETL"
  tags                  = local.common_tags
}

module "glue_jobs" {
  source                = "../../modules/glue_jobs"
  role_arn              = module.iam.role_arn
  region                = var.region
  bronze_bucket         = module.s3_data_lake.bucket_names["bronze"]
  silver_bucket         = module.s3_data_lake.bucket_names["silver"]
  gold_bucket           = module.s3_data_lake.bucket_names["gold"]
  drive_folder_id       = var.drive_folder_id
  drive_secret_name     = module.secrets.secret_name
  dynamodb_table_name   = module.dynamodb.table_name
  # abspath() resolves this once, here, relative to the directory you run
  # `terraform` in (this folder) - passing it down as-is would leave the
  # child module resolving a relative path against ITS OWN directory
  # instead, which silently points at the wrong place.
  glue_scripts_dir      = abspath(var.glue_scripts_dir)
  resource_name_prefix  = local.resource_name_prefix
  tags                  = local.common_tags
}

module "glue_crawlers" {
  source                = "../../modules/glue_crawlers"
  role_arn              = module.iam.role_arn
  database_name         = module.glue_catalog.database_name
  bronze_bucket         = module.s3_data_lake.bucket_names["bronze"]
  silver_bucket         = module.s3_data_lake.bucket_names["silver"]
  gold_bucket           = module.s3_data_lake.bucket_names["gold"]
  resource_name_prefix  = local.resource_name_prefix
  tags                  = local.common_tags
}

module "glue_workflow" {
  source                = "../../modules/glue_workflow"
  workflow_name         = "${local.resource_name_prefix}Pipeline"
  resource_name_prefix  = local.resource_name_prefix
  job_names             = module.glue_jobs.job_names
  crawler_names         = module.glue_crawlers.crawler_names
  start_schedule        = var.workflow_start_schedule
  tags                  = local.common_tags
}

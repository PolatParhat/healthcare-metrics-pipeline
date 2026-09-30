# Uploads each script to the exact bucket/key the original CLI build used,
# then creates the 4 Glue jobs on top of them. Source filenames match
# aws/scripts/ in this repo - the source of truth for these jobs (already
# named to match their deployed S3 key, so no renaming happens at upload
# time here, unlike an earlier draft of this module) - including the
# (slightly odd but deliberate, matching what was actually validated)
# detail that both the Silver AND Gold scripts upload to the SILVER
# bucket's scripts/ prefix, not their own bucket.

locals {
  ingestion_script        = "${var.glue_scripts_dir}/ingest_pbj_data.py"
  validate_bronze_script  = "${var.glue_scripts_dir}/HealthcareMetricsValidateBronze.py"
  transform_silver_script = "${var.glue_scripts_dir}/HealthcareMetricsTransformSilver.py"
  gold_etl_script         = "${var.glue_scripts_dir}/HealthcareMetricsGoldETL.py"
}

resource "aws_s3_object" "ingestion_script" {
  bucket = var.bronze_bucket
  key    = "scripts/ingest_pbj_data.py"
  source = local.ingestion_script
  etag   = filemd5(local.ingestion_script)
}

resource "aws_s3_object" "validate_bronze_script" {
  bucket = var.bronze_bucket
  key    = "scripts/HealthcareMetricsValidateBronze.py"
  source = local.validate_bronze_script
  etag   = filemd5(local.validate_bronze_script)
}

resource "aws_s3_object" "transform_silver_script" {
  bucket = var.silver_bucket
  key    = "scripts/HealthcareMetricsTransformSilver.py"
  source = local.transform_silver_script
  etag   = filemd5(local.transform_silver_script)
}

resource "aws_s3_object" "gold_etl_script" {
  bucket = var.silver_bucket
  key    = "scripts/HealthcareMetricsGoldETL.py"
  source = local.gold_etl_script
  etag   = filemd5(local.gold_etl_script)
}

# Job 1 - Bronze Ingestion (Python Shell, not Spark - it's just Drive API
# calls and S3 uploads, no need for a Spark cluster).
resource "aws_glue_job" "ingestion" {
  name     = "${var.resource_name_prefix}Ingestion"
  role_arn = var.role_arn
  timeout  = 30

  command {
    name            = "pythonshell"
    script_location = "s3://${var.bronze_bucket}/${aws_s3_object.ingestion_script.key}"
    python_version  = "3.9"
  }

  max_capacity = 1

  default_arguments = {
    "--DRIVE_FOLDER_ID"            = var.drive_folder_id
    "--BRONZE_BUCKET"              = var.bronze_bucket
    "--SECRET_NAME"                = var.drive_secret_name
    "--DYNAMODB_TABLE"             = var.dynamodb_table_name
    "--AWS_REGION"                 = var.region
    "--additional-python-modules"  = "google-api-python-client,google-auth,google-auth-httplib2"
  }

  tags = var.tags
}

# Job 2 - Bronze Data Quality Gate (Spark ETL - Glue Data Quality requires it).
resource "aws_glue_job" "validate_bronze" {
  name              = "${var.resource_name_prefix}ValidateBronze"
  role_arn          = var.role_arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 30

  command {
    name            = "glueetl"
    script_location = "s3://${var.bronze_bucket}/${aws_s3_object.validate_bronze_script.key}"
    python_version   = "3"
  }

  default_arguments = {
    "--BRONZE_BUCKET"                   = var.bronze_bucket
    "--AWS_REGION"                      = var.region
    "--job-language"                    = "python"
    "--enable-metrics"                  = "true"
    "--enable-continuous-cloudwatch-log" = "true"
  }

  tags = var.tags
}

# Job 3 - Silver Transform (the join + all 5 metrics' raw columns).
resource "aws_glue_job" "transform_silver" {
  name              = "${var.resource_name_prefix}TransformSilver"
  role_arn          = var.role_arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 30

  command {
    name            = "glueetl"
    script_location = "s3://${var.silver_bucket}/${aws_s3_object.transform_silver_script.key}"
    python_version   = "3"
  }

  default_arguments = {
    "--BRONZE_BUCKET"                   = var.bronze_bucket
    "--SILVER_BUCKET"                   = var.silver_bucket
    "--AWS_REGION"                      = var.region
    "--job-language"                    = "python"
    "--enable-metrics"                  = "true"
    "--enable-continuous-cloudwatch-log" = "true"
  }

  tags = var.tags
}

# Job 4 - Gold Aggregation (facility_metrics + state_summary rollups).
resource "aws_glue_job" "gold_etl" {
  name              = "${var.resource_name_prefix}GoldETL"
  role_arn          = var.role_arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 30

  command {
    name            = "glueetl"
    script_location = "s3://${var.silver_bucket}/${aws_s3_object.gold_etl_script.key}"
    python_version   = "3"
  }

  default_arguments = {
    "--SILVER_BUCKET"                   = var.silver_bucket
    "--GOLD_BUCKET"                     = var.gold_bucket
    "--AWS_REGION"                      = var.region
    "--job-language"                    = "python"
    "--enable-metrics"                  = "true"
    "--enable-continuous-cloudwatch-log" = "true"
  }

  tags = var.tags
}

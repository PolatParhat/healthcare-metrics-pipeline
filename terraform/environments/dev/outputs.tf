output "bucket_names" {
  value = module.s3_data_lake.bucket_names
}

output "glue_database_name" {
  value = module.glue_catalog.database_name
}

output "glue_role_arn" {
  value = module.iam.role_arn
}

output "dynamodb_table_name" {
  value = module.dynamodb.table_name
}

output "drive_secret_arn" {
  value = module.secrets.secret_arn
}

output "sns_topic_arn" {
  value = module.alerting.topic_arn
}

output "job_names" {
  value = module.glue_jobs.job_names
}

output "crawler_names" {
  value = module.glue_crawlers.crawler_names
}

output "workflow_name" {
  value = module.glue_workflow.workflow_name
}

output "next_steps" {
  value = <<-EOT
    1. Confirm the SNS subscription email sent to your alert_email address.
    2. Populate the real Google Drive service-account secret:
       aws secretsmanager put-secret-value --region ${var.region} \
         --secret-id ${module.secrets.secret_name} \
         --secret-string file:///path/to/your/downloaded-service-account-key.json
    3. Run the pipeline once by hand:
       aws glue start-workflow-run --region ${var.region} --name ${module.glue_workflow.workflow_name}
  EOT
}

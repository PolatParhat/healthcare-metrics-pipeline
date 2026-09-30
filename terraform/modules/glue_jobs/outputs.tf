output "job_names" {
  value = {
    ingestion        = aws_glue_job.ingestion.name
    validate_bronze  = aws_glue_job.validate_bronze.name
    transform_silver = aws_glue_job.transform_silver.name
    gold_etl         = aws_glue_job.gold_etl.name
  }
}

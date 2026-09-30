output "crawler_names" {
  value = {
    bronze = aws_glue_crawler.bronze.name
    silver = aws_glue_crawler.silver.name
    gold   = aws_glue_crawler.gold.name
  }
}

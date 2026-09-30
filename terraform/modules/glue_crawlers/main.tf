# 3 crawlers - Bronze, Silver, Gold. The Gold crawler is the one that
# actually matters to get right: it MUST use 2 separate S3 targets
# (facility_metrics/ and state_summary/), never one target pointed at the
# whole gold/ prefix. facility_metrics and state_summary both use a
# dataset=<value>/ folder-naming convention that LOOKS like Hive-style
# partitioning to the crawler - one root target merges them into a single
# "gold" table with dataset/ingestion_date as partition keys, which is a
# real correctness risk: SUM(total_nurse_hours) FROM gold without filtering
# on the dataset partition would silently sum facility-level and
# state-level totals together. See PIPELINE_BUILD_GUIDE.md Step 12.

resource "aws_glue_crawler" "bronze" {
  name          = "${var.resource_name_prefix}BronzeCrawler"
  role          = var.role_arn
  database_name = var.database_name
  table_prefix  = "bronze_dataset_"

  s3_target {
    path = "s3://${var.bronze_bucket}/raw/"
  }

  tags = var.tags
}

resource "aws_glue_crawler" "silver" {
  name          = "${var.resource_name_prefix}SilverCrawler"
  role          = var.role_arn
  database_name = var.database_name

  s3_target {
    path = "s3://${var.silver_bucket}/silver/"
  }

  tags = var.tags
}

resource "aws_glue_crawler" "gold" {
  name          = "${var.resource_name_prefix}GoldCrawler"
  role          = var.role_arn
  database_name = var.database_name

  s3_target {
    path = "s3://${var.gold_bucket}/gold/dataset=facility_metrics/"
  }

  s3_target {
    path = "s3://${var.gold_bucket}/gold/dataset=state_summary/"
  }

  tags = var.tags
}

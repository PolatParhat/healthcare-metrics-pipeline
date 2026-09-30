# One shared database across all 3 zones - Bronze tables (bronze_dataset_*),
# the Silver table, and the 2 Gold tables all register here.
resource "aws_glue_catalog_database" "this" {
  name = var.database_name
}

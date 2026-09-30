# Holds ingest_pbj_data.py's Google Drive "page token" (sync cursor) under
# state_id="google_drive_sync", so re-runs only pick up files changed since
# last time. On-demand billing - this table gets one read + one write per
# ingestion job run, nowhere near enough traffic to justify provisioned
# capacity.
resource "aws_dynamodb_table" "sync_state" {
  name         = var.table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "state_id"

  attribute {
    name = "state_id"
    type = "S"
  }

  tags = var.tags
}

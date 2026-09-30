# One bucket per medallion zone (bronze/silver/gold). No internal folder
# structure is pre-created here - each Glue job writes its own raw/silver/gold
# prefixes with dataset=/ingestion_date= partitions on first run, exactly like
# the original CLI build (PIPELINE_BUILD_GUIDE.md, Step 1).

resource "aws_s3_bucket" "zone" {
  for_each = var.bucket_names

  bucket = each.value
  tags   = merge(var.tags, { Zone = each.key })
}

resource "aws_s3_bucket_public_access_block" "zone" {
  for_each = aws_s3_bucket.zone

  bucket                  = each.value.id
  block_public_acls       = true
  ignore_public_acls      = true
  block_public_policy     = true
  restrict_public_buckets = true
}

# Not in the original CLI build, but a reasonable default for a bucket
# holding the only copy of Silver/Gold data - protects against an
# accidental overwrite/delete during job re-runs.
resource "aws_s3_bucket_versioning" "zone" {
  for_each = aws_s3_bucket.zone

  bucket = each.value.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "zone" {
  for_each = aws_s3_bucket.zone

  bucket = each.value.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

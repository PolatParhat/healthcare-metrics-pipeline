variable "bucket_names" {
  description = "Map of zone key (bronze/silver/gold) to full S3 bucket name."
  type        = map(string)
}

variable "tags" {
  type    = map(string)
  default = {}
}

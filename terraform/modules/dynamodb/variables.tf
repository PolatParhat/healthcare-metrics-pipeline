variable "table_name" {
  type    = string
  default = "HealthcareMetricsSyncState"
}

variable "tags" {
  type    = map(string)
  default = {}
}

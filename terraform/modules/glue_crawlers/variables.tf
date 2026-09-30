variable "role_arn" {
  type = string
}

variable "database_name" {
  type = string
}

variable "bronze_bucket" {
  type = string
}

variable "silver_bucket" {
  type = string
}

variable "gold_bucket" {
  type = string
}

variable "resource_name_prefix" {
  type    = string
  default = "HealthcareMetrics"
}

variable "tags" {
  type    = map(string)
  default = {}
}

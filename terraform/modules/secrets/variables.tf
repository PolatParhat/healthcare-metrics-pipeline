variable "secret_name" {
  type    = string
  default = "healthcare-metrics/google-drive-creds"
}

variable "tags" {
  type    = map(string)
  default = {}
}

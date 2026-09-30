# Creates the SECRET CONTAINER only - never its real value. The Google
# service-account JSON key must never pass through Terraform state (state is
# plaintext on disk), so this seeds a placeholder and then ignores the
# secret_string on every future apply. Populate the real value yourself,
# once, after `terraform apply`, with:
#
#   aws secretsmanager put-secret-value \
#     --region <region> \
#     --secret-id ${var.secret_name} \
#     --secret-string file:///path/to/your/downloaded-service-account-key.json
resource "aws_secretsmanager_secret" "drive_creds" {
  name = var.secret_name
  tags = var.tags
}

resource "aws_secretsmanager_secret_version" "drive_creds" {
  secret_id     = aws_secretsmanager_secret.drive_creds.id
  secret_string = jsonencode({ placeholder = "replace via aws secretsmanager put-secret-value, see module docstring" })

  lifecycle {
    ignore_changes = [secret_string]
  }
}

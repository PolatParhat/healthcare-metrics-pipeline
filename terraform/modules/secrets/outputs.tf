output "secret_name" {
  value = aws_secretsmanager_secret.drive_creds.name
}

output "secret_arn" {
  value = aws_secretsmanager_secret.drive_creds.arn
}

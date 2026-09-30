output "table_name" {
  value = aws_dynamodb_table.sync_state.name
}

output "table_arn" {
  value = aws_dynamodb_table.sync_state.arn
}

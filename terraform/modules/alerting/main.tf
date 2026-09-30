# One SNS topic (email-subscribed) + 2 EventBridge rules:
#   - fires on ANY Glue job/crawler/workflow FAILED/TIMEOUT/ERROR across the
#     whole pipeline (no jobName filter, so it automatically covers any job
#     you add later)
#   - fires specifically when the Gold job SUCCEEDS, as a "pipeline finished"
#     signal

resource "aws_sns_topic" "alerts" {
  name = var.topic_name
  tags = var.tags
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
  # AWS emails var.alert_email a confirmation link after apply - subscription
  # stays PendingConfirmation, and no alerts deliver, until it's clicked.
}

data "aws_iam_policy_document" "allow_eventbridge_publish" {
  statement {
    sid     = "AllowEventBridgePublish"
    effect  = "Allow"
    actions = ["SNS:Publish"]
    resources = [aws_sns_topic.alerts.arn]

    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }

    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = ["arn:aws:events:${var.region}:${var.account_id}:rule/${var.resource_name_prefix}*"]
    }
  }
}

resource "aws_sns_topic_policy" "allow_eventbridge" {
  arn    = aws_sns_topic.alerts.arn
  policy = data.aws_iam_policy_document.allow_eventbridge_publish.json
}

resource "aws_cloudwatch_event_rule" "job_failure" {
  name = "${var.resource_name_prefix}JobFailureAlerts"
  event_pattern = jsonencode({
    source      = ["aws.glue"]
    detail-type = ["Glue Job State Change", "Glue Crawler State Change", "Glue Workflow State Change"]
    detail      = { state = ["FAILED", "TIMEOUT", "ERROR"] }
  })
  tags = var.tags
}

resource "aws_cloudwatch_event_target" "job_failure_to_sns" {
  rule = aws_cloudwatch_event_rule.job_failure.name
  arn  = aws_sns_topic.alerts.arn
}

resource "aws_cloudwatch_event_rule" "gold_etl_success" {
  name = "${var.resource_name_prefix}GoldETLSuccessAlert"
  event_pattern = jsonencode({
    source      = ["aws.glue"]
    detail-type = ["Glue Job State Change"]
    detail      = { state = ["SUCCEEDED"], jobName = [var.success_job_name] }
  })
  tags = var.tags
}

resource "aws_cloudwatch_event_target" "gold_etl_success_to_sns" {
  rule = aws_cloudwatch_event_rule.gold_etl_success.name
  arn  = aws_sns_topic.alerts.arn
}

# Deliberately scoped custom role - NOT the broad AWS-managed AWSGlueServiceRole.
# Every Glue job/crawler/workflow this role can touch is limited to resources
# named "${var.resource_name_prefix}*" and the 3 healthcare-metrics-* buckets.
# Mirrors PIPELINE_BUILD_GUIDE.md Step 3 exactly (4 policies, same order, same
# fixes already baked in - the DynamoDB/DataQuality policies were both added
# reactively during the original CLI build after real AccessDeniedExceptions).

data "aws_iam_policy_document" "trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = var.role_name
  assume_role_policy = data.aws_iam_policy_document.trust.json
  tags               = var.tags
}

# Policy 1 - S3 read/write on the 3 buckets, plus reading the Drive secret.
data "aws_iam_policy_document" "access" {
  statement {
    sid       = "S3DataLakeAccess"
    effect    = "Allow"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket", "s3:DeleteObject"]
    resources = concat(var.bucket_arns, [for a in var.bucket_arns : "${a}/*"])
  }

  statement {
    sid       = "ReadDriveServiceAccountSecret"
    effect    = "Allow"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = ["arn:aws:secretsmanager:${var.region}:${var.account_id}:secret:${var.drive_secret_name}-*"]
  }
}

resource "aws_iam_role_policy" "access" {
  name   = "HealthcareMetricsAccess"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.access.json
}

# Policy 2 - read/write the ingestion sync-cursor table.
data "aws_iam_policy_document" "dynamodb" {
  statement {
    effect    = "Allow"
    actions   = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem"]
    resources = [var.dynamodb_table_arn]
  }
}

resource "aws_iam_role_policy" "dynamodb" {
  name   = "HealthcareMetricsDynamoDBAccess"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.dynamodb.json
}

# Policy 3 - control the jobs/crawlers/workflow themselves, plus full Data
# Catalog + partition CRUD, plus CloudWatch Logs for the job's own log group.
data "aws_iam_policy_document" "glue_actions" {
  statement {
    sid    = "ControlOwnJobsCrawlersWorkflow"
    effect = "Allow"
    actions = [
      "glue:GetJob", "glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun",
      "glue:GetCrawler", "glue:StartCrawler", "glue:StopCrawler",
      "glue:GetWorkflow", "glue:GetWorkflowRun", "glue:StartWorkflowRun", "glue:GetWorkflowRunProperties",
    ]
    resources = [
      "arn:aws:glue:${var.region}:${var.account_id}:job/${var.resource_name_prefix}*",
      "arn:aws:glue:${var.region}:${var.account_id}:crawler/${var.resource_name_prefix}*",
      "arn:aws:glue:${var.region}:${var.account_id}:workflow/${var.resource_name_prefix}*",
    ]
  }

  statement {
    sid    = "DataCatalogAndPartitionCRUD"
    effect = "Allow"
    actions = [
      "glue:GetDatabase", "glue:CreateDatabase", "glue:GetTable", "glue:GetTables",
      "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable",
      "glue:GetPartition", "glue:GetPartitions", "glue:CreatePartition", "glue:BatchCreatePartition",
      "glue:UpdatePartition", "glue:DeletePartition", "glue:BatchDeletePartition", "glue:BatchGetPartition",
    ]
    resources = [
      "arn:aws:glue:${var.region}:${var.account_id}:catalog",
      "arn:aws:glue:${var.region}:${var.account_id}:database/${var.glue_database_name}*",
      "arn:aws:glue:${var.region}:${var.account_id}:table/${var.glue_database_name}*/*",
    ]
  }

  statement {
    sid       = "GlueJobLogging"
    effect    = "Allow"
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["arn:aws:logs:${var.region}:${var.account_id}:log-group:/aws-glue/*"]
  }
}

resource "aws_iam_role_policy" "glue_actions" {
  name   = "HealthcareMetricsGlueActions"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue_actions.json
}

# Policy 4 - AWS Glue Data Quality is a separate action namespace from
# regular Glue job/crawler control. validate_bronze.py's
# EvaluateDataQuality.apply() call needs these specifically, plus
# cloudwatch:PutMetricData for enableDataQualityCloudWatchMetrics. Glue Data
# Quality's action list isn't cleanly scopable to specific resources (the
# original CLI build hit 2 AccessDeniedExceptions iterating on this one),
# so this statement uses "*" - narrower than ideal, matches what was
# actually proven to work.
data "aws_iam_policy_document" "data_quality" {
  statement {
    sid    = "GlueDataQuality"
    effect = "Allow"
    actions = [
      "glue:StartDataQualityRulesetEvaluationRun",
      "glue:GetDataQualityRulesetEvaluationRun",
      "glue:CancelDataQualityRulesetEvaluationRun",
      "glue:GetDataQualityResult",
      "glue:BatchGetDataQualityResult",
      "glue:ListDataQualityResults",
      "glue:CreateDataQualityRuleset",
      "glue:GetDataQualityRuleset",
      "glue:UpdateDataQualityRuleset",
      "glue:PublishDataQuality",
    ]
    resources = ["*"]
  }

  statement {
    sid       = "DataQualityCloudWatchMetrics"
    effect    = "Allow"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = ["Glue/DataQuality"]
    }
  }
}

resource "aws_iam_role_policy" "data_quality" {
  name   = "HealthcareMetricsDataQualityPolicy"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.data_quality.json
}

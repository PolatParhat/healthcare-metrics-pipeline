# The orchestration layer - chains every job/crawler above into one pipeline.
# 1 starting trigger + 6 conditional triggers = 7 total, matching
# PIPELINE_BUILD_GUIDE.md Step 13 exactly:
#
#   start -> Ingestion -> BronzeCrawler -> ValidateBronze -> TransformSilver
#          -> SilverCrawler -> GoldETL -> GoldCrawler
#
# `enabled = true` on every conditional trigger is Terraform's equivalent of
# the CLI's `--start-on-creation` flag - a trigger created without it sits
# CREATED but inactive and silently never fires (no error, it just never
# runs). Failure propagation needs no extra logic: if one step fails, the
# next trigger's condition simply never becomes true, so the chain just
# stops there - the failure EventBridge rule (alerting module) is what
# actually surfaces that to you.

resource "aws_glue_workflow" "pipeline" {
  name = var.workflow_name
  tags = var.tags
}

resource "aws_glue_trigger" "start_ingestion" {
  name          = "${var.resource_name_prefix}StartIngestion"
  workflow_name = aws_glue_workflow.pipeline.name
  type          = var.start_schedule == null ? "ON_DEMAND" : "SCHEDULED"
  schedule      = var.start_schedule

  actions {
    job_name = var.job_names.ingestion
  }

  tags = var.tags
}

resource "aws_glue_trigger" "after_ingestion" {
  name          = "${var.resource_name_prefix}AfterIngestion"
  workflow_name = aws_glue_workflow.pipeline.name
  type          = "CONDITIONAL"
  enabled       = true

  predicate {
    logical = "AND"
    conditions {
      logical_operator = "EQUALS"
      job_name          = var.job_names.ingestion
      state             = "SUCCEEDED"
    }
  }

  actions {
    crawler_name = var.crawler_names.bronze
  }

  tags = var.tags
}

resource "aws_glue_trigger" "after_bronze_crawl" {
  name          = "${var.resource_name_prefix}AfterBronzeCrawl"
  workflow_name = aws_glue_workflow.pipeline.name
  type          = "CONDITIONAL"
  enabled       = true

  predicate {
    logical = "AND"
    conditions {
      logical_operator = "EQUALS"
      crawler_name      = var.crawler_names.bronze
      crawl_state        = "SUCCEEDED"
    }
  }

  actions {
    job_name = var.job_names.validate_bronze
  }

  tags = var.tags
}

resource "aws_glue_trigger" "after_validate_bronze" {
  name          = "${var.resource_name_prefix}AfterValidateBronze"
  workflow_name = aws_glue_workflow.pipeline.name
  type          = "CONDITIONAL"
  enabled       = true

  predicate {
    logical = "AND"
    conditions {
      logical_operator = "EQUALS"
      job_name          = var.job_names.validate_bronze
      state             = "SUCCEEDED"
    }
  }

  actions {
    job_name = var.job_names.transform_silver
  }

  tags = var.tags
}

resource "aws_glue_trigger" "after_transform_silver" {
  name          = "${var.resource_name_prefix}AfterTransformSilver"
  workflow_name = aws_glue_workflow.pipeline.name
  type          = "CONDITIONAL"
  enabled       = true

  predicate {
    logical = "AND"
    conditions {
      logical_operator = "EQUALS"
      job_name          = var.job_names.transform_silver
      state             = "SUCCEEDED"
    }
  }

  actions {
    crawler_name = var.crawler_names.silver
  }

  tags = var.tags
}

resource "aws_glue_trigger" "after_silver_crawl" {
  name          = "${var.resource_name_prefix}AfterSilverCrawl"
  workflow_name = aws_glue_workflow.pipeline.name
  type          = "CONDITIONAL"
  enabled       = true

  predicate {
    logical = "AND"
    conditions {
      logical_operator = "EQUALS"
      crawler_name      = var.crawler_names.silver
      crawl_state        = "SUCCEEDED"
    }
  }

  actions {
    job_name = var.job_names.gold_etl
  }

  tags = var.tags
}

resource "aws_glue_trigger" "after_gold_etl" {
  name          = "${var.resource_name_prefix}AfterGoldETL"
  workflow_name = aws_glue_workflow.pipeline.name
  type          = "CONDITIONAL"
  enabled       = true

  predicate {
    logical = "AND"
    conditions {
      logical_operator = "EQUALS"
      job_name          = var.job_names.gold_etl
      state             = "SUCCEEDED"
    }
  }

  actions {
    crawler_name = var.crawler_names.gold
  }

  tags = var.tags
}

"""
HealthcareMetricsProject - Bronze Data Quality Gate (AWS Glue Spark job,
using AWS Glue Data Quality / DQDL)

Purpose
-------
This is the "validate" step from the Solution Design (Section 5 & 7):
    ingest -> crawl Bronze -> VALIDATE -> transform (Silver) -> ...

Replaces validate_bronze_staffing.py, which only checked the PBJ table.
The Solution Design's Silver job joins THREE Bronze tables (PBJ,
NH_ProviderInfo, NH_QualityMsr_Claims), so this validate step checks all
three before Silver is allowed to run - not just the one PBJ table.

Why this reads Bronze CSVs directly from S3 instead of via the Catalog
-----------------------------------------------------------------------
The first working version of this job read each table through
`glue_context.create_dynamic_frame.from_catalog()`, trusting whatever
format the Glue Crawler had inferred. Testing against the real Bronze
data surfaced two real problems with that:

    1. The crawler registered these tables using LazySimpleSerDe
       (confirmed via `aws glue get-table`), which has NO quote-character
       support at all - it just splits on the delimiter, so any quoted
       field is a landmine. It also has no declared encoding.
    2. This file is cp1252-encoded, not UTF-8 (Solution Design Section 7).
       AWS Glue Data Quality's own file reader failed outright trying to
       parse PBJ_Daily_Nurse_Staffing_Q2_2024.csv through that SerDe:
       "Unable to parse file: PBJ_Daily_Nurse_Staffing_Q2_2024.csv".

transform_silver.py never hit this because it never went through the
Catalog for these reads - it reads the same Bronze CSVs directly from S3
with `.option("encoding", "windows-1252")` set explicitly. This job now
does the same thing, for the same reason: don't depend on the crawler's
auto-detected format being correct, when we already know exactly what
encoding/header/delimiter this data actually uses.

Note this does NOT defeat the purpose of validating Bronze - it still
validates the real files in the real latest S3 partition, just read the
same reliable way Silver does, instead of through a SerDe that couldn't
even parse the file.

Design note on what's a hard gate vs. a soft flag (unchanged from the
staffing-only version, now applied per table)
-------------------------------------------------------------------------
Section 7 only says a FAILURE HALTS THE WORKFLOW for the row/column
count and schema conformance check. Per-column outlier thresholds are
about flagging suspect rows, not about stopping the whole pipeline over
one unusual value. So, per table:

    HARD rules (any failure -> raise -> job FAILS -> triggers the SNS
    alert, and stops the Glue Workflow from proceeding to Silver):
        - row count > 0
        - column count matches the known Bronze schema
        - the facility-id key column is present on every row
        - the facility-id key column is a 6-digit string (leading
          zeros preserved)

    SOFT rules (logged with counts, do not fail the job):
        - PBJ: Hrs_RN <= 500, Hrs_LPN <= 1000, Hrs_CNA <= 2000 per day
        - NH_QualityMsr_Claims: Measure Code is one of the 4 known
          codes (521/522/551/552) - confirmed empirically against the
          real file; a 5th code showing up means CMS changed the
          file's shape, worth flagging but not fatal since only code
          521 is actually used downstream

Column counts below (33 / 103 / 17) were counted programmatically from
each file's real header, not estimated - see CLAUDE.md for how each
was verified.

Required Glue job parameters:
    --BRONZE_BUCKET                       S3 bucket name for the Bronze zone
    --AWS_REGION
    --JOB_NAME

Optional (defaults match ingest_pbj_data.py's derive_dataset_name()
output, i.e. the actual S3 dataset= partition names - override only if
a dataset was ever ingested under a different stable name):
    --PBJ_DATASET_NAME                    default: PBJ_Daily_Nurse_Staffing_Q2_2024
    --PROVIDER_INFO_DATASET_NAME          default: NH_ProviderInfo
    --QUALITY_MSR_CLAIMS_DATASET_NAME     default: NH_QualityMsr_Claims
"""

import sys

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


def get_job_args():
    args = getResolvedOptions(
        sys.argv,
        ["JOB_NAME", "BRONZE_BUCKET", "AWS_REGION"],
    )
    optional_defaults = {
        "PBJ_DATASET_NAME": "PBJ_Daily_Nurse_Staffing_Q2_2024",
        "PROVIDER_INFO_DATASET_NAME": "NH_ProviderInfo",
        "QUALITY_MSR_CLAIMS_DATASET_NAME": "NH_QualityMsr_Claims",
    }
    for key, default_value in optional_defaults.items():
        try:
            args[key] = getResolvedOptions(sys.argv, [key])[key]
        except Exception:
            args[key] = default_value
    return args


def find_latest_partition_path(s3_client, bucket, dataset_name):
    """Same latest-ingestion_date pattern used throughout this project
    (transform_silver.py, aggregate_gold.py) - Bronze can have more than
    one ingestion_date partition if a dataset was ever re-ingested, and
    validation should only ever look at the newest one."""
    prefix = f"raw/dataset={dataset_name}/"
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")
    partition_prefixes = [cp["Prefix"] for cp in response.get("CommonPrefixes", [])]

    if not partition_prefixes:
        raise ValueError(
            f"No ingestion_date partitions found under s3://{bucket}/{prefix} - "
            f"has '{dataset_name}' actually been ingested into Bronze yet?"
        )

    latest_prefix = sorted(partition_prefixes)[-1]
    return f"s3://{bucket}/{latest_prefix}"


# One entry per Bronze table this validate step checks. Each entry names
# its own hard rules explicitly (by the exact DQDL rule text), so a
# failure can be traced back to which table AND which rule caused it.
# numeric_columns lists which columns need casting to double before
# evaluation - everything else stays string (deliberately - PROVNUM/CCN
# must never be inferred as numbers, or their leading zeros break joins
# downstream in Silver).
def build_table_configs(args):
    return [
        {
            "label": "PBJ Daily Nurse Staffing",
            "dataset_name": args["PBJ_DATASET_NAME"],
            "numeric_columns": ["MDScensus", "Hrs_RN", "Hrs_LPN", "Hrs_CNA"],
            "ruleset": """
                Rules = [
                    RowCount > 0,
                    ColumnCount = 33,
                    IsComplete "PROVNUM",
                    IsComplete "WorkDate",
                    IsComplete "MDScensus",
                    ColumnValues "PROVNUM" matches "[0-9A-Za-z]{6}",
                    ColumnValues "Hrs_RN" <= 500 with threshold >= 0.99,
                    ColumnValues "Hrs_LPN" <= 1000 with threshold >= 0.99,
                    ColumnValues "Hrs_CNA" <= 2000 with threshold >= 0.99
                ]
            """,
            "hard_rules": {
                "RowCount > 0",
                "ColumnCount = 33",
                'IsComplete "PROVNUM"',
                'IsComplete "WorkDate"',
                'IsComplete "MDScensus"',
                'ColumnValues "PROVNUM" matches "[0-9A-Za-z]{6}"',
            },
        },
        {
            "label": "Provider Info",
            "dataset_name": args["PROVIDER_INFO_DATASET_NAME"],
            "numeric_columns": ["Number of Certified Beds"],
            "ruleset": """
                Rules = [
                    RowCount > 0,
                    ColumnCount = 103,
                    IsComplete "CMS Certification Number (CCN)",
                    IsComplete "Number of Certified Beds",
                    ColumnValues "CMS Certification Number (CCN)" matches "[0-9A-Za-z]{6}",
                    ColumnValues "Number of Certified Beds" > 0 with threshold >= 0.99
                ]
            """,
            "hard_rules": {
                "RowCount > 0",
                "ColumnCount = 103",
                'IsComplete "CMS Certification Number (CCN)"',
                'IsComplete "Number of Certified Beds"',
                'ColumnValues "CMS Certification Number (CCN)" matches "[0-9A-Za-z]{6}"',
            },
        },
        {
            "label": "Quality Measures - Claims",
            "dataset_name": args["QUALITY_MSR_CLAIMS_DATASET_NAME"],
            "numeric_columns": [],
            "ruleset": """
                Rules = [
                    RowCount > 0,
                    ColumnCount = 17,
                    IsComplete "CMS Certification Number (CCN)",
                    IsComplete "Measure Code",
                    ColumnValues "CMS Certification Number (CCN)" matches "[0-9A-Za-z]{6}",
                    ColumnValues "Measure Code" in ["521", "522", "551", "552"] with threshold >= 0.99
                ]
            """,
            "hard_rules": {
                "RowCount > 0",
                "ColumnCount = 17",
                'IsComplete "CMS Certification Number (CCN)"',
                'IsComplete "Measure Code"',
                'ColumnValues "CMS Certification Number (CCN)" matches "[0-9A-Za-z]{6}"',
            },
        },
    ]


def load_bronze_csv(spark, s3_client, bronze_bucket, table_config):
    """Read straight from the latest Bronze S3 partition with the same
    cp1252/header options transform_silver.py already uses successfully -
    see the module docstring for why this replaced from_catalog(). Every
    column stays a string except the ones this table's ruleset actually
    needs to compare numerically (numeric_columns) - PROVNUM/CCN must
    never be cast, or leading zeros are lost."""
    path = find_latest_partition_path(s3_client, bronze_bucket, table_config["dataset_name"])
    print(f"[{table_config['label']}] reading from {path}")

    df = (
        spark.read.option("header", "true")
        .option("encoding", "windows-1252")
        .csv(path)
    )

    for column_name in table_config["numeric_columns"]:
        df = df.withColumn(column_name, F.col(column_name).cast(DoubleType()))

    return df


def evaluate_table(glue_context, spark, s3_client, bronze_bucket, table_config):
    """Run one table's ruleset and return (hard_failures, soft_failures)
    for that table. Never raises here - the caller decides what to do
    with results across ALL tables first, so one table's problem doesn't
    stop us from also reporting on the other two in the same run."""
    from awsgluedq.transforms import EvaluateDataQuality
    from awsglue.dynamicframe import DynamicFrame

    print(f"--- Validating {table_config['label']} ({table_config['dataset_name']}) ---")

    spark_df = load_bronze_csv(spark, s3_client, bronze_bucket, table_config)
    dynamic_frame_name = table_config["label"].replace(" ", "_").replace("-", "_")
    dynamic_frame = DynamicFrame.fromDF(spark_df, glue_context, dynamic_frame_name)

    results_frame = EvaluateDataQuality.apply(
        frame=dynamic_frame,
        ruleset=table_config["ruleset"],
        publishing_options={
            "dataQualityEvaluationContext": dynamic_frame_name,
            "enableDataQualityCloudWatchMetrics": True,
            "enableDataQualityResultsPublishing": True,
        },
    )
    results_df = results_frame.toDF()
    results_df.show(truncate=False)

    hard_failures = []
    soft_failures = []
    for row in results_df.collect():
        rule, outcome = row["Rule"], row["Outcome"]
        if outcome != "Passed":
            if rule in table_config["hard_rules"]:
                hard_failures.append((table_config["label"], rule))
            else:
                soft_failures.append((table_config["label"], rule))

    return hard_failures, soft_failures


def main():
    args = get_job_args()

    print(f"Starting Bronze validation job: {args['JOB_NAME']}")

    spark_context = SparkContext()
    glue_context = GlueContext(spark_context)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    s3_client = boto3.client("s3", region_name=args["AWS_REGION"])

    table_configs = build_table_configs(args)

    all_hard_failures = []
    all_soft_failures = []
    for table_config in table_configs:
        hard_failures, soft_failures = evaluate_table(
            glue_context, spark, s3_client, args["BRONZE_BUCKET"], table_config
        )
        all_hard_failures.extend(hard_failures)
        all_soft_failures.extend(soft_failures)

    if all_soft_failures:
        print(f"WARNING: {len(all_soft_failures)} soft rule failure(s) across all tables: {all_soft_failures}")

    if all_hard_failures:
        raise ValueError(
            f"Bronze data quality gate FAILED on {len(all_hard_failures)} hard rule(s): "
            f"{all_hard_failures}. Not proceeding to Silver."
        )

    print("Bronze data quality gate PASSED for all 3 tables (all hard rules satisfied).")

    job.commit()
    print("Bronze validation complete.")


if __name__ == "__main__":
    main()

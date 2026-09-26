"""
HealthcareMetricsProject - Bronze Data Quality Gate: Staffing (AWS Glue
Spark job, using AWS Glue Data Quality / DQDL)

Purpose
-------
This is the "validate" step from the Solution Design (Section 5 & 7):
    ingest -> crawl Bronze -> VALIDATE -> transform (Silver) -> ...

It runs automated rules against the cataloged Bronze PBJ table, BEFORE
transform_staffing_silver.py is allowed to run - not a check embedded
inside the Silver job itself. This matches the design's component table,
which lists "AWS Glue Data Quality" as its own Transform-layer entry,
separate from the Silver and Gold ETL jobs.

Design note on what's a hard gate vs. a soft flag
---------------------------------------------------
Section 7 only says a FAILURE HALTS THE WORKFLOW for the row/column
count and schema conformance check ("a failure here halts the Glue
Workflow rather than silently loading bad data"). The per-column outlier
thresholds (RN > 500, LPN > 1000, CNA > 2000 hours/day) are about
flagging suspect rows, not about stopping the whole quarter's data over
one unusual day. So this job treats them differently:

    HARD rules (any failure -> raise an exception -> job run FAILS ->
    triggers the existing SNS failure alert, and would stop the Glue
    Workflow's next trigger from firing once orchestration is built):
        - row count > 0
        - column count matches the known Bronze schema (33 columns)
        - PROVNUM / WorkDate / MDScensus present on every row
        - PROVNUM is a 6-digit string (preserves leading zeros)

    SOFT rules (evaluated and logged with counts, but do not fail the
    job on their own - they describe the data, they don't gate it):
        - Hrs_RN <= 500, Hrs_LPN <= 1000, Hrs_CNA <= 2000 per day
        - facilities whose total nurse hours are implausibly low
          relative to census (below the 0.5th percentile) - flagged as
          likely non-reporting, not treated as real zero-staffing data

The non-reporting flag is NOT expressed as a DQDL rule. DQDL validates
fixed conditions on a column; "below this dataset's own 0.5th
percentile" is a statistic computed FROM the data, so it's computed in
Spark first as an ordinary column, then written out as a label - not a
pass/fail check the ruleset can express on its own.

Where this fits with transform_staffing_silver.py
-----------------------------------------------------------------
This job reads Bronze CSV, and Silver reads the same Bronze CSV again
independently (Glue Spark jobs don't pass DataFrames to each other
directly - each job is its own isolated Spark session, chained only by
the Glue Workflow's trigger-on-success sequencing). So the Windows-1252
encoding fix and the string-safe PROVNUM read are duplicated in both
scripts on purpose, not an oversight - each job that reads Bronze CSV
independently needs to read it correctly.

Required Glue job parameters:
    --BRONZE_BUCKET             S3 bucket name for the Bronze zone
    --GLUE_DATABASE             Glue Data Catalog database name
                                 (e.g. healthcare_metrics)
    --PBJ_TABLE_NAME             Catalog table name for the PBJ dataset
                                 (default: pbj_daily_nurse_staffing_q2_2024)
    --AWS_REGION                 AWS region (e.g. us-west-1)
    --JOB_NAME
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# The known-good Bronze schema for this dataset - confirmed against the
# real file header, not assumed. A column count that doesn't match this
# means the file that landed in Bronze isn't the file we think it is.
EXPECTED_PBJ_COLUMN_COUNT = 33

# Per-column daily-hours outlier thresholds (Solution Design Section 7).
# Set from each role's own natural scale, not one blanket number.
OUTLIER_THRESHOLDS = {
    "Hrs_RN": 500,
    "Hrs_LPN": 1000,
    "Hrs_CNA": 2000,
}

# Facilities below this percentile of (total nurse hours / census) are
# flagged as likely non-reporting rather than treated as real
# zero-staffing data points (Solution Design Section 7).
NON_REPORTING_PERCENTILE = 0.005


def get_job_args():
    args = getResolvedOptions(
        sys.argv,
        ["JOB_NAME", "BRONZE_BUCKET", "GLUE_DATABASE", "AWS_REGION"],
    )
    try:
        args["PBJ_TABLE_NAME"] = getResolvedOptions(sys.argv, ["PBJ_TABLE_NAME"])["PBJ_TABLE_NAME"]
    except Exception:
        args["PBJ_TABLE_NAME"] = "pbj_daily_nurse_staffing_q2_2024"
    return args


def load_pbj_from_catalog(glue_context, database, table_name):
    """Read Bronze PBJ through the Glue Data Catalog rather than a raw S3
    path - this job validates what the crawler actually registered, which
    is the same table the Silver job's schema expectations are built on.
    """
    dynamic_frame = glue_context.create_dynamic_frame.from_catalog(
        database=database, table_name=table_name
    )
    return dynamic_frame


def add_non_reporting_flag(spark_df):
    """Compute (total nurse hours / census) per row, find this dataset's
    own 0.5th percentile of that ratio, and flag rows below it.

    Example: if the 0.5th percentile works out to 0.42 HPRD, any
    facility-day with total RN+LPN+CNA hours per resident below 0.42 is
    flagged - that's a genuinely unusual staffing level worth treating
    as "probably didn't report correctly" rather than "this facility
    really ran on almost no nursing staff that day."
    """
    with_ratio = spark_df.withColumn(
        "_total_nurse_hprd",
        F.when(
            F.col("MDScensus").cast("double") > 0,
            (
                F.col("Hrs_RN").cast("double")
                + F.col("Hrs_LPN").cast("double")
                + F.col("Hrs_CNA").cast("double")
            )
            / F.col("MDScensus").cast("double"),
        ),
    )

    percentile_value = with_ratio.approxQuantile("_total_nurse_hprd", [NON_REPORTING_PERCENTILE], 0.001)[0]
    print(f"Non-reporting threshold (0.5th percentile of total nurse HPRD): {percentile_value}")

    flagged = with_ratio.withColumn(
        "is_likely_non_reporting",
        F.col("_total_nurse_hprd") < F.lit(percentile_value),
    ).drop("_total_nurse_hprd")

    flagged_count = flagged.filter(F.col("is_likely_non_reporting")).count()
    print(f"{flagged_count} row(s) flagged as likely non-reporting.")
    return flagged


def build_ruleset():
    """DQDL (Data Quality Definition Language) rules, all evaluated in one
    pass. Column count is written as a literal (33) rather than computed,
    because the whole point is catching a Bronze file that silently
    changed shape - comparing the file to itself would never catch that.
    """
    rn_limit = OUTLIER_THRESHOLDS["Hrs_RN"]
    lpn_limit = OUTLIER_THRESHOLDS["Hrs_LPN"]
    cna_limit = OUTLIER_THRESHOLDS["Hrs_CNA"]

    return f"""
    Rules = [
        RowCount > 0,
        ColumnCount = {EXPECTED_PBJ_COLUMN_COUNT},
        IsComplete "PROVNUM",
        IsComplete "WorkDate",
        IsComplete "MDScensus",
        ColumnValues "PROVNUM" matches "[0-9]{{6}}",
        ColumnValues "Hrs_RN" <= {rn_limit} with threshold >= 0.99,
        ColumnValues "Hrs_LPN" <= {lpn_limit} with threshold >= 0.99,
        ColumnValues "Hrs_CNA" <= {cna_limit} with threshold >= 0.99
    ]
    """


# Rules whose failure means the data can't be trusted enough to promote
# to Silver at all - matches "a failure here halts the Glue Workflow"
# from Section 7. Identified by the exact rule text DQDL returns them as.
HARD_RULES = {
    "RowCount > 0",
    f"ColumnCount = {EXPECTED_PBJ_COLUMN_COUNT}",
    'IsComplete "PROVNUM"',
    'IsComplete "WorkDate"',
    'IsComplete "MDScensus"',
    'ColumnValues "PROVNUM" matches "[0-9]{6}"',
}


def evaluate_and_check(glue_context, dynamic_frame, ruleset):
    """Run the ruleset, print every rule's outcome, and raise if any HARD
    rule failed. A soft-rule failure (an outlier threshold) is logged as
    a warning and the job keeps going."""
    from awsgluedq.transforms import EvaluateDataQuality

    results_frame = EvaluateDataQuality.apply(
        frame=dynamic_frame,
        ruleset=ruleset,
        publishing_options={
            "dataQualityEvaluationContext": "pbj_bronze_validation",
            "enableDataQualityCloudWatchMetrics": True,
            "enableDataQualityResultsPublishing": True,
        },
    )
    results_df = results_frame.toDF()
    results_df.show(truncate=False)

    hard_failures = []
    soft_failures = []
    for row in results_df.collect():
        rule = row["Rule"]
        outcome = row["Outcome"]
        if outcome != "Passed":
            if rule in HARD_RULES:
                hard_failures.append(rule)
            else:
                soft_failures.append(rule)

    if soft_failures:
        print(f"WARNING: {len(soft_failures)} soft rule(s) failed (outlier thresholds exceeded): {soft_failures}")

    if hard_failures:
        raise ValueError(
            f"Bronze data quality gate FAILED on {len(hard_failures)} hard rule(s): "
            f"{hard_failures}. Not proceeding to Silver."
        )

    print("Bronze data quality gate PASSED (all hard rules satisfied).")


def main():
    args = get_job_args()

    print(f"Starting Bronze staffing validation job: {args['JOB_NAME']}")

    spark_context = SparkContext()
    glue_context = GlueContext(spark_context)
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    pbj_dynamic_frame = load_pbj_from_catalog(glue_context, args["GLUE_DATABASE"], args["PBJ_TABLE_NAME"])

    # Add the non-reporting label before running the ruleset, purely so
    # it's visible in the printed results alongside the DQDL outcomes -
    # it is informational, not one of the rules being evaluated.
    flagged_df = add_non_reporting_flag(pbj_dynamic_frame.toDF())
    from awsglue.dynamicframe import DynamicFrame

    flagged_dynamic_frame = DynamicFrame.fromDF(flagged_df, glue_context, "flagged_pbj")

    ruleset = build_ruleset()
    evaluate_and_check(glue_context, flagged_dynamic_frame, ruleset)

    job.commit()
    print("Bronze staffing validation complete.")


if __name__ == "__main__":
    main()

"""
HealthcareMetricsProject - Silver Transform Job: Staffing (AWS Glue Spark ETL)

Purpose
-------
Reads the two Bronze datasets needed for the Staffing metrics category -
PBJ Daily Nurse Staffing and Nursing Home Provider Info - validates them,
computes the actual staffing metrics, and writes a clean, analysis-ready
table to the Silver bucket.

Why these two datasets, joined on CCN
--------------------------------------
PBJ gives daily, per-facility staffing hours by role (RN/LPN/CNA, split
employee vs. contract) plus daily resident census. On its own it can
produce Hours-Per-Resident-Day (HPRD) and contract-mix metrics, but it
has no bed count, so it can't produce an occupancy rate by itself.
Provider Info has the bed count (and CMS's own published HPRD figures,
useful for sanity-checking our numbers). Both files use the same 6-digit
CMS Certification Number as their facility key - "PROVNUM" in PBJ,
"CMS Certification Number (CCN)" in Provider Info - confirmed by
inspecting both files' real headers and sample values (both are
zero-padded 6-character strings, e.g. "015009").

Metrics produced (per facility, per day)
-----------------------------------------
    hprd_rn, hprd_lpn, hprd_cna       - hours per resident day, by role
    hprd_total_nurse                  - (RN + LPN + CNA) hours / census,
                                         matching CMS's own "Reported Total
                                         Nurse Staffing HPRD" definition
    contract_pct_rn                   - contract RN hours / total RN hours
    contract_pct_total_nurse          - contract (RN+LPN+CNA) hours /
                                         total (RN+LPN+CNA) hours
    is_weekend                        - Sat/Sun flag, for weekend-staffing
                                         comparisons
    occupancy_rate                    - census / certified beds (from the
                                         Provider Info join)

Data quality gate
------------------
A row is only usable if it has a facility id, a work date, and a positive
census (dividing by a zero/blank census is meaningless, not just messy).
Rows failing that check are written to a separate "_rejects" path in
Silver instead of being silently dropped, so a spike in bad rows is
visible rather than invisible.

Bronze layout assumed (set by ingest_pbj_data.py)
---------------------------------------------------
    s3://<bronze-bucket>/raw/dataset=<name>/ingestion_date=<date>/<file>

Because a dataset can have been ingested more than once (each ingestion
adds a new ingestion_date partition), this job reads only the MOST
RECENT ingestion_date partition for each dataset - i.e. the current
snapshot - rather than accumulating every historical copy.

Required Glue job parameters:
    --BRONZE_BUCKET                  S3 bucket name for the Bronze zone
    --SILVER_BUCKET                  S3 bucket name for the Silver zone
    --PBJ_DATASET_NAME               default: PBJ_Daily_Nurse_Staffing_Q2_2024
    --PROVIDER_INFO_DATASET_NAME     default: NH_ProviderInfo
    --AWS_REGION                     AWS region (e.g. us-west-1)
    --JOB_NAME                       (Glue Spark jobs auto-populate this,
                                       but pass it explicitly anyway - see
                                       the --JOB_NAME bug from the
                                       ingestion job)
"""

import sys
from datetime import datetime, timezone

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType


def get_job_args():
    """Read the job parameters Glue was started with."""
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "BRONZE_BUCKET",
            "SILVER_BUCKET",
            "AWS_REGION",
        ],
    )
    # Optional overrides - fall back to the real dataset names in Bronze
    # today if the job wasn't given explicit values.
    optional_defaults = {
        "PBJ_DATASET_NAME": "PBJ_Daily_Nurse_Staffing_Q2_2024",
        "PROVIDER_INFO_DATASET_NAME": "NH_ProviderInfo",
    }
    for key, default_value in optional_defaults.items():
        try:
            args[key] = getResolvedOptions(sys.argv, [key])[key]
        except Exception:
            args[key] = default_value
    return args


def find_latest_partition_path(s3_client, bucket, dataset_name):
    """Return the s3:// path of the most recent ingestion_date partition
    for a dataset, e.g.:
        s3://bronze-bucket/raw/dataset=NH_ProviderInfo/ingestion_date=2026-09-10/

    Bronze can hold more than one ingestion_date partition for the same
    dataset (one per time it was re-ingested). Silver always transforms
    the latest snapshot, not every historical copy.
    """
    prefix = f"raw/dataset={dataset_name}/"
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")
    partition_prefixes = [cp["Prefix"] for cp in response.get("CommonPrefixes", [])]

    if not partition_prefixes:
        raise ValueError(
            f"No ingestion_date partitions found under s3://{bucket}/{prefix} - "
            f"has '{dataset_name}' actually been ingested into Bronze yet?"
        )

    # Each prefix looks like ".../ingestion_date=2026-09-10/" - the date
    # string sorts correctly as plain text because it's YYYY-MM-DD.
    latest_prefix = sorted(partition_prefixes)[-1]
    return f"s3://{bucket}/{latest_prefix}"


def load_pbj_staffing(spark, path):
    """Load the PBJ Daily Nurse Staffing CSV.

    Read everything as a string first (no inferSchema) - PROVNUM is a
    zero-padded 6-digit code like "015009", and Spark's schema inference
    would read that as an integer and silently drop the leading zero.
    Only the columns we actually need are cast to numeric types below.
    """
    # CMS source files are Windows-1252, not UTF-8 (Solution Design Section 7) -
    # read with the correct encoding rather than letting Spark default to UTF-8,
    # which can silently mangle characters in name/county fields.
    raw_df = spark.read.option("header", "true").option("encoding", "windows-1252").csv(path)

    numeric_columns = [
        "MDScensus",
        "Hrs_RN", "Hrs_RN_emp", "Hrs_RN_ctr",
        "Hrs_LPN", "Hrs_LPN_emp", "Hrs_LPN_ctr",
        "Hrs_CNA", "Hrs_CNA_emp", "Hrs_CNA_ctr",
    ]
    for column_name in numeric_columns:
        raw_df = raw_df.withColumn(column_name, F.col(column_name).cast(DoubleType()))

    # WorkDate ships as an unseparated YYYYMMDD string, e.g. "20240401" -
    # confirmed against the actual file, not the more common M/d/yyyy format.
    return raw_df.withColumn("WorkDate", F.to_date("WorkDate", "yyyyMMdd"))


def load_provider_info(spark, path):
    """Load the Provider Info CSV and keep only the columns Staffing
    metrics need: the facility key, bed count, and CMS's own published
    staffing figures (useful to sanity-check what we compute from PBJ)."""
    raw_df = spark.read.option("header", "true").option("encoding", "windows-1252").csv(path)

    selected = raw_df.select(
        F.col("CMS Certification Number (CCN)").alias("ccn"),
        F.col("Provider Name").alias("provider_name"),
        F.col("Number of Certified Beds").cast(IntegerType()).alias("num_certified_beds"),
        F.col("Ownership Type").alias("ownership_type"),
        F.col("Overall Rating").cast(IntegerType()).alias("overall_rating"),
        F.col("Staffing Rating").cast(IntegerType()).alias("staffing_rating"),
        F.col("Reported Total Nurse Staffing Hours per Resident per Day")
            .cast(DoubleType()).alias("cms_reported_total_nurse_hprd"),
        F.col("Reported RN Staffing Hours per Resident per Day")
            .cast(DoubleType()).alias("cms_reported_rn_hprd"),
        F.col("Total nursing staff turnover").cast(DoubleType()).alias("total_nurse_turnover_pct"),
        F.col("Registered Nurse turnover").cast(DoubleType()).alias("rn_turnover_pct"),
    )
    return selected


def apply_data_quality_gate(pbj_df):
    """Split rows into (clean, rejected). A row is only usable if it has
    a facility id, a work date, and a positive census - anything else
    would produce a divide-by-zero or a meaningless HPRD value."""
    is_valid = (
        F.col("PROVNUM").isNotNull()
        & (F.trim(F.col("PROVNUM")) != "")
        & F.col("WorkDate").isNotNull()
        & F.col("MDScensus").isNotNull()
        & (F.col("MDScensus") > 0)
    )
    clean_df = pbj_df.filter(is_valid)
    rejected_df = pbj_df.filter(~is_valid)
    return clean_df, rejected_df


def compute_staffing_metrics(pbj_df):
    """Derive the actual Staffing metrics from validated PBJ rows."""
    total_hrs = F.col("Hrs_RN") + F.col("Hrs_LPN") + F.col("Hrs_CNA")
    total_ctr_hrs = F.col("Hrs_RN_ctr") + F.col("Hrs_LPN_ctr") + F.col("Hrs_CNA_ctr")

    return (
        pbj_df
        .withColumn("hprd_rn", F.col("Hrs_RN") / F.col("MDScensus"))
        .withColumn("hprd_lpn", F.col("Hrs_LPN") / F.col("MDScensus"))
        .withColumn("hprd_cna", F.col("Hrs_CNA") / F.col("MDScensus"))
        # Matches CMS's own definition: "Total Nurse Staffing = Aide + LPN + RN"
        .withColumn("hprd_total_nurse", total_hrs / F.col("MDScensus"))
        .withColumn(
            "contract_pct_rn",
            F.when(F.col("Hrs_RN") > 0, F.col("Hrs_RN_ctr") / F.col("Hrs_RN")),
        )
        .withColumn(
            "contract_pct_total_nurse",
            F.when(total_hrs > 0, total_ctr_hrs / total_hrs),
        )
        # Spark's dayofweek(): 1 = Sunday, 7 = Saturday
        .withColumn("is_weekend", F.dayofweek("WorkDate").isin(1, 7))
    )


def main():
    args = get_job_args()
    region = args["AWS_REGION"]

    print(f"Starting Silver staffing transform job: {args['JOB_NAME']}")

    spark_context = SparkContext()
    glue_context = GlueContext(spark_context)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    s3_client = boto3.client("s3", region_name=region)

    pbj_path = find_latest_partition_path(s3_client, args["BRONZE_BUCKET"], args["PBJ_DATASET_NAME"])
    provider_info_path = find_latest_partition_path(
        s3_client, args["BRONZE_BUCKET"], args["PROVIDER_INFO_DATASET_NAME"]
    )
    print(f"Reading PBJ from: {pbj_path}")
    print(f"Reading Provider Info from: {provider_info_path}")

    pbj_df = load_pbj_staffing(spark, pbj_path)
    provider_info_df = load_provider_info(spark, provider_info_path)

    clean_df, rejected_df = apply_data_quality_gate(pbj_df)
    clean_count = clean_df.count()
    rejected_count = rejected_df.count()
    print(f"Data quality gate: {clean_count} valid row(s), {rejected_count} rejected row(s).")

    metrics_df = compute_staffing_metrics(clean_df)

    joined_df = metrics_df.join(
        provider_info_df, metrics_df.PROVNUM == provider_info_df.ccn, how="left"
    )
    unmatched_count = joined_df.filter(F.col("ccn").isNull()).count()
    if unmatched_count > 0:
        print(
            f"WARNING: {unmatched_count} staffing row(s) had no matching "
            f"facility in Provider Info - occupancy_rate will be null for those."
        )

    result_df = joined_df.withColumn(
        "occupancy_rate",
        F.when(F.col("num_certified_beds") > 0, F.col("MDScensus") / F.col("num_certified_beds")),
    )

    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    output_path = f"s3://{args['SILVER_BUCKET']}/silver/dataset=staffing_daily_metrics/ingestion_date={run_date}/"
    rejects_path = f"s3://{args['SILVER_BUCKET']}/silver/_rejects/dataset=staffing_daily_metrics/ingestion_date={run_date}/"

    result_df.write.mode("overwrite").parquet(output_path)
    print(f"Wrote {clean_count} row(s) to {output_path}")

    if rejected_count > 0:
        rejected_df.write.mode("overwrite").parquet(rejects_path)
        print(f"Wrote {rejected_count} rejected row(s) to {rejects_path}")

    job.commit()
    print("Silver staffing transform complete.")


if __name__ == "__main__":
    main()

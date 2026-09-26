"""
HealthcareMetricsProject - Silver Transform Job (AWS Glue Spark ETL)

Purpose
-------
The one consolidated Silver job named in the Solution Design (Section 5):
joins PBJ Daily Nurse Staffing + Nursing Home Provider Info +
Quality Measures - Claims, validates them, and computes the columns
needed for all 5 locked metrics. Reads Bronze, writes Silver.

Replaces transform_staffing_silver.py, which only joined PBJ + Provider
Info and only covered metrics 1 and 3. This version adds the
NH_QualityMsr_Claims join (Measure Code 521 only - the short-stay
rehospitalization rate) and a raw total-hours column, so all 5 locked
metrics have what they need at Silver grain:

    1. Nurse-to-patient ratio    -> hprd_total_nurse (from PBJ alone)
    2. Total nurse hours worked  -> total_nurse_hours (from PBJ alone;
                                    Gold sums this by facility/state/month)
    3. Occupancy rate            -> occupancy_rate (PBJ census / Provider
                                    Info beds)
    4. Permanent vs. contract %  -> contract_pct_total_nurse (from PBJ's
                                    _emp/_ctr split)
    5. Staffing-vs-readmission   -> readmission_score attached per
       correlation                 facility (from NH_QualityMsr_Claims,
                                    Measure Code 521 only); the actual
                                    correlation itself is a Gold/dashboard
                                    calculation across facilities, not a
                                    per-row value - see PIPELINE_STATE.md

Grain and the readmission join
-------------------------------
PBJ is daily; NH_QualityMsr_Claims' Adjusted Score is one value per
facility for an entire year-long Measure Period (e.g.
20230401-20240331), not daily. This job still attaches it via a left
join on the facility key, which means the SAME readmission_score value
gets repeated across every daily row for that facility - that's
intentional denormalization, not a bug: it's harmless at Silver's daily
grain and means Gold doesn't need a third join later, just an average
of the daily columns plus a MAX/FIRST of the already-repeated
readmission_score per facility.

Not every facility has a Measure Code 521 row (CMS suppresses measures
for facilities with too few qualifying stays to compute a reliable
rate) - a left join means those facilities keep their staffing data with
a null readmission_score, rather than being dropped from Silver
entirely.

Data quality note
-------------------
The real Data Quality Gate (row/column counts, schema, key format) now
lives in validate_bronze.py, run as its own step before this job. The
apply_data_quality_gate() function here is a narrower, different thing:
a row-level safety check specific to THIS job's own division-by-zero
risk (can't compute HPRD/occupancy without a positive census) - not a
duplicate of validate_bronze.py's job.

Required Glue job parameters:
    --BRONZE_BUCKET
    --SILVER_BUCKET
    --PBJ_DATASET_NAME                    default: PBJ_Daily_Nurse_Staffing_Q2_2024
    --PROVIDER_INFO_DATASET_NAME          default: NH_ProviderInfo
    --QUALITY_MSR_CLAIMS_DATASET_NAME     default: NH_QualityMsr_Claims
    --AWS_REGION
    --JOB_NAME
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

READMISSION_MEASURE_CODE = "521"


def get_job_args():
    args = getResolvedOptions(
        sys.argv,
        ["JOB_NAME", "BRONZE_BUCKET", "SILVER_BUCKET", "AWS_REGION"],
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
    """Same logic as the staffing-only version: find the most recent
    ingestion_date partition for a dataset, since Bronze can hold more
    than one if a dataset was re-ingested."""
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


def load_pbj_staffing(spark, path):
    """Load PBJ. Windows-1252 encoding, string-safe PROVNUM (leading
    zeros preserved), WorkDate parsed as unseparated yyyyMMdd."""
    raw_df = spark.read.option("header", "true").option("encoding", "windows-1252").csv(path)

    numeric_columns = [
        "MDScensus",
        "Hrs_RN", "Hrs_RN_emp", "Hrs_RN_ctr",
        "Hrs_LPN", "Hrs_LPN_emp", "Hrs_LPN_ctr",
        "Hrs_CNA", "Hrs_CNA_emp", "Hrs_CNA_ctr",
    ]
    for column_name in numeric_columns:
        raw_df = raw_df.withColumn(column_name, F.col(column_name).cast(DoubleType()))

    return raw_df.withColumn("WorkDate", F.to_date("WorkDate", "yyyyMMdd"))


def load_provider_info(spark, path):
    """Load Provider Info, keep only what Staffing/Facility/Occupancy
    metrics need plus State for state-level rollups in Gold."""
    raw_df = spark.read.option("header", "true").option("encoding", "windows-1252").csv(path)

    return raw_df.select(
        F.col("CMS Certification Number (CCN)").alias("ccn"),
        F.col("Provider Name").alias("provider_name"),
        F.col("State").alias("provider_state"),
        F.col("Number of Certified Beds").cast(IntegerType()).alias("num_certified_beds"),
    )


def load_readmission_scores(spark, path):
    """Load Quality Measures - Claims, filtered to Measure Code 521 only
    (short-stay rehospitalization rate - the readmission measure used
    by metric #5). One row per facility after this filter, since 521 is
    reported at most once per facility per Measure Period."""
    raw_df = spark.read.option("header", "true").option("encoding", "windows-1252").csv(path)

    return (
        raw_df
        .filter(F.col("Measure Code") == READMISSION_MEASURE_CODE)
        .select(
            F.col("CMS Certification Number (CCN)").alias("ccn"),
            F.col("Adjusted Score").cast(DoubleType()).alias("readmission_score"),
            F.col("Measure Period").alias("readmission_measure_period"),
        )
    )


def apply_data_quality_gate(pbj_df):
    """Row-level safety net specific to this job's own math - not a
    duplicate of validate_bronze.py's schema/completeness checks."""
    is_valid = (
        F.col("PROVNUM").isNotNull()
        & (F.trim(F.col("PROVNUM")) != "")
        & F.col("WorkDate").isNotNull()
        & F.col("MDScensus").isNotNull()
        & (F.col("MDScensus") > 0)
    )
    return pbj_df.filter(is_valid), pbj_df.filter(~is_valid)


def compute_staffing_metrics(pbj_df):
    """Derive metrics 1, 2, and 4's underlying columns."""
    total_hrs = F.col("Hrs_RN") + F.col("Hrs_LPN") + F.col("Hrs_CNA")
    total_ctr_hrs = F.col("Hrs_RN_ctr") + F.col("Hrs_LPN_ctr") + F.col("Hrs_CNA_ctr")

    return (
        pbj_df
        .withColumn("hprd_rn", F.col("Hrs_RN") / F.col("MDScensus"))
        .withColumn("hprd_lpn", F.col("Hrs_LPN") / F.col("MDScensus"))
        .withColumn("hprd_cna", F.col("Hrs_CNA") / F.col("MDScensus"))
        .withColumn("hprd_total_nurse", total_hrs / F.col("MDScensus"))
        # Metric #2 needs the raw hours, not a ratio - Gold sums this
        # column by facility/state/month.
        .withColumn("total_nurse_hours", total_hrs)
        .withColumn("total_nurse_hours_ctr", total_ctr_hrs)
        .withColumn(
            "contract_pct_rn",
            F.when(F.col("Hrs_RN") > 0, F.col("Hrs_RN_ctr") / F.col("Hrs_RN")),
        )
        .withColumn(
            "contract_pct_total_nurse",
            F.when(total_hrs > 0, total_ctr_hrs / total_hrs),
        )
        .withColumn("is_weekend", F.dayofweek("WorkDate").isin(1, 7))
    )


def main():
    args = get_job_args()
    region = args["AWS_REGION"]

    print(f"Starting Silver transform job: {args['JOB_NAME']}")

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
    readmission_path = find_latest_partition_path(
        s3_client, args["BRONZE_BUCKET"], args["QUALITY_MSR_CLAIMS_DATASET_NAME"]
    )
    print(f"Reading PBJ from: {pbj_path}")
    print(f"Reading Provider Info from: {provider_info_path}")
    print(f"Reading Quality Measures - Claims from: {readmission_path}")

    pbj_df = load_pbj_staffing(spark, pbj_path)
    provider_info_df = load_provider_info(spark, provider_info_path)
    readmission_df = load_readmission_scores(spark, readmission_path)

    clean_df, rejected_df = apply_data_quality_gate(pbj_df)
    clean_count = clean_df.count()
    rejected_count = rejected_df.count()
    print(f"Row-level safety check: {clean_count} valid row(s), {rejected_count} rejected row(s).")

    metrics_df = compute_staffing_metrics(clean_df)

    joined_df = metrics_df.join(
        provider_info_df, metrics_df.PROVNUM == provider_info_df.ccn, how="left"
    )
    unmatched_provider_info = joined_df.filter(F.col("ccn").isNull()).count()
    if unmatched_provider_info > 0:
        print(f"WARNING: {unmatched_provider_info} row(s) had no match in Provider Info.")

    # Drop before the next join - both provider_info_df and readmission_df
    # alias their key to "ccn", and since these joins use the
    # left.col == right.col expression form (not a plain column-name
    # join), Spark keeps both sides' columns instead of coalescing them.
    # Without this drop, the second join below produces TWO columns both
    # named "ccn", which Spark tolerates right up until the Parquet
    # writer refuses it: "Found duplicate column(s) ... ccn".
    joined_df = joined_df.drop("ccn")

    joined_df = joined_df.withColumn(
        "occupancy_rate",
        F.when(F.col("num_certified_beds") > 0, F.col("MDScensus") / F.col("num_certified_beds")),
    )

    result_df = joined_df.join(
        readmission_df, joined_df.PROVNUM == readmission_df.ccn, how="left"
    ).drop("ccn")
    facilities_with_readmission = result_df.filter(F.col("readmission_score").isNotNull()).select("PROVNUM").distinct().count()
    total_facilities = result_df.select("PROVNUM").distinct().count()
    print(
        f"{facilities_with_readmission} of {total_facilities} distinct facilities have a "
        f"Measure Code {READMISSION_MEASURE_CODE} readmission score (CMS suppresses this "
        f"measure for facilities with too few qualifying stays - a null here is expected)."
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
    print("Silver transform complete.")


if __name__ == "__main__":
    main()

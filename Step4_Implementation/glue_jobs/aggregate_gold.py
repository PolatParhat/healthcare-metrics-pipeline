"""
HealthcareMetricsProject - Gold Aggregation Job (AWS Glue Spark ETL)

Purpose
-------
The one consolidated Gold job named in the Solution Design (Section 5):
rolls the Silver daily staffing_daily_metrics table up into the two
tables the dashboard actually queries:

    facility_metrics  - grain: (facility, month)
    state_summary     - grain: (state, month)

Reads Silver, writes Gold.

Why two different aggregation strategies are used
-----------------------------------------------------
facility_metrics can just AVERAGE each day's already-computed ratios
(hprd_total_nurse, occupancy_rate), because within ONE facility, the
denominator (census, beds) doesn't change who it's being compared
against day to day - averaging the daily ratios is mathematically the
same as recomputing the ratio from summed totals.

state_summary is different: a state has many facilities of very
different sizes. Averaging each facility's own average HPRD would let a
20-bed facility count exactly as much as a 200-bed facility, which
biases the state number toward small facilities for no good reason.
So state_summary instead sums the raw hours/census/beds across every
facility-day in that state first, THEN divides - a
census/capacity-weighted average, not an average of averages.

contract_pct_total_nurse follows the same "sum before dividing" rule at
BOTH grains, for the same reason discussed when this column was first
built in transform_silver.py: averaging daily percentages would let a
day with 2 total hours logged count as heavily as a day with 240 hours.

readmission_score is handled separately from the other metrics: it's
one value per facility (not daily), so facility_metrics just carries it
through unchanged, and state_summary averages across DISTINCT facilities
in that state (using facility_metrics as the source for this one column)
rather than the daily Silver rows, which would count each facility's
score once per day it reported and skew the state average toward
facilities with more reporting days.

Required Glue job parameters:
    --SILVER_BUCKET
    --GOLD_BUCKET
    --SILVER_DATASET_NAME     default: staffing_daily_metrics
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


def get_job_args():
    args = getResolvedOptions(
        sys.argv,
        ["JOB_NAME", "SILVER_BUCKET", "GOLD_BUCKET", "AWS_REGION"],
    )
    try:
        args["SILVER_DATASET_NAME"] = getResolvedOptions(sys.argv, ["SILVER_DATASET_NAME"])["SILVER_DATASET_NAME"]
    except Exception:
        args["SILVER_DATASET_NAME"] = "staffing_daily_metrics"
    return args


def find_latest_partition_path(s3_client, bucket, prefix_root, dataset_name):
    """Same latest-ingestion_date pattern used throughout this project,
    generalized with prefix_root since Silver uses 'silver/dataset=...'
    where Bronze used 'raw/dataset=...'."""
    prefix = f"{prefix_root}/dataset={dataset_name}/"
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")
    partition_prefixes = [cp["Prefix"] for cp in response.get("CommonPrefixes", [])]

    if not partition_prefixes:
        raise ValueError(
            f"No ingestion_date partitions found under s3://{bucket}/{prefix} - "
            f"has '{dataset_name}' actually been written to Silver yet?"
        )

    latest_prefix = sorted(partition_prefixes)[-1]
    return f"s3://{bucket}/{latest_prefix}"


def load_silver_daily(spark, path):
    """Silver is Parquet, not CSV - no header/encoding options needed,
    types are already correct from transform_silver.py. Adds year_month
    for the (facility, month) / (state, month) grouping grain."""
    df = spark.read.parquet(path)
    return df.withColumn("year_month", F.date_format(F.col("WorkDate"), "yyyy-MM"))


def build_facility_metrics(silver_df):
    """Grain: (facility, month). Daily ratios can be safely averaged
    here since a single facility's denominators don't change who
    they're being compared against."""
    return silver_df.groupBy("PROVNUM", "provider_name", "provider_state", "year_month").agg(
        F.avg("hprd_total_nurse").alias("avg_hprd_total_nurse"),
        F.sum("total_nurse_hours").alias("total_nurse_hours"),
        F.sum("total_nurse_hours_ctr").alias("_total_nurse_hours_ctr"),
        F.avg("occupancy_rate").alias("avg_occupancy_rate"),
        F.first("readmission_score", ignorenulls=True).alias("readmission_score"),
        F.countDistinct("WorkDate").alias("num_days_reported"),
    ).withColumn(
        "contract_pct_total_nurse",
        F.when(F.col("total_nurse_hours") > 0, F.col("_total_nurse_hours_ctr") / F.col("total_nurse_hours")),
    ).drop("_total_nurse_hours_ctr")


def build_state_summary(silver_df, facility_metrics_df):
    """Grain: (state, month). Staffing/occupancy/contract numbers are
    computed by summing raw daily totals first and dividing after -
    census/capacity-weighted, not an average of each facility's own
    average. Readmission is averaged across distinct facilities instead,
    pulled from facility_metrics rather than the daily rows."""
    state_daily_rollup = silver_df.groupBy("provider_state", "year_month").agg(
        F.sum("total_nurse_hours").alias("total_nurse_hours"),
        F.sum("total_nurse_hours_ctr").alias("_total_nurse_hours_ctr"),
        F.sum("MDScensus").alias("_total_census"),
        F.sum("num_certified_beds").alias("_total_beds"),
        F.countDistinct("PROVNUM").alias("num_facilities_reporting"),
    ).withColumn(
        "avg_hprd_total_nurse",
        F.when(F.col("_total_census") > 0, F.col("total_nurse_hours") / F.col("_total_census")),
    ).withColumn(
        "avg_occupancy_rate",
        F.when(F.col("_total_beds") > 0, F.col("_total_census") / F.col("_total_beds")),
    ).withColumn(
        "contract_pct_total_nurse",
        F.when(F.col("total_nurse_hours") > 0, F.col("_total_nurse_hours_ctr") / F.col("total_nurse_hours")),
    ).drop("_total_nurse_hours_ctr", "_total_census", "_total_beds")

    state_readmission = facility_metrics_df.groupBy("provider_state", "year_month").agg(
        F.avg("readmission_score").alias("avg_readmission_score")
    )

    return state_daily_rollup.join(state_readmission, ["provider_state", "year_month"], how="left")


def main():
    args = get_job_args()
    region = args["AWS_REGION"]

    print(f"Starting Gold aggregation job: {args['JOB_NAME']}")

    spark_context = SparkContext()
    glue_context = GlueContext(spark_context)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    s3_client = boto3.client("s3", region_name=region)

    silver_path = find_latest_partition_path(
        s3_client, args["SILVER_BUCKET"], "silver", args["SILVER_DATASET_NAME"]
    )
    print(f"Reading Silver from: {silver_path}")

    silver_df = load_silver_daily(spark, silver_path)
    silver_row_count = silver_df.count()
    print(f"Loaded {silver_row_count} Silver row(s).")

    facility_metrics_df = build_facility_metrics(silver_df)
    facility_metrics_count = facility_metrics_df.count()
    print(f"Built facility_metrics: {facility_metrics_count} (facility, month) row(s).")

    state_summary_df = build_state_summary(silver_df, facility_metrics_df)
    state_summary_count = state_summary_df.count()
    print(f"Built state_summary: {state_summary_count} (state, month) row(s).")

    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    facility_metrics_path = f"s3://{args['GOLD_BUCKET']}/gold/dataset=facility_metrics/ingestion_date={run_date}/"
    state_summary_path = f"s3://{args['GOLD_BUCKET']}/gold/dataset=state_summary/ingestion_date={run_date}/"

    facility_metrics_df.write.mode("overwrite").parquet(facility_metrics_path)
    print(f"Wrote facility_metrics to {facility_metrics_path}")

    state_summary_df.write.mode("overwrite").parquet(state_summary_path)
    print(f"Wrote state_summary to {state_summary_path}")

    job.commit()
    print("Gold aggregation complete.")


if __name__ == "__main__":
    main()

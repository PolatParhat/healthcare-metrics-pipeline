# Healthcare Metrics Project - Step 4 (AWS Build-Out) - State Tracker

Last updated: 2026-09-25. Read this whole file at the start of a new session before doing anything else - it exists so context isn't lost across sessions.

`Step4_Implementation/PIPELINE_STATE.md` is retired - this file is now the single tracker. Don't maintain both.

## What this project is

CMS PBJ nurse staffing + Nursing Home Compare data -> AWS pipeline -> Streamlit dashboard.
Full architecture/scope is the SME-approved design doc: `Healthcare_Metrics_Solution_Design_1.docx` (v1.2). This tracker is a working-state supplement to that doc, not a replacement - if they conflict, the doc wins and this file should be corrected.

## Standing working rules (do not relearn these the hard way)

- **Never create/provision AWS resources directly.** Give copy-paste-ready AWS CLI commands for the user to run themselves. This applies to every AWS resource: S3 buckets, IAM roles/policies, Glue jobs/crawlers/databases, DynamoDB tables, Secrets Manager secrets, SNS/EventBridge.
- **Writing/editing the actual Glue job `.py` files is a normal, direct deliverable** - not a restricted action. Write them, commit them straight into the user's connected project folder.
- **Never handle AWS credentials, access keys, or the Google service-account key contents directly.** Direct the user to enter those themselves.
- **Never assert a fact about the user's actual data without checking it first.** This session has been burned twice: (1) claiming the PBJ staffing file was missing from Bronze when it was actually there - only checked one run's log, not the real bucket; (2) assuming `transform_staffing_silver.py` should be domain-split into multiple Silver jobs, when the design doc explicitly describes ONE consolidated Silver job. Read the real file/header/doc before asserting anything about schema, data, or architecture.
- **The user's connected local folder** is `/Users/fulatipaerhati/Documents/DataEngineerBootcamp/Projects/HealthcareMetricsProject/` - reachable via the remote-devices bridge. Source data lives under `Data/`, Glue job code lives under `Step4_Implementation/glue_jobs/`.

## AWS resources that exist today (created by the user, from earlier CLI steps)

| Resource | Name | Notes |
|---|---|---|
| Region | `us-west-1` | All resources below are in this region |
| S3 Bronze bucket | `healthcare-metrics-bronze-941377112484` | Raw landing zone |
| S3 Silver bucket | `healthcare-metrics-silver-941377112484` | **Proposed name, not yet confirmed created** - verify before relying on it |
| IAM role | `HealthcareMetricsGlueRole` | Custom scoped policy, NOT the broad `AWSGlueServiceRole` managed policy (deliberately replaced early in this project) |
| DynamoDB table | `HealthcareMetricsSyncState` | Holds the Google Drive sync cursor (`state_id="google_drive_sync"`, `page_token` attribute) |
| Secrets Manager secret | `healthcare-metrics/google-drive-creds` | Google service-account JSON key |
| Glue Data Catalog database | `healthcare_metrics` | Shared across Bronze/Silver/Gold zones |
| SNS + EventBridge | Set up for job failure alerts (and optionally success) | Wired to the ingestion Glue job; needs to be extended to any new jobs as they're created |
| Glue Crawler | Exists, scans Bronze | May need a re-run - not confirmed it has crawled every current Bronze dataset (in particular, confirm it picked up `NH_ProviderInfo` and `NH_QualityMsr_Claims` before `validate_bronze.py`/`transform_silver.py` try to read them from the Catalog) |

## Bronze S3 key scheme (set by `ingest_pbj_data.py`)

```
s3://<bronze-bucket>/raw/dataset=<stable_dataset_name>/ingestion_date=<YYYY-MM-DD>/<original_filename>
```

`derive_dataset_name()` strips date/fiscal-year suffixes from filenames so the same conceptual dataset lands in one Glue table over time instead of a new table every month. A dataset can have more than one `ingestion_date` partition if it was re-ingested - downstream jobs should read the **latest** partition only (see `find_latest_partition_path()` pattern in the Silver job).

## Source files inventory (verified directly against the user's local `Data/` folder and actual file headers - not guessed)

Local path: `Data/PBJ_Daily_Nurse_Staffing_Q2_2024.csv` (core staffing file) + `Data/supportingFiles/` (20 more CSVs + `NH_Data_Dictionary.pdf`). All of these are already ingested into Bronze.

Key verified facts about the data itself:
- **Encoding: Windows-1252 (cp1252), not UTF-8.** Stated in the Solution Design doc Section 7. Must be set explicitly on every `spark.read.csv(...)` call (`.option("encoding", "windows-1252")`) - Spark defaults to UTF-8 otherwise.
- **PBJ file** (`PBJ_Daily_Nurse_Staffing_Q2_2024.csv`) real header (33 columns, confirmed by reading the actual file):
  `PROVNUM, PROVNAME, CITY, STATE, COUNTY_NAME, COUNTY_FIPS, CY_Qtr, WorkDate, MDScensus, Hrs_RNDON(_emp/_ctr), Hrs_RNadmin(_emp/_ctr), Hrs_RN(_emp/_ctr), Hrs_LPNadmin(_emp/_ctr), Hrs_LPN(_emp/_ctr), Hrs_CNA(_emp/_ctr), Hrs_NAtrn(_emp/_ctr), Hrs_MedAide(_emp/_ctr)`
  - `PROVNUM` is a zero-padded 6-character string (e.g. `"015009"`) - must be read as STRING, never let Spark infer it as an integer (drops the leading zero and breaks joins).
  - `WorkDate` is an **unseparated `YYYYMMDD` string** (e.g. `"20240401"`) - parse with `to_date(col, "yyyyMMdd")`, NOT `"M/d/yyyy"`.
  - This file is NOT covered by `NH_Data_Dictionary.pdf` - it's a separate CMS file with its own format; the header above came from reading the real CSV directly.
- **`NH_ProviderInfo_Oct2024.csv`** key column is literally `"CMS Certification Number (CCN)"` (with spaces/parens) - also a zero-padded 6-char string, quoted in the CSV. Confirmed same value format as PBJ's `PROVNUM` (join key match verified with real sample rows). Relevant columns for this project: `Number of Certified Beds`, `Overall Rating`, `Staffing Rating`, `Reported RN/Total Nurse Staffing HPRD`, `Total nursing staff turnover`, `Registered Nurse turnover`.
- **`NH_QualityMsr_Claims_Oct2024.csv`** - long/skinny format, one row per facility per measure code, NOT one row per facility. Only 4 distinct Measure Codes exist in this file:
  - `521` = "Percentage of short-stay residents who were rehospitalized after a nursing home admission" **<- this is the readmission measure used for metric #5**
  - `522` = outpatient ED visits (short-stay)
  - `551` = hospitalizations per 1000 long-stay resident days
  - `552` = outpatient ED visits per 1000 long-stay resident days
  - Score is annual (`Measure Period` like `20230401-20240331`), not daily - grain mismatch with PBJ, handled by only joining/aggregating at Gold, not repeating meaningfully at Silver's daily grain.
- Full data dictionary (`NH_Data_Dictionary.pdf`) covers Tables 2-27 (ProviderInfo, Ownership, Penalties, COVID Vax, MDS/Claims QM, SNF QRP, SNF VBP) but does NOT cover the PBJ file at all.
- **Cost metrics are not buildable from any file in this dataset.** Only real dollar figures anywhere: `NH_Penalties`' `Fine Amount` (regulatory fines) and one *national* aggregate incentive-payment total in the VBP aggregate file. No payroll, per-stay cost, or revenue data exists anywhere.
- No "department" concept exists anywhere - this is nursing-home/SNF data, not hospital data. No per-shift, per-employee, or per-patient-stay records exist either (rules out overtime %, shifts-per-nurse, ALOS, patient throughput, shift-time-of-day metrics).

## Metrics - LOCKED (5 total, confirmed by the user)

1. **Nurse-to-patient ratio** (Staffing) - `(Hrs_RN + Hrs_LPN + Hrs_CNA) / MDScensus`, by facility/state. No department breakdown (doesn't exist).
2. **Total nurse hours worked** (Staffing) - by facility/state/month, raw sum of PBJ hours rolled up monthly.
3. **Occupancy rate** (Facility) - `MDScensus / Number of Certified Beds`, monthly. Only Q2 2024 data exists, so "trend" is within-quarter (April/May/June), not a full year.
4. **Permanent vs. contract staffing ratio** (Operational) - PBJ's `_emp` vs `_ctr` hours split, by role and total.
5. **Staffing-vs-readmission correlation** (Quality) - facility-level average HPRD/occupancy (from Gold) plotted against Measure Code 521's Adjusted Score from `NH_QualityMsr_Claims`.

Cost metrics category: ruled out entirely, no data exists. "Department"-scoped versions of any metric: not possible, no such dimension in SNF data.

## File plan (per the Solution Design doc's `ingest -> crawl Bronze -> validate -> transform (Silver) -> crawl Silver -> aggregate (Gold) -> crawl Gold` flow)

The doc describes ONE consolidated Silver job and ONE consolidated Gold job (not split per metric/domain) - confirmed from Section 5's component table language ("the join across PBJ + NH_ProviderInfo + NH_QualityMsr_Claims" as a single job description).

| File | Status | Purpose |
|---|---|---|
| `glue_jobs/ingest_pbj_data.py` | **Done, working in production** | Python Shell job. Incremental Drive->Bronze ingestion via Changes API + DynamoDB cursor. Handles first-run backfill and dataset-name-stable partitioning. |
| `glue_jobs/validate_bronze.py` | **Done** | Checks all 3 Bronze tables (PBJ, `NH_ProviderInfo`, `NH_QualityMsr_Claims`) via real AWS Glue Data Quality (DQDL + `EvaluateDataQuality`), looping through each with its own ruleset. Hard rules (row/column count, key completeness/format) raise and fail the job; soft rules (PBJ hour outlier thresholds, `NH_QualityMsr_Claims` measure-code check) just log warnings. Replaces `validate_bronze_staffing.py` (now superseded, still present on disk but unused). **Open item:** this job reads via `from_catalog()`, which has no "latest ingestion_date partition only" filter like the other jobs do - if a table is ever re-ingested, this would validate old + new partitions together. Not yet fixed.
| `glue_jobs/transform_silver.py` | **Done** | The one consolidated Silver job. Joins PBJ + `NH_ProviderInfo` + `NH_QualityMsr_Claims` (Measure Code 521 only - readmission). Daily grain. Produces `hprd_rn/lpn/cna/total_nurse`, `total_nurse_hours` (raw sum, metric #2), `occupancy_rate`, `contract_pct_total_nurse`, `readmission_score` (repeated per day per facility - annual value, not daily). Left joins throughout so unmatched facilities keep their staffing data with nulls rather than being dropped. Replaces `transform_staffing_silver.py` (superseded, still on disk, unused).
| `glue_jobs/aggregate_gold.py` | **Done** | Reads Silver (Parquet), builds `facility_metrics` (facility+month, daily ratios simply averaged since one facility's denominators don't change) and `state_summary` (state+month, staffing/occupancy/contract computed by summing raw daily totals THEN dividing - census/capacity-weighted, not an average of facility averages - to avoid small facilities skewing the state number as much as large ones). Readmission is averaged across distinct facilities per state, sourced from `facility_metrics`, not the daily rows (avoids over-weighting facilities with more reporting days). Correlation itself (metric #5) is NOT computed here - it's a Streamlit/pandas `.corr()` calculation on the `facility_metrics` table at dashboard load time, not a stored Gold column.

## Testing progress (2026-09-25/26) - real errors found and fixed while running the chain (see also the Glue Workflow section above for the IAM/orchestration-specific bugs found after this list)

6. **`HealthcareMetricsTransformSilver` now PASSES** (after the `ccn` duplicate-column fix, re-uploaded and re-run). Output spot-checked against known ground truth from way back when we first inspected the raw files - PROVNUM 015009 on 2024-04-01: `hprd_total_nurse`=4.73098 (matches (55.7+25.5+160.08)/51 exactly), `occupancy_rate`=0.894737 (matches 51/57 beds exactly), `readmission_score`=16.990363 (matches the real Measure Code 521 Adjusted Score for that facility exactly). Real, working output, not just "job didn't crash." Row count 1,325,004 vs. 1,325,324 raw PBJ rows - the 320-row gap is `apply_data_quality_gate()`'s row-level rejection (division-by-zero risk), expected, not a bug. Moving on to testing `HealthcareMetricsGoldETL` next.
6a. **`HealthcareMetricsTransformSilver` first run (before the fix above) failed:** `AnalysisException: Found duplicate column(s) when inserting into ... : ccn`. Real bug, not a data issue: both `load_provider_info()` and `load_readmission_scores()` alias their key column to `ccn`, and both joins in `main()` use the `left.col == right.col` expression form rather than a plain column-name join - Spark keeps both sides' columns in that form instead of coalescing them, so after joining both Provider Info and Quality Claims the result carried two separate `ccn` columns. Everything upstream tolerated it (including the `F.col("ccn").isNull()` unmatched-count check right after the first join, since only one `ccn` existed yet); only the Parquet writer's schema check caught it. **Fixed** in both `Step4_Implementation/glue_jobs/transform_silver.py` and `aws/scripts/HealthcareMetricsTransformSilver.py` (verified in sync): `.drop("ccn")` added right after each join, before the next one runs. **Still need to do on AWS:** re-upload the script, re-run (no `update-job` needed - only script content changed).

4. **`HealthcareMetricsValidateBronze` now PASSES end to end** (re-created after an accidental job deletion, using the corrected `--BRONZE_BUCKET`/`--AWS_REGION`-only parameter set). Bronze validation is done and confirmed working - moving on to testing `HealthcareMetricsTransformSilver` next.
5. **First real (non-config) failure, and a genuinely new data fact:** all 3 hard `matches "[0-9]{6}"` rules failed - PROVNUM and CCN. Checked directly against the real local files (not guessed): ~1.6-1.8% of rows in ALL THREE tables (PBJ 21,385/1,325,324; Provider Info 261/14,814; Quality Claims 1,044/59,256) have a legitimate 6-character ALPHANUMERIC facility ID (e.g. `01A193`), not a pure 6-digit one. Consistent rate across 3 independent files = a real CMS ID format, not corrupted data or a parsing bug (an earlier print of mine briefly suggested apostrophe-wrapping too - that was a repr-of-repr bug in my own diagnostic script, not real; the raw values are plain alphanumeric, no extra characters). This doesn't affect Silver's joins - PROVNUM/CCN are never cast to numeric anywhere in this project, so these facilities were never actually at risk of being dropped, only of failing this over-strict validation rule. **Fixed:** all `matches "[0-9]{6}"` rules (PBJ's PROVNUM, Provider Info's and Quality Claims' CCN) changed to `matches "[0-9A-Za-z]{6}"` in both `Step4_Implementation/glue_jobs/validate_bronze.py` and `aws/scripts/HealthcareMetricsValidateBronze.py` (verified in sync). **Still need to do on AWS:** re-upload the script to S3 (no `update-job` needed this time - only the script content changed, not `ScriptLocation`/`DefaultArguments`), then re-run.

1. **`EntityNotFoundException` on `HealthcareMetricsValidateBronze`'s first run.** `from_catalog()` couldn't find the tables under the script's default names. Root cause: the crawler registered all Bronze tables with a `bronze_dataset_` prefix (e.g. `bronze_dataset_pbj_daily_nurse_staffing_q2_2024`, `bronze_dataset_nh_providerinfo`, `bronze_dataset_nh_qualitymsr_claims`), not the bare names the script expected. Confirmed via `aws glue get-tables --database-name healthcare_metrics` - all 3 tables (plus every other CMS file) ARE registered, crawler is fine, it's just a naming-convention mismatch.
2. **Bigger issue found on the second run:** `EvaluateDataQuality.apply()` itself failed with `Unable to parse file: PBJ_Daily_Nurse_Staffing_Q2_2024.csv`. Root cause, confirmed via `aws glue get-table ... --query Table.StorageDescriptor.SerdeInfo`: the crawler classified all these tables with `LazySimpleSerDe`, which has NO quote-character support and no declared encoding - and this file is cp1252, not UTF-8 (per Solution Design Section 7). AWS Glue Data Quality's own file reader choked on it outright.
3. **Fix (already made, both `Step4_Implementation/glue_jobs/validate_bronze.py` and `aws/scripts/HealthcareMetricsValidateBronze.py` are updated and in sync):** `validate_bronze.py` no longer reads via `from_catalog()` at all. It now reads each Bronze table directly from its latest S3 partition with `.option("encoding", "windows-1252")` - the exact same proven-working approach `transform_silver.py` already uses - then wraps the resulting Spark DataFrame with `DynamicFrame.fromDF()` before handing it to `EvaluateDataQuality.apply()`. Numeric DQDL rules (`Hrs_RN <= 500` etc., `Number of Certified Beds > 0`) get their specific columns cast to `DoubleType` first; PROVNUM/CCN and everything else stays string, same leading-zero-safety rule as everywhere else in this project.
   - **Job parameters changed as a result:** `--GLUE_DATABASE`, `--PBJ_TABLE_NAME`, `--PROVIDER_INFO_TABLE_NAME`, `--QUALITY_MSR_CLAIMS_TABLE_NAME` are GONE (no longer used - nothing reads the Catalog anymore). New required param: `--BRONZE_BUCKET`. New optional params (same dataset-name convention as `transform_silver.py`, defaults already correct): `--PBJ_DATASET_NAME`, `--PROVIDER_INFO_DATASET_NAME`, `--QUALITY_MSR_CLAIMS_DATASET_NAME`.
   - **Still need to do on AWS (not done yet):** re-upload the fixed script to S3, and run `aws glue update-job` on `HealthcareMetricsValidateBronze` to replace its `DefaultArguments` (drop the Catalog-related ones, add `--BRONZE_BUCKET`), then re-run.

## Verified findings from checking `aws/` folder directly (2026-09-25) - read before doing AWS provisioning steps

- **Found and fixed a real bug:** `aws/scripts/validate_bronze.py` (the staging copy the user uploads to S3 from) actually contained `transform_silver.py`'s content under the wrong filename - the docstring literally said "Silver Transform Job". If this had been uploaded to S3 as-is and pointed at by a Glue job named validate-bronze, that job would have silently run Silver transform logic instead of the data quality gate. Fixed by copying the correct `Step4_Implementation/glue_jobs/validate_bronze.py` content over it. `aws/scripts/transform_silver.py` and `aws/scripts/aggregate_gold.py` were already correct (verified byte-for-byte against `Step4_Implementation/glue_jobs/`, aside from a trailing newline).
- **Glue job names are NOT free choice - they're constrained by IAM/EventBridge already in place:**
  - `aws/glue-scoped-policy.json` only grants `glue:StartJobRun`/`GetJobRun`/etc. on `arn:aws:glue:us-west-1:941377112484:job/HealthcareMetrics*` - a job whose name doesn't start with the literal string `HealthcareMetrics` cannot be started under this role.
  - `aws/glue-success-pattern.json` (the EventBridge rule for success notifications) is filtered to `"jobName": ["HealthcareMetricsGoldETL"]` specifically - this name is already baked into that rule from an earlier session, not something decided this session.
  - Every other resource in `aws/*.json` follows the same `HealthcareMetrics<PascalCase>` convention (`HealthcareMetricsSyncState`, `HealthcareMetricsAlerts`, `HealthcareMetricsGlueRole`).
  - **Conclusion: the 3 new jobs should be named `HealthcareMetricsValidateBronze`, `HealthcareMetricsTransformSilver`, and `HealthcareMetricsGoldETL`** (not the lowercase-hyphen `validate-bronze`/`transform-silver`/`aggregate-gold` this file previously proposed) - `HealthcareMetricsGoldETL` in particular is not optional, since the success EventBridge rule already expects that exact name for the Gold job.
- **Correction to the EventBridge item below:** `aws/glue-failure-pattern.json` has NO `jobName` filter at all - it matches `FAILED`/`TIMEOUT`/`ERROR` from any Glue job, crawler, or workflow. Failure alerts already cover the 3 new jobs with zero changes needed. Only the SUCCESS pattern is name-scoped, and only to one job (`HealthcareMetricsGoldETL`) - if success notifications are wanted for validate/transform too, that pattern needs its `jobName` list extended, but this is optional, not a blocker like previously written.
- `aws/glue-inline-policy.json`'s S3 permissions are already a wildcard - `arn:aws:s3:::healthcare-metrics-*-941377112484(/*)` - so a new Gold bucket named `healthcare-metrics-gold-941377112484` needs NO policy change for S3 access. The Silver bucket is in the same boat once it exists.
- Not yet confirmed from files alone (couldn't check without running AWS CLI, which I don't run myself): whether `healthcare-metrics-silver-941377112484` actually exists yet, the exact registered name of the original ingestion Glue job, and whether Data Quality-specific IAM actions (`glue:StartDataQualityRulesetEvaluationRun`, `glue:GetDataQualityResult`, `cloudwatch:PutMetricData` for the `enableDataQualityCloudWatchMetrics` option) are covered anywhere - `glue-scoped-policy.json` as read does NOT list these, so `validate_bronze.py` will likely fail on IAM until they're added.

## Crawlers - COMPLETE (2026-09-26)

`HealthcareMetricsSilverCrawler` and `HealthcareMetricsGoldCrawler` created and run. Real bug found and fixed on Gold: the crawler initially pointed at the whole `gold/` root, and since `gold/dataset=facility_metrics/` and `gold/dataset=state_summary/` share the `dataset=<value>` folder naming (looks like Hive partitioning), the crawler merged BOTH into one table literally named `gold`, with `dataset`/`ingestion_date` as partition keys. Confirmed via `get-table`: the merged schema shared `total_nurse_hours` between facility-grain and state-grain data under one column - `SELECT SUM(total_nurse_hours) FROM gold` without filtering on the `dataset` partition would silently sum facility-level and state-level totals together. **Fixed:** deleted the bad `gold` table and the crawler, recreated the crawler with two explicit S3 targets (`gold/dataset=facility_metrics/` and `gold/dataset=state_summary/` separately) instead of one target at the root - now produces two distinct, correctly-separated tables.

Silver's crawler needed no fix - only one `dataset=` value exists under `silver/`, so there was nothing to wrongly merge. It correctly produced `dataset_staffing_daily_metrics` (the real output) and `_rejects` (the 320-row rejects output) as two separate, correctly-distinct tables. Optional, not a bug: if rejected rows shouldn't be Catalog-visible to analysts, rescope this crawler to just `silver/dataset=staffing_daily_metrics/` later - low priority, not done.

## Glue Workflow orchestration - COMPLETE (2026-09-26)

`HealthcareMetricsPipeline` workflow built with 7 triggers chaining the full flow: `HealthcareMetricsIngestion -> HealthcareMetricsBronzeCrawler -> HealthcareMetricsValidateBronze -> HealthcareMetricsTransformSilver -> HealthcareMetricsSilverCrawler -> HealthcareMetricsGoldETL -> HealthcareMetricsGoldCrawler`. One real bug found and fixed: `HealthcareMetricsGlueActions` (the role's original scoped policy) was missing `glue:GetPartition` (singular) and other partition CRUD actions - only had the plural/batch forms. A crawler re-crawling an existing table needs the singular action to check a specific partition; without it, `HealthcareMetricsBronzeCrawler` failed with `AccessDeniedException`. Fixed by adding `glue:GetPartition`, `glue:CreatePartition`, `glue:UpdatePartition`, `glue:DeletePartition`, `glue:BatchDeletePartition` to the policy (both the live IAM policy and the local `aws/glue-scoped-policy.json` reference file, kept in sync).

**Full clean-state test - PASSED (2026-09-26):** Bronze/Silver/Gold S3 zones emptied, ingestion's DynamoDB sync cursor deleted (forces full backfill), all Catalog tables deleted, then the entire workflow run from `HealthcareMetricsStartIngestion` through to `HealthcareMetricsGoldCrawler` - confirmed working end to end from a genuinely empty state, not just a re-run on top of existing data. This is the last real validation of the AWS build-out - the pipeline itself (ingest -> crawl -> validate -> transform -> crawl -> aggregate -> crawl) is done and proven.

## Manual test chain - COMPLETE (2026-09-25/26)

All 3 jobs (`HealthcareMetricsValidateBronze` -> `HealthcareMetricsTransformSilver` -> `HealthcareMetricsGoldETL`) now run successfully end to end, in order, with real bugs found and fixed along the way (see the "Testing progress" section above for the full list: crawler naming mismatch, LazySimpleSerDe/encoding parse failure, missing Data Quality + CloudWatch IAM actions, an over-strict `[0-9]{6}` regex that didn't account for real alphanumeric CMS facility IDs, and a duplicate-`ccn`-column bug from an expression-form join). Gold output cross-checked against known-correct values (PROVNUM 015009) and came back exactly right - `num_days_reported` matches the calendar exactly, `readmission_score` carries through correctly, `avg_hprd_total_nurse` is in the expected range.

**Open design decision, not a bug, for whenever the dashboard's correlation view gets built:** metric #5's correlation was tested at facility-**month** grain (43,685 rows - one row per facility per month), which weights a fully-reporting facility 3x versus one with partial data. Decide before building that chart whether to collapse to one row per facility first (e.g. average `avg_hprd_total_nurse` across its months) or keep facility-month grain deliberately.

Immediate next step (as of this file's last update): the manual test chain is done - all 4 Glue job scripts are now written, tested, and correctly staged in both `Step4_Implementation/glue_jobs/` and `aws/scripts/`. Nothing has been provisioned/run on AWS for validate/transform_silver/aggregate_gold yet - remaining work, in order:
1. Confirm whether the Silver bucket exists yet; create it and the Gold bucket if not.
2. Upload the 3 scripts from `aws/scripts/` to S3, and create the 3 Glue job resources using the corrected `HealthcareMetrics*`-prefixed names above (`HealthcareMetricsValidateBronze`, `HealthcareMetricsTransformSilver`, `HealthcareMetricsGoldETL`) so they're covered by the existing IAM policy and EventBridge rules without further changes to those.
3. Extend `HealthcareMetricsGlueRole`'s policy: add the Glue Data Quality + CloudWatch metrics actions `validate_bronze.py` needs (not currently in `glue-scoped-policy.json`). Bucket and Catalog access are already covered by the existing wildcards - no change needed there.
4. Confirm the Glue Crawler has registered `NH_ProviderInfo` and `NH_QualityMsr_Claims` as Catalog tables (re-run the crawler if not) - `validate_bronze.py` reads via `from_catalog()`, so it needs these to exist first.
5. (Optional, not a blocker) Extend `aws/glue-success-pattern.json`'s `jobName` list if success notifications are wanted for the validate/transform jobs too - failure alerts already cover all 3 new jobs with no changes.
6. Known open item on `validate_bronze.py`: it reads via `from_catalog()` with no "latest partition only" filter, unlike the other jobs - could validate stale + current partitions together if a table is ever re-ingested. Not yet fixed.
7. Run the whole chain manually once (validate -> transform_silver -> aggregate_gold) and check the actual Gold output before building Glue Workflow orchestration to chain them automatically.
8. `transform_staffing_silver.py` and `validate_bronze_staffing.py` are both fully superseded now - safe to delete (from both `Step4_Implementation/glue_jobs/` and anywhere else they were staged) once the new ones are confirmed working.

## Mistakes made this session (so they aren't repeated)

- Assumed PBJ staffing file was missing from Bronze based on one run's log instead of the real bucket contents - user corrected with an S3 console link.
- Assumed `WorkDate` was `M/d/yyyy` format without checking - it's actually unseparated `yyyyMMdd`. Caught before shipping by reading the real file.
- Missed the cp1252 encoding requirement entirely in the first version of the Silver job, despite it being explicitly stated in the Solution Design doc - only caught after the user shared the doc.
- Told the user "yes, one Silver job per metric category" before reading the Solution Design doc closely - the doc actually specifies ONE consolidated Silver job and ONE consolidated Gold job, not domain-split. Corrected once the doc's actual wording was checked.
- Theorized a wrong root cause (backlog from failed runs) for an unexpected 10-file re-ingestion during ingestion testing, before the real cause (Google Drive permission-propagation lag on a newly-shared folder) was confirmed.

General lesson driving all of these: **check the real file/doc/bucket before asserting anything specific about this project's data or design - don't reason from filenames, memory of an earlier message, or general domain knowledge alone.**

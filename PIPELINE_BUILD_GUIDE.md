# CMS PBJ Nurse Staffing Pipeline — Full Build Guide

This is a complete, ordered, copy-paste walkthrough for building the entire
pipeline from nothing: raw CMS files on Google Drive all the way to a live
Streamlit dashboard. It assumes no prior AWS knowledge beyond having an AWS
account and the AWS CLI installed and configured (`aws configure`).

Every AWS CLI command below is meant to be run **by you**, in your own
terminal, with your own credentials — nothing here should ever be run by an
AI assistant on your behalf, since that would require handing over AWS
credentials, which you should never do.

If you already have this repository, the actual working code
(`Step4_Implementation/glue_jobs/*.py`, `aws/scripts/*.py`, `dashboard/`) is
already written and tested — this guide is about provisioning the AWS
resources those files need and wiring them together in the right order. If
you don't have the repo yet, clone/download it first; the exact file paths
referenced below assume you're running commands from the project root.

---

## 0. What you're building

```
Google Drive folder (CMS PBJ + Nursing Home Compare CSVs)
        │
        │  (1) Ingestion — Glue Python Shell job, incremental via Drive Changes API
        ▼
S3 Bronze bucket  (raw/dataset=<name>/ingestion_date=<date>/<file>.csv)
        │
        │  (2) Bronze Crawler — registers each dataset as a Glue Catalog table
        ▼
        │  (3) Validate — Glue Data Quality gate (hard rules fail the job; soft rules just warn)
        ▼
S3 Silver bucket  (silver/dataset=staffing_daily_metrics/ingestion_date=<date>/*.parquet)
        │
        │  (4) Silver Crawler
        │  (5) Transform — join PBJ + Provider Info + Quality Claims, compute per-day metrics
        ▼
S3 Gold bucket  (gold/dataset=facility_metrics|state_summary/ingestion_date=<date>/*.parquet)
        │
        │  (6) Gold Aggregation — roll Silver up to (facility, month) and (state, month)
        │  (7) Gold Crawler
        ▼
Streamlit dashboard  (reads Gold Parquet directly from S3, no Athena)
```

A **Glue Workflow** chains steps 1–7 together with triggers so the whole
thing runs as one push-button (or scheduled) pipeline. **SNS + EventBridge**
send you an alert if any step fails.

Everything is provisioned in one AWS region. The commands below default to
`us-west-1` — change `AWS_REGION` if you want a different one, just keep it
consistent everywhere.

---

## 1. Prerequisites

- An AWS account, and the AWS CLI v2 installed and configured (`aws configure`) with an IAM user/role that can create S3 buckets, IAM roles/policies, Glue resources, DynamoDB tables, Secrets Manager secrets, SNS topics, and EventBridge rules.
- A Google account with access to Google Cloud Console (for the service account that reads the shared Drive folder) — only needed if you want the automated Drive→S3 ingestion job; if you'd rather upload CSVs to S3 by hand, you can skip Step 6 entirely and `aws s3 cp` your files straight into the Bronze key layout described there.
- Python 3.9+ locally if you want to run the Streamlit dashboard locally as well as deployed.
- The CMS source files: `PBJ_Daily_Nurse_Staffing_<quarter>.csv`, `NH_ProviderInfo_<month>.csv`, and `NH_QualityMsr_Claims_<month>.csv`, downloaded from the [CMS Provider Data Catalog](https://data.cms.gov/provider-data/). These are public files CMS re-publishes periodically — filenames change with each release, but this pipeline's ingestion job is built to derive a stable dataset name from the filename (see Step 6), and the Bronze/Silver/Gold jobs are built to read whatever the **latest** ingestion produced. This guide's DQDL rules and `_DATASET_NAME` defaults reference the specific files this project's original build validated (33 / 103 / 17 columns respectively) — if you're using a different CMS release with a different column count, update the `ColumnCount` rule in `validate_bronze.py` to match the real header (verify by actually opening the file, don't guess).

---

## 2. One-time account setup

Set these shell variables — every command below reuses them, so get them
right once:

```bash
export AWS_REGION=us-west-1
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

export BRONZE_BUCKET="healthcare-metrics-bronze-${AWS_ACCOUNT_ID}"
export SILVER_BUCKET="healthcare-metrics-silver-${AWS_ACCOUNT_ID}"
export GOLD_BUCKET="healthcare-metrics-gold-${AWS_ACCOUNT_ID}"
export GLUE_DB=healthcare_metrics
export GLUE_ROLE_NAME=HealthcareMetricsGlueRole
export DYNAMO_TABLE=HealthcareMetricsSyncState
export DRIVE_SECRET_NAME=healthcare-metrics/google-drive-creds
export SNS_TOPIC_NAME=HealthcareMetricsAlerts

echo "Account: $AWS_ACCOUNT_ID  Region: $AWS_REGION"
echo "Buckets: $BRONZE_BUCKET / $SILVER_BUCKET / $GOLD_BUCKET"
```

Every resource name below is prefixed `HealthcareMetrics` (jobs, crawlers,
workflow, IAM role/policies, DynamoDB table, SNS topic) — the IAM policy in
Step 4 scopes access to exactly that prefix, so **don't rename resources
without also updating the policy's `Resource` ARNs**, or you'll hit
`AccessDeniedException` (this bit the original build more than once — see
the Troubleshooting appendix).

---

## 3. Step 1 — S3 buckets (Bronze / Silver / Gold)

Three buckets, one per data zone. `us-west-1` needs an explicit
`LocationConstraint`; drop that flag if you use `us-east-1` instead.

```bash
for BUCKET in "$BRONZE_BUCKET" "$SILVER_BUCKET" "$GOLD_BUCKET"; do
  aws s3api create-bucket \
    --bucket "$BUCKET" \
    --region "$AWS_REGION" \
    --create-bucket-configuration LocationConstraint="$AWS_REGION"

  aws s3api put-public-access-block \
    --bucket "$BUCKET" \
    --public-access-block-configuration \
      BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
done
```

There's no bucket-internal folder structure to pre-create — each Glue job
writes its own `raw/`, `silver/`, `gold/` prefixes with `dataset=`/
`ingestion_date=` partitions the first time it runs.

---

## 4. Step 2 — Glue Data Catalog database

One shared database across all three zones (Bronze tables, the Silver
table, and the two Gold tables all register here):

```bash
aws glue create-database \
  --region "$AWS_REGION" \
  --database-input '{"Name": "'"$GLUE_DB"'"}'
```

---

## 5. Step 3 — IAM role for Glue jobs & crawlers

This is the single role every Glue job and crawler in this pipeline
assumes. It is a **deliberately scoped custom role**, not the broad AWS
managed `AWSGlueServiceRole` — everything it can touch is limited to
resources named `HealthcareMetrics*` and the three `healthcare-metrics-*`
buckets.

**Trust policy** (who can assume this role — the Glue service itself):

```bash
cat > /tmp/glue-trust-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [{"Effect": "Allow", "Principal": {"Service": "glue.amazonaws.com"}, "Action": "sts:AssumeRole"}]
}
EOF

aws iam create-role \
  --role-name "$GLUE_ROLE_NAME" \
  --assume-role-policy-document file:///tmp/glue-trust-policy.json
```

**Policy 1 — `HealthcareMetricsAccess`** (S3 read/write on the three
buckets, plus reading the Google Drive service-account secret):

```bash
cat > /tmp/glue-inline-policy.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": ["s3:GetObject", "s3:PutObject", "s3:ListBucket", "s3:DeleteObject"],
            "Resource": [
                "arn:aws:s3:::healthcare-metrics-*-${AWS_ACCOUNT_ID}",
                "arn:aws:s3:::healthcare-metrics-*-${AWS_ACCOUNT_ID}/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": ["secretsmanager:GetSecretValue"],
            "Resource": "arn:aws:secretsmanager:${AWS_REGION}:${AWS_ACCOUNT_ID}:secret:${DRIVE_SECRET_NAME}-*"
        }
    ]
}
EOF

aws iam put-role-policy \
  --role-name "$GLUE_ROLE_NAME" \
  --policy-name HealthcareMetricsAccess \
  --policy-document file:///tmp/glue-inline-policy.json
```

**Policy 2 — `HealthcareMetricsDynamoDBAccess`** (read/write the ingestion
sync-cursor table):

```bash
cat > /tmp/glue-dynamo-policy.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem"],
            "Resource": "arn:aws:dynamodb:${AWS_REGION}:${AWS_ACCOUNT_ID}:table/${DYNAMO_TABLE}"
        }
    ]
}
EOF

aws iam put-role-policy \
  --role-name "$GLUE_ROLE_NAME" \
  --policy-name HealthcareMetricsDynamoDBAccess \
  --policy-document file:///tmp/glue-dynamo-policy.json
```

**Policy 3 — `HealthcareMetricsGlueActions`** (control the jobs/crawlers/
workflow themselves, plus full Data Catalog + partition CRUD — this is the
policy that had to be extended twice during the original build; the
version below already includes both fixes):

```bash
cat > /tmp/glue-scoped-policy.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "glue:GetJob", "glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun",
                "glue:GetCrawler", "glue:StartCrawler", "glue:StopCrawler",
                "glue:GetWorkflow", "glue:GetWorkflowRun", "glue:StartWorkflowRun", "glue:GetWorkflowRunProperties"
            ],
            "Resource": [
                "arn:aws:glue:${AWS_REGION}:${AWS_ACCOUNT_ID}:job/HealthcareMetrics*",
                "arn:aws:glue:${AWS_REGION}:${AWS_ACCOUNT_ID}:crawler/HealthcareMetrics*",
                "arn:aws:glue:${AWS_REGION}:${AWS_ACCOUNT_ID}:workflow/HealthcareMetrics*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "glue:GetDatabase", "glue:CreateDatabase", "glue:GetTable", "glue:GetTables",
                "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable",
                "glue:GetPartition", "glue:GetPartitions", "glue:CreatePartition", "glue:BatchCreatePartition",
                "glue:UpdatePartition", "glue:DeletePartition", "glue:BatchDeletePartition", "glue:BatchGetPartition"
            ],
            "Resource": [
                "arn:aws:glue:${AWS_REGION}:${AWS_ACCOUNT_ID}:catalog",
                "arn:aws:glue:${AWS_REGION}:${AWS_ACCOUNT_ID}:database/${GLUE_DB}*",
                "arn:aws:glue:${AWS_REGION}:${AWS_ACCOUNT_ID}:table/${GLUE_DB}*/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
            "Resource": "arn:aws:logs:${AWS_REGION}:${AWS_ACCOUNT_ID}:log-group:/aws-glue/*"
        }
    ]
}
EOF

aws iam put-role-policy \
  --role-name "$GLUE_ROLE_NAME" \
  --policy-name HealthcareMetricsGlueActions \
  --policy-document file:///tmp/glue-scoped-policy.json
```

**Policy 4 — `HealthcareMetricsDataQualityPolicy`** (AWS Glue Data Quality
is a separate action namespace from regular Glue job/crawler control — the
`validate_bronze.py` job's `EvaluateDataQuality.apply()` call needs these
specifically, plus `cloudwatch:PutMetricData` for its
`enableDataQualityCloudWatchMetrics` option):

```bash
cat > /tmp/glue-dataquality-policy.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "glue:StartDataQualityRulesetEvaluationRun",
                "glue:GetDataQualityRulesetEvaluationRun",
                "glue:CancelDataQualityRulesetEvaluationRun",
                "glue:GetDataQualityResult",
                "glue:BatchGetDataQualityResult",
                "glue:ListDataQualityResults",
                "glue:CreateDataQualityRuleset",
                "glue:GetDataQualityRuleset",
                "glue:UpdateDataQualityRuleset",
                "glue:PublishDataQuality"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": ["cloudwatch:PutMetricData"],
            "Resource": "*",
            "Condition": {
                "StringEquals": {"cloudwatch:namespace": "Glue/DataQuality"}
            }
        }
    ]
}
EOF

aws iam put-role-policy \
  --role-name "$GLUE_ROLE_NAME" \
  --policy-name HealthcareMetricsDataQualityPolicy \
  --policy-document file:///tmp/glue-dataquality-policy.json
```

> **Note on Policy 4:** AWS Glue Data Quality's exact IAM action list isn't
> as clearly documented as the rest of Glue, and the original build found
> the two `AccessDeniedException`s above (`cloudwatch:PutMetricData`, then
> `glue:PublishDataQuality`) one at a time, iteratively, by re-running the
> job and reading each error message. If you still get
> `AccessDeniedException` for some other `glue:*DataQuality*` action after
> applying this policy, add that exact action name from the error message
> to this policy and re-run — don't guess ahead of what the real error
> tells you.

---

## 6. Step 4 — Supporting resources (DynamoDB, Secrets Manager, Google Drive)

**DynamoDB table** — holds the ingestion job's Google Drive "page token"
(sync cursor), so re-runs only pick up files that changed since last time:

```bash
aws dynamodb create-table \
  --region "$AWS_REGION" \
  --table-name "$DYNAMO_TABLE" \
  --attribute-definitions AttributeName=state_id,AttributeType=S \
  --key-schema AttributeName=state_id,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST
```

**Google Cloud service account** (one-time, done in the Google Cloud
Console, not the AWS CLI):

1. Create (or reuse) a Google Cloud project.
2. Enable the **Google Drive API** for that project.
3. Create a **service account**, and generate a JSON key for it (Console →
   IAM & Admin → Service Accounts → your account → Keys → Add key → JSON).
   Download the key file.
4. In Google Drive, share the folder containing the CMS CSVs with that
   service account's email address (it looks like
   `something@your-project.iam.gserviceaccount.com`) — **Viewer** access is
   enough, since ingestion only reads.
5. Note the shared folder's Drive **folder ID** (the long ID in the
   folder's URL after `/folders/`) — you'll pass this as
   `--DRIVE_FOLDER_ID` when creating the ingestion job in Step 8.

**Secrets Manager secret** — store that downloaded JSON key so the
ingestion job can authenticate without the key ever living in S3 or in
job parameters:

```bash
aws secretsmanager create-secret \
  --region "$AWS_REGION" \
  --name "$DRIVE_SECRET_NAME" \
  --secret-string file:///path/to/your/downloaded-service-account-key.json
```

(Replace the path with wherever you saved the key file in step 3 above.
Never commit this key file to git — this project's `.gitignore` already
excludes files matching its naming pattern; double-check before your first
commit either way.)

---

## 7. Step 5 — SNS + EventBridge alerting

One SNS topic, subscribed with your email, and two EventBridge rules: one
that fires on ANY Glue job/crawler/workflow failure across this whole
pipeline, and one that fires specifically when the final Gold job
succeeds (a simple "the pipeline finished" signal).

```bash
SNS_TOPIC_ARN=$(aws sns create-topic --region "$AWS_REGION" --name "$SNS_TOPIC_NAME" --query TopicArn --output text)
echo "SNS topic: $SNS_TOPIC_ARN"

aws sns subscribe \
  --region "$AWS_REGION" \
  --topic-arn "$SNS_TOPIC_ARN" \
  --protocol email \
  --notification-endpoint "you@example.com"   # <- replace with your real email, then confirm the subscription email AWS sends you
```

Let EventBridge publish to that topic:

```bash
cat > /tmp/sns-topic-policy.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowEventBridgePublish",
            "Effect": "Allow",
            "Principal": {"Service": "events.amazonaws.com"},
            "Action": "SNS:Publish",
            "Resource": "${SNS_TOPIC_ARN}",
            "Condition": {
                "ArnEquals": {"aws:SourceArn": "arn:aws:events:${AWS_REGION}:${AWS_ACCOUNT_ID}:rule/HealthcareMetrics*"}
            }
        }
    ]
}
EOF

aws sns set-topic-attributes \
  --region "$AWS_REGION" \
  --topic-arn "$SNS_TOPIC_ARN" \
  --attribute-name Policy \
  --attribute-value file:///tmp/sns-topic-policy.json
```

Failure rule — **no `jobName` filter**, so it covers every job, crawler,
and the workflow itself, including any you add later:

```bash
cat > /tmp/glue-failure-pattern.json << 'EOF'
{
    "source": ["aws.glue"],
    "detail-type": ["Glue Job State Change", "Glue Crawler State Change", "Glue Workflow State Change"],
    "detail": {"state": ["FAILED", "TIMEOUT", "ERROR"]}
}
EOF

aws events put-rule \
  --region "$AWS_REGION" \
  --name HealthcareMetricsJobFailureAlerts \
  --event-pattern file:///tmp/glue-failure-pattern.json

aws events put-targets \
  --region "$AWS_REGION" \
  --rule HealthcareMetricsJobFailureAlerts \
  --targets "Id"="1","Arn"="${SNS_TOPIC_ARN}"
```

Success rule — scoped to just the Gold job, as a simple "pipeline
finished" ping (extend the `jobName` list if you want success emails for
other steps too):

```bash
cat > /tmp/glue-success-pattern.json << 'EOF'
{
    "source": ["aws.glue"],
    "detail-type": ["Glue Job State Change"],
    "detail": {"state": ["SUCCEEDED"], "jobName": ["HealthcareMetricsGoldETL"]}
}
EOF

aws events put-rule \
  --region "$AWS_REGION" \
  --name HealthcareMetricsGoldETLSuccessAlert \
  --event-pattern file:///tmp/glue-success-pattern.json

aws events put-targets \
  --region "$AWS_REGION" \
  --rule HealthcareMetricsGoldETLSuccessAlert \
  --targets "Id"="1","Arn"="${SNS_TOPIC_ARN}"
```

---

## 8. Step 6 — Bronze ingestion job (`ingest_pbj_data.py`)

**What it does:** pulls new/changed files from the shared Google Drive
folder and lands them in the Bronze bucket, using Google Drive's Changes
API so re-runs are incremental, not full re-downloads. On its very first
run (no saved cursor yet) it backfills every file currently in the folder,
then starts tracking changes from that point on.

Each file lands at:

```
s3://<bronze-bucket>/raw/dataset=<stable_dataset_name>/ingestion_date=<YYYY-MM-DD>/<original_filename>
```

`derive_dataset_name()` strips date/fiscal-year suffixes from the raw CMS
filename (e.g. `NH_ProviderInfo_Oct2024.csv` → dataset name
`NH_ProviderInfo`) so the same conceptual dataset accumulates partitions
over time in one Glue table, instead of a new table every month. If a
dataset is re-ingested, downstream jobs are all written to read only the
**latest** `ingestion_date` partition (see `find_latest_partition_path()`
in every downstream script) — that pattern is what makes safe re-runs
possible without deleting old data.

This is an AWS Glue **Python Shell** job (not Spark) — it's just API
calls and file copies, no need for a Spark cluster.

Upload the script and create the job:

```bash
aws s3 cp aws/scripts/ingest_pbj_data.py "s3://${BRONZE_BUCKET}/scripts/ingest_pbj_data.py"

aws glue create-job \
  --region "$AWS_REGION" \
  --name HealthcareMetricsIngestion \
  --role "$GLUE_ROLE_NAME" \
  --command "Name=pythonshell,ScriptLocation=s3://${BRONZE_BUCKET}/scripts/ingest_pbj_data.py,PythonVersion=3.9" \
  --default-arguments '{
    "--DRIVE_FOLDER_ID": "REPLACE_WITH_YOUR_DRIVE_FOLDER_ID",
    "--BRONZE_BUCKET": "'"$BRONZE_BUCKET"'",
    "--SECRET_NAME": "'"$DRIVE_SECRET_NAME"'",
    "--DYNAMODB_TABLE": "'"$DYNAMO_TABLE"'",
    "--AWS_REGION": "'"$AWS_REGION"'",
    "--additional-python-modules": "google-api-python-client,google-auth,google-auth-httplib2"
  }' \
  --max-capacity 1 \
  --timeout 30
```

Replace `REPLACE_WITH_YOUR_DRIVE_FOLDER_ID` with the folder ID from Step 4.
Run it once by hand to test before wiring it into the workflow:

```bash
aws glue start-job-run --region "$AWS_REGION" --job-name HealthcareMetricsIngestion
# then poll:
aws glue get-job-run --region "$AWS_REGION" --job-name HealthcareMetricsIngestion --run-id <the RunId from above> --query "JobRun.JobRunState"
```

If it fails, its cursor in DynamoDB is deliberately **not** advanced (see
the script's docstring) — fix the problem and re-run; it retries from the
same starting point rather than silently skipping files.

---

## 9. Step 7 — Bronze Crawler

Registers every dataset landed in Bronze as its own Glue Catalog table,
under a `bronze_dataset_` name prefix (this prefix is why the downstream
jobs read Bronze **directly from S3** instead of via the Catalog — see the
Troubleshooting appendix's first entry for exactly what went wrong when an
earlier version tried to use the Catalog names directly).

```bash
aws glue create-crawler \
  --region "$AWS_REGION" \
  --name HealthcareMetricsBronzeCrawler \
  --role "$GLUE_ROLE_NAME" \
  --database-name "$GLUE_DB" \
  --table-prefix bronze_dataset_ \
  --targets '{"S3Targets": [{"Path": "s3://'"$BRONZE_BUCKET"'/raw/"}]}'

aws glue start-crawler --region "$AWS_REGION" --name HealthcareMetricsBronzeCrawler
```

Wait for it to finish (`aws glue get-crawler --name HealthcareMetricsBronzeCrawler --query "Crawler.State"` should read `READY` again), then confirm your 3 key tables exist:

```bash
aws glue get-tables --region "$AWS_REGION" --database-name "$GLUE_DB" --query "TableList[].Name"
```

---

## 10. Step 8 — Bronze Data Quality Gate (`validate_bronze.py`)

**What it does:** before Silver is allowed to run, this job checks all
three Bronze tables (PBJ, Provider Info, Quality Claims) using real **AWS
Glue Data Quality** (the DQDL rule language + `EvaluateDataQuality.apply()`
API), reading each one's latest S3 partition directly (not via the
Catalog — see the note above).

Each table has its own ruleset, split into **hard** rules (any failure
raises an exception, which fails the job and stops the pipeline before
Silver runs) and **soft** rules (logged as warnings, don't fail the job):

| Table | Hard rules | Soft rules |
|---|---|---|
| PBJ Daily Nurse Staffing | row count > 0; column count = 33; `PROVNUM`/`WorkDate`/`MDScensus` complete; `PROVNUM` matches `[0-9A-Za-z]{6}` | `Hrs_RN` ≤ 500, `Hrs_LPN` ≤ 1000, `Hrs_CNA` ≤ 2000 per day (≥99% of rows) |
| Provider Info | row count > 0; column count = 103; CCN + bed count complete; CCN matches `[0-9A-Za-z]{6}` | `Number of Certified Beds` > 0 (≥99% of rows) |
| Quality Measures – Claims | row count > 0; column count = 17; CCN + Measure Code complete; CCN matches `[0-9A-Za-z]{6}` | `Measure Code` is one of `521`/`522`/`551`/`552` (≥99% of rows) |

**Why the facility-ID regex is `[0-9A-Za-z]{6}` and not `[0-9]{6}`:** CMS
facility IDs (PROVNUM / CCN) are usually 6 digits, but roughly 1.6–1.8% of
real facilities across all three files have a legitimate 6-character
**alphanumeric** ID (e.g. `01A193`) — confirmed directly against the raw
files, not a data quality problem. A pure-digit regex would hard-fail the
pipeline on real, valid CMS data.

**Why row/column counts are hard rules but per-column thresholds are
soft:** a wrong row or column count means something structural broke
upstream (wrong file, truncated download, CMS changed the format) and the
whole run should stop. A few unusually high staffing hours in one day at
one facility is worth flagging, not worth halting the entire pipeline
over.

Upload and create the job (Spark ETL this time, not Python Shell — Glue
Data Quality requires a Spark job):

```bash
aws s3 cp aws/scripts/HealthcareMetricsValidateBronze.py "s3://${BRONZE_BUCKET}/scripts/HealthcareMetricsValidateBronze.py"

aws glue create-job \
  --region "$AWS_REGION" \
  --name HealthcareMetricsValidateBronze \
  --role "$GLUE_ROLE_NAME" \
  --glue-version "4.0" \
  --number-of-workers 2 \
  --worker-type G.1X \
  --command "Name=glueetl,ScriptLocation=s3://${BRONZE_BUCKET}/scripts/HealthcareMetricsValidateBronze.py,PythonVersion=3" \
  --default-arguments '{
    "--BRONZE_BUCKET": "'"$BRONZE_BUCKET"'",
    "--AWS_REGION": "'"$AWS_REGION"'",
    "--job-language": "python",
    "--enable-metrics": "true",
    "--enable-continuous-cloudwatch-log": "true"
  }' \
  --timeout 30

aws glue start-job-run --region "$AWS_REGION" --job-name HealthcareMetricsValidateBronze
```

If this fails with `AccessDeniedException` mentioning `cloudwatch:PutMetricData` or `glue:PublishDataQuality`, that's Policy 4 from Step 5 — double check it's attached (`aws iam list-role-policies --role-name $GLUE_ROLE_NAME`).

---

## 11. Step 9 — Silver Transform (`transform_silver.py`) — where the 5 metrics actually get computed

This is the one consolidated Silver job: it joins the three Bronze tables
and computes every column the 5 locked metrics need, at **daily
grain** (one row per facility per day).

**Reads** (each from its latest Bronze S3 partition, `cp1252`-encoded):
- PBJ Daily Nurse Staffing (`PROVNUM` kept as a string — never let it be inferred as a number, or leading zeros like `"015009"` get silently dropped and joins break)
- Provider Info (`NH_ProviderInfo`) — key column `CMS Certification Number (CCN)`, aliased to `ccn`
- Quality Measures – Claims (`NH_QualityMsr_Claims`), filtered down to **Measure Code 521 only** ("percentage of short-stay residents rehospitalized after a nursing home admission" — the readmission measure used by metric 5)

**A row-level safety check** runs first (separate from, and narrower than,
the Bronze Data Quality gate in Step 8): any PBJ row missing `PROVNUM`,
`WorkDate`, or a positive `MDScensus` is pulled out into a `_rejects`
output rather than risking a division-by-zero downstream. On the original
build's real data this rejected 320 of 1,325,324 rows (0.02%) — expected,
not a bug.

### The metrics, exactly as computed here

| # | Metric | Formula | Column(s) produced |
|---|---|---|---|
| 1 | **Nurse-to-patient ratio** (staffing intensity, "HPRD" = hours per resident day) | `(Hrs_RN + Hrs_LPN + Hrs_CNA) / MDScensus`, per facility per day | `hprd_total_nurse` (+ `hprd_rn`, `hprd_lpn`, `hprd_cna` individually) |
| 2 | **Total nurse hours worked** | Raw sum, no division: `Hrs_RN + Hrs_LPN + Hrs_CNA` | `total_nurse_hours` (Gold sums this further by facility/state/month) |
| 3 | **Occupancy rate** | `MDScensus / Number of Certified Beds` (from the Provider Info join) | `occupancy_rate` |
| 4 | **Permanent vs. contract staffing ratio** | `(Hrs_RN_ctr + Hrs_LPN_ctr + Hrs_CNA_ctr) / (Hrs_RN + Hrs_LPN + Hrs_CNA)` — PBJ's own `_ctr` (contract) vs. combined total split | `contract_pct_total_nurse` (permanent share is just `1 - contract_pct_total_nurse`) |
| 5 | **Staffing vs. readmission correlation** | Not a per-row formula — this job just attaches each facility's fixed `Adjusted Score` for Measure Code 521 to every one of its daily rows (a left join, so facilities CMS doesn't report this measure for keep a `null` rather than being dropped). The actual **correlation** is computed later, at the dashboard, across facilities — see Step 15. | `readmission_score` |

All four ratio/percentage columns use `F.when(<denominator> > 0, ...)` so a
zero or missing denominator produces a `null`, never a divide-by-zero
crash.

**Two real bugs the original build hit and fixed here, worth knowing about
if you're typing this out yourself:**
- Both the Provider Info and Quality Claims joins key on a column aliased
  `ccn` on both sides. If you write the join as
  `metrics_df.join(other_df, metrics_df.PROVNUM == other_df.ccn)` (the
  **expression** form), Spark keeps both sides' `ccn` columns instead of
  coalescing them — harmless until the Parquet writer refuses to write a
  schema with two identically-named columns. Fix: `.drop("ccn")` right
  after each join, before the next one runs.
- `WorkDate` in the raw PBJ file is an unseparated `yyyyMMdd` string (e.g.
  `"20240401"`), not `M/d/yyyy` — parse with `F.to_date("WorkDate",
  "yyyyMMdd")`.

Upload and create the job:

```bash
aws s3 cp aws/scripts/HealthcareMetricsTransformSilver.py "s3://${SILVER_BUCKET}/scripts/HealthcareMetricsTransformSilver.py"

aws glue create-job \
  --region "$AWS_REGION" \
  --name HealthcareMetricsTransformSilver \
  --role "$GLUE_ROLE_NAME" \
  --glue-version "4.0" \
  --number-of-workers 2 \
  --worker-type G.1X \
  --command "Name=glueetl,ScriptLocation=s3://${SILVER_BUCKET}/scripts/HealthcareMetricsTransformSilver.py,PythonVersion=3" \
  --default-arguments '{
    "--BRONZE_BUCKET": "'"$BRONZE_BUCKET"'",
    "--SILVER_BUCKET": "'"$SILVER_BUCKET"'",
    "--AWS_REGION": "'"$AWS_REGION"'",
    "--job-language": "python",
    "--enable-metrics": "true",
    "--enable-continuous-cloudwatch-log": "true"
  }' \
  --timeout 30

aws glue start-job-run --region "$AWS_REGION" --job-name HealthcareMetricsTransformSilver
```

Output lands at
`s3://<silver-bucket>/silver/dataset=staffing_daily_metrics/ingestion_date=<date>/*.parquet`
(and rejected rows, if any, at the parallel `silver/_rejects/...` path).

---

## 12. Step 10 — Silver Crawler

```bash
aws glue create-crawler \
  --region "$AWS_REGION" \
  --name HealthcareMetricsSilverCrawler \
  --role "$GLUE_ROLE_NAME" \
  --database-name "$GLUE_DB" \
  --targets '{"S3Targets": [{"Path": "s3://'"$SILVER_BUCKET"'/silver/"}]}'

aws glue start-crawler --region "$AWS_REGION" --name HealthcareMetricsSilverCrawler
```

This produces two separate tables: `dataset_staffing_daily_metrics` (the
real output) and `_rejects` (the safety-check rejects) — they stay
separate because `silver/dataset=.../` and `silver/_rejects/dataset=.../`
are structurally different prefixes, so the crawler doesn't try to merge
them (contrast with the Gold crawler in the next step, where two datasets
*did* get wrongly merged and needed a fix).

---

## 13. Step 11 — Gold Aggregation (`aggregate_gold.py`)

**What it does:** reads Silver's daily Parquet output and rolls it up into
the two tables the dashboard actually queries:

- **`facility_metrics`** — grain: (facility, month)
- **`state_summary`** — grain: (state, month)

**Why two different aggregation strategies are used:**

For `facility_metrics`, daily ratios can just be **averaged** — within one
facility, the denominator (census, beds) doesn't change who it's being
compared against day to day, so averaging the daily ratios gives the same
answer as recomputing the ratio from summed totals.

For `state_summary`, that's not true — a state contains facilities of very
different sizes. Averaging each facility's own average HPRD would let a
20-bed facility count exactly as much as a 200-bed facility, biasing the
state number toward small facilities. So `state_summary` instead **sums
the raw hours/census/beds across every facility-day in the state first,
then divides** — a census/capacity-weighted average, not an average of
averages. `contract_pct_total_nurse` follows the same "sum before
dividing" rule at both grains, for the same reason (a day with 240 hours
logged shouldn't count the same as a day with 2 hours logged).

`readmission_score` is handled separately again: since it's one fixed
value per facility (not daily), `facility_metrics` just carries it through
unchanged (`F.first(readmission_score, ignorenulls=True)`), and
`state_summary` averages it across **distinct facilities** in that state
(pulled from `facility_metrics`, not the daily Silver rows — averaging
from the daily rows would count a facility's score once per day it
reported, skewing toward facilities with more reporting days).

| Output table | Column | How it's built |
|---|---|---|
| `facility_metrics` | `avg_hprd_total_nurse` | `AVG(hprd_total_nurse)` across the facility's days that month |
| `facility_metrics` | `total_nurse_hours` | `SUM(total_nurse_hours)` across the month |
| `facility_metrics` | `avg_occupancy_rate` | `AVG(occupancy_rate)` across the month |
| `facility_metrics` | `contract_pct_total_nurse` | `SUM(contract hours) / SUM(total hours)` across the month |
| `facility_metrics` | `readmission_score` | carried through unchanged (fixed per facility) |
| `facility_metrics` | `num_days_reported` | count of distinct `WorkDate`s that month |
| `state_summary` | `avg_hprd_total_nurse` | `SUM(total_nurse_hours) / SUM(MDScensus)` across every facility-day in the state that month |
| `state_summary` | `avg_occupancy_rate` | `SUM(MDScensus) / SUM(num_certified_beds)` across the state that month |
| `state_summary` | `contract_pct_total_nurse` | `SUM(contract hours) / SUM(total hours)` across the state that month |
| `state_summary` | `avg_readmission_score` | `AVG(readmission_score)` across distinct facilities in that state |
| `state_summary` | `num_facilities_reporting` | count of distinct `PROVNUM`s that month |

Metric 5's actual **correlation coefficient** is deliberately *not* stored
here as a Gold column — it's a `pandas`/`numpy` calculation done at
dashboard load time across the `facility_metrics` table (see Step 15),
since a single scalar correlation number isn't really "data at a grain," it's
a computed statistic over the whole table.

```bash
aws s3 cp aws/scripts/HealthcareMetricsGoldETL.py "s3://${SILVER_BUCKET}/scripts/HealthcareMetricsGoldETL.py"

aws glue create-job \
  --region "$AWS_REGION" \
  --name HealthcareMetricsGoldETL \
  --role "$GLUE_ROLE_NAME" \
  --glue-version "4.0" \
  --number-of-workers 2 \
  --worker-type G.1X \
  --command "Name=glueetl,ScriptLocation=s3://${SILVER_BUCKET}/scripts/HealthcareMetricsGoldETL.py,PythonVersion=3" \
  --default-arguments '{
    "--SILVER_BUCKET": "'"$SILVER_BUCKET"'",
    "--GOLD_BUCKET": "'"$GOLD_BUCKET"'",
    "--AWS_REGION": "'"$AWS_REGION"'",
    "--job-language": "python",
    "--enable-metrics": "true",
    "--enable-continuous-cloudwatch-log": "true"
  }' \
  --timeout 30

aws glue start-job-run --region "$AWS_REGION" --job-name HealthcareMetricsGoldETL
```

---

## 14. Step 12 — Gold Crawler (two targets — this one matters)

**Do not point this crawler at the whole `gold/` prefix as one target.**
`facility_metrics` and `state_summary` both use a `dataset=<value>/`
folder-naming convention, which looks like Hive-style partitioning to the
crawler — if given one root target, it will merge both datasets into a
single table literally named `gold`, with `dataset` and `ingestion_date`
as partition keys. That's a real correctness risk, not just an
inconvenience: the merged schema shares columns like `total_nurse_hours`
between facility-grain and state-grain data, so
`SELECT SUM(total_nurse_hours) FROM gold` without filtering on the
`dataset` partition would silently sum facility-level and state-level
totals together.

Use **two explicit S3 targets** instead, so each dataset gets its own
correctly-separated table:

```bash
aws glue create-crawler \
  --region "$AWS_REGION" \
  --name HealthcareMetricsGoldCrawler \
  --role "$GLUE_ROLE_NAME" \
  --database-name "$GLUE_DB" \
  --targets '{"S3Targets": [
    {"Path": "s3://'"$GOLD_BUCKET"'/gold/dataset=facility_metrics/"},
    {"Path": "s3://'"$GOLD_BUCKET"'/gold/dataset=state_summary/"}
  ]}'

aws glue start-crawler --region "$AWS_REGION" --name HealthcareMetricsGoldCrawler
```

Confirm you got two distinct tables, not one merged `gold` table:

```bash
aws glue get-tables --region "$AWS_REGION" --database-name "$GLUE_DB" \
  --query "TableList[?starts_with(Name, 'facility') || starts_with(Name, 'state')].Name"
```

---

## 15. Step 13 — Glue Workflow orchestration

Chains every step above into one pipeline: a starting trigger kicks off
ingestion, and six conditional triggers each fire only after the previous
step reports `SUCCEEDED`.

```bash
aws glue create-workflow --region "$AWS_REGION" --name HealthcareMetricsPipeline
```

**Trigger 1 — starts the whole pipeline** (`ON_DEMAND` here; switch the
`--type` to `SCHEDULED` with a `--schedule` cron expression if you want it
to run automatically, e.g. monthly when CMS publishes new files):

```bash
aws glue create-trigger \
  --region "$AWS_REGION" \
  --name HealthcareMetricsStartIngestion \
  --workflow-name HealthcareMetricsPipeline \
  --type ON_DEMAND \
  --actions '[{"JobName": "HealthcareMetricsIngestion"}]'
```

**Triggers 2–7 — each conditional on the previous step succeeding:**

```bash
aws glue create-trigger \
  --region "$AWS_REGION" \
  --name HealthcareMetricsAfterIngestion \
  --workflow-name HealthcareMetricsPipeline \
  --type CONDITIONAL \
  --start-on-creation \
  --predicate '{"Logical": "AND", "Conditions": [{"LogicalOperator": "EQUALS", "JobName": "HealthcareMetricsIngestion", "State": "SUCCEEDED"}]}' \
  --actions '[{"CrawlerName": "HealthcareMetricsBronzeCrawler"}]'

aws glue create-trigger \
  --region "$AWS_REGION" \
  --name HealthcareMetricsAfterBronzeCrawl \
  --workflow-name HealthcareMetricsPipeline \
  --type CONDITIONAL \
  --start-on-creation \
  --predicate '{"Logical": "AND", "Conditions": [{"LogicalOperator": "EQUALS", "CrawlerName": "HealthcareMetricsBronzeCrawler", "CrawlState": "SUCCEEDED"}]}' \
  --actions '[{"JobName": "HealthcareMetricsValidateBronze"}]'

aws glue create-trigger \
  --region "$AWS_REGION" \
  --name HealthcareMetricsAfterValidateBronze \
  --workflow-name HealthcareMetricsPipeline \
  --type CONDITIONAL \
  --start-on-creation \
  --predicate '{"Logical": "AND", "Conditions": [{"LogicalOperator": "EQUALS", "JobName": "HealthcareMetricsValidateBronze", "State": "SUCCEEDED"}]}' \
  --actions '[{"JobName": "HealthcareMetricsTransformSilver"}]'

aws glue create-trigger \
  --region "$AWS_REGION" \
  --name HealthcareMetricsAfterTransformSilver \
  --workflow-name HealthcareMetricsPipeline \
  --type CONDITIONAL \
  --start-on-creation \
  --predicate '{"Logical": "AND", "Conditions": [{"LogicalOperator": "EQUALS", "JobName": "HealthcareMetricsTransformSilver", "State": "SUCCEEDED"}]}' \
  --actions '[{"CrawlerName": "HealthcareMetricsSilverCrawler"}]'

aws glue create-trigger \
  --region "$AWS_REGION" \
  --name HealthcareMetricsAfterSilverCrawl \
  --workflow-name HealthcareMetricsPipeline \
  --type CONDITIONAL \
  --start-on-creation \
  --predicate '{"Logical": "AND", "Conditions": [{"LogicalOperator": "EQUALS", "CrawlerName": "HealthcareMetricsSilverCrawler", "CrawlState": "SUCCEEDED"}]}' \
  --actions '[{"JobName": "HealthcareMetricsGoldETL"}]'

aws glue create-trigger \
  --region "$AWS_REGION" \
  --name HealthcareMetricsAfterGoldETL \
  --workflow-name HealthcareMetricsPipeline \
  --type CONDITIONAL \
  --start-on-creation \
  --predicate '{"Logical": "AND", "Conditions": [{"LogicalOperator": "EQUALS", "JobName": "HealthcareMetricsGoldETL", "State": "SUCCEEDED"}]}' \
  --actions '[{"CrawlerName": "HealthcareMetricsGoldCrawler"}]'
```

**`--start-on-creation` is required** on every conditional trigger — a
trigger created without it sits `CREATED` but inactive, and silently never
fires (no error, it just never runs). Failure propagation needs no extra
logic: if one step fails, the next trigger's condition simply never
becomes true, so the chain just stops there — that's also what the
failure EventBridge rule from Step 5 is watching for.

Run the whole pipeline:

```bash
aws glue start-workflow-run --region "$AWS_REGION" --name HealthcareMetricsPipeline
```

Watch the whole chain's state as one object (rather than checking every
job/crawler individually):

```bash
RUN_ID=$(aws glue get-workflow-runs --region "$AWS_REGION" --name HealthcareMetricsPipeline --max-results 1 --query "Runs[0].WorkflowRunId" --output text)
aws glue get-workflow-run --region "$AWS_REGION" --name HealthcareMetricsPipeline --run-id "$RUN_ID" --include-graph
```

---

## 16. Step 14 — Verifying it worked

After a full workflow run, spot-check the actual Gold output rather than
just trusting "the job didn't fail":

```bash
# Pull the Gold facility_metrics table into a local pandas check (needs boto3 + pyarrow + pandas locally)
python3 - << 'PYEOF'
import boto3, io, pandas as pd

s3 = boto3.client("s3", region_name="us-west-1")
bucket = "REPLACE_WITH_YOUR_GOLD_BUCKET"
prefix_root = "gold/dataset=facility_metrics/"

resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix_root, Delimiter="/")
latest = sorted(cp["Prefix"] for cp in resp["CommonPrefixes"])[-1]

keys = [o["Key"] for o in s3.list_objects_v2(Bucket=bucket, Prefix=latest)["Contents"] if o["Key"].endswith(".parquet")]
df = pd.concat(pd.read_parquet(io.BytesIO(s3.get_object(Bucket=bucket, Key=k)["Body"].read())) for k in keys)

print(df.shape)
print(df.head())
PYEOF
```

Things worth checking on real output: row counts are in a sane range (not
zero, not obviously truncated), `avg_hprd_total_nurse` values are in a
plausible range (roughly 2–6 for most facilities), `occupancy_rate` is
between 0 and 1 for the overwhelming majority of rows, and
`readmission_score` is populated for most (not all — CMS suppresses this
measure for facilities with too few qualifying stays) facilities.

---

## 17. Step 15 — Streamlit dashboard

The dashboard code is already built at `dashboard/app.py` (see
`dashboard/README.md` for run/deploy instructions) — it reads
`facility_metrics` and `state_summary` directly from S3 as Parquet
(no Athena), covering all 5 metrics across 5 tabs.

**Metric 5's correlation, computed at dashboard load time:** the
dashboard does *not* correlate on the raw `facility_metrics` table
directly (facility-month grain — 43,685 rows in the original build).
`readmission_score` is a fixed value per facility, so correlating at
facility-month grain would let a facility that reported 12 months count
12× as heavily as one that reported only 1 month — an artifact of
reporting completeness, not of anything real about staffing or outcomes.
Instead, `app.py`'s `facility_level_staffing_readmission()` first
collapses to **one row per facility** (its days-reported-weighted average
HPRD), so every facility counts exactly once, then computes a plain
Pearson correlation (`pandas.Series.corr()`) between that and
`readmission_score` across facilities.

Deploy it to Streamlit Community Cloud by pointing at `dashboard/app.py`
in your GitHub repo, with your AWS credentials entered into Streamlit's
own **Settings → Secrets** UI (see `dashboard/secrets.toml.example` for
the exact format) — never commit real credentials to git.

---

## 18. Teardown — resetting to a clean state

If you want to blow away all the data and Catalog tables and re-run the
whole pipeline from a genuinely empty state (useful for testing the
workflow end to end):

```bash
# Empty all 3 S3 zones
aws s3 rm "s3://${BRONZE_BUCKET}" --recursive
aws s3 rm "s3://${SILVER_BUCKET}" --recursive
aws s3 rm "s3://${GOLD_BUCKET}" --recursive

# Reset the ingestion cursor so the next run does a full backfill again
aws dynamodb delete-item --region "$AWS_REGION" --table-name "$DYNAMO_TABLE" --key '{"state_id": {"S": "google_drive_sync"}}'

# Delete every Catalog table so the crawlers rebuild them from scratch
for T in $(aws glue get-tables --region "$AWS_REGION" --database-name "$GLUE_DB" --query "TableList[].Name" --output text); do
  aws glue delete-table --region "$AWS_REGION" --database-name "$GLUE_DB" --name "$T"
done
```

Then re-run the workflow (Step 13's `start-workflow-run` command) — it
should reproduce the exact same pipeline end to end. The original build
verified this exact clean-state reset + re-run worked correctly before
calling the AWS build-out done.

---

## Appendix A — Full metrics reference

| # | Metric | Category | Grain it's computed at | Formula | Where |
|---|---|---|---|---|---|
| 1 | Nurse-to-patient ratio (HPRD) | Staffing | Daily → averaged monthly (facility), census-weighted (state) | `(Hrs_RN + Hrs_LPN + Hrs_CNA) / MDScensus` | `transform_silver.py` (daily) → `aggregate_gold.py` (monthly) |
| 2 | Total nurse hours worked | Staffing | Daily → summed monthly | `Hrs_RN + Hrs_LPN + Hrs_CNA` (raw, no division) | `transform_silver.py` → `aggregate_gold.py` |
| 3 | Occupancy rate | Facility | Daily → averaged monthly (facility), census/bed-weighted (state) | `MDScensus / Number of Certified Beds` | `transform_silver.py` → `aggregate_gold.py` |
| 4 | Permanent vs. contract staffing % | Operational | Daily → summed-then-divided monthly | `SUM(Hrs_*_ctr) / SUM(Hrs_RN + Hrs_LPN + Hrs_CNA)` | `transform_silver.py` → `aggregate_gold.py` |
| 5 | Staffing-vs-readmission correlation | Quality | Cross-facility statistic (Pearson r) | `avg_hprd_total_nurse` (one row per facility) vs. `readmission_score` (CMS Measure Code 521, "Adjusted Score") | Attached per-facility in `transform_silver.py`; correlation itself computed in the Streamlit dashboard, not stored in Gold |

Ruled out entirely, and why: **cost metrics** — no payroll, per-stay cost,
or revenue data exists anywhere in this CMS dataset (only real dollar
figures anywhere are `NH_Penalties`' regulatory fine amounts and one
*national* aggregate incentive-payment total — neither usable per
facility). **Department-scoped metrics** — this is nursing-home/SNF data,
not hospital data; there's no department, per-shift, per-employee, or
per-patient-stay dimension anywhere in the source files.

## Appendix B — Real bugs hit during the original build (so you don't have to re-discover them)

1. **`EntityNotFoundException` reading Bronze via `from_catalog()`.** The
   crawler registers Bronze tables with a `bronze_dataset_` prefix (Step
   7's `--table-prefix`), not the bare dataset name. Fix used here:
   `validate_bronze.py` doesn't use `from_catalog()` at all — it reads
   straight from the latest S3 partition, same as Silver/Gold do.
2. **`Unable to parse file: ....csv` from `EvaluateDataQuality.apply()`.**
   The Bronze crawler classifies these CSVs with `LazySimpleSerDe`, which
   has no encoding option and no quote-char support — and this data is
   `cp1252`, not UTF-8. Same fix as above: read directly from S3 with
   `.option("encoding", "windows-1252")` instead of trusting the crawler's
   auto-detected format.
3. **`AccessDeniedException: cloudwatch:PutMetricData` then
   `glue:PublishDataQuality`.** AWS Glue Data Quality is a separate IAM
   action namespace from regular Glue job control — see Policy 4 in Step
   5.
4. **DQDL hard-rule failure: `PROVNUM`/`CCN` doesn't match `[0-9]{6}`.**
   Real CMS data fact, not corrupted data — ~1.6–1.8% of facility IDs are
   legitimately alphanumeric. Fixed by using `[0-9A-Za-z]{6}` instead.
5. **`AnalysisException: Found duplicate column(s) ... : ccn`.** Using the
   `left.col == right.col` **expression** form of `.join()` keeps both
   sides' columns instead of coalescing them. Fixed with `.drop("ccn")`
   after each join.
6. **Gold crawler merged `facility_metrics` and `state_summary` into one
   `gold` table.** Both datasets share the `dataset=<value>/`
   folder-naming convention, which the crawler read as Hive partitioning
   of a single table. Fixed with two explicit S3 targets instead of one
   root target — see Step 12.
7. **`glue:GetPartition` (singular) `AccessDeniedException` during
   workflow runs.** The original scoped policy only had the plural/batch
   partition actions (`GetPartitions`, `BatchGetPartition`), not the
   singular ones a crawler needs when re-crawling an existing table. Fixed
   by adding the full partition CRUD action set — already included in
   Policy 3 (Step 5) above.

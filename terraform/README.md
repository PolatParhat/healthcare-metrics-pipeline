# HealthcareMetricsProject — Terraform

Infrastructure-as-code version of the pipeline built manually in
`PIPELINE_BUILD_GUIDE.md`. Same resources, same names, same IAM scoping —
this just replaces the copy-paste AWS CLI commands with `terraform apply`.

## Layout

```
terraform/
├── bootstrap/              one-time: creates the S3 bucket + DynamoDB table
│                            that the real stack stores its state in
├── environments/
│   └── dev/                 the actual pipeline (composition root)
└── modules/
    ├── s3_data_lake/        Bronze/Silver/Gold buckets
    ├── iam/                 HealthcareMetricsGlueRole + its 4 policies
    ├── glue_catalog/        the shared Glue database
    ├── dynamodb/             ingestion sync-cursor table
    ├── secrets/              Drive service-account secret (container only)
    ├── alerting/             SNS topic + 2 EventBridge rules
    ├── glue_jobs/            uploads scripts, creates the 4 Glue jobs
    ├── glue_crawlers/        Bronze/Silver/Gold crawlers
    └── glue_workflow/        the workflow + all 7 triggers
```

One module per AWS service, one environment folder that wires them
together — add `environments/prod/` later (copy `dev/`, change
`terraform.tfvars`) if you ever need a second copy of this stack; the
modules don't change.

## What Terraform does NOT do here (on purpose)

- **Create the Google Cloud service account** or share the Drive folder
  with it — that's Google Cloud Console, not AWS, so it's outside
  Terraform's reach. Still a manual step, same as in the CLI guide.
- **Populate the real secret value** — `modules/secrets` creates the
  Secrets Manager *container* with a placeholder, and
  `lifecycle { ignore_changes = [secret_string] }` so Terraform never
  touches it again after the first apply. You set the real value yourself
  with the AWS CLI (command is in the `next_steps` output after apply) —
  the actual credential JSON should never pass through Terraform state,
  which is plaintext on disk.
- **Confirm the SNS email subscription** — AWS emails you a link after
  apply; nobody but you can click it.
- **Run the pipeline** — Terraform builds the workflow and its triggers,
  but doesn't start a run. You do that with
  `aws glue start-workflow-run`, same as before.

## First-time setup

**1. Bootstrap the remote state backend** (once, ever):

```bash
cd bootstrap
terraform init
terraform apply
```

Note the `state_bucket_name` output — you need it in step 2.

**2. Point `environments/dev` at that backend.** Terraform won't let a
backend block reference a variable, so edit
`environments/dev/versions.tf` by hand and replace
`healthcare-metrics-tfstate-<your-account-id>` with the real bucket name
from step 1's output (region/lock-table name only need changing if you
overrode their defaults in bootstrap).

**3. Set your real values:**

```bash
cd ../environments/dev
cp terraform.tfvars.example terraform.tfvars
# edit terraform.tfvars: drive_folder_id, alert_email, glue_scripts_dir
```

**4. Init and apply:**

```bash
terraform init
terraform plan    # review what it's about to create
terraform apply
```

**5. Finish the manual steps** the `next_steps` output prints: confirm the
SNS email, populate the real Drive secret, then run the pipeline once:

```bash
terraform output next_steps
```

## Making a change later

Edit the relevant module (or a Glue job `.py` file — `glue_jobs` re-uploads
a script automatically whenever its content changes, via
`etag = filemd5(...)`), then:

```bash
cd environments/dev
terraform plan
terraform apply
```

## Tearing down

```bash
cd environments/dev
terraform destroy
```

S3 buckets with `force_destroy` unset (the default here, deliberately —
this is a data lake, an accidental `destroy` shouldn't silently delete
your only copy of Silver/Gold data) will fail to delete if they still
have objects in them. Empty them first if you actually want them gone:

```bash
aws s3 rm s3://healthcare-metrics-bronze-<account-id> --recursive
aws s3 rm s3://healthcare-metrics-silver-<account-id> --recursive
aws s3 rm s3://healthcare-metrics-gold-<account-id> --recursive
terraform destroy
```

The `bootstrap/` state bucket is intentionally separate and untouched by
this — destroy it by hand, last, only if you're done with the project
entirely.

## Resource-name map (Terraform module → CLI guide step)

| Module | CLI guide step | Key resources |
|---|---|---|
| `s3_data_lake` | Step 1 | 3 buckets: `healthcare-metrics-{bronze,silver,gold}-<account-id>` |
| `glue_catalog` | Step 2 | Glue database `healthcare_metrics` |
| `iam` | Step 3 | `HealthcareMetricsGlueRole` + 4 inline policies |
| `dynamodb`, `secrets` | Step 4 | `HealthcareMetricsSyncState`, `healthcare-metrics/google-drive-creds` |
| `alerting` | Step 5 | `HealthcareMetricsAlerts` SNS topic, 2 EventBridge rules |
| `glue_jobs` | Steps 6, 8, 9, 11 | `HealthcareMetrics{Ingestion,ValidateBronze,TransformSilver,GoldETL}` |
| `glue_crawlers` | Steps 7, 10, 12 | `HealthcareMetrics{Bronze,Silver,Gold}Crawler` |
| `glue_workflow` | Step 13 | `HealthcareMetricsPipeline` + 7 triggers |

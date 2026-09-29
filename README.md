# Healthcare Metrics Pipeline

A data engineering pipeline that turns CMS's public nursing-home staffing and
quality-measure files into five staffing/quality metrics, served through a
Streamlit dashboard. Built as a Bronze → Silver → Gold pipeline on AWS Glue,
with data quality gates, workflow orchestration, and failure alerting.

**Data source:** [CMS Provider Data Catalog](https://data.cms.gov/provider-data/) —
Payroll-Based Journal (PBJ) daily nurse staffing, Nursing Home Provider Info,
and Quality Measures - Claims. All three are public files CMS re-publishes
periodically.

## Architecture

```
Google Drive (CMS CSVs)
        │  incremental sync (Drive Changes API + DynamoDB cursor)
        ▼
   Bronze (S3, raw CSV)
        │  AWS Glue Data Quality gate — hard rules fail the pipeline,
        │  soft rules just log a warning
        ▼
   Silver (S3, Parquet)
        │  joins PBJ + Provider Info + Quality Measures on facility ID,
        │  computes daily staffing ratios (one row per facility per day)
        ▼
   Gold (S3, Parquet — 2 tables)
        │  facility_metrics (facility, month)  +  state_summary (state, month)
        ▼
   Streamlit dashboard
        reads Gold directly from S3 (boto3 + pandas/pyarrow, no query engine)
```

Every stage is a separate AWS Glue job, chained together by one Glue
Workflow (a starting trigger + 6 conditional triggers — each step only
fires once the previous one reports `SUCCEEDED`). See
`Project Architecture Design/` for the full solution design doc and
architecture diagram.

## The 5 metrics

| # | Metric | What it measures |
|---|---|---|
| 1 | Nurse-to-patient staffing intensity (HPRD) | Nurse hours worked per resident-day: `(RN + LPN + CNA hours) / census` |
| 2 | Total nurse hours worked | Raw staffing volume per facility/state/month |
| 3 | Occupancy rate | Residents in the building ÷ certified bed count |
| 4 | Permanent vs. contract staffing mix | Share of nurse hours covered by contract/agency staff vs. permanent staff |
| 5 | Staffing intensity vs. 30-day readmission correlation | Pearson correlation between HPRD and CMS's short-stay rehospitalization measure, computed once per facility (not per facility-month, to avoid over-weighting facilities that reported more months) |

## Repo structure

```
aws/                      IAM policy documents + the 4 Glue job scripts
  glue-*.json               Trust policy + the 4 scoped IAM policies for the Glue role
  scripts/                  ingest_pbj_data.py, HealthcareMetricsValidateBronze.py,
                             HealthcareMetricsTransformSilver.py, HealthcareMetricsGoldETL.py
dashboard/                 Streamlit app (reads Gold from S3 directly)
  app.py
  requirements.txt / requirements-dev.txt
  tests/test_app_smoke.py    Headless smoke tests against synthetic S3 data
eda/                       Exploratory data analysis (pre-pipeline validation)
  01_verify_integrity.py
  02_initial_eda.py
  eda_output/                 Generated plots (missing values, census distribution, etc.)
Project Architecture Design/ Solution design doc + architecture diagram
Data/                      Local CMS source files (gitignored — see below)
```

The `main` branch is the pipeline as built and deployed by hand via the AWS
CLI (see `aws/`). A separate **`terraform` branch** has the same pipeline
expressed as Terraform modules, for anyone who wants to stand it up as
infrastructure-as-code instead.

## Tech stack

AWS S3, Glue (Spark ETL + Python Shell + Glue Data Quality), Glue Data
Catalog, Glue Workflow/Triggers, DynamoDB (ingestion sync cursor), Secrets
Manager, SNS + EventBridge (failure/success alerting) · Python, PySpark,
pandas, boto3, pyarrow · Streamlit + Plotly (dashboard) · pytest +
`streamlit.testing.v1.AppTest` (dashboard tests)

## Running the dashboard locally

```bash
cd dashboard
pip install -r requirements.txt
streamlit run app.py
```

Needs AWS credentials with read access to the Gold S3 bucket — either an
existing `aws configure` profile, or a `.streamlit/secrets.toml` (copy
`secrets.toml.example` and fill in real values; this path is gitignored).

Run the test suite (no AWS credentials needed — it patches `boto3.Session`
with synthetic data):

```bash
pip install -r requirements-dev.txt
pytest tests/ -v
```

## Deploying the pipeline

The jobs, crawlers, and workflow in `aws/` were created via the AWS CLI
(IAM role → S3 buckets → Glue Catalog database → the 4 Glue jobs → 3
crawlers → the Glue Workflow and its 7 triggers → SNS/EventBridge
alerting). Check out the `terraform` branch for the same pipeline as
reusable Terraform modules instead.

The dashboard is deployed on [Streamlit Community Cloud](https://share.streamlit.io),
pointed at this repo's `dashboard/app.py`, with AWS credentials entered
directly into Streamlit's own secrets UI (never committed to this repo).

# Dashboard

Streamlit app that reads the Gold layer (`facility_metrics`, `state_summary`)
directly from S3 and presents all five staffing/quality metrics from the
Solution Design. See `app.py`'s module docstring for the data-access design
and the facility-vs-facility-month grain decision for the correlation metric.

## Run locally

Requires AWS CLI credentials already configured locally (`aws configure`)
with read access to `healthcare-metrics-gold-941377112484`, OR a
`.streamlit/secrets.toml` (copy `secrets.toml.example` and fill in real
values - this path is git-ignored).

```bash
pip install -r requirements.txt
streamlit run app.py
```

## Tests

`tests/test_app_smoke.py` runs the real app against synthetic data (no AWS
credentials needed) using Streamlit's own `AppTest` framework - it patches
`boto3.Session` with a fake S3 client serving synthetic Parquet, so the real
partition-discovery/pagination code in `app.py` still runs, just against
fake bytes instead of the real bucket. It checks that all 5 tabs render
without an unhandled exception, and hand-verifies the days-reported-weighted
average used for the metric-5 correlation against a manual calculation.

```bash
pip install -r requirements-dev.txt
pytest tests/ -v
```

## Deploy on Streamlit Community Cloud

1. Push this repo to GitHub (see the project root for git setup).
2. Go to https://share.streamlit.io, sign in, and create a new app pointing
   at this repo, with `dashboard/app.py` as the entrypoint file.
3. In the app's **Settings -> Secrets**, paste in the contents of
   `secrets.toml.example` with your real AWS access key / secret key for an
   IAM user or role that has read access to the Gold bucket - do this
   directly in Streamlit's UI, never by committing a real secrets file.
4. Deploy. The app re-reads the latest `ingestion_date` partition under
   `s3://healthcare-metrics-gold-941377112484/gold/dataset=.../` each time
   its hour-long cache expires, so re-running the Glue Workflow with new
   data will show up automatically without redeploying the app.

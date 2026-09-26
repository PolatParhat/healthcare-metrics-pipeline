"""
HealthcareMetricsProject - Streamlit Dashboard

Purpose
-------
Reads the Gold layer (facility_metrics, state_summary) directly from S3 as
Parquet and presents the five staffing/quality metrics from the Solution
Design:

    1. Nurse-to-patient staffing intensity (hours per resident day, HPRD)
    2. Total nurse hours
    3. Occupancy rate
    4. Permanent vs. contract staffing mix
    5. Staffing intensity vs. 30-day readmission correlation

Data access: direct S3 Parquet read via boto3 + pandas/pyarrow (no Athena).
Deployment target: Streamlit Community Cloud. AWS credentials are read from
`st.secrets["aws"]` (entered by the project owner directly into Streamlit
Cloud's own encrypted secrets UI) and fall back to boto3's default
credential chain for local development (e.g. an existing `aws configure`
profile) - this file never hardcodes or asks for credentials.

Design decision - facility vs. facility-month grain for metric 5
------------------------------------------------------------------
`facility_metrics` is built at (facility, month) grain, but
`readmission_score` is a single claims-based value per facility (see
`aggregate_gold.py`'s `F.first(readmission_score, ignorenulls=True)`) - it
does not change month to month. Correlating on the raw facility-month table
would duplicate every facility's fixed readmission_score once per reporting
month, so a facility that reported 12 months would count 12x as heavily in
the correlation as one that reported 1 month - an artifact of reporting
completeness, not of anything real about its staffing or outcomes.
`facility_level_staffing_readmission()` below collapses to one row per
facility (its days-reported-weighted average HPRD) before computing the
correlation, so every facility counts once.
"""

import io
from datetime import datetime

import boto3
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# ---------------------------------------------------------------------------
# Project constants (verified against CLAUDE.md / the Glue jobs - not guessed)
# ---------------------------------------------------------------------------

GOLD_BUCKET = "healthcare-metrics-gold-941377112484"
AWS_REGION = "us-west-1"

FACILITY_METRICS_DATASET = "facility_metrics"
STATE_SUMMARY_DATASET = "state_summary"

# ---------------------------------------------------------------------------
# dataviz skill palette (references/palette.md) - validated, not eyeballed.
# Categorical order is fixed and never cycled; only the first 3 slots are
# valid for all-pairs contexts (scatter/choropleth/comparisons of >2 series).
# ---------------------------------------------------------------------------

CATEGORICAL = [
    "#2a78d6",  # slot 1 - blue   (primary series / "permanent")
    "#eb6834",  # slot 2 - orange (secondary series / "contract")
    "#1baf7a",  # slot 3 - aqua   (tertiary series)
]
SEQUENTIAL_BLUE = [
    "#cde2fb", "#b7d3f6", "#9ec5f4", "#86b6ef", "#6da7ec", "#5598e7",
    "#3987e5", "#2a78d6", "#256abf", "#1c5cab", "#184f95", "#104281", "#0d366b",
]
STATUS = {
    "good": "#0ca30c",
    "warning": "#fab219",
    "serious": "#ec835a",
    "critical": "#d03b3b",
}
CHART_SURFACE = "#fcfcfb"
PAGE_PLANE = "#f9f9f7"
INK_PRIMARY = "#0b0b0b"
INK_SECONDARY = "#52514e"
INK_MUTED = "#898781"
GRIDLINE = "#e1e0d9"
BASELINE = "#c3c2b7"

METRIC_LABELS = {
    "avg_hprd_total_nurse": "Nurse hours per resident day (HPRD)",
    "avg_occupancy_rate": "Occupancy rate",
    "contract_pct_total_nurse": "Contract staff share of nurse hours",
    "total_nurse_hours": "Total nurse hours",
}

# ---------------------------------------------------------------------------
# S3 access
# ---------------------------------------------------------------------------


def get_boto3_session() -> boto3.Session:
    """Prefer Streamlit secrets (how the deployed app on Streamlit Community
    Cloud gets AWS access - entered by the project owner into Streamlit's own
    secrets UI, never typed into this code). Falls back to boto3's default
    credential chain for local development against an existing AWS CLI
    profile.

    st.secrets raises (rather than just returning False from `in`) when no
    secrets.toml file exists at all anywhere Streamlit looks for one - which
    is exactly the supported local-dev case documented in README.md (run
    against an `aws configure` profile, no secrets file needed). Without this
    try/except, that supported case crashes the app instead of falling
    through to the default credential chain."""
    try:
        has_aws_secrets = "aws" in st.secrets
    except Exception:
        has_aws_secrets = False

    if has_aws_secrets:
        return boto3.Session(
            aws_access_key_id=st.secrets["aws"]["aws_access_key_id"],
            aws_secret_access_key=st.secrets["aws"]["aws_secret_access_key"],
            region_name=st.secrets["aws"].get("region", AWS_REGION),
        )
    return boto3.Session(region_name=AWS_REGION)


def _find_latest_partition_prefix(s3_client, bucket: str, dataset_name: str) -> str:
    """Same latest-ingestion_date pattern used throughout the Glue jobs
    (find_latest_partition_path in validate_bronze.py / aggregate_gold.py),
    applied here to the Gold bucket."""
    prefix = f"gold/dataset={dataset_name}/"
    paginator = s3_client.get_paginator("list_objects_v2")
    partition_prefixes = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        partition_prefixes.extend(cp["Prefix"] for cp in page.get("CommonPrefixes", []))

    if not partition_prefixes:
        raise FileNotFoundError(
            f"No ingestion_date partitions found under s3://{bucket}/{prefix} - "
            f"has the Gold aggregation job run yet?"
        )
    return sorted(partition_prefixes)[-1]


@st.cache_data(ttl=3600, show_spinner="Loading data from S3...")
def load_gold_dataset(dataset_name: str) -> pd.DataFrame:
    """Downloads every Parquet part-file under the latest ingestion_date
    partition for one Gold dataset and concatenates them into one DataFrame.
    Cached for an hour so repeat views/filters don't re-hit S3."""
    session = get_boto3_session()
    s3 = session.client("s3")

    prefix = _find_latest_partition_prefix(s3, GOLD_BUCKET, dataset_name)

    paginator = s3.get_paginator("list_objects_v2")
    parquet_keys = []
    for page in paginator.paginate(Bucket=GOLD_BUCKET, Prefix=prefix):
        parquet_keys.extend(
            obj["Key"] for obj in page.get("Contents", []) if obj["Key"].endswith(".parquet")
        )

    if not parquet_keys:
        raise FileNotFoundError(f"No .parquet files found under s3://{GOLD_BUCKET}/{prefix}")

    frames = []
    for key in parquet_keys:
        body = s3.get_object(Bucket=GOLD_BUCKET, Key=key)["Body"].read()
        frames.append(pd.read_parquet(io.BytesIO(body)))

    df = pd.concat(frames, ignore_index=True)
    df.attrs["source_partition"] = f"s3://{GOLD_BUCKET}/{prefix}"
    return df


# ---------------------------------------------------------------------------
# Data prep
# ---------------------------------------------------------------------------


def facility_level_staffing_readmission(facility_metrics: pd.DataFrame) -> pd.DataFrame:
    """Collapse facility_metrics to one row per facility for metric 5 - see
    the module docstring for why. avg_hprd_total_nurse is weighted by
    num_days_reported so a facility's higher-volume months count more than
    a month it barely reported, matching the same "sum before dividing"
    discipline used throughout the Gold job."""
    working = facility_metrics.dropna(subset=["avg_hprd_total_nurse", "readmission_score"]).copy()
    working["_weighted_hprd"] = working["avg_hprd_total_nurse"] * working["num_days_reported"]

    grouped = working.groupby(["PROVNUM", "provider_name", "provider_state"], as_index=False).agg(
        _total_weighted_hprd=("_weighted_hprd", "sum"),
        _total_days=("num_days_reported", "sum"),
        readmission_score=("readmission_score", "first"),
        months_reported=("year_month", "nunique"),
    )
    grouped["avg_hprd_total_nurse"] = grouped["_total_weighted_hprd"] / grouped["_total_days"]
    return grouped.drop(columns=["_total_weighted_hprd", "_total_days"])


def latest_year_month(df: pd.DataFrame) -> str:
    return sorted(df["year_month"].dropna().unique())[-1]


# ---------------------------------------------------------------------------
# Chart theming - one shared template so every chart gets the same surface,
# ink, and gridline treatment from the validated palette (dataviz skill).
# ---------------------------------------------------------------------------


def themed_layout(fig: go.Figure, title: str, y_title: str = "", x_title: str = "") -> go.Figure:
    fig.update_layout(
        title=title,
        plot_bgcolor=CHART_SURFACE,
        paper_bgcolor=CHART_SURFACE,
        font=dict(color=INK_PRIMARY, family="system-ui, -apple-system, 'Segoe UI', sans-serif"),
        legend=dict(bgcolor=CHART_SURFACE, bordercolor=GRIDLINE, borderwidth=1),
        hovermode="x unified",
        margin=dict(t=60, l=10, r=10, b=10),
    )
    fig.update_xaxes(title=x_title, showgrid=True, gridcolor=GRIDLINE, linecolor=BASELINE, tickfont=dict(color=INK_MUTED))
    fig.update_yaxes(title=y_title, showgrid=True, gridcolor=GRIDLINE, linecolor=BASELINE, tickfont=dict(color=INK_MUTED))
    return fig


def format_pct(x: float) -> str:
    return f"{x * 100:.1f}%" if pd.notna(x) else "-"


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------


def main():
    st.set_page_config(
        page_title="CMS Nurse Staffing Metrics",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    st.title("CMS Nursing Home Staffing & Quality Metrics")
    st.caption(
        "Payroll-Based Journal (PBJ) staffing data joined with CMS Provider Info "
        "and quality-measure claims data - Bronze -> Silver -> Gold on AWS Glue."
    )

    try:
        facility_metrics = load_gold_dataset(FACILITY_METRICS_DATASET)
        state_summary = load_gold_dataset(STATE_SUMMARY_DATASET)
    except Exception as exc:  # noqa: BLE001 - surface any S3/credentials error to the viewer
        st.error(
            "Could not load data from S3. If you're running this locally, make sure your "
            "AWS CLI credentials are configured (`aws configure`). If this is the deployed "
            "app, check the `[aws]` block in Streamlit Cloud's app secrets.\n\n"
            f"Details: {exc}"
        )
        st.stop()

    latest_month = latest_year_month(facility_metrics)
    st.sidebar.markdown("### Data freshness")
    st.sidebar.caption(f"Latest reporting month in the data: **{latest_month}**")
    st.sidebar.caption(f"Facility-months loaded: {len(facility_metrics):,}")
    st.sidebar.caption(f"Read from: `{facility_metrics.attrs.get('source_partition', 'n/a')}`")

    tab_overview, tab_state, tab_compare, tab_readmission, tab_facility = st.tabs(
        [
            "National Overview",
            "State Deep Dive",
            "Compare States",
            "Staffing vs. Readmission",
            "Facility Explorer",
        ]
    )

    # ------------------------------------------------------------------
    # Tab 1 - National Overview
    # ------------------------------------------------------------------
    with tab_overview:
        latest_facility_rows = facility_metrics[facility_metrics["year_month"] == latest_month]

        st.caption(
            f"KPIs below are a simple average across the {len(latest_facility_rows):,} facilities "
            f"that reported in {latest_month} (not population-weighted by resident census)."
        )
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Avg. HPRD (all staff)", f"{latest_facility_rows['avg_hprd_total_nurse'].mean():.2f}")
        c2.metric("Avg. occupancy rate", format_pct(latest_facility_rows["avg_occupancy_rate"].mean()))
        c3.metric("Avg. contract staff share", format_pct(latest_facility_rows["contract_pct_total_nurse"].mean()))
        c4.metric("Total nurse hours (national)", f"{latest_facility_rows['total_nurse_hours'].sum():,.0f}")

        st.divider()

        left, right = st.columns([3, 2])

        with left:
            st.subheader(f"Average HPRD by state - {latest_month}")
            latest_state_rows = state_summary[state_summary["year_month"] == latest_month].copy()
            latest_state_rows["state_upper"] = latest_state_rows["provider_state"].str.strip().str.upper()

            fig = go.Figure(
                go.Choropleth(
                    locations=latest_state_rows["state_upper"],
                    z=latest_state_rows["avg_hprd_total_nurse"],
                    locationmode="USA-states",
                    colorscale=[[i / (len(SEQUENTIAL_BLUE) - 1), c] for i, c in enumerate(SEQUENTIAL_BLUE)],
                    colorbar_title="HPRD",
                    marker_line_color=CHART_SURFACE,
                )
            )
            fig.update_layout(
                geo=dict(scope="usa", bgcolor=CHART_SURFACE),
                paper_bgcolor=CHART_SURFACE,
                margin=dict(t=10, l=0, r=0, b=0),
            )
            st.plotly_chart(fig, use_container_width=True)

        with right:
            st.subheader("National trend")
            national_trend = (
                facility_metrics.groupby("year_month", as_index=False)["avg_hprd_total_nurse"]
                .mean()
                .sort_values("year_month")
            )
            fig = go.Figure(
                go.Scatter(
                    x=national_trend["year_month"],
                    y=national_trend["avg_hprd_total_nurse"],
                    mode="lines+markers",
                    line=dict(color=CATEGORICAL[0], width=2),
                    marker=dict(size=8),
                    name="Avg. HPRD",
                )
            )
            fig = themed_layout(fig, "Avg. HPRD, all facilities, by month", y_title="HPRD")
            st.plotly_chart(fig, use_container_width=True)

        with st.expander("Table view - state summary"):
            st.dataframe(latest_state_rows.drop(columns=["state_upper"]), use_container_width=True)

    # ------------------------------------------------------------------
    # Tab 2 - State Deep Dive
    # ------------------------------------------------------------------
    with tab_state:
        states = sorted(state_summary["provider_state"].dropna().unique())
        chosen_state = st.selectbox("Choose a state", states, key="state_deep_dive_state")

        state_rows = state_summary[state_summary["provider_state"] == chosen_state].sort_values("year_month")

        st.subheader(f"{chosen_state} - metrics over time")
        metric_cols = st.columns(2)
        metrics_to_plot = [
            ("avg_hprd_total_nurse", "HPRD"),
            ("avg_occupancy_rate", "Occupancy rate"),
            ("contract_pct_total_nurse", "Contract staff share"),
            ("total_nurse_hours", "Total nurse hours"),
        ]
        for i, (col, label) in enumerate(metrics_to_plot):
            with metric_cols[i % 2]:
                fig = go.Figure(
                    go.Scatter(
                        x=state_rows["year_month"],
                        y=state_rows[col],
                        mode="lines+markers",
                        line=dict(color=CATEGORICAL[0], width=2),
                        marker=dict(size=6),
                        name=label,
                    )
                )
                fig = themed_layout(fig, label, y_title=label)
                st.plotly_chart(fig, use_container_width=True)

        with st.expander("Table view"):
            st.dataframe(state_rows, use_container_width=True)

    # ------------------------------------------------------------------
    # Tab 3 - Compare States
    # ------------------------------------------------------------------
    with tab_compare:
        st.caption(
            "Limited to 3 states at a time - past 3 series, the validated categorical "
            "palette can no longer guarantee every pair is distinguishable."
        )
        states = sorted(state_summary["provider_state"].dropna().unique())
        default_states = states[:2] if len(states) >= 2 else states
        chosen_states = st.multiselect(
            "States to compare", states, default=default_states, max_selections=3, key="compare_states_multiselect"
        )

        metric_choice_label = st.selectbox("Metric", list(METRIC_LABELS.values()), key="compare_metric")
        metric_choice = next(k for k, v in METRIC_LABELS.items() if v == metric_choice_label)

        if chosen_states:
            fig = go.Figure()
            for i, state in enumerate(chosen_states):
                rows = state_summary[state_summary["provider_state"] == state].sort_values("year_month")
                fig.add_trace(
                    go.Scatter(
                        x=rows["year_month"],
                        y=rows[metric_choice],
                        mode="lines+markers",
                        line=dict(color=CATEGORICAL[i % len(CATEGORICAL)], width=2),
                        marker=dict(size=6),
                        name=state,
                    )
                )
            fig = themed_layout(fig, metric_choice_label, y_title=metric_choice_label)
            st.plotly_chart(fig, use_container_width=True)

            with st.expander("Table view"):
                compare_table = state_summary[state_summary["provider_state"].isin(chosen_states)][
                    ["provider_state", "year_month", metric_choice]
                ].sort_values(["provider_state", "year_month"])
                st.dataframe(compare_table, use_container_width=True)
        else:
            st.info("Pick at least one state to compare.")

    # ------------------------------------------------------------------
    # Tab 4 - Staffing vs. Readmission (metric 5)
    # ------------------------------------------------------------------
    with tab_readmission:
        st.caption(
            "One point per facility (not per facility-month) - see the note in app.py's "
            "docstring on why facility-month grain would over-weight facilities that "
            "reported more months without that reflecting anything about their staffing "
            "or outcomes."
        )
        facility_level = facility_level_staffing_readmission(facility_metrics)

        correlation = facility_level["avg_hprd_total_nurse"].corr(facility_level["readmission_score"])
        st.metric("Correlation (HPRD vs. readmission score)", f"{correlation:.3f}", help="Pearson r across facilities")

        fig = go.Figure(
            go.Scatter(
                x=facility_level["avg_hprd_total_nurse"],
                y=facility_level["readmission_score"],
                mode="markers",
                marker=dict(color=CATEGORICAL[0], size=6, opacity=0.5),
                text=facility_level["provider_name"] + " (" + facility_level["provider_state"] + ")",
                hovertemplate="%{text}<br>HPRD: %{x:.2f}<br>Readmission score: %{y:.2f}<extra></extra>",
                name="Facility",
            )
        )

        # Simple linear fit for a visual trendline (numpy only - no statsmodels dependency).
        valid = facility_level.dropna(subset=["avg_hprd_total_nurse", "readmission_score"])
        if len(valid) >= 2:
            slope, intercept = np.polyfit(valid["avg_hprd_total_nurse"], valid["readmission_score"], 1)
            x_line = np.linspace(valid["avg_hprd_total_nurse"].min(), valid["avg_hprd_total_nurse"].max(), 50)
            fig.add_trace(
                go.Scatter(
                    x=x_line,
                    y=slope * x_line + intercept,
                    mode="lines",
                    line=dict(color=CATEGORICAL[1], width=2, dash="dash"),
                    name="Linear fit",
                )
            )

        fig = themed_layout(
            fig,
            "Staffing intensity vs. readmission score, one point per facility",
            y_title="Readmission score",
            x_title="Avg. HPRD (days-reported-weighted)",
        )
        st.plotly_chart(fig, use_container_width=True)

        with st.expander("Table view"):
            st.dataframe(
                facility_level.sort_values("readmission_score", ascending=False),
                use_container_width=True,
            )

    # ------------------------------------------------------------------
    # Tab 5 - Facility Explorer
    # ------------------------------------------------------------------
    with tab_facility:
        search = st.text_input(
            "Search by facility name or CMS Certification Number (CCN/PROVNUM)", key="facility_search_input"
        )

        if search:
            mask = facility_metrics["provider_name"].str.contains(search, case=False, na=False) | facility_metrics[
                "PROVNUM"
            ].str.contains(search, case=False, na=False)
            matches = facility_metrics[mask]
        else:
            matches = facility_metrics

        options = (
            matches[["PROVNUM", "provider_name", "provider_state"]]
            .drop_duplicates()
            .sort_values("provider_name")
        )
        options["label"] = options["provider_name"] + " - " + options["provider_state"] + " (" + options["PROVNUM"] + ")"

        if options.empty:
            st.info("No facilities match that search.")
        else:
            chosen_label = st.selectbox("Facility", options["label"], key="facility_select")
            chosen_provnum = options.loc[options["label"] == chosen_label, "PROVNUM"].iloc[0]

            facility_rows = facility_metrics[facility_metrics["PROVNUM"] == chosen_provnum].sort_values("year_month")
            readmission_val = facility_rows["readmission_score"].dropna()

            c1, c2 = st.columns(2)
            c1.metric("Months reported", f"{len(facility_rows):,}")
            c2.metric("Readmission score", f"{readmission_val.iloc[0]:.2f}" if len(readmission_val) else "n/a")

            st.subheader("HPRD over time")
            fig = go.Figure(
                go.Scatter(
                    x=facility_rows["year_month"],
                    y=facility_rows["avg_hprd_total_nurse"],
                    mode="lines+markers",
                    line=dict(color=CATEGORICAL[0], width=2),
                    marker=dict(size=6),
                    name="HPRD",
                )
            )
            fig = themed_layout(fig, "HPRD by month", y_title="HPRD")
            st.plotly_chart(fig, use_container_width=True)

            st.subheader("Permanent vs. contract staffing mix")
            mix = facility_rows.copy()
            mix["contract_pct"] = mix["contract_pct_total_nurse"] * 100
            mix["permanent_pct"] = 100 - mix["contract_pct"]
            fig = go.Figure()
            fig.add_trace(
                go.Bar(x=mix["year_month"], y=mix["permanent_pct"], name="Permanent", marker_color=CATEGORICAL[0])
            )
            fig.add_trace(
                go.Bar(x=mix["year_month"], y=mix["contract_pct"], name="Contract", marker_color=CATEGORICAL[1])
            )
            fig.update_layout(barmode="stack")
            fig = themed_layout(fig, "Staffing mix by month", y_title="% of nurse hours")
            st.plotly_chart(fig, use_container_width=True)

            with st.expander("Table view"):
                st.dataframe(facility_rows, use_container_width=True)

    st.divider()
    st.caption(f"Dashboard last loaded {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}.")


if __name__ == "__main__":
    main()

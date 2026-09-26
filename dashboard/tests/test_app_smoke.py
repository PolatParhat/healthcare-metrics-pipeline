"""
Smoke tests for dashboard/app.py.

These do NOT touch real AWS credentials or the real Gold bucket. Instead
they patch boto3.Session itself with a fake S3 client that serves synthetic
Parquet bytes shaped exactly like the real facility_metrics/state_summary
tables (same columns/dtypes, plus the real edge cases: facilities that only
reported 1-2 of 3 months, and a null readmission_score for ~15% of
facilities, matching CMS's own suppression of that measure).

Patching boto3.Session (rather than monkeypatching app.load_gold_dataset
directly) means the test still exercises the REAL partition-discovery and
S3-pagination/download code in app.py - _find_latest_partition_prefix() and
load_gold_dataset() - not just the pure-Python helper functions. That's the
part most likely to have a real bug (wrong prefix string, wrong pagination
kwargs), so it's worth actually running rather than bypassing.

streamlit.testing.v1.AppTest runs the real app.py script headlessly and
reports any unhandled exception via `at.exception` - since st.tabs() renders
every tab's body on every script run (the tabs UI only controls client-side
visibility), a single at.run() already exercises all 5 tabs' default-state
code paths in one pass.

Run with: python3 -m pytest test_app_smoke.py -v
"""

import io
from pathlib import Path
from unittest import mock

import numpy as np
import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

APP_PATH = str(Path(__file__).resolve().parent.parent / "app.py")
GOLD_BUCKET = "healthcare-metrics-gold-941377112484"

STATES = ["CA", "TX", "NY", "FL", "OH"]
YEAR_MONTHS = ["2024-04", "2024-05", "2024-06"]
N_FACILITIES = 30


def _build_synthetic_facility_metrics() -> pd.DataFrame:
    rng = np.random.default_rng(42)
    rows = []
    for i in range(N_FACILITIES):
        provnum = f"{i + 1:06d}"
        state = STATES[i % len(STATES)]
        # Some facilities report all 3 months, some only 1-2 - the exact
        # edge case facility_level_staffing_readmission() exists to handle:
        # a fixed readmission_score must not get over-weighted just because
        # a facility happened to report more months.
        n_months = int(rng.integers(1, len(YEAR_MONTHS) + 1))
        months = list(rng.choice(YEAR_MONTHS, size=n_months, replace=False))
        # ~15% of facilities have no readmission score (CMS suppresses this
        # measure for facilities with too few qualifying stays).
        readmission_score = None if rng.random() < 0.15 else round(float(rng.uniform(5, 25)), 2)

        for ym in months:
            rows.append(
                {
                    "PROVNUM": provnum,
                    "provider_name": f"Facility {i + 1}",
                    "provider_state": state,
                    "year_month": ym,
                    "avg_hprd_total_nurse": round(float(rng.uniform(3.0, 5.0)), 3),
                    "total_nurse_hours": round(float(rng.uniform(3000, 9000)), 1),
                    "avg_occupancy_rate": round(float(rng.uniform(0.65, 0.98)), 4),
                    "readmission_score": readmission_score,
                    "num_days_reported": int(rng.integers(20, 31)),
                    "contract_pct_total_nurse": round(float(rng.uniform(0.0, 0.35)), 4),
                }
            )
    return pd.DataFrame(rows)


def _build_synthetic_state_summary(facility_metrics: pd.DataFrame) -> pd.DataFrame:
    # Mirrors aggregate_gold.py's build_state_summary: sum raw totals first,
    # then divide (census/capacity-weighted), not an average of averages.
    working = facility_metrics.copy()
    working["_contract_hours"] = working["contract_pct_total_nurse"] * working["total_nurse_hours"]

    grouped = working.groupby(["provider_state", "year_month"], as_index=False).agg(
        total_nurse_hours=("total_nurse_hours", "sum"),
        _contract_hours=("_contract_hours", "sum"),
        avg_occupancy_rate=("avg_occupancy_rate", "mean"),
        avg_hprd_total_nurse=("avg_hprd_total_nurse", "mean"),
        num_facilities_reporting=("PROVNUM", "nunique"),
    )
    grouped["contract_pct_total_nurse"] = grouped["_contract_hours"] / grouped["total_nurse_hours"]

    readmission = (
        facility_metrics.dropna(subset=["readmission_score"])
        .groupby(["provider_state", "year_month"], as_index=False)["readmission_score"]
        .mean()
        .rename(columns={"readmission_score": "avg_readmission_score"})
    )

    return grouped.drop(columns=["_contract_hours"]).merge(
        readmission, on=["provider_state", "year_month"], how="left"
    )


def _to_parquet_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    return buf.getvalue()


class _FakeS3Client:
    """Serves the two Gold datasets through the exact same paginated
    list_objects_v2 + get_object calls app.py's real code makes."""

    def __init__(self, dataset_bytes: dict):
        self._dataset_bytes = dataset_bytes

    def get_paginator(self, operation_name):
        assert operation_name == "list_objects_v2"
        return self

    def paginate(self, Bucket, Prefix, Delimiter=None, **kwargs):
        assert Bucket == GOLD_BUCKET
        if Delimiter == "/":
            # Partition-discovery call: "gold/dataset=<name>/"
            dataset_name = Prefix.split("dataset=")[1].rstrip("/")
            assert dataset_name in self._dataset_bytes, f"unexpected dataset {dataset_name!r}"
            partition_prefix = f"{Prefix}ingestion_date=2026-01-01/"
            return iter([{"CommonPrefixes": [{"Prefix": partition_prefix}]}])
        # File-listing call under the already-resolved partition prefix.
        dataset_name = Prefix.split("dataset=")[1].split("/")[0]
        key = f"{Prefix}part-00000.parquet"
        return iter([{"Contents": [{"Key": key}]}])

    def get_object(self, Bucket, Key):
        assert Bucket == GOLD_BUCKET
        dataset_name = Key.split("dataset=")[1].split("/")[0]
        return {"Body": io.BytesIO(self._dataset_bytes[dataset_name])}


class _FakeSession:
    def __init__(self, s3_client):
        self._s3_client = s3_client

    def client(self, service_name, **kwargs):
        assert service_name == "s3"
        return self._s3_client


@pytest.fixture()
def synthetic_datasets():
    facility_metrics = _build_synthetic_facility_metrics()
    state_summary = _build_synthetic_state_summary(facility_metrics)
    return facility_metrics, state_summary


@pytest.fixture()
def patched_boto3(synthetic_datasets):
    facility_metrics, state_summary = synthetic_datasets
    dataset_bytes = {
        "facility_metrics": _to_parquet_bytes(facility_metrics),
        "state_summary": _to_parquet_bytes(state_summary),
    }
    fake_session = _FakeSession(_FakeS3Client(dataset_bytes))
    with mock.patch("boto3.Session", return_value=fake_session):
        yield


def _assert_no_exceptions(at: AppTest, label: str):
    assert not at.exception, f"Unhandled exception(s) in {label}: {[str(e) for e in at.exception]}"


def test_default_state_renders_with_no_exceptions(patched_boto3):
    at = AppTest.from_file(APP_PATH)
    at.run(timeout=60)
    _assert_no_exceptions(at, "default run (all 5 tabs' default state)")
    # Sidebar data-freshness captions should reflect the fake partition.
    sidebar_text = " ".join(c.value for c in at.sidebar.caption)
    assert "2026-01-01" in sidebar_text or "ingestion_date=2026-01-01" in sidebar_text


def test_facility_search_renders_with_no_exceptions(patched_boto3):
    at = AppTest.from_file(APP_PATH)
    at.run(timeout=60)
    at.text_input(key="facility_search_input").set_value("Facility 1").run(timeout=60)
    _assert_no_exceptions(at, "facility search interaction")


def test_compare_states_metric_switch_renders_with_no_exceptions(patched_boto3):
    at = AppTest.from_file(APP_PATH)
    at.run(timeout=60)
    at.selectbox(key="compare_metric").set_value("Total nurse hours").run(timeout=60)
    _assert_no_exceptions(at, "compare-states metric switch")


def test_state_deep_dive_switch_renders_with_no_exceptions(patched_boto3):
    at = AppTest.from_file(APP_PATH)
    at.run(timeout=60)
    other_state = [s for s in STATES if s != at.selectbox(key="state_deep_dive_state").value][0]
    at.selectbox(key="state_deep_dive_state").set_value(other_state).run(timeout=60)
    _assert_no_exceptions(at, "state deep dive state switch")


def test_facility_level_weighting_matches_hand_calculation(synthetic_datasets):
    """Correctness check, not just crash check: hand-verify the
    days-reported-weighted average for one synthetic facility against
    app.py's facility_level_staffing_readmission()."""
    import importlib.util

    spec = importlib.util.spec_from_file_location("dashboard_app_under_test", APP_PATH)
    app_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(app_module)

    facility_metrics, _ = synthetic_datasets
    facility_level = app_module.facility_level_staffing_readmission(facility_metrics)

    # Pick a facility that reported more than one month, to actually
    # exercise the weighting (not just a pass-through of a single row).
    counts = facility_metrics.groupby("PROVNUM").size()
    multi_month_provnum = counts[counts > 1].index[0]

    rows = facility_metrics[facility_metrics["PROVNUM"] == multi_month_provnum]
    expected_weighted_avg = (rows["avg_hprd_total_nurse"] * rows["num_days_reported"]).sum() / rows[
        "num_days_reported"
    ].sum()

    actual_row = facility_level[facility_level["PROVNUM"] == multi_month_provnum].iloc[0]
    assert actual_row["avg_hprd_total_nurse"] == pytest.approx(expected_weighted_avg, rel=1e-9)

    # And confirm it's NOT the same as a naive unweighted mean whenever
    # num_days_reported actually varies across that facility's months -
    # this is the whole point of the grain-collapse design decision.
    naive_mean = rows["avg_hprd_total_nurse"].mean()
    if rows["num_days_reported"].nunique() > 1:
        assert actual_row["avg_hprd_total_nurse"] != pytest.approx(naive_mean, rel=1e-9)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))

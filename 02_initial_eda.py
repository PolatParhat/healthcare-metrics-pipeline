
from pathlib import Path

import matplotlib
# matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

MASTER = "Data/PBJ_Daily_Nurse_Staffing_Q2_2024.csv"
ENCODING = "cp1252"
OUT_DIR = Path("eda_output")
OUT_DIR.mkdir(exist_ok=True)

HOURS_COLS = ["Hrs_RNDON", "Hrs_RNadmin", "Hrs_RN", "Hrs_LPNadmin",
              "Hrs_LPN", "Hrs_CNA", "Hrs_NAtrn", "Hrs_MedAide"]


def main():
    df = pd.read_csv(MASTER, encoding=ENCODING, low_memory=False,
                      dtype={"PROVNUM": str})
    n = len(df)
    print(f"Loaded {n:,} rows, {df.shape[1]} cols\n")

    df["WorkDate_parsed"] = pd.to_datetime(df["WorkDate"], format="%Y%m%d", errors="coerce")
    bad_dates = df["WorkDate_parsed"].isna().sum()
    print(f"WorkDate unparseable: {bad_dates} ({bad_dates/n:.2%})")
    print(f"Date range: {df['WorkDate_parsed'].min()} to {df['WorkDate_parsed'].max()}")

    missing_pct = (df.isna().sum() / n * 100).sort_values(ascending=False)
    nonzero_missing = missing_pct[missing_pct > 0]
    print(f"\nColumns with missing values: {len(nonzero_missing)}")
    if len(nonzero_missing):
        print(nonzero_missing)

    exact_dupes = df.duplicated().sum()
    key_dupes = df.duplicated(subset=["PROVNUM", "WorkDate"]).sum()
    print(f"\nExact duplicate rows: {exact_dupes} ({exact_dupes/n:.2%})")
    print(f"Duplicate (PROVNUM, WorkDate) keys: {key_dupes} ({key_dupes/n:.2%})")

    print("\n=== Outlier scan (IQR) on hours columns ===")
    print("NOTE: several hours columns are mostly zero (contract/training hours),")
    print("so IQR fences collapse to ~0 and flag most nonzero rows as 'outliers'.")
    print("Treat this as a skew signal, not a literal error count - use domain")
    print("thresholds (e.g. Hrs_RN > 500/day) for real data-quality flags.")
    rows = []
    for col in HOURS_COLS:
        s = df[col].dropna()
        q1, q3 = s.quantile(0.25), s.quantile(0.75)
        hi = q3 + 1.5 * (q3 - q1)
        rows.append({"col": col, "negative": int((s < 0).sum()),
                     "above_iqr_fence": int((s > hi).sum()),
                     "upper_fence": round(hi, 1), "max": round(s.max(), 1)})
    print(pd.DataFrame(rows).to_string(index=False))

    print(f"\nRows with MDScensus == 0: {(df['MDScensus']==0).sum()}")
    print(f"Rows with Hrs_RN > 500 in a single facility-day (likely data errors): "
          f"{(df['Hrs_RN'] > 500).sum()}")

    print(f"\nDistinct facilities (PROVNUM): {df['PROVNUM'].nunique()}")
    print(f"Distinct states: {df['STATE'].nunique()}")

    # --- plots ---
    plt.figure(figsize=(9, 6))
    if len(nonzero_missing):
        nonzero_missing.plot(kind="barh")
        plt.xlabel("% missing")
    else:
        plt.text(0.5, 0.5, "No missing values in any column", ha="center", va="center", fontsize=14)
        plt.axis("off")
    plt.title("Missing values by column")
    plt.tight_layout()
    plt.savefig(OUT_DIR / "eda_missing_values.png", dpi=150)
    plt.close()

    plt.figure(figsize=(8, 5))
    df["MDScensus"].dropna().plot(kind="hist", bins=60)
    plt.xlabel("MDScensus (patients)")
    plt.title("Distribution of daily census")
    plt.tight_layout()
    plt.savefig(OUT_DIR / "eda_census_distribution.png", dpi=150)
    plt.close()

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.boxplot([df[c].dropna().values for c in HOURS_COLS],
               tick_labels=HOURS_COLS, vert=False, showfliers=True)
    ax.set_xscale("symlog")
    ax.set_title("Distribution / outliers - total hours columns (log scale)")
    plt.tight_layout()
    plt.savefig(OUT_DIR / "eda_hours_boxplots.png", dpi=150)
    plt.close()

    corr_cols = ["MDScensus"] + HOURS_COLS
    corr = df[corr_cols].corr(numeric_only=True)
    plt.figure(figsize=(8, 6))
    plt.imshow(corr, cmap="coolwarm", vmin=-1, vmax=1)
    plt.xticks(range(len(corr_cols)), corr_cols, rotation=90)
    plt.yticks(range(len(corr_cols)), corr_cols)
    for i in range(len(corr_cols)):
        for j in range(len(corr_cols)):
            plt.text(j, i, f"{corr.iloc[i, j]:.2f}", ha="center", va="center", fontsize=7)
    plt.colorbar(label="correlation")
    plt.title("Census vs staffing hours - correlation")
    plt.tight_layout()
    plt.savefig(OUT_DIR / "eda_correlation_heatmap.png", dpi=150)
    plt.close()

    print(f"\nCensus correlation ranking:\n{corr['MDScensus'].sort_values(ascending=False)}")
    print(f"\nSaved 4 PNGs to {OUT_DIR}/")


if __name__ == "__main__":
    main()

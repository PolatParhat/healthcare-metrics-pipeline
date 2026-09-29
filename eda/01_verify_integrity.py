import time
import pandas as pd
import glob
from pathlib import Path

MASTER = "Data/PBJ_Daily_Nurse_Staffing_Q2_2024.csv"
SUPPORTING_DIR = "Data/supportingFiles"
ENCODING="cp1252"

EXPECTED_MASTER_COLUMNS = [
    "PROVNUM", "PROVNAME", "CITY", "STATE", "COUNTY_NAME", "COUNTY_FIPS",
    "CY_Qtr", "WorkDate", "MDScensus",
    "Hrs_RNDON", "Hrs_RNDON_emp", "Hrs_RNDON_ctr",
    "Hrs_RNadmin", "Hrs_RNadmin_emp", "Hrs_RNadmin_ctr",
    "Hrs_RN", "Hrs_RN_emp", "Hrs_RN_ctr",
    "Hrs_LPNadmin", "Hrs_LPNadmin_emp", "Hrs_LPNadmin_ctr",
    "Hrs_LPN", "Hrs_LPN_emp", "Hrs_LPN_ctr",
    "Hrs_CNA", "Hrs_CNA_emp", "Hrs_CNA_ctr",
    "Hrs_NAtrn", "Hrs_NAtrn_emp", "Hrs_NAtrn_ctr",
    "Hrs_MedAide", "Hrs_MedAide_emp", "Hrs_MedAide_ctr",
]


def check_file(path: str, is_master: bool = False) -> dict:
    result = {"file": path, "ok": False, "rows": None, "cols": None, "error": ""}
    t0 = time.time()
    try:
        df = pd.read_csv(path, low_memory=False, on_bad_lines="error", encoding=ENCODING)
        result["ok"] = True
        result["rows"] = len(df)
        result["cols"] = len(df.columns)
        result["seconds"] = round(time.time() - t0, 2)
        if is_master:
            missing = set(EXPECTED_MASTER_COLUMNS) - set(df.columns)
            extra = set(df.columns) - set(EXPECTED_MASTER_COLUMNS)
            result["schema_match"] = not missing and not extra
            if missing:
                print(f"  MISSING expected columns: {sorted(missing)}")
            if extra:
                print(f"  UNEXPECTED extra columns: {sorted(extra)}")
    except Exception as e:  # noqa: BLE001
        result["error"] = f"{type(e).__name__}: {e}"
    return result


def main():
    print(f"=== Master file ===")
    r = check_file(MASTER, is_master=True)
    status = "OK" if r["ok"] else "FAIL"
    print(f"{status}  {r['file']}  rows={r.get('rows')}  cols={r.get('cols')}  {r.get('error','')}")

    print(f"\n=== Supporting files ({SUPPORTING_DIR}/) ===")
    all_ok = r["ok"]
    for f in sorted(glob.glob(f"{SUPPORTING_DIR}/*.csv")):
        res = check_file(f)
        status = "OK  " if res["ok"] else "FAIL"
        print(f"{status} {res['file']}  rows={res.get('rows')}  cols={res.get('cols')}  {res.get('error','')}")
        all_ok = all_ok and res["ok"]

    other = [p for p in Path(SUPPORTING_DIR).glob("*") if p.suffix.lower() != ".csv"]
    if other:
        print(f"\nNon-CSV files present (not integrity-checked the same way): {[p.name for p in other]}")

    print("\nAll files parsed cleanly." if all_ok else "\nSome files FAILED - fix before Step 2.")
    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

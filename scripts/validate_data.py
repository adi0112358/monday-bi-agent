#!/usr/bin/env python3
from pathlib import Path
import sys
import pandas as pd

DEALS_CLEAN = Path("data/cleaned/Deal_funnel_Data.cleaned.csv")
WO_CLEAN = Path("data/cleaned/Work_Order_Tracker_Data.cleaned.csv")
DEALS_ANOM = Path("data/reports/Deal_funnel_Data.anomaly_report.csv")
WO_ANOM = Path("data/reports/Work_Order_Tracker_Data.anomaly_report.csv")

DEALS_REQUIRED = [
    "Deal Name", "Owner code", "Client Code", "Deal Status", "Close Date (A)",
    "Closure Probability", "Masked Deal value", "Tentative Close Date",
    "Deal Stage", "Product deal", "Sector/service", "Created Date",
    "source_row_number", "quality_flag"
]

WO_REQUIRED = [
    "Deal name masked", "Customer Name Code", "Execution Status", "Sector",
    "Type of Work", "Amount Receivable (Masked)", "BD/KAM Personnel code",
    "source_row_number", "quality_flag"
]

ALLOWED_DEAL_STATUS = {"Open", "Won", "Dead", "On Hold"}
ALLOWED_PROB = {"High", "Medium", "Low"}

def info(msg):
    print(f"[INFO] {msg}")

def warn(msg):
    print(f"[WARN] {msg}")

def fail(msg):
    print(f"[FAIL] {msg}")

def check_file(path: Path):
    if not path.exists():
        fail(f"Missing file: {path}")
        return False
    if path.stat().st_size == 0:
        fail(f"Empty file: {path}")
        return False
    info(f"Found: {path} ({path.stat().st_size} bytes)")
    return True

def missing_columns(df, required):
    return [c for c in required if c not in df.columns]

def to_numeric_safe(series):
    return pd.to_numeric(series, errors="coerce")

def parse_dates_mixed(series):
    n = pd.to_numeric(series, errors="coerce")
    d_from_serial = pd.to_datetime("1899-12-30") + pd.to_timedelta(n, unit="D")
    d_from_text = pd.to_datetime(series, errors="coerce", format="mixed")
    return d_from_serial.where(~n.isna(), d_from_text)

def main():
    ok = True

    for p in [DEALS_CLEAN, WO_CLEAN, DEALS_ANOM, WO_ANOM]:
        ok = check_file(p) and ok

    if not ok:
        sys.exit(1)

    deals = pd.read_csv(DEALS_CLEAN)
    wo = pd.read_csv(WO_CLEAN)

    # Column checks
    miss = missing_columns(deals, DEALS_REQUIRED)
    if miss:
        fail(f"Deals missing columns: {miss}")
        ok = False
    else:
        info("Deals required columns: OK")

    miss = missing_columns(wo, WO_REQUIRED)
    if miss:
        fail(f"Work orders missing columns: {miss}")
        ok = False
    else:
        info("Work orders required columns: OK")

    # Row counts
    info(f"Deals rows: {len(deals)}")
    info(f"Work orders rows: {len(wo)}")

    # Duplicate checks
    dups_deals = deals.duplicated().sum()
    dups_wo = wo.duplicated().sum()
    if dups_deals > 0:
        warn(f"Deals exact duplicates remaining: {dups_deals}")
    else:
        info("Deals exact duplicates: 0")
    if dups_wo > 0:
        warn(f"Work orders exact duplicates remaining: {dups_wo}")
    else:
        info("Work orders exact duplicates: 0")

    # Deals categorical checks
    if "Deal Status" in deals.columns:
        bad = deals["Deal Status"].dropna()
        bad = bad[~bad.isin(ALLOWED_DEAL_STATUS)]
        if len(bad) > 0:
            warn(f"Invalid Deal Status values remaining: {sorted(bad.unique().tolist())[:10]}")
        else:
            info("Deal Status values: OK")

    if "Closure Probability" in deals.columns:
        bad = deals["Closure Probability"].dropna()
        bad = bad[~bad.isin(ALLOWED_PROB)]
        if len(bad) > 0:
            warn(f"Invalid Closure Probability values remaining: {sorted(bad.unique().tolist())[:10]}")
        else:
            info("Closure Probability values: OK")

    # Type/parse sanity
    if "Masked Deal value" in deals.columns:
        num = to_numeric_safe(deals["Masked Deal value"])
        parse_rate = num.notna().mean()
        info(f"Deals numeric parse rate (Masked Deal value): {parse_rate:.1%}")

    deal_date_cols = ["Close Date (A)", "Tentative Close Date", "Created Date"]
    for c in deal_date_cols:
        if c in deals.columns:
            parsed = parse_dates_mixed(deals[c])
            rate = parsed.notna().mean()
            info(f"Deals date parse rate ({c}): {rate:.1%}")

    if "Amount Receivable (Masked)" in wo.columns:
        recv = to_numeric_safe(wo["Amount Receivable (Masked)"])
        neg = (recv < 0).sum()
        info(f"Work orders negative receivables: {int(neg)}")

    # Joinability check
    if "Deal Name" in deals.columns and "Deal name masked" in wo.columns:
        dset = set(deals["Deal Name"].dropna().astype(str).str.strip())
        wset = set(wo["Deal name masked"].dropna().astype(str).str.strip())
        overlap = len(dset & wset)
        info(f"Cross-board overlap (Deal Name ↔ Deal name masked): {overlap}")
        if overlap == 0:
            warn("No overlap found for primary join key.")

    # Anomaly report counts
    da = pd.read_csv(DEALS_ANOM)
    wa = pd.read_csv(WO_ANOM)
    info(f"Deals anomaly rows: {len(da)}")
    info(f"Work orders anomaly rows: {len(wa)}")

    if ok:
        print("\nVALIDATION: PASS (with warnings if shown)")
        sys.exit(0)
    else:
        print("\nVALIDATION: FAIL")
        sys.exit(1)

if __name__ == "__main__":
    main()

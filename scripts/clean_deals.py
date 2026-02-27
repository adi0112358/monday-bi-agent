#!/usr/bin/env python3
import re
import pandas as pd
from pathlib import Path

INPUT = Path("data/raw/Deal funnel Data.csv")
CLEAN_OUT = Path("data/cleaned/Deal_funnel_Data.cleaned.csv")
ANOM_OUT = Path("data/reports/Deal_funnel_Data.anomaly_report.csv")

CRITICAL = ["Deal Name", "Deal Status", "Deal Stage", "Created Date"]
ALLOWED_STATUS = {"Open", "Won", "Dead", "On Hold"}
ALLOWED_PROB = {"High", "Medium", "Low"}

def norm_text(x):
    if pd.isna(x):
        return pd.NA
    s = str(x).strip()
    return pd.NA if s == "" else s
def parse_date_col(series):
    # Handles both Excel serial and ISO-like strings
    n = pd.to_numeric(series, errors="coerce")
    d_from_serial = pd.to_datetime("1899-12-30") + pd.to_timedelta(n, unit="D")
    d_from_text = pd.to_datetime(series, errors="coerce", format="mixed")

    out = d_from_serial.where(~n.isna(), d_from_text)
    return out.dt.date
def add_flag(df, mask, flag):
    df.loc[mask, "quality_flag"] = df.loc[mask, "quality_flag"].apply(
        lambda v: flag if not v else f"{v}|{flag}"
    )

def main():
    df = pd.read_csv(INPUT, encoding="utf-8-sig")
    df["source_row_number"] = df.index + 2  # csv line number (header is line 1)
    df["quality_flag"] = ""

    # Normalize text
    for c in ["Deal Name","Owner code","Client Code","Deal Status","Closure Probability","Masked Deal value","Tentative Close Date","Deal Stage","Product deal","Sector/service","Created Date","Close Date (A)"]:
        if c in df.columns:
            df[c] = df[c].map(norm_text)

    # 1) Embedded header rows
    header_like = (df["Deal Status"] == "Deal Status") | (df["Deal Stage"] == "Deal Stage")
    add_flag(df, header_like, "embedded_header_row")

    # 2) Category cleanup
    if "Deal Status" in df.columns:
        bad_status = df["Deal Status"].notna() & ~df["Deal Status"].isin(ALLOWED_STATUS)
        add_flag(df, bad_status, "invalid_deal_status")
        df.loc[bad_status, "Deal Status"] = pd.NA

    if "Closure Probability" in df.columns:
        bad_prob = df["Closure Probability"].notna() & ~df["Closure Probability"].isin(ALLOWED_PROB)
        add_flag(df, bad_prob, "invalid_closure_probability")
        df.loc[bad_prob, "Closure Probability"] = pd.NA

    # 3) Date normalization
    for c in ["Close Date (A)", "Tentative Close Date", "Created Date"]:
        if c in df.columns:
            raw = df[c].copy()
            df[c] = parse_date_col(df[c])
            invalid = raw.notna() & df[c].isna()
            add_flag(df, invalid, f"invalid_{c.lower().replace(' ','_').replace('(','').replace(')','')}")

    # 4) Numeric normalization
    if "Masked Deal value" in df.columns:
        raw = df["Masked Deal value"].copy()
        df["Masked Deal value"] = pd.to_numeric(df["Masked Deal value"], errors="coerce")
        invalid_num = raw.notna() & df["Masked Deal value"].isna()
        add_flag(df, invalid_num, "invalid_masked_deal_value")

    # 5) Owner code validation
    if "Owner code" in df.columns:
        owner_bad = df["Owner code"].notna() & ~df["Owner code"].str.match(r"^OWNER_\d{3}$", na=False)
        owner_missing = df["Owner code"].isna()
        add_flag(df, owner_bad, "invalid_owner_code")
        add_flag(df, owner_missing, "missing_owner_code")

    # 6) Missing critical fields
    for c in CRITICAL:
        if c in df.columns:
            miss = df[c].isna()
            add_flag(df, miss, f"missing_{c.lower().replace(' ','_')}")

    # 7) Dedup markers
    exact_dup = df.duplicated(keep="first")
    add_flag(df, exact_dup, "exact_duplicate")

    key = ["Deal Name", "Client Code", "Created Date", "Deal Stage"]
    if all(c in df.columns for c in key):
        key_dup = df.duplicated(subset=key, keep="first")
        add_flag(df, key_dup, "business_key_duplicate")

    # Anomaly report (all flagged rows)
    anomalies = df[df["quality_flag"] != ""].copy()
    anomalies.to_csv(ANOM_OUT, index=False)

    # Final cleaned dataset:
    # Drop embedded headers + exact duplicates; keep remaining rows (even incomplete) with cleaned types
    cleaned = df[~header_like & ~exact_dup].copy()
    cleaned.to_csv(CLEAN_OUT, index=False)

    print("Wrote:")
    print(f"- {CLEAN_OUT}")
    print(f"- {ANOM_OUT}")
    print(f"Rows input={len(df)}, cleaned={len(cleaned)}, anomalies={len(anomalies)}")

if __name__ == "__main__":
    main()

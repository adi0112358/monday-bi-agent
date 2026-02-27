#!/usr/bin/env python3
import pandas as pd
from pathlib import Path

INPUT = Path("data/raw/Work_Order_Tracker Data.csv")
CLEAN_OUT = Path("data/cleaned/Work_Order_Tracker_Data.cleaned.csv")
ANOM_OUT = Path("data/reports/Work_Order_Tracker_Data.anomaly_report.csv")

CRITICAL = [
    "Deal name masked",
    "Customer Name Code",
    "Execution Status",
    "Sector",
    "Type of Work",
]

DATE_COLS = [
    "Data Delivery Date",
    "Date of PO/LOI",
    "Probable Start Date",
    "Probable End Date",
    "Last invoice date",
    "Collection Date",
]

NUM_COLS = [
    "Amount in Rupees (Excl of GST) (Masked)",
    "Amount in Rupees (Incl of GST) (Masked)",
    "Billed Value in Rupees (Excl of GST.) (Masked)",
    "Billed Value in Rupees (Incl of GST.) (Masked)",
    "Collected Amount in Rupees (Incl of GST.) (Masked)",
    "Amount to be billed in Rs. (Exl. of GST) (Masked)",
    "Amount to be billed in Rs. (Incl. of GST) (Masked)",
    "Amount Receivable (Masked)",
]


def norm_text(x):
    if pd.isna(x):
        return pd.NA
    s = str(x).strip()
    return pd.NA if s == "" else s


def parse_date_col(series):
    # Works with both Excel serial dates and textual dates.
    n = pd.to_numeric(series, errors="coerce")
    d_from_serial = pd.to_datetime("1899-12-30") + pd.to_timedelta(n, unit="D")
    d_from_text = pd.to_datetime(series, errors="coerce", format="mixed")
    out = d_from_serial.where(~n.isna(), d_from_text)
    return out.dt.date


def sanitize_flag_name(name):
    return (
        name.lower()
        .replace(" ", "_")
        .replace("/", "_")
        .replace("(", "")
        .replace(")", "")
        .replace(".", "")
    )


def add_flag(df, mask, flag):
    mask = mask.fillna(False)
    df.loc[mask, "quality_flag"] = df.loc[mask, "quality_flag"].apply(
        lambda v: flag if pd.isna(v) or v == "" else f"{v}|{flag}"
    )


def main():
    # Header is row 2 in raw file; row 1 is blank placeholders.
    df = pd.read_csv(INPUT, encoding="utf-8-sig", header=1)

    # Normalize text columns first
    for c in df.columns:
        if df[c].dtype == "object":
            df[c] = df[c].map(norm_text)

    # Tracking columns
    df["source_row_number"] = df.index + 3  # CSV line number where data starts
    df["quality_flag"] = ""

    # Status typo cleanup
    if "Billing Status" in df.columns:
        billed_typo = df["Billing Status"] == "BIlled"
        add_flag(df, billed_typo, "billing_status_typo_corrected")
        df.loc[billed_typo, "Billing Status"] = "Billed"

    # Normalize dates
    for c in DATE_COLS:
        if c in df.columns:
            raw = df[c].copy()
            df[c] = parse_date_col(df[c])
            invalid = raw.notna() & df[c].isna()
            add_flag(df, invalid, f"invalid_{sanitize_flag_name(c)}")

    # Normalize numeric columns
    for c in NUM_COLS:
        if c in df.columns:
            raw = df[c].copy()
            df[c] = pd.to_numeric(df[c], errors="coerce")
            invalid = raw.notna() & df[c].isna()
            add_flag(df, invalid, f"invalid_{sanitize_flag_name(c)}")

    # Owner code validation
    if "BD/KAM Personnel code" in df.columns:
        owner = df["BD/KAM Personnel code"]
        owner_bad = owner.notna() & ~owner.str.match(r"^OWNER_\d{3}$", na=False)
        owner_missing = owner.isna()
        add_flag(df, owner_bad, "invalid_owner_code")
        add_flag(df, owner_missing, "missing_owner_code")

    # Missing critical fields
    for c in CRITICAL:
        if c in df.columns:
            add_flag(df, df[c].isna(), f"missing_{sanitize_flag_name(c)}")

    # Financial anomaly: negative receivables
    if "Amount Receivable (Masked)" in df.columns:
        neg_recv = df["Amount Receivable (Masked)"] < 0
        add_flag(df, neg_recv, "negative_amount_receivable")

    # Exact duplicates
    exact_dup = df.duplicated(keep="first")
    add_flag(df, exact_dup, "exact_duplicate")

    # Outputs
    anomalies = df[df["quality_flag"] != ""].copy()
    anomalies.to_csv(ANOM_OUT, index=False)

    cleaned = df[~exact_dup].copy()
    cleaned.to_csv(CLEAN_OUT, index=False)

    print("Wrote:")
    print(f"- {CLEAN_OUT}")
    print(f"- {ANOM_OUT}")
    print(f"Rows input={len(df)}, cleaned={len(cleaned)}, anomalies={len(anomalies)}")


if __name__ == "__main__":
    main()

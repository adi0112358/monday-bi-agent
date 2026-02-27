import pandas as pd
from app.config import DATA_BACKEND, MONDAY_WORK_ORDERS_BOARD_ID, WO_CSV
from app.tools.trace import timed_call
from app.tools.monday_client import fetch_board_items

# Fill this after running scripts/probe_monday_boards.py.
# Example:
# WO_COLUMN_MAP = {
#     "sector_column_id": "Sector",
#     "receivable_column_id": "Amount Receivable (Masked)",
# }
WO_COLUMN_MAP = {
    "dropdown_mm0zr1a": "Customer Name Code",
    "dropdown_mm0zvp6r": "Serial #",
    "color_mm0zggjj": "Nature of Work",
    "color_mm0zgmsk": "Last executed month of recurring project",
    "color_mm0zngha": "Execution Status",
    "date_mm0zj4dm": "Data Delivery Date",
    "date_mm0zv71s": "Date of PO/LOI",
    "color_mm0z2jm6": "Document Type",
    "date_mm0zb88": "Probable Start Date",
    "date_mm0z9w1q": "Probable End Date",
    "color_mm0zj4ev": "BD/KAM Personnel code",
    "color_mm0zms75": "Sector",
    "color_mm0zrda8": "Type of Work",
    "color_mm0z467s": "Is any Skylark software platform part of the client deliverables in this deal?",
    "date_mm0zhche": "Last invoice date",
    "dropdown_mm0zys2b": "latest invoice no.",
    "numeric_mm0z14t8": "Amount in Rupees (Excl of GST) (Masked)",
    "numeric_mm0z7155": "Amount in Rupees (Incl of GST) (Masked)",
    "numeric_mm0z9gxw": "Billed Value in Rupees (Excl of GST.) (Masked)",
    "numeric_mm0zees1": "Billed Value in Rupees (Incl of GST.) (Masked)",
    "numeric_mm0zqba6": "Collected Amount in Rupees (Incl of GST.) (Masked)",
    "numeric_mm0zdtk6": "Amount to be billed in Rs. (Exl. of GST) (Masked)",
    "numeric_mm0zxvep": "Amount to be billed in Rs. (Incl. of GST) (Masked)",
    "numeric_mm0zhacq": "Amount Receivable (Masked)",
    "color_mm0ztcf6": "AR Priority account",
    "numeric_mm0zswea": "Quantity by Ops",
    "dropdown_mm0zqn8j": "Quantities as per PO",
    "numeric_mm0zdj78": "Quantity billed (till date)",
    "numeric_mm0z5wg8": "Balance in quantity",
    "color_mm0z98zq": "Invoice Status",
    "text_mm0z687n": "Expected Billing Month",
    "color_mm0z4mzb": "Actual Billing Month",
    "text_mm0z9qjg": "Actual Collection Month",
    "color_mm0zh4ja": "WO Status (billed)",
    "text_mm0zf6zn": "Collection status",
    "text_mm0z2wqs": "Collection Date",
    "color_mm0zrfab": "Billing Status",
    "numeric_mm0z43q2": "source_row_number",
    "color_mm0zcj4v": "quality_flag",
}


def _ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    required = [
        "Deal name masked",
        "Sector",
        "Amount Receivable (Masked)",
    ]
    for col in required:
        if col not in df.columns:
            df[col] = pd.NA
    return df


def _load_local(sector=None):
    df = pd.read_csv(WO_CSV)
    df = _ensure_columns(df)
    if sector and "Sector" in df.columns:
        df = df[df["Sector"].astype(str).str.lower() == sector.lower()]
    return df


def _load_monday(sector=None):
    items = fetch_board_items(MONDAY_WORK_ORDERS_BOARD_ID)
    rows = []
    for item in items:
        row = {"Deal name masked": item.get("name")}
        for col in item.get("column_values", []):
            row[col.get("id")] = col.get("text")
        rows.append(row)

    df = pd.DataFrame(rows)
    if not df.empty and WO_COLUMN_MAP:
        df = df.rename(columns=WO_COLUMN_MAP)

    df = _ensure_columns(df)
    if sector and "Sector" in df.columns:
        df = df[df["Sector"].astype(str).str.lower() == sector.lower()]
    return df

def get_work_orders(tracer, sector=None):
    def _load():
        if DATA_BACKEND == "monday":
            return _load_monday(sector=sector)
        return _load_local(sector=sector)

    return timed_call(
        tracer,
        "get_work_orders",
        f"backend={DATA_BACKEND}, sector={sector}",
        _load,
    )

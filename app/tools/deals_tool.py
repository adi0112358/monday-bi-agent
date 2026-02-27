import pandas as pd
from app.config import DATA_BACKEND, DEALS_CSV, MONDAY_DEALS_BOARD_ID
from app.tools.trace import timed_call
from app.tools.monday_client import fetch_board_items

# Fill this after running scripts/probe_monday_boards.py.
# Example:
# DEALS_COLUMN_MAP = {
#     "status_column_id": "Deal Status",
#     "stage_column_id": "Deal Stage",
#     "sector_column_id": "Sector/service",
#     "masked_value_column_id": "Masked Deal value",
# }
DEALS_COLUMN_MAP = {
    "color_mm0zcca8": "Owner code",
    "dropdown_mm0zwvgm": "Client Code",
    "color_mm0zcw27": "Deal Status",
    "date_mm0z5gvp": "Close Date (A)",
    "color_mm0z8axa": "Closure Probability",
    "numeric_mm0z82j0": "Masked Deal value",
    "date_mm0zr1sa": "Tentative Close Date",
    "color_mm0zaay1": "Deal Stage",
    "color_mm0z484": "Product deal",
    "color_mm0znxwb": "Sector/service",
    "date_mm0zyjxa": "Created Date",
    "numeric_mm0z5vb4": "source_row_number",
    "color_mm0zw56s": "quality_flag",
}


def _ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    required = [
        "Deal Name",
        "Deal Status",
        "Deal Stage",
        "Sector/service",
        "Masked Deal value",
    ]
    for col in required:
        if col not in df.columns:
            df[col] = pd.NA
    return df


def _load_local(sector=None):
    df = pd.read_csv(DEALS_CSV)
    df = _ensure_columns(df)
    if sector:
        df = df[df["Sector/service"].astype(str).str.lower() == sector.lower()]
    return df


def _load_monday(sector=None):
    items = fetch_board_items(MONDAY_DEALS_BOARD_ID)
    rows = []
    for item in items:
        row = {"Deal Name": item.get("name")}
        for col in item.get("column_values", []):
            row[col.get("id")] = col.get("text")
        rows.append(row)

    df = pd.DataFrame(rows)
    if not df.empty and DEALS_COLUMN_MAP:
        df = df.rename(columns=DEALS_COLUMN_MAP)

    df = _ensure_columns(df)
    if sector:
        df = df[df["Sector/service"].astype(str).str.lower() == sector.lower()]
    return df

def get_deals(tracer, sector=None):
    def _load():
        if DATA_BACKEND == "monday":
            return _load_monday(sector=sector)
        return _load_local(sector=sector)

    return timed_call(
        tracer,
        "get_deals",
        f"backend={DATA_BACKEND}, sector={sector}",
        _load,
    )

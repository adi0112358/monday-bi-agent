import pandas as pd


def _num(series):
    cleaned = (
        series.astype(str)
        .str.replace(",", "", regex=False)
        .str.replace("₹", "", regex=False)
        .str.strip()
        .replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
    )
    return pd.to_numeric(cleaned, errors="coerce")


def pipeline_summary(deals: pd.DataFrame):
    by_status = deals["Deal Status"].value_counts(dropna=False).to_dict()
    by_stage = deals["Deal Stage"].value_counts(dropna=False).head(8).to_dict()
    return {"by_status": by_status, "top_stages": by_stage, "rows": len(deals)}


def receivable_summary(work_orders: pd.DataFrame):
    col = "Amount Receivable (Masked)"
    if col not in work_orders.columns:
        return {"total_receivable": None, "negative_count": None}
    s = _num(work_orders[col])
    return {"total_receivable": float(s.fillna(0).sum()), "negative_count": int((s < 0).sum())}


def cross_board_overlap(deals: pd.DataFrame, work_orders: pd.DataFrame):
    d = set(deals["Deal Name"].dropna().astype(str).str.strip())
    w = set(work_orders["Deal name masked"].dropna().astype(str).str.strip())
    return {"overlap_count": len(d & w)}


def pipeline_by_stage_status(deals: pd.DataFrame):
    pivot = (
        deals.pivot_table(
            index="Deal Stage",
            columns="Deal Status",
            values="Deal Name",
            aggfunc="count",
            fill_value=0,
        )
        .sort_values(by=list(deals["Deal Status"].dropna().unique())[:1], ascending=False)
        .head(10)
    )
    return {"stage_status_table": pivot.to_dict()}


def sector_performance(deals: pd.DataFrame, work_orders: pd.DataFrame):
    deals_sector = deals.groupby("Sector/service", dropna=False)["Deal Name"].count().rename("deal_count")
    won_sector = (
        deals[deals["Deal Status"] == "Won"]
        .groupby("Sector/service")["Deal Name"]
        .count()
        .rename("won_count")
    )
    wo_sector = work_orders.groupby("Sector", dropna=False)["Deal name masked"].count().rename("work_order_count")

    merged = pd.concat([deals_sector, won_sector, wo_sector], axis=1).fillna(0)
    merged["win_rate"] = (merged["won_count"] / merged["deal_count"].replace(0, pd.NA)).fillna(0)
    merged = merged.sort_values("deal_count", ascending=False).head(10)
    return {"sector_metrics": merged.reset_index().to_dict(orient="records")}


def conversion_metrics(deals: pd.DataFrame):
    total = max(len(deals), 1)
    won = int((deals["Deal Status"] == "Won").sum())
    dead = int((deals["Deal Status"] == "Dead").sum())
    open_ = int((deals["Deal Status"] == "Open").sum())
    return {
        "won_count": won,
        "dead_count": dead,
        "open_count": open_,
        "won_rate": won / total,
        "dead_rate": dead / total,
        "open_rate": open_ / total,
        "total_deals": len(deals),
    }


def receivable_risk(work_orders: pd.DataFrame):
    col = "Amount Receivable (Masked)"
    if col not in work_orders.columns:
        return {"negative_rows": 0, "high_outstanding_rows": 0, "threshold": None}

    s = _num(work_orders[col]).fillna(0)
    threshold = float(s.quantile(0.9)) if len(s) else 0.0
    return {
        "negative_rows": int((s < 0).sum()),
        "high_outstanding_rows": int((s >= threshold).sum()),
        "threshold": threshold,
        "total_outstanding": float(s.sum()),
    }

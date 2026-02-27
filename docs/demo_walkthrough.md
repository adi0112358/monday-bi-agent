# Monday Go-Live Checklist (6-Hour Runbook)

## T-0: Start Timer
- [ ] Start assignment timer.
- [ ] Confirm local app works in `DATA_BACKEND=local`.

## 1) Monday Setup
- [ ] Log in to monday.com workspace.
- [ ] Create/import **Deals** board from `data/cleaned/Deal_funnel_Data.cleaned.csv`.
- [ ] Create/import **Work Orders** board from `data/cleaned/Work_Order_Tracker_Data.cleaned.csv`.
- [ ] Verify key columns exist after import.

## 2) Credentials + Env
- [ ] Generate monday API token.
- [ ] Update `.env`:
  - [ ] `MONDAY_API_TOKEN=...`
  - [ ] `MONDAY_API_URL=https://api.monday.com/v2`
  - [ ] `MONDAY_DEALS_BOARD_ID=...`
  - [ ] `MONDAY_WORK_ORDERS_BOARD_ID=...`
- [ ] Keep `DATA_BACKEND=local` until mapping is ready.

## 3) Probe Boards (Column Mapping)
- [ ] Run:
```bash
python3 scripts/probe_monday_boards.py

# Monday BI Agent

Founder-facing BI assistant that answers business questions using Deals and Work Orders data, with clarification prompts, data-quality caveats, and visible tool/API trace.

## Features
- Conversational query handling for pipeline, conversion, sector performance, and receivable risk
- LLM-based intent parsing and clarification detection (with deterministic fallback)
- Clarification flow when timeframe is missing
- Cross-board analysis using `Deal Name` <-> `Deal name masked`
- Visible execution trace per query (`intent_parse`, data fetch, analytics)
- Backend switch:
  - `DATA_BACKEND=local` for local development
  - `DATA_BACKEND=monday` for live monday API mode

## Tech Stack
- Python
- Streamlit
- Pandas
- Requests (Monday GraphQL API)
- python-dotenv

## Project Structure
- `app/agent/`: orchestration and intent routing
- `app/tools/`: local/monday data tools and trace utilities
- `app/services/`: analytics functions
- `data/cleaned/`: cleaned datasets
- `data/reports/`: anomaly reports
- `scripts/`: cleaning, validation, board probe, sample queries

## Setup
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Environment
Create `.env` in project root:

```env
DATA_BACKEND=local
GEMINI_API_KEY=your_gemini_key_here
GEMINI_MODEL=gemini-2.0-flash
MONDAY_API_TOKEN=your_token_here
MONDAY_API_URL=https://api.monday.com/v2
MONDAY_DEALS_BOARD_ID=123456789
MONDAY_WORK_ORDERS_BOARD_ID=987654321
DEALS_CSV=data/cleaned/Deal_funnel_Data.cleaned.csv
WO_CSV=data/cleaned/Work_Order_Tracker_Data.cleaned.csv
```

## Run
```bash
export PYTHONPATH=.
streamlit run app/main.py
```

## Data Processing
```bash
python3 scripts/clean_deals.py
python3 scripts/clean_work_orders.py
python3 scripts/validate_data.py
```

Generated outputs:
- `data/cleaned/Deal_funnel_Data.cleaned.csv`
- `data/cleaned/Work_Order_Tracker_Data.cleaned.csv`
- `data/reports/Deal_funnel_Data.anomaly_report.csv`
- `data/reports/Work_Order_Tracker_Data.anomaly_report.csv`

## Local Validation Results

### Dataset Validation (`scripts/validate_data.py`)
- Deals rows: `344`
- Work orders rows: `176`
- Deals exact duplicates: `0`
- Work orders exact duplicates: `0`
- Deal status values: valid
- Closure probability values: valid
- Deals numeric parse rate (`Masked Deal value`): `48.0%`
- Deals date parse rates:
  - `Close Date (A)`: `7.6%`
  - `Tentative Close Date`: `78.5%`
  - `Created Date`: `99.7%`
- Work orders negative receivables: `11`
- Cross-board overlap (`Deal Name` <-> `Deal name masked`): `52`
- Anomaly rows:
  - Deals: `41`
  - Work Orders: `29`

### Query Validation (Local Mode)
1. **Q:** How is our pipeline in renewables this quarter?  
   **A:** Pipeline for Renewables has 111 deals: 54 won, 8 open, and 48 dead.  
   **Trace:** `intent_parse`, `get_deals`, `get_work_orders`, `analytics_compute`

2. **Q:** How is our pipeline in renewables?  
   **A:** Which timeframe should I use (this quarter, last quarter, this month, or all-time)?  
   **Trace:** `intent_parse`, `clarification`

3. **Q:** Show receivable risk in mining this month  
   **A:** Receivable risk for Mining shows 10 negative receivable rows and 10 high-outstanding rows.  
   **Trace:** `intent_parse`, `get_deals`, `get_work_orders`, `analytics_compute`

4. **Q:** What is our conversion rate in railways all-time?  
   **A:** Conversion for Railways: win rate 40.0%, dead rate 27.5%, open rate 32.5%.  
   **Trace:** `intent_parse`, `get_deals`, `get_work_orders`, `analytics_compute`

5. **Q:** How is sector performance all-time?  
   **A:** Sector performance across all sectors is computed from deals and work orders. Top sector by deal volume: Renewables.  
   **Trace:** `intent_parse`, `get_deals`, `get_work_orders`, `analytics_compute`

## monday Live Mode (When Ready)
1. Import cleaned CSVs into monday boards.
2. Set real token and board IDs in `.env`.
3. Set `DATA_BACKEND=monday`.
4. Run `python3 scripts/probe_monday_boards.py` and map column IDs.
5. Restart app and validate sample queries.

## Demo Queries
Use `sample_queries.md`.

## Known Caveats
- Some deal fields are sparse (especially close date and deal value).
- Some work-order columns are mostly empty by source design.
- Client codes are not reliable as a cross-board join key in provided data.
- LLM parser includes deterministic fallback. If provider quota/rate limits are hit, the app falls back to rule-based intent parsing.

## Submission Links
- Hosted app: `<add_link>`
- monday Deals board: `<add_link>`
- monday Work Orders board: `<add_link>`

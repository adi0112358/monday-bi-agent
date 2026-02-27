# Decision Log: Monday BI Agent

## 1) Objective and Constraints
The objective is to build a founder-facing BI agent that answers business questions across Deals and Work Orders data in monday.com.  
Key assignment constraints:
- Live monday calls at query time
- No preloading/cache in live mode
- Visible tool/action trace
- Resilience to messy data
- Conversational behavior with clarification when needed

Given these constraints and a 6-hour implementation window, the design prioritizes reliability, transparency, and fast iteration.

## 2) Architecture Decision
Chosen architecture:
- **UI**: Streamlit conversational interface
- **Orchestration**: Intent + clarification + analytics routing in `orchestrator.py`
- **Data access layer**: tool abstraction with backend switch
  - `local` backend for offline development/testing
  - `monday` backend for live production behavior
- **Analytics layer**: deterministic metric functions in `analytics.py`
- **Trace layer**: explicit per-step action trace in `trace.py`

Why this architecture:
- Keeps logic modular and testable.
- Enables local build-out before timed live integration.
- Makes monday integration a backend swap, not a rewrite.

## 3) Data Strategy and Resilience
The source data contained anomalies (embedded header rows, missing values, typo variants, sparse columns, negative receivables, mixed date formats).  
To handle this robustly:
- Built cleaning scripts for Deals and Work Orders.
- Produced anomaly reports for auditability.
- Normalized dates, numerics, and categorical values.
- Preserved caveats in response output instead of hiding uncertainty.

Outputs:
- Cleaned datasets for development
- Row-level anomaly reports for traceability

Rationale:
- Prevents brittle analytics logic.
- Supports transparent communication of data quality issues.

## 4) Query Understanding and Conversation Design
The query flow uses intent routing with clarification-first behavior:
- Intent buckets: pipeline, receivables, conversion, sector performance, overview
- Sector extraction from query text
- Clarification prompt when timeframe is missing for business-summary questions

Why:
- Founder questions are often ambiguous (“How is our pipeline?”).
- Clarification reduces incorrect assumptions.
- This is more reliable than silently defaulting to a timeframe.

## 5) Business Logic and Cross-Board Analysis
Core metrics implemented:
- Pipeline status/stage summaries
- Conversion rates (won/dead/open)
- Receivable risk (negative rows, high-outstanding threshold)
- Sector-level performance
- Cross-board overlap

Cross-board linkage:
- Primary join signal: `Deal Name` ↔ `Deal name masked`
- Secondary context: sector and owner
- Not used as primary join: client codes (non-overlapping in provided data)

Why:
- Name overlap is the strongest practical bridge between the two source sets.
- Keeps cross-board outputs defensible under assignment time constraints.

## 6) Live Monday Integration Approach
The codebase is structured for live mode with environment-controlled backend:
- `DATA_BACKEND=local` during pre-timer build
- `DATA_BACKEND=monday` for assignment run

Monday integration design:
- GraphQL client wrapper
- Board-based fetch per query (no cache)
- Column-id mapping to canonical analytics schema
- Trace records for each data fetch step

Why:
- Satisfies “live query-time API calls” requirement.
- Preserves one analytics/orchestration path for both backends.

## 7) Trace Visibility Decision
Every query emits a visible trace block with steps like:
- `intent_parse`
- `get_deals`
- `get_work_orders`
- `analytics_compute`
(or `clarification`)

Why:
- Meets explicit evaluator requirement for action visibility.
- Improves trust and debugging.

## 8) Tradeoffs
Intentional tradeoffs under time constraints:
- Prioritized deterministic analytics over complex LLM-heavy reasoning.
- Prioritized robust caveats over aggressive imputation.
- Focused on reliable core metrics rather than deep forecasting.
- Implemented lightweight conversational memory first; deeper multi-turn context is extensible.

## 9) Limitations and Mitigations
Limitations:
- Some fields are sparse (e.g., close dates, masked values).
- Certain work-order columns are mostly empty by source design.
- Column-id mapping must be configured after monday board import.
- Name-based cross-board linkage may not capture all relationships.

Mitigations:
- Explicit caveats in every response.
- Anomaly reports included.
- Clear mapping/probe workflow for monday columns.
- Modular structure allows rapid iteration of mapping and logic.

## 10) Outcome
The solution is designed to be:
- Transparent (visible trace + caveats)
- Resilient (cleaning + anomaly handling)
- Practical (modular backend switch)
- Assignment-aligned (live monday mode, no-cache behavior, conversational query handling)

from app.tools.trace import Tracer
from app.tools.deals_tool import get_deals
from app.tools.work_orders_tool import get_work_orders
from app.agent.llm_router import DEFAULT_CLARIFICATION, parse_query_with_llm
from app.services.analytics import (
    pipeline_summary,
    receivable_summary,
    cross_board_overlap,
    pipeline_by_stage_status,
    sector_performance,
    conversion_metrics,
    receivable_risk,
)

SECTORS = ["mining", "renewables", "railways", "powerline", "construction", "others"]
TIME_HINTS = ["this quarter", "last quarter", "this month", "last month", "this year", "last year", "all-time", "q1", "q2", "q3", "q4"]


def _extract_sector(q):
    for s in SECTORS:
        if s in q:
            return s
    return None


def _detect_intent(q):
    if any(k in q for k in ["receivable", "collection", "outstanding", "accounts receivable"]):
        return "receivables"
    if any(k in q for k in ["conversion", "won rate", "win rate", "dead rate"]):
        return "conversion"
    if any(k in q for k in ["sector", "industry", "segment"]):
        return "sector_performance"
    if any(k in q for k in ["stage", "pipeline"]):
        return "pipeline"
    return "overview"


def _needs_time_clarification(q):
    business = any(k in q for k in ["pipeline", "revenue", "sector", "performance", "receivable", "deals", "conversion"])
    has_time = any(t in q for t in TIME_HINTS)
    return business and not has_time


def _scope_text(sector):
    return f"for {sector.title()}" if sector else "across all sectors"


def _append_plain_caveat(text):
    return f"{text} Note: results may be affected by missing or inconsistent source data."


def _parse_query(question: str, tracer: Tracer):
    q = question.lower()
    # Try LLM parse first. If it fails, fallback to deterministic parser.
    try:
        parsed = parse_query_with_llm(question)
        intent = parsed["intent"]
        sector = parsed["sector"]
        needs_clarification = parsed["needs_clarification"]
        clarification_question = parsed["clarification_question"] or DEFAULT_CLARIFICATION
        tracer.add(
            "llm_intent_parse",
            f"intent={intent}, sector={sector}, timeframe={parsed.get('timeframe')}",
            rows=0,
            ms=0,
        )
        return {
            "source": "llm",
            "intent": intent,
            "sector": sector,
            "needs_clarification": needs_clarification,
            "clarification_question": clarification_question,
        }
    except Exception as exc:
        intent = _detect_intent(q)
        sector = _extract_sector(q)
        needs_clarification = _needs_time_clarification(q)
        tracer.add(
            "intent_parse_fallback",
            f"intent={intent}, sector={sector}, reason={exc}",
            rows=0,
            ms=0,
        )
        return {
            "source": "rules",
            "intent": intent,
            "sector": sector,
            "needs_clarification": needs_clarification,
            "clarification_question": DEFAULT_CLARIFICATION,
        }


def answer_question(question: str):
    tracer = Tracer()
    parsed = _parse_query(question, tracer)
    intent = parsed["intent"]
    sector = parsed["sector"]

    if parsed["needs_clarification"]:
        tracer.add("clarification", "Missing timeframe for business question", rows=0, ms=0)
        return {
            "clarification_needed": True,
            "question": parsed["clarification_question"],
            "caveats": ["I can answer now, but timeframe assumptions may be wrong."],
        }, tracer.dump()

    try:
        deals = get_deals(tracer, sector=sector)
        wos = get_work_orders(tracer, sector=sector)
    except Exception as exc:
        tracer.add("error", f"data_fetch_failed: {exc}", rows=0, ms=0)
        return {
            "clarification_needed": False,
            "error": "Data fetch failed. Check monday token, board IDs, and column mappings.",
            "details": str(exc),
            "next_question_suggestion": "Try local mode or verify monday configuration and retry.",
            "caveats": [
                "No analytics were computed because live data access failed.",
            ],
        }, tracer.dump()

    pipe = pipeline_summary(deals)
    recv = receivable_summary(wos)
    overlap = cross_board_overlap(deals, wos)

    # intent-specific block
    if intent == "pipeline":
        details = pipeline_by_stage_status(deals)
        status = pipe["by_status"]
        won = int(status.get("Won", 0))
        open_ = int(status.get("Open", 0))
        dead = int(status.get("Dead", 0))
        final_answer = (
            f"Pipeline {_scope_text(sector)} has {pipe['rows']} deals: "
            f"{won} won, {open_} open, and {dead} dead."
        )
    elif intent == "sector_performance":
        details = sector_performance(deals, wos)
        top = details["sector_metrics"][0]["index"] if details.get("sector_metrics") else "N/A"
        final_answer = (
            f"Sector performance {_scope_text(sector)} is computed from deals and work orders. "
            f"Top sector by deal volume: {top}."
        )
    elif intent == "conversion":
        details = conversion_metrics(deals)
        final_answer = (
            f"Conversion {_scope_text(sector)}: "
            f"win rate {details['won_rate']:.1%}, dead rate {details['dead_rate']:.1%}, "
            f"open rate {details['open_rate']:.1%}."
        )
    elif intent == "receivables":
        details = receivable_risk(wos)
        final_answer = (
            f"Receivable risk {_scope_text(sector)} shows "
            f"{details['negative_rows']} negative receivable rows and "
            f"{details['high_outstanding_rows']} high-outstanding rows."
        )
    else:
        details = {
            "pipeline": pipeline_by_stage_status(deals),
            "conversion": conversion_metrics(deals),
            "receivable_risk": receivable_risk(wos),
            "sector_performance": sector_performance(deals, wos),
        }
        final_answer = (
            f"Overview {_scope_text(sector)}: {pipe['rows']} deals, "
            f"total receivables {recv['total_receivable']:.2f}, and "
            f"{overlap['overlap_count']} cross-board linked deals."
        )

    final_answer = _append_plain_caveat(final_answer)

    tracer.add("analytics_compute", f"intent={intent}", rows=0, ms=0)

    answer = {
        "clarification_needed": False,
        "intent_parser_source": parsed["source"],
        "intent": intent,
        "final_answer": final_answer,
        "summary": f"Analyzed {pipe['rows']} deals and {len(wos)} work orders" + (f" for {sector.title()}" if sector else ""),
        "key_metrics": {
            "pipeline_rows": pipe["rows"],
            "negative_receivables": recv["negative_count"],
            "cross_board_overlap": overlap["overlap_count"],
        },
        "details": details,
        "pipeline": pipe,
        "receivables": recv,
        "cross_board": overlap,
        "caveats": [
            "Data includes missing values and flagged anomalies.",
            "Close dates and deal values are sparse in parts of deals data.",
        ],
        "next_question_suggestion": "Do you want this split by owner or by deal stage?",
    }
    return answer, tracer.dump()

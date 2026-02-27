import json
from typing import Any

import requests

from app.config import GEMINI_API_KEY, GEMINI_MODEL

ALLOWED_INTENTS = {
    "pipeline",
    "receivables",
    "conversion",
    "sector_performance",
    "overview",
}

ALLOWED_SECTORS = {
    "mining",
    "renewables",
    "railways",
    "powerline",
    "construction",
    "others",
    None,
}

DEFAULT_CLARIFICATION = "Which timeframe should I use (this quarter, last quarter, this month, or all-time)?"


def _validate_payload(payload: dict[str, Any]) -> dict[str, Any]:
    intent = str(payload.get("intent", "overview")).strip()
    if intent not in ALLOWED_INTENTS:
        intent = "overview"

    sector = payload.get("sector")
    if sector is not None:
        sector = str(sector).strip().lower()
    if sector not in ALLOWED_SECTORS:
        sector = None

    timeframe = payload.get("timeframe")
    if timeframe is not None:
        timeframe = str(timeframe).strip() or None

    needs_clarification = bool(payload.get("needs_clarification", False))
    clarification_question = str(payload.get("clarification_question") or DEFAULT_CLARIFICATION).strip()

    return {
        "intent": intent,
        "sector": sector,
        "timeframe": timeframe,
        "needs_clarification": needs_clarification,
        "clarification_question": clarification_question or DEFAULT_CLARIFICATION,
    }


def parse_query_with_llm(question: str) -> dict[str, Any]:
    if not GEMINI_API_KEY:
        raise RuntimeError("GEMINI_API_KEY is not configured")

    system = (
        "You are an intent parser for a BI agent. Return ONLY valid JSON with keys: "
        "intent, sector, timeframe, needs_clarification, clarification_question. "
        "intent must be one of: pipeline, receivables, conversion, sector_performance, overview. "
        "sector must be one of: mining, renewables, railways, powerline, construction, others, or null. "
        "If timeframe is missing for a business summary question, set needs_clarification=true "
        "and provide a short clarification_question."
    )

    user = f"Question: {question}"

    url = (
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
    )

    payload = {
        "system_instruction": {"parts": [{"text": system}]},
        "contents": [{"role": "user", "parts": [{"text": user}]}],
        "generationConfig": {
            "temperature": 0,
            "responseMimeType": "application/json",
        },
    }

    resp = requests.post(url, json=payload, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    try:
        text = data["candidates"][0]["content"]["parts"][0]["text"]
    except Exception as exc:
        raise RuntimeError(f"Unexpected Gemini response: {data}") from exc

    parsed = json.loads(text)
    if not isinstance(parsed, dict):
        raise RuntimeError("Gemini returned non-dict payload")
    return _validate_payload(parsed)

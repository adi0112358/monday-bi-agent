import requests
from app.config import MONDAY_API_TOKEN, MONDAY_API_URL


def run_monday_query(query: str, variables: dict | None = None) -> dict:
    if not MONDAY_API_TOKEN:
        raise RuntimeError("MONDAY_API_TOKEN is not set")

    resp = requests.post(
        MONDAY_API_URL,
        json={"query": query, "variables": variables or {}},
        headers={
            "Authorization": MONDAY_API_TOKEN,
            "Content-Type": "application/json",
        },
        timeout=30,
    )
    resp.raise_for_status()
    payload = resp.json()
    if "errors" in payload:
        raise RuntimeError(f"Monday API errors: {payload['errors']}")
    return payload.get("data", {})


def fetch_board_items(board_id: str, limit: int = 500) -> list[dict]:
    if not board_id:
        raise RuntimeError("Board ID is not configured")

    items: list[dict] = []
    cursor = None

    first_page_query = """
    query ($board_id: ID!, $limit: Int!) {
      boards(ids: [$board_id]) {
        items_page(limit: $limit) {
          cursor
          items {
            id
            name
            column_values {
              id
              text
            }
          }
        }
      }
    }
    """

    next_page_query = """
    query ($cursor: String!, $limit: Int!) {
      next_items_page(cursor: $cursor, limit: $limit) {
        cursor
        items {
          id
          name
          column_values {
            id
            text
          }
        }
      }
    }
    """

    data = run_monday_query(
        first_page_query,
        {"board_id": str(board_id), "limit": limit},
    )
    boards = data.get("boards", [])
    if not boards:
        return items

    page = boards[0].get("items_page", {})
    items.extend(page.get("items", []))
    cursor = page.get("cursor")

    while cursor:
        next_data = run_monday_query(
            next_page_query,
            {"cursor": cursor, "limit": limit},
        )
        next_page = next_data.get("next_items_page", {})
        items.extend(next_page.get("items", []))
        cursor = next_page.get("cursor")

    return items

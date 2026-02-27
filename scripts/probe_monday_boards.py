import os
import requests

from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")


load_dotenv()

API_URL = os.getenv("MONDAY_API_URL", "https://api.monday.com/v2")
TOKEN = os.getenv("MONDAY_API_TOKEN")
DEALS_BOARD_ID = os.getenv("MONDAY_DEALS_BOARD_ID")
WO_BOARD_ID = os.getenv("MONDAY_WORK_ORDERS_BOARD_ID")

if not TOKEN:
    raise SystemExit("MONDAY_API_TOKEN missing in .env")
if not DEALS_BOARD_ID or not WO_BOARD_ID:
    raise SystemExit("MONDAY_DEALS_BOARD_ID or MONDAY_WORK_ORDERS_BOARD_ID missing in .env")

query = """
query ($board_ids: [ID!]) {
  boards(ids: $board_ids) {
    id
    name
    columns {
      id
      title
      type
    }
    items_page(limit: 3) {
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

variables = {"board_ids": [str(DEALS_BOARD_ID), str(WO_BOARD_ID)]}

resp = requests.post(
    API_URL,
    json={"query": query, "variables": variables},
    headers={"Authorization": TOKEN, "Content-Type": "application/json"},
    timeout=30,
)
resp.raise_for_status()
payload = resp.json()

if "errors" in payload:
    print("GraphQL errors:", payload["errors"])
    raise SystemExit(1)

for b in payload["data"]["boards"]:
    print("\n" + "=" * 80)
    print(f"Board: {b['name']} (id={b['id']})")
    print("- Columns:")
    for c in b["columns"]:
        print(f"  {c['id']:<24} | {c['type']:<12} | {c['title']}")
    print("- Sample items:")
    for it in b["items_page"]["items"]:
        print(f"  Item: {it['name']} (id={it['id']})")
        for cv in it["column_values"][:8]:
            print(f"    {cv['id']:<24} -> {cv.get('text')}")

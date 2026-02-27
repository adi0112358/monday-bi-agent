import os
try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    def load_dotenv():
        return None

load_dotenv()

DATA_BACKEND = os.getenv("DATA_BACKEND", "local")
DEALS_CSV = os.getenv("DEALS_CSV", "data/cleaned/Deal_funnel_Data.cleaned.csv")
WO_CSV = os.getenv("WO_CSV", "data/cleaned/Work_Order_Tracker_Data.cleaned.csv")
MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN", "")
MONDAY_API_URL = os.getenv("MONDAY_API_URL", "https://api.monday.com/v2")
MONDAY_DEALS_BOARD_ID = os.getenv("MONDAY_DEALS_BOARD_ID", "")
MONDAY_WORK_ORDERS_BOARD_ID = os.getenv("MONDAY_WORK_ORDERS_BOARD_ID", "")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")


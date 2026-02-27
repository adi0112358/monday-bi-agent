import sys
import traceback
from pathlib import Path

import streamlit as st

# Ensure repo root is importable when Streamlit runs app/main.py directly.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

st.set_page_config(page_title="Monday BI Agent", layout="wide")
try:
    from app.config import DATA_BACKEND
except Exception:
    DATA_BACKEND = "local"

mode = "Live Monday Mode" if DATA_BACKEND == "monday" else "Local Mode"
st.title(f"Monday BI Agent ({mode})")

try:
    from app.agent.orchestrator import answer_question
except Exception as exc:
    st.error("Startup import failed. See traceback below.")
    st.code("".join(traceback.format_exception(exc)))
    st.stop()

q = st.text_input("Ask a founder-level question", "How is our pipeline in renewables?")
if st.button("Run"):
    try:
        ans, trace = answer_question(q)
    except Exception as exc:
        st.error("Query execution failed. See traceback below.")
        st.code("".join(traceback.format_exception(exc)))
        st.stop()

    st.subheader("Answer")
    if ans.get("clarification_needed"):
        st.warning(ans.get("question", "Please clarify your request."))
    else:
        st.markdown(f"**{ans.get('final_answer', '')}**")
    st.json(ans)
    with st.expander("Tool/API Trace", expanded=True):
        st.json(trace)

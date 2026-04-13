"""
Chat Interface Component for UFDR Analysis Tool.
Provides a ChatGPT-style conversational UI with:
- Streaming responses
- Table rendering for structured data (contacts, calls, messages)
- Rich media evidence cards for unstructured data
"""

import streamlit as st
import pandas as pd
import time
from datetime import datetime
from itertools import islice

# Import backend modules
try:
    from rag.query_engine import QueryEngine
except ImportError:
    QueryEngine = None


@st.cache_resource(show_spinner="Loading AI Models...")
def get_query_engine():
    """Cached loader for the Query Engine to avoid reloading models on every rerun."""
    if not QueryEngine:
        return None
    return QueryEngine()


# ──────────────────────────────────────────────────
#  Table Rendering for Structured Browse Results
# ──────────────────────────────────────────────────

def _render_as_table(citations: list[dict], query_type: str) -> bool:
    """
    If the data is structured (browse results), render as a Pandas DataFrame.
    Returns True if a table was rendered, False otherwise.
    """
    if query_type != "browse" or not citations:
        return False

    data_type = citations[0].get("data_type", "")

    # Define display columns per data type
    column_map = {
        "contact": {
            "Name": "name",
            "Phone": "phone",
            "Email": "email",
        },
        "message": {
            "From": "sender_raw",
            "To": "receiver_raw",
            "Message": "text",
            "Time": "timestamp",
            "App": "app",
        },
        "call": {
            "Caller": "caller_raw",
            "Receiver": "receiver_raw",
            "Duration (s)": "duration_seconds",
            "Time": "timestamp",
            "Direction": "direction",
        },
        "media": {
            "Filename": "filename",
            "Type": "media_type",
            "Size": "file_size",
            "Time": "timestamp",
        },
        "location": {
            "Latitude": "latitude",
            "Longitude": "longitude",
            "Time": "timestamp",
            "Accuracy": "accuracy",
        },
    }

    col_config = column_map.get(data_type)
    if not col_config:
        return False

    rows = []
    for c in citations:
        meta = c.get("metadata", {})
        row = {}
        for display_name, db_col in col_config.items():
            val = meta.get(db_col, "")
            # Truncate long text
            if isinstance(val, str) and len(val) > 120:
                val = val[:117] + "..."  # type: ignore[index]
            row[display_name] = val
        rows.append(row)

    if not rows:
        return False

    df = pd.DataFrame(rows)

    # Style the table
    st.caption(f"📊 Found **{len(df)}** {data_type}s")
    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        height=min(400, 35 * len(df) + 38),  # Auto-height, max 400px
    )
    return True


# ──────────────────────────────────────────────────
#  Evidence Card Rendering (for semantic/exact results)
# ──────────────────────────────────────────────────

def render_evidence_card(citation: dict):
    """Render a single evidence citation as a compact card."""
    data_type = citation.get("data_type", "unknown")
    text = citation.get("text", "")
    meta = citation.get("metadata", {})
    case_id = citation.get("case_id", meta.get("case_id", ""))

    icons = {
        "message": "💬", "contact": "👤", "call": "📞",
        "media": "🖼️", "location": "📍", "video_transcript": "🎥",
        "statistics": "📊",
    }
    icon = icons.get(data_type, "📄")

    with st.expander(f"{icon} {text[:100]}{'…' if len(text) > 100 else ''}", expanded=False):
        st.markdown(text)
        # Show key metadata
        useful_meta = {
            k: v for k, v in meta.items()
            if v and k not in ("data_type", "case_id", "source") and str(v).strip()
        }
        if useful_meta:
            cols = st.columns(min(len(useful_meta), 3))
            for i, (k, v) in enumerate(islice(useful_meta.items(), 6)):
                with cols[i % len(cols)]:
                    st.caption(f"**{k.replace('_', ' ').title()}**: {v}")
        st.caption(f"🏷️ Type: `{data_type}` · Case: `{case_id}`")


# ──────────────────────────────────────────────────
#  Visualization Rendering
# ──────────────────────────────────────────────────

def _render_chart(citations: list[dict], data_type: str):
    """Render a time-series chart for messages/calls."""
    if not citations:
        return

    # Extract timestamps
    timestamps = []
    for c in citations:
        ts = c.get("metadata", {}).get("timestamp", "")
        if ts:
            try:
                # Try parsing ISO format
                dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
                timestamps.append(dt)
            except ValueError:
                pass

    if not timestamps:
        return

    df = pd.DataFrame({"Time": timestamps})
    df["Count"] = 1

    # Resample by hour or day depending on range
    if (max(timestamps) - min(timestamps)).days > 2:
        resampled = df.set_index("Time").resample("d").count().reset_index()
        x_label = "Date"
    else:
        resampled = df.set_index("Time").resample("h").count().reset_index()
        x_label = "Time (Hourly)"

    st.caption(f"📈 {data_type.title()} Activity Over Time")
    st.line_chart(resampled, x="Time", y="Count", color="#2ecc71")


def _render_map(citations: list[dict]):
    """Render a map for location data."""
    locations = []
    for c in citations:
        meta = c.get("metadata", {})
        try:
            lat = float(meta.get("latitude", 0))
            lon = float(meta.get("longitude", 0))
            if lat != 0 and lon != 0:
                locations.append({"lat": lat, "lon": lon})
        except (ValueError, TypeError):
            continue

    if locations:
        st.caption(f"📍 Found {len(locations)} locations")
        st.map(pd.DataFrame(locations), size=20, color="#e74c3c")


# ──────────────────────────────────────────────────
#  Message Display
# ──────────────────────────────────────────────────

def display_chat_message(role: str, content: str, citations: list | None = None, query_type: str = ""):
    """Render a single chat message with citations and visualizations."""
    avatar = "👤" if role == "user" else "🤖"
    with st.chat_message(role, avatar=avatar):
        st.markdown(content)

        if citations and role == "assistant":
            st.markdown("---")

            # Determine dominant data type
            data_types = [c.get("data_type", "unknown") for c in citations]
            if data_types:
                dominant_type = max(set(data_types), key=data_types.count)
            else:
                dominant_type = "unknown"

            # 1. Timeline Chart (for messages/calls)
            if dominant_type in ("message", "call"):
                _render_chart(citations, dominant_type)

            # 2. Map (for locations)
            if dominant_type == "location" or any(c.get("data_type") == "location" for c in citations):
                _render_map(
                    [c for c in citations if c.get("data_type") == "location"])

            # 3. Data Table (Structured View)
            # Only show table if we have structured data and it hasn't been rendered as a map only
            if dominant_type in ("contact", "message", "call", "location", "media"):
                with st.expander(f"📊 View {dominant_type.title()} Data Table", expanded=True):
                    # Filter citations to ensure table consistency
                    table_citations = [c for c in citations if c.get(
                        "data_type") == dominant_type]
                    # force table render logic
                    _render_as_table(table_citations, "browse")

            # 4. Evidence Cards (Detailed View)
            with st.expander(f"📋 {len(citations)} Supporting Trace Items", expanded=False):
                for c in islice(citations, 15):
                    render_evidence_card(c)
                if len(citations) > 15:
                    st.caption(f"*...and {len(citations) - 15} more items*")


# ──────────────────────────────────────────────────
#  Main Chat Loop
# ──────────────────────────────────────────────────

def render_chat_interface(selected_cases: list[str]):
    """Main chat interface render loop."""

    # Initialize chat history
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Display empty state if no messages
    if not st.session_state.messages:
        st.info(
            "👋 Welcome to Unified Search! You can ask questions about the case evidence.")
        st.markdown("**Example queries:**")
        st.markdown("- *Who was in contact with the suspect?*")
        st.markdown("- *Show me the latest messages.*")
        st.markdown("- *Find location data near Central Park.*")

    # Display existing chat history
    for message in st.session_state.messages:
        display_chat_message(
            message["role"],
            message["content"],
            message.get("citations"),
            message.get("query_type", ""),
        )

    # Chat Input (pinned to bottom by Streamlit)
    if prompt := st.chat_input("Ask about the case evidence…"):
        # 1. Show user message
        st.session_state.messages.append({"role": "user", "content": prompt})
        display_chat_message("user", prompt)

        # 2. Generate AI response
        with st.chat_message("assistant", avatar="🤖"):
            placeholder = st.empty()
            placeholder.markdown("🔍 *Searching forensic data…*")

            try:
                engine = get_query_engine()
                if not engine:
                    raise ImportError(
                        "RAG Engine not loaded. Check dependencies.")

                result = engine.query(
                    query_text=prompt,
                    case_ids=selected_cases,
                    n_results=50,
                    use_llm=True,
                )

                answer = result.get("answer", "")
                citations = result.get("citations", [])
                query_type = result.get("query_type", "semantic")

                # Display answer with typing effect
                display_text = answer if answer else "Here are the results I found:"
                full_response = ""
                for chunk in display_text.split():
                    full_response += chunk + " "
                    time.sleep(0.015)
                    placeholder.markdown(full_response + "▌")
                placeholder.markdown(full_response)

                # Save to history
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": full_response,
                    "citations": citations,
                    "query_type": query_type,
                })

                # Rerun to render citations properly from cached session state
                st.rerun()

            except Exception as e:
                placeholder.error(f"Error: {str(e)}")

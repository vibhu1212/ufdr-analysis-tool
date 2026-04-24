## 2024-04-05 - Streamlit Button Alignment & Tooltips
**Learning:** Using invalid kwargs like `width="stretch"` in Streamlit `st.button` fails silently, leaving buttons unaligned and poorly structured. In addition, providing tooltips (`help` parameter) is crucial for primary action buttons to improve user confidence and accessibility.
**Action:** Always use `use_container_width=True` for column-filling buttons and include `help` descriptions for UI accessibility and user guidance.
## 2024-03-24 - Initial Learnings\n**Learning:** This repo is a Streamlit application primarily, with some minimal React code in `frontend/src/`. It doesn't use `package.json` or standard node package managers at the root level.\n**Action:** Focus on Streamlit UI elements in Python, or the specific React file if applicable, but note that standard `pnpm` commands might not apply if it's not a standard node project.

## 2024-04-24 - Conversational UI Empty States
**Learning:** Chat interfaces and conversational components suffer from the "blank canvas" problem when `session_state.messages` is empty, leading to poor initial interaction UX as users don't know what types of queries are supported.
**Action:** Always provide an empty state (e.g., a welcome message with concrete example queries) when chat history is empty to guide users and improve initial interaction intuitiveness.

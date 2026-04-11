## 2024-04-05 - Streamlit Button Alignment & Tooltips
**Learning:** Using invalid kwargs like `width="stretch"` in Streamlit `st.button` fails silently, leaving buttons unaligned and poorly structured. In addition, providing tooltips (`help` parameter) is crucial for primary action buttons to improve user confidence and accessibility.
**Action:** Always use `use_container_width=True` for column-filling buttons and include `help` descriptions for UI accessibility and user guidance.
## 2024-03-24 - Initial Learnings\n**Learning:** This repo is a Streamlit application primarily, with some minimal React code in `frontend/src/`. It doesn't use `package.json` or standard node package managers at the root level.\n**Action:** Focus on Streamlit UI elements in Python, or the specific React file if applicable, but note that standard `pnpm` commands might not apply if it's not a standard node project.
## 2024-04-11 - Streamlit Chat Empty State
**Learning:** Conversational chat interfaces in Streamlit suffer from a 'blank canvas' problem when the chat history is initially empty, providing poor initial interaction UX.
**Action:** Always provide an empty state (e.g., a welcome message with example queries) when `st.session_state.messages` is empty to guide users on how to interact with the system.

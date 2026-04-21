## 2024-04-05 - Streamlit Button Alignment & Tooltips
**Learning:** Using invalid kwargs like `width="stretch"` in Streamlit `st.button` fails silently, leaving buttons unaligned and poorly structured. In addition, providing tooltips (`help` parameter) is crucial for primary action buttons to improve user confidence and accessibility.
**Action:** Always use `use_container_width=True` for column-filling buttons and include `help` descriptions for UI accessibility and user guidance.
## 2024-03-24 - Initial Learnings\n**Learning:** This repo is a Streamlit application primarily, with some minimal React code in `frontend/src/`. It doesn't use `package.json` or standard node package managers at the root level.\n**Action:** Focus on Streamlit UI elements in Python, or the specific React file if applicable, but note that standard `pnpm` commands might not apply if it's not a standard node project.

## 2024-05-23 - Empty States in Chat Interfaces
**Learning:** Streamlit chat interfaces can feel intimidating when empty. Adding a styled welcome message with example queries provides immediate guidance to users without requiring a separate "help" section. Hardcoded HTML works best for consistent styling across themes.
**Action:** Always include an `if not st.session_state.messages:` block in conversational UIs to show an empty state.

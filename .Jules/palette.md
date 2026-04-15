## 2024-04-05 - Streamlit Button Alignment & Tooltips
**Learning:** Using invalid kwargs like `width="stretch"` in Streamlit `st.button` fails silently, leaving buttons unaligned and poorly structured. In addition, providing tooltips (`help` parameter) is crucial for primary action buttons to improve user confidence and accessibility.
**Action:** Always use `use_container_width=True` for column-filling buttons and include `help` descriptions for UI accessibility and user guidance.
## 2024-03-24 - Initial Learnings\n**Learning:** This repo is a Streamlit application primarily, with some minimal React code in `frontend/src/`. It doesn't use `package.json` or standard node package managers at the root level.\n**Action:** Focus on Streamlit UI elements in Python, or the specific React file if applicable, but note that standard `pnpm` commands might not apply if it's not a standard node project.

## 2024-04-15 - Chat Interface Blank Canvas Problem
**Learning:** Conversational chat interfaces often face the "blank canvas" problem when chat history is empty, which can leave users unsure of what to ask or how to interact. Providing an empty state with a welcome message and example queries significantly improves the initial interaction UX and makes the app more intuitive.
**Action:** Always provide an empty state containing helpful examples and guidance when designing chat interfaces or any features that start with an empty query history.

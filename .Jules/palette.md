## 2024-04-05 - Streamlit Button Alignment & Tooltips
**Learning:** Using invalid kwargs like `width="stretch"` in Streamlit `st.button` fails silently, leaving buttons unaligned and poorly structured. In addition, providing tooltips (`help` parameter) is crucial for primary action buttons to improve user confidence and accessibility.
**Action:** Always use `use_container_width=True` for column-filling buttons and include `help` descriptions for UI accessibility and user guidance.
## 2024-03-24 - Initial Learnings\n**Learning:** This repo is a Streamlit application primarily, with some minimal React code in `frontend/src/`. It doesn't use `package.json` or standard node package managers at the root level.\n**Action:** Focus on Streamlit UI elements in Python, or the specific React file if applicable, but note that standard `pnpm` commands might not apply if it's not a standard node project.

## 2024-04-20 - [Conversational Interface Blank Canvas]
**Learning:** Empty states in conversational chat interfaces play a crucial role in providing users a starting point with examples, especially in forensic analysis tool interfaces, to avoid the 'blank canvas' problem. It should be prioritized to reduce cognitive load and display immediate examples on what they can ask about a case context.
**Action:** When implementing chat interfaces or updating Streamlit UI components involving chat history, always verify an initial empty state is provided to guide the user to their first action or prompt.

## 2024-04-05 - Streamlit Button Alignment & Tooltips
**Learning:** Using invalid kwargs like `width="stretch"` in Streamlit `st.button` fails silently, leaving buttons unaligned and poorly structured. In addition, providing tooltips (`help` parameter) is crucial for primary action buttons to improve user confidence and accessibility.
**Action:** Always use `use_container_width=True` for column-filling buttons and include `help` descriptions for UI accessibility and user guidance.
## 2024-03-24 - Initial Learnings\n**Learning:** This repo is a Streamlit application primarily, with some minimal React code in `frontend/src/`. It doesn't use `package.json` or standard node package managers at the root level.\n**Action:** Focus on Streamlit UI elements in Python, or the specific React file if applicable, but note that standard `pnpm` commands might not apply if it's not a standard node project.
## 2026-04-28 - Streamlit Empty States
**Learning:** By default, Streamlit components like chat interfaces present a confusing blank screen to users when no data or history is present.
**Action:** When implementing new features or components that rely on state, proactively add visually appealing empty states using `st.markdown` with inline styling to guide the user on how to start. Avoid using Streamlit CSS variables for custom injected HTML to prevent theme rendering issues.

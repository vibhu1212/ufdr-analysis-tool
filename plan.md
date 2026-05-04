1.  **Refactor `anomaly_detection_viz.py` unique contacts calculation**
    -   Replace the slow `lambda x: len(set(x['sender_digits']) | set(x['receiver_digits']))` with `pd.concat([all_comms[['date', 'sender_digits']].rename(columns={'sender_digits': 'contact'}), all_comms[['date', 'receiver_digits']].rename(columns={'receiver_digits': 'contact'})]).groupby('date')['contact'].nunique()` for faster performance using native pandas operations.
2.  **Refactor `graph_export.py` unique contacts calculation**
    -   Replace `lambda x: len(set(x))` in the `agg` dictionary with `'nunique'` string literal in `all_comms.groupby('date').agg(...)` to leverage pandas' native C-based unique counting mechanism.
3.  **Complete pre commit steps to ensure proper testing, verification, review, and reflection are done.**
4.  **Submit the PR**
    -   Submit the PR with title '⚡ Bolt: Optimize Pandas unique contact calculations' and a description detailing the performance improvement.

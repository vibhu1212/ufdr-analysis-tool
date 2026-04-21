## 2025-04-07 - [Optimize Database Batch Inserts]
**Learning:** During database ingestion in `ingest/database_writer.py`, iterating over records and firing off a `SELECT` statement per record to check for duplicates created a severe N+1 problem. This slowed down ingestion significantly.
**Action:** Replaced the N+1 `SELECT` statements with a batch pre-fetch strategy. Specifically, chunked records (e.g., 400 at a time) and used an `OR` chained query to load existing keys into memory for O(1) duplicate checking. This honors the SQLite parameter limit (<999) while radically reducing the number of round trips. Next time, always avoid N+1 database operations inside iteration blocks, especially during batch operations.

## 2026-04-21 - [Pandas DataFrame Row Iteration Performance Boost]
**Learning:** Replaced slow `df.iterrows()` Python-level loops with `df.itertuples(index=False)` in the visualization module for major performance speedups during large datasets row traversal. In situations calling for raw coordinate data matrices (like heatmaps), completely avoiding the loop with vectorized `df[['lat', 'lon']].values.tolist()` provided maximum efficiency and directly bypassed row creation completely.
**Action:** Always prefer `itertuples` over `iterrows` or vectorized extractions directly to basic data types where appropriate to significantly improve computation bounds on long lists.

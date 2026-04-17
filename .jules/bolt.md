## 2025-04-07 - [Optimize Database Batch Inserts]
**Learning:** During database ingestion in `ingest/database_writer.py`, iterating over records and firing off a `SELECT` statement per record to check for duplicates created a severe N+1 problem. This slowed down ingestion significantly.
**Action:** Replaced the N+1 `SELECT` statements with a batch pre-fetch strategy. Specifically, chunked records (e.g., 400 at a time) and used an `OR` chained query to load existing keys into memory for O(1) duplicate checking. This honors the SQLite parameter limit (<999) while radically reducing the number of round trips. Next time, always avoid N+1 database operations inside iteration blocks, especially during batch operations.

## 2024-05-18 - [Optimize Pandas DataFrame Iteration]
**Learning:** `DataFrame.iterrows()` is known to be extremely slow because it yields a Series object for each row, adding significant overhead.
**Action:** Replaced `iterrows()` with `DataFrame.itertuples(index=False)` in Pandas operations (especially those in data visualization modules) for much faster, tuple-based iteration. Where practical for multi-column extraction, used vectorized methods like `df[['lat', 'lon']].values.tolist()` to sidestep Python-level looping entirely.

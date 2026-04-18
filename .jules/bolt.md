## 2025-04-07 - [Optimize Database Batch Inserts]
**Learning:** During database ingestion in `ingest/database_writer.py`, iterating over records and firing off a `SELECT` statement per record to check for duplicates created a severe N+1 problem. This slowed down ingestion significantly.
**Action:** Replaced the N+1 `SELECT` statements with a batch pre-fetch strategy. Specifically, chunked records (e.g., 400 at a time) and used an `OR` chained query to load existing keys into memory for O(1) duplicate checking. This honors the SQLite parameter limit (<999) while radically reducing the number of round trips. Next time, always avoid N+1 database operations inside iteration blocks, especially during batch operations.

## 2025-05-18 - [Optimize Pandas iteration]
**Learning:** `df.iterrows()` in Pandas is notoriously slow. By swapping it out with `df.itertuples(index=False)` for row-by-row operations, iteration is significantly faster. Moreover, vectorization strategies, like `df[['latitude', 'longitude']].values.tolist()` completely bypass the python-level loop, yielding massive performance gains when creating maps and heatmaps.
**Action:** When working with Pandas DataFrames, avoid `df.iterrows()`. Use `df.itertuples()` for row iteration, or prefer vectorized methods directly against columns when forming lists or performing math.

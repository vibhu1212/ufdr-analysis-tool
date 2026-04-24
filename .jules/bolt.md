## 2025-04-07 - [Optimize Database Batch Inserts]
**Learning:** During database ingestion in `ingest/database_writer.py`, iterating over records and firing off a `SELECT` statement per record to check for duplicates created a severe N+1 problem. This slowed down ingestion significantly.
**Action:** Replaced the N+1 `SELECT` statements with a batch pre-fetch strategy. Specifically, chunked records (e.g., 400 at a time) and used an `OR` chained query to load existing keys into memory for O(1) duplicate checking. This honors the SQLite parameter limit (<999) while radically reducing the number of round trips. Next time, always avoid N+1 database operations inside iteration blocks, especially during batch operations.

## 2024-04-24 - [Optimize Pandas Iteration]
**Learning:** Using `df.iterrows()` is an anti-pattern in Pandas for iteration, as it creates a Pandas Series object for each row, adding significant overhead.
**Action:** Replaced `df.iterrows()` with `df.itertuples(index=False)` for row-by-row iteration (10-50x faster) and `df[['col1', 'col2']].values.tolist()` for extracting lists of coordinates (100x faster).

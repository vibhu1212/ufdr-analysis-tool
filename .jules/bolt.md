## 2025-04-07 - [Optimize Database Batch Inserts]
**Learning:** During database ingestion in `ingest/database_writer.py`, iterating over records and firing off a `SELECT` statement per record to check for duplicates created a severe N+1 problem. This slowed down ingestion significantly.
**Action:** Replaced the N+1 `SELECT` statements with a batch pre-fetch strategy. Specifically, chunked records (e.g., 400 at a time) and used an `OR` chained query to load existing keys into memory for O(1) duplicate checking. This honors the SQLite parameter limit (<999) while radically reducing the number of round trips. Next time, always avoid N+1 database operations inside iteration blocks, especially during batch operations.

## 2024-05-18 - [Optimize Pandas Aggregation]
**Learning:** In Pandas, using custom Python lambda functions inside `.agg()` (such as `lambda x: len(set(x))`) creates significant overhead, because the lambda is evaluated for every group in the DataFrame at the Python level.
**Action:** Replace custom lambdas with native string identifiers like `'nunique'` in `groupby(...).agg(...)` whenever possible. This simple change pushes the calculation down to C-level code and typically yields a 10-50x speedup in aggregation time.

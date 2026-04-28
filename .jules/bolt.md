## 2025-04-07 - [Optimize Database Batch Inserts]
**Learning:** During database ingestion in `ingest/database_writer.py`, iterating over records and firing off a `SELECT` statement per record to check for duplicates created a severe N+1 problem. This slowed down ingestion significantly.
**Action:** Replaced the N+1 `SELECT` statements with a batch pre-fetch strategy. Specifically, chunked records (e.g., 400 at a time) and used an `OR` chained query to load existing keys into memory for O(1) duplicate checking. This honors the SQLite parameter limit (<999) while radically reducing the number of round trips. Next time, always avoid N+1 database operations inside iteration blocks, especially during batch operations.

## 2025-04-07 - [Optimize Pandas unique count aggregation]
**Learning:** In Pandas `groupby().agg(...)`, using a custom lambda function like `lambda x: len(set(x))` to count unique values drops to Python-level execution for each group, making it extremely slow.
**Action:** Always use the native string identifier `'nunique'` in Pandas aggregation functions instead of custom lambdas for calculating unique counts. It utilizes vectorized/Cythonized operations under the hood and is typically 10-50x faster.

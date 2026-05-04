## 2025-04-07 - [Optimize Database Batch Inserts]
**Learning:** During database ingestion in `ingest/database_writer.py`, iterating over records and firing off a `SELECT` statement per record to check for duplicates created a severe N+1 problem. This slowed down ingestion significantly.
**Action:** Replaced the N+1 `SELECT` statements with a batch pre-fetch strategy. Specifically, chunked records (e.g., 400 at a time) and used an `OR` chained query to load existing keys into memory for O(1) duplicate checking. This honors the SQLite parameter limit (<999) while radically reducing the number of round trips. Next time, always avoid N+1 database operations inside iteration blocks, especially during batch operations.

## 2025-05-04 - Native Pandas unqiue counting
**Learning:** Using `lambda x: len(set(x))` within Pandas `.groupby().apply()` or `.agg()` is a significant performance bottleneck because it drops the data out of optimized C-extensions and into Python space for every group.
**Action:** Always replace these patterns with native Pandas operations. For single columns in `.agg()`, use the string literal `'nunique'`. For complex logical unions across multiple columns in `.apply()`, use `pd.concat` to combine the columns (checking `not df.empty` first) and chain `.groupby().nunique()`.

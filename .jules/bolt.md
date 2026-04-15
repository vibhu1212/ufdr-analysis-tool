## 2025-04-07 - [Optimize Database Batch Inserts]
**Learning:** During database ingestion in `ingest/database_writer.py`, iterating over records and firing off a `SELECT` statement per record to check for duplicates created a severe N+1 problem. This slowed down ingestion significantly.
**Action:** Replaced the N+1 `SELECT` statements with a batch pre-fetch strategy. Specifically, chunked records (e.g., 400 at a time) and used an `OR` chained query to load existing keys into memory for O(1) duplicate checking. This honors the SQLite parameter limit (<999) while radically reducing the number of round trips. Next time, always avoid N+1 database operations inside iteration blocks, especially during batch operations.

## 2024-05-24 - [Optimize Pandas Iteration in Visualizations]
**Learning:** Using `df.iterrows()` inside visualization modules (like `geo_viz.py`) for data extraction creates a significant performance bottleneck due to the overhead of creating Pandas Series objects for each row.
**Action:** Always replace `df.iterrows()` with `df.itertuples(index=False)` for 10-50x faster row iteration when creating map markers or other visual elements. For extracting coordinate pairs or simple lists (like heatmap data), avoid iteration entirely by using vectorized approaches like `df[['lat', 'lon']].values.tolist()`.

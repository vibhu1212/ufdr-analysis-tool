## 2025-02-13 - Bulk Inserts with `executemany`
**Learning:** In Python SQLite, inserting rows one by one in a loop (using `cursor.execute`) causes immense overhead due to individual statement parsing and context switching.
**Action:** Replaced N+1 row-by-row inserts in `parser/ufdr_ingestor.py` with `cursor.executemany` to achieve 10x-50x speedups for bulk data ingestion operations. Next time, always default to `executemany` for batch processing in Python with SQLite.

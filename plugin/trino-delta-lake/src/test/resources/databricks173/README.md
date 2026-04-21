Data generated using Databricks 17.3:

```sql
CREATE TABLE parquet_column_index_with_non_stats_column
(x int, y timestamp)
USING DELTA
LOCATION ?;

INSERT INTO parquet_column_index_with_non_stats_column VALUES 
(1, '2025-01-01 10:10:10Z'),
(2, NULL), 
(3, '2026-01-01 10:10:10Z');
```

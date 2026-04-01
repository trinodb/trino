## DuckDB Ducklake Extension: Temporal partition values in catalog metadata are literal calendar values, not epoch-based

### Problem

The [Ducklake spec](https://ducklake.select/) describes temporal partition transforms (`year`, `month`, `day`, `hour`) using Iceberg-style epoch-based semantics:

- `year(date)` → years since 1970
- `month(date)` → months since 1970-01-01
- `day(date)` → days since 1970-01-01
- `hour(timestamp)` → hours since 1970-01-01 00:00:00

These semantics apply to the **catalog metadata** (`ducklake_file_partition_value.partition_value`), which is used for partition pruning. The Hive-style directory names (`year=2023/month=6/`) are a separate concern — human-readable path components are fine there.

The issue is that `partition_value` in the catalog database contains literal calendar values (e.g., `2023`, `6`, `15`) instead of epoch-based integers (e.g., `53`, `641`, `19523`).

### Why epoch-based values matter

Epoch-based partition values produce a **monotonically increasing integer** for temporal data. This means range queries across time are a single comparison instead of compound predicates:

```sql
-- Epoch-based month: "everything before March 2024" is one comparison
WHERE month_partition_value < 650

-- Literal calendar values: requires compound logic across year + month
WHERE year_partition_value < 2024
   OR (year_partition_value = 2024 AND month_partition_value < 3)
```

This matters for partition pruning in query engines — epoch-based values let the optimizer eliminate files with a single scalar comparison rather than multi-column logic.

### Evidence: SQLite catalog backend

Test catalog created with DuckDB 1.5 + ducklake extension:

```sql
CREATE TABLE temporal_partitioned_table (
    id INTEGER, event_name VARCHAR, event_date DATE, amount DOUBLE
);
ALTER TABLE temporal_partitioned_table SET PARTITIONED BY (year(event_date), month(event_date));

INSERT INTO temporal_partitioned_table VALUES
    (1, 'Jan Event', '2023-01-15', 100.0),
    (3, 'Jun Event', '2023-06-10', 200.0),
    (5, 'Next Year', '2024-03-05', 300.0);
```

Querying the **catalog metadata** (not the file paths):

```sql
-- Query the internal catalog metadata values used for partition pruning
SELECT fpv.data_file_id, fpv.partition_key_index, fpv.partition_value,
       df.path, df.record_count
FROM ducklake_file_partition_value fpv
JOIN ducklake_data_file df
  ON fpv.data_file_id = df.data_file_id AND fpv.table_id = df.table_id
WHERE fpv.table_id = (
    SELECT table_id FROM ducklake_table WHERE table_name = 'temporal_partitioned_table'
)
ORDER BY fpv.data_file_id, fpv.partition_key_index;
```

### Results: catalog `partition_value` column

| data_file_id | partition_key_index | partition_value | path (for reference) |
|---|---|---|---|
| 18 | 0 (year) | **2023** | `year=2023/month=1/...parquet` |
| 18 | 1 (month) | **1** | `year=2023/month=1/...parquet` |
| 19 | 0 (year) | **2023** | `year=2023/month=6/...parquet` |
| 19 | 1 (month) | **6** | `year=2023/month=6/...parquet` |
| 20 | 0 (year) | **2024** | `year=2024/month=3/...parquet` |
| 20 | 1 (month) | **3** | `year=2024/month=3/...parquet` |

### Expected vs Actual (`partition_value` in catalog metadata)

| Transform | Input Date | Expected (epoch) | Actual (catalog metadata) |
|---|---|---|---|
| `year(2023-01-15)` | 2023-01-15 | 53 | **2023** |
| `year(2024-03-05)` | 2024-03-05 | 54 | **2024** |
| `month(2023-06-10)` | 2023-06-10 | 641 | **6** |
| `month(2024-03-20)` | 2024-03-20 | 650 | **3** |

Daily partition table confirms the same — `day` values in `partition_value` are day-of-month (1–31), not days-since-epoch:

| Transform | Input Date | Expected (epoch) | Actual (catalog metadata) |
|---|---|---|---|
| `day(2023-06-15)` | 2023-06-15 | 19523 | **15** |
| `day(2024-01-10)` | 2024-01-10 | 19732 | **10** |

### Confirmed with DuckDB as catalog backend

The same behavior occurs when using DuckDB itself as the catalog database (not just SQLite):

```sql
ATTACH 'ducklake:/path/to/catalog.duckdb' AS ducklake_db (DATA_PATH '/path/to/data');

-- When DuckDB is the catalog, metadata tables are in a special schema:
SELECT fpv.data_file_id, fpv.partition_key_index, fpv.partition_value,
       df.path, df.record_count
FROM __ducklake_metadata_ducklake_db.ducklake_file_partition_value fpv
JOIN __ducklake_metadata_ducklake_db.ducklake_data_file df
  ON fpv.data_file_id = df.data_file_id AND fpv.table_id = df.table_id
WHERE fpv.table_id = (
    SELECT table_id FROM __ducklake_metadata_ducklake_db.ducklake_table
    WHERE table_name = 'temporal_table'
)
ORDER BY fpv.data_file_id, fpv.partition_key_index;
```

Results identical — literal calendar values in `partition_value`:

| data_file_id | partition_key_index | partition_value | path (for reference) |
|---|---|---|---|
| 3 | 0 (year) | **2023** | `year=2023/month=1/...parquet` |
| 3 | 1 (month) | **1** | `year=2023/month=1/...parquet` |
| 4 | 0 (year) | **2023** | `year=2023/month=6/...parquet` |
| 4 | 1 (month) | **6** | `year=2023/month=6/...parquet` |
| 5 | 0 (year) | **2024** | `year=2024/month=3/...parquet` |
| 5 | 1 (month) | **3** | `year=2024/month=3/...parquet` |

### Impact

- Any consumer implementing the spec literally (epoch-based transforms for `partition_value`) will compute **wrong partition pruning predicates** against catalogs created by the DuckDB extension
- The monotonic ordering benefit of epoch-based values is lost, requiring compound multi-column predicates for temporal range scans
- Either the spec should be updated to match the implementation, or the extension should be updated to match the spec

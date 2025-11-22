Data generated using Databricks 17.2
Only the transaction log is included here, to test retrieving clustered information across all operation types.
Keep only the commit info for the operations to be tested, and set the version starting from 0.

```sql
CREATE TABLE test_retreve_clustered_fields
(col_before_rename_1 int, col_before_rename_2 int)
USING delta
CLUSTER BY (col_before_rename_1,col_before_rename_2)
LOCATION ?

ALTER TABLE test_retreve_clustered_fields RENAME COLUMN col_before_rename_1 TO col_rename_first_time_1;

ALTER TABLE test_retreve_clustered_fields RENAME COLUMN col_rename_first_time_1 TO col_rename_second_time_1;

ALTER TABLE test_retreve_clustered_fields RENAME COLUMN col_rename_second_time_1 TO col_rename_latest_time_1;
```

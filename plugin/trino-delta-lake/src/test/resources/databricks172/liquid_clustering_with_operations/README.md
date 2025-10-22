Data generated using Databricks 17.2
Only the transaction log is included here, to test retrieving clustered information across all operation types.

```sql
CREATE TABLE test_retreve_clustered_fields
(col_1 int, col_2 int)
USING delta
CLUSTER BY (col_1,col_2)
LOCATION ?
```

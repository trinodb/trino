Data generated using Databricks 17.2
Only the transaction log is included here, to test retrieving clustered information across all operation types.
Keep only the commit info for the operations to be tested, and set the version starting from 0.

```sql
CREATE TABLE test_retreve_clustered_fields
(col_1 int, col_2 int)
USING delta
CLUSTER BY (col_1,col_2)
LOCATION ?

CREATE OR REPLACE TABLE test_retreve_clustered_fields_copy
USING delta
CLUSTER BY (col_1, col_2)
AS
select * from test_retreve_clustered_fields
```

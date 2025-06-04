Data generated using Databricks 12.2:

```sql
CREATE TABLE default.test_deletion_vectors (
  a INT,
  b INT) 
USING delta 
LOCATION 's3://trino-ci-test/test_deletion_vectors' 
TBLPROPERTIES ('delta.enableDeletionVectors' = true);
```

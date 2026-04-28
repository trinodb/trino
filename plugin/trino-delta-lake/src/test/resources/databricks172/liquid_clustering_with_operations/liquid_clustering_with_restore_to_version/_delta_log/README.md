Data generated using Databricks 17.2
Only the transaction log is included here, to test retrieving clustered information across all operation types.
Keep only the commit info for the operations to be tested, and set the version starting from 0.

```sql
CREATE TABLE test_retreve_clustered_fields
(commit_0_col_1 int, commit_0_col_2 int)
USING delta
CLUSTER BY (commit_0_col_1,commit_0_col_2)
LOCATION ?;

INSERT INTO test_retreve_clustered_fields VALUES (1,2);
INSERT INTO test_retreve_clustered_fields VALUES (3,4);
INSERT INTO test_retreve_clustered_fields VALUES (5,6);
INSERT INTO test_retreve_clustered_fields VALUES (7,8);

RESTORE TABLE test_retreve_clustered_fields TO VERSION AS OF 2;
```

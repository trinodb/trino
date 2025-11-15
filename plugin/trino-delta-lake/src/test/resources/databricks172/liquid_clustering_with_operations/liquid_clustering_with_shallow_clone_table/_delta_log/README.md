Data generated using Databricks 17.2
Only the transaction log is included here, to test retrieving clustered information across all operation types.
Keep only the commit info for the operations to be tested, and set the version starting from 0.

```sql
CREATE TABLE source_table
(commit_0_col_1 int, commit_0_col_2 int)
USING delta
CLUSTER BY (commit_0_col_1,commit_0_col_2)
LOCATION ?;

%sql
CREATE TABLE cloned_table DEEP CLONE source_table;
```

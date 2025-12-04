Data generated using Databricks 17.2
Only the transaction log is included here, to test retrieving clustered information across all operation types.
Keep only the commit info for the operations to be tested, and set the version starting from 0.

```sql
CREATE TABLE test_retreve_clustered_fields
(col_1 int, col_2 int)
USING delta
CLUSTER BY (col_1,col_2)
LOCATION ?

CREATE OR REPLACE TEMP VIEW source_table AS
SELECT
    1 AS col_1,
    2 AS col_2;

MERGE INTO test_retreve_clustered_fields AS target
USING source_table AS source
ON target.col_1 = source.col_1
WHEN MATCHED THEN
UPDATE SET
    target.col_1 = source.col_1,
    target.col_2 = source.col_2
WHEN NOT MATCHED THEN
INSERT (col_1, col_2)
VALUES (source.col_1, source.col_2);
```

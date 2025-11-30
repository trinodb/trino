The table is created in a way that produces a corrupted metadata file by applying multiple truncate levels on the same column.
It is created in Trino 476 using `IcebergMinioHiveMetastoreQueryRunnerMain`, with an initial truncate(a, 1) partitioning,
followed by an update to truncate(a, 10) on the same column, which introduces the conflicting partition transforms.

Use `trino` to create the table content:

```sql
-- Create a table that uses truncate(a, 1) as the initial partition transform
CREATE TABLE conflict_truncate (a varchar) WITH (location ='s3://test-partition-evolution/conflict_truncate', partitioning = ARRAY['truncate(a, 1)']);
INSERT INTO conflict_truncate VALUES 'abc';

-- Update the table's partitioning to use a different truncate width (10). This introduces conflicting truncate levels on the same column
ALTER TABLE conflict_truncate SET PROPERTIES partitioning = ARRAY['truncate(a, 10)'];
INSERT INTO conflict_truncate VALUES 'abcd';
```

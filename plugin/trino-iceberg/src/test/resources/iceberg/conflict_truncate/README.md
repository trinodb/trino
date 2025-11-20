Used `IcebergMinioHiveMetastoreQueryRunnerMain` to generate the Iceberg table with conflicting partitioning.

Use `trino` to create the table content

```sql
CREATE TABLE conflict_truncate (a varchar) WITH (location ='s3://test-partition-evolution/conflict_truncate', partitioning = ARRAY['truncate(a, 1)']);
INSERT INTO conflict_truncate VALUES 'abc';
ALTER TABLE conflict_truncate SET PROPERTIES partitioning = ARRAY['truncate(a, 10)'];
INSERT INTO conflict_truncate VALUES 'abcd';
```

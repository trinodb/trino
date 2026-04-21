Created with Trino using Iceberg version 1.10.1
```sql
CREATE TABLE source_table (a INT);
INSERT INTO source_table VALUES 0;

CREATE MATERIALIZED VIEW materialized_view AS SELECT a FROM source_table;
REFRESH MATERIALIZED VIEW materialized_view;

DELETE FROM source_table WHERE a = 0;
INSERT INTO source_table VALUES 1;
REFRESH MATERIALIZED VIEW materialized_view;

DELETE FROM source_table WHERE a = 1;
INSERT INTO source_table VALUES 2;
REFRESH MATERIALIZED VIEW materialized_view;

DELETE FROM source_table WHERE a = 2;
INSERT INTO source_table VALUES 3;
REFRESH MATERIALIZED VIEW materialized_view;

DELETE FROM source_table WHERE a = 3;
INSERT INTO source_table VALUES 4;
REFRESH MATERIALIZED VIEW materialized_view;

ALTER TABLE source_table EXECUTE optimize;
SET SESSION iceberg.expire_snapshots_min_retention='0s';
ALTER TABLE source_table EXECUTE expire_snapshots(retention_threshold => '0s');
SET SESSION iceberg.remove_orphan_files_min_retention = '0s';
ALTER TABLE source_table EXECUTE remove_orphan_files(retention_threshold => '0s');
```

Snapshot metadata was altered so that their creation times are sufficiently old. Unnecessary source_table metadata was removed.

Data generated using the following SQL run on a Databricks notebook:
```
DROP TABLE IF EXISTS test_null_count;

CREATE TABLE test_null_count (
`i` INT
)
USING delta LOCATION 's3://starburst-alex/ajo/delta_7_3/test_null_count';

ALTER TABLE test_null_count SET TBLPROPERTIES ( delta.checkpoint.writeStatsAsStruct = true, delta.checkpoint.writeStatsAsJson = false );

INSERT INTO test_null_count VALUES 1;
INSERT INTO test_null_count VALUES 2;
INSERT INTO test_null_count VALUES null;

INSERT INTO test_null_count VALUES 1;
INSERT INTO test_null_count VALUES 2;
INSERT INTO test_null_count VALUES null;

INSERT INTO test_null_count VALUES 1;
INSERT INTO test_null_count VALUES 2;
INSERT INTO test_null_count VALUES null;
```
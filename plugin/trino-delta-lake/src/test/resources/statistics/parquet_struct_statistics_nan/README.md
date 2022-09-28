Data generated using the following SQL run on a Databricks notebook:

```
DROP TABLE IF EXISTS test_nan_types;

CREATE TABLE test_nan_types (
`fl` FLOAT,
`dou` DOUBLE
)
USING delta LOCATION 's3://starburst-alex/ajo/delta_7_3/test_nan_types';

ALTER TABLE test_nan_types SET TBLPROPERTIES ( delta.checkpoint.writeStatsAsStruct = true, delta.checkpoint.writeStatsAsJson = false );

INSERT INTO test_nan_types VALUES (0.0, 0.0);
INSERT INTO test_nan_types VALUES (10.0, 10.0);
INSERT INTO test_nan_types VALUES (100.0, 100.0);

INSERT INTO test_nan_types VALUES (0.0, 0.0);
INSERT INTO test_nan_types VALUES (10.0, 10.0);
INSERT INTO test_nan_types VALUES (100.0, 100.0);

INSERT INTO test_nan_types VALUES (0.0, 0.0);
INSERT INTO test_nan_types VALUES (10.0, 10.0);
INSERT INTO test_nan_types VALUES (float('NaN'), double('NaN'));
```
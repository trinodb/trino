Data generated using Databricks 19.2 on a SQL warehouse (`current_version()` reported
Databricks SQL 2026.32). The partition column is also stored inside each data file as
parquet `INT96`, the JSON log carries partition values as `2024-01-15T10:30:00.123000Z`,
and version 2 is a checkpoint with `partitionValues_parsed`:

```sql
CREATE TABLE timestamp_tz_partition (id INT, part TIMESTAMP)
USING DELTA PARTITIONED BY (part)
TBLPROPERTIES ('delta.checkpointInterval' = '2', 'delta.checkpoint.writeStatsAsStruct' = 'true');

INSERT INTO timestamp_tz_partition VALUES
(1, CAST('2024-01-15T10:30:00.123+00:00' AS TIMESTAMP)),
(2, CAST('2024-06-20T16:45:30.456+00:00' AS TIMESTAMP)),
(3, NULL);

INSERT INTO timestamp_tz_partition VALUES
(4, CAST('2024-11-05T09:15:45.789+00:00' AS TIMESTAMP));
```

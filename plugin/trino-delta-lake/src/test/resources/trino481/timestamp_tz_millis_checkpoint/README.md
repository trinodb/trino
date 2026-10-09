Data generated using Trino 481, which mapped the Delta `timestamp` type to
`timestamp(3) with time zone` and wrote it as parquet `INT64 (TIMESTAMP(MILLIS,false))`.
The `checkpoint_interval` property makes version 2 write a checkpoint, so the
`stats_parsed` struct for `ts` carries the same legacy encoding:

```sql
CREATE TABLE timestamp_tz_millis_checkpoint (id INTEGER, ts TIMESTAMP WITH TIME ZONE)
WITH (checkpoint_interval = 2);

INSERT INTO timestamp_tz_millis_checkpoint VALUES
(1, TIMESTAMP '2024-01-15 10:30:00.123 UTC'),
(2, TIMESTAMP '2024-06-20 16:45:30.456 UTC'),
(3, NULL);

INSERT INTO timestamp_tz_millis_checkpoint VALUES
(4, TIMESTAMP '2024-11-05 09:15:45.789 UTC');
```

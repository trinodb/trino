Data generated using Trino 481, which mapped the Delta `timestamp` type to
`timestamp(3) with time zone`. The `checkpoint_interval` property makes version 2
write a checkpoint. Its `partitionValues_parsed.part` field is declared as
`INT64 (TIMESTAMP(MILLIS,false))` but holds the zone-packed long, not the epoch
millis: the `2024-11-05 09:15:45.789` partition reads back as `2194-08-26 15:40:05.151`.
The checkpoint schema declared the field as `timestamp(3)` while the writer
supplied a `timestamp(3) with time zone` value.

```sql
CREATE TABLE timestamp_tz_millis_partitioned (id INTEGER, part TIMESTAMP WITH TIME ZONE)
WITH (partitioned_by = ARRAY['part'], checkpoint_interval = 2);

INSERT INTO timestamp_tz_millis_partitioned VALUES
(1, TIMESTAMP '2024-01-15 10:30:00.123 UTC'),
(2, TIMESTAMP '2024-06-20 16:45:30.456 UTC'),
(3, NULL);

INSERT INTO timestamp_tz_millis_partitioned VALUES
(4, TIMESTAMP '2024-11-05 09:15:45.789 UTC');
```

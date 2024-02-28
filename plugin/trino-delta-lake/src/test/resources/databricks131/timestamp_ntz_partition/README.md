Data generated using Databricks 13.1:

```sql
CREATE TABLE default.test_timestamp_ntz_partition
(id int, part timestamp_ntz)
USING delta 
PARTITIONED BY (part)
LOCATION 's3://bucket/table'
TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported');

INSERT INTO default.test_timestamp_ntz_partition VALUES
(1, NULL),
(2, TIMESTAMP_NTZ '-9999-12-31T23:59:59.999999'),
(3, TIMESTAMP_NTZ '-0001-01-01T00:00:00.000000'),
(4, TIMESTAMP_NTZ '0000-01-01T00:00:00.000000'),
(5, TIMESTAMP_NTZ '1582-10-05T00:00:00.000000'),
(6, TIMESTAMP_NTZ '1582-10-14T23:59:59.999999'),
(7, TIMESTAMP_NTZ '2020-12-31T01:02:03.123456'),
(8, TIMESTAMP_NTZ '9999-12-31T23:59:59.999999');
```

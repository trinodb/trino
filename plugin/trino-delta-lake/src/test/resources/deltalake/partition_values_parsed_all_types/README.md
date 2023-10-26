Data generated using OSS Delta Lake 3.0.0:

```sql
CREATE TABLE test_partition_values_parsed_all_types (
 id INT,
 part_boolean BOOLEAN,
 part_tinyint TINYINT,
 part_smallint SMALLINT,
 part_int INT,
 part_bigint BIGINT,
 part_short_decimal DECIMAL(5,2),
 part_long_decimal DECIMAL(21,3),
 part_double DOUBLE,
 part_float REAL,
 part_varchar STRING,
 part_date DATE,
 part_timestamp TIMESTAMP,
 part_timestamp_ntz TIMESTAMP_NTZ
)
USING delta 
PARTITIONED BY (part_boolean, part_tinyint, part_smallint, part_int, part_bigint, part_short_decimal, part_long_decimal, part_double, part_float, part_varchar, part_date, part_timestamp, part_timestamp_ntz)
LOCATION 's3://test-bucket/test_partition_values_parsed_all_types'
TBLPROPERTIES (
 delta.checkpoint.writeStatsAsStruct = true,
 delta.checkpoint.writeStatsAsJson = false,
 delta.checkpointInterval = 3
);

INSERT INTO test_partition_values_parsed_all_types VALUES (
    1,
    true,
    1,
    10,
    100,
    1000,
    CAST('123.12' AS DECIMAL(5,2)),
    CAST('123456789012345678.123' AS DECIMAL(21,3)),
    1.2,
    3.4,
    'a',
    DATE '2020-08-21',
    TIMESTAMP '2020-10-21 01:00:00.123 UTC',
    TIMESTAMP '2023-01-02 01:02:03.456'
);
INSERT INTO test_partition_values_parsed_all_types VALUES (
    2,
    false,
    2,
    20,
    200,
    2000,
    CAST('223.12' AS DECIMAL(5,2)),
    CAST('223456789012345678.123' AS DECIMAL(21,3)),
    10.2,
    30.4,
    'b',
    DATE '2020-08-22',
    TIMESTAMP '2020-10-22 01:00:00.123 UTC',
    TIMESTAMP '2023-01-03 01:02:03.456'
);
INSERT INTO test_partition_values_parsed_all_types VALUES (
    3,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL
);
```

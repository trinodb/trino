Data generated using the following SQL run on a Databricks notebook:
```
DROP TABLE IF EXISTS parquet_struct_statistics;

CREATE TABLE parquet_struct_statistics (
`ts` TIMESTAMP,
`str` STRING,
`dec_short` DECIMAL(5,1),
`dec_long` DECIMAL(25,3),
`l` BIGINT,
`in` INT,
`sh` SMALLINT,
`byt` TINYINT,
`fl` FLOAT,
`dou` DOUBLE,
`bool` BOOLEAN,
`bin` BINARY,
`dat` DATE,
`arr` ARRAY<INT>,
`m` MAP<INT, STRING>,
`row` STRUCT<`s1`: INT, `s2`: STRING>)
USING delta LOCATION 's3://starburst-alex/ajo/delta_7_3/parquet_struct_statistics';

ALTER TABLE parquet_struct_statistics SET TBLPROPERTIES ( delta.checkpoint.writeStatsAsStruct = true, delta.checkpoint.writeStatsAsJson = false );

INSERT INTO parquet_struct_statistics VALUES (TIMESTAMP '2960-10-31 01:00:00', "a", 10.1, 999999999999.123,  10000000, 20000000, 123, 42, 0.123, 0.321, true, X'0000000000000000000', DATE '5000-01-01', ARRAY(1, 2, 3), MAP(1, "a"), STRUCT(1, "a"));
INSERT INTO parquet_struct_statistics VALUES (TIMESTAMP '1990-10-31 01:00:00', "b", -10.1, -999999999999.123, -10000000, -20000000, -123, -42, -0.123, -0.321, true, X'0002', DATE '1900-01-01', ARRAY(4), MAP(2, "b"), STRUCT(2, "b"));
INSERT INTO parquet_struct_statistics VALUES (TIMESTAMP '2020-10-31 01:00:00', "c", 0, 0, 0, 0, 0, 0, 0.0, 0.0, true, X'000001', DATE '2020-01-01', ARRAY(5), MAP(3, "c"), STRUCT(3, "c"));

INSERT INTO parquet_struct_statistics VALUES (TIMESTAMP '2960-10-31 01:00:00', "a", 10.1, 999999999999.123,  10000000, 20000000, 123, 42, 0.123, 0.321, true, X'0000000000000000000', DATE '5000-01-01', ARRAY(1, 2, 3), MAP(1, "a"), STRUCT(1, "a"));
INSERT INTO parquet_struct_statistics VALUES (TIMESTAMP '1990-10-31 01:00:00', "b", -10.1, -999999999999.123, -10000000, -20000000, -123, -42, -0.123, -0.321, true, X'0002', DATE '1900-01-01', ARRAY(4), MAP(2, "b"), STRUCT(2, "b"));
INSERT INTO parquet_struct_statistics VALUES (TIMESTAMP '2020-10-31 01:00:00', "c", 0, 0, 0, 0, 0, 0, 0.0, 0.0, true, X'000001', DATE '2020-01-01', ARRAY(5), MAP(3, "c"), STRUCT(3, "c"));

INSERT INTO parquet_struct_statistics VALUES (TIMESTAMP '2960-10-31 01:00:00', "a", 10.1, 999999999999.123,  10000000, 20000000, 123, 42, 0.123, 0.321, true, X'0000000000000000000', DATE '5000-01-01', ARRAY(1, 2, 3), MAP(1, "a"), STRUCT(1, "a"));
INSERT INTO parquet_struct_statistics VALUES (TIMESTAMP '1990-10-31 01:00:00', "b", -10.1, -999999999999.123, -10000000, -20000000, -123, -42, -0.123, -0.321, true, X'0002', DATE '1900-01-01', ARRAY(4), MAP(2, "b"), STRUCT(2, "b"));
INSERT INTO parquet_struct_statistics VALUES (TIMESTAMP '2020-10-31 01:00:00', "c", 0, 0, 0, 0, 0, 0, 0.0, 0.0, true, X'000001', DATE '2020-01-01', ARRAY(5), MAP(3, "c"), STRUCT(3, "c"));
```
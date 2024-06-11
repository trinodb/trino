Data generated using Databricks 15.3:

```sql
CREATE TABLE default.test_variant_types
USING delta
LOCATION ?
AS SELECT
       CAST(true AS variant) AS col_boolean,
       CAST(1 AS variant) AS col_long,
       CAST(CAST('0.2' AS float) AS variant) AS col_float,
       CAST(CAST('0.3' AS double) AS variant) AS col_double,
       CAST(CAST('0.4' AS decimal) AS variant) AS col_decimal,
       CAST('test data' AS variant) AS col_string,
       CAST(X'65683F' AS variant) AS col_binary,
       CAST(date '2021-02-03' AS variant) AS col_date,
       CAST(timestamp '2001-08-22 01:02:03.321 UTC' AS variant) AS col_timestamp,
       CAST(timestamp_ntz '2021-01-02 12:34:56.123456' AS variant) AS col_timestampntz,
       CAST(array(1) AS variant) AS col_array,
       CAST(map('key1', 1, 'key2', 2) AS variant) AS col_map,
       CAST(named_struct('x', 1) AS variant) AS col_struct;
```

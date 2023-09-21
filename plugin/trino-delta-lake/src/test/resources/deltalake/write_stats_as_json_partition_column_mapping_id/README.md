Data generated using OSS Delta Lake 2.4.0:

```sql
CREATE TABLE default.?
 (a_number INT, a_string STRING, array_col ARRAY<STRUCT<array_struct_element: STRING>>, nested STRUCT<field1: STRING>)
 USING delta 
 PARTITIONED BY (a_string)
 LOCATION 's3://?/databricks-compatibility-test-?'
 TBLPROPERTIES (
 'delta.checkpointInterval' = 1, 
 'delta.checkpoint.writeStatsAsJson' = ?, 
 'delta.checkpoint.writeStatsAsStruct' = ?, 
 'delta.columnMapping.mode' = 'id'
)
```

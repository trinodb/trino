Data generated using OSS Delta Lake 3.2.0.
Updated `minWriterVersion` to the max supported version + 1 in json file.

```sql
CREATE TABLE default.unsupported_writer_version
(col int) 
USING DELTA 
LOCATION ?
TBLPROPERTIES ('delta.minWriterVersion' = '7');
```

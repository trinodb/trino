Data generated using OSS Delta Lake 3.2.0:

```sql
CREATE TABLE default.test_type_widening_unsupported
(col byte)
USING DELTA 
LOCATION 's3://test-bucket/databricks-compatibility-test-test-widening-unsupported'
TBLPROPERTIES ('delta.enableTypeWidening'=true);

INSERT INTO default.test_type_widening_unsupported VALUES 127;
ALTER TABLE default.test_type_widening_unsupported CHANGE COLUMN col TYPE short;
```

Manually updated `toType` field in 00000000000000000002.json to `unsupported`. 

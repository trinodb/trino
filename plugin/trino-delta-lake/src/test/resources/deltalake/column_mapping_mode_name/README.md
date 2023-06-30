Data generated using OSS Delta Lake 2.3.0:

```sql
CREATE TABLE default.test 
(x INT)
USING delta
LOCATION 's3://trino-ci-test/test'
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
```

Data generated using OSS Delta Lake 3.0.0:

```sql
CREATE TABLE default.test_skip_stats
(id INT, lower INT, `UPPER` INT, `a,comma` INT, `a.dot` INT, a STRUCT<nested INT>)
USING delta 
LOCATION 's3://test-bucket/test_skip_stats3'
TBLPROPERTIES ('delta.dataSkippingStatsColumns' = 'lower,UPPER,a\\.dot,a.nested', 'delta.columnMapping.mode' = 'name');
```

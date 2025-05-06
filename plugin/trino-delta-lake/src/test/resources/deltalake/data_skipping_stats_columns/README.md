Data generated using OSS Delta Lake 3.3.0:

```sql
CREATE TABLE data_skippping_stats_columns
(id INT, lower INT, `UPPER` INT, `a,comma` INT, `a.dot` INT, a STRUCT<nested INT>)
USING delta 
LOCATION ?
TBLPROPERTIES ('delta.dataSkippingStatsColumns' = 'lower,UPPER,`a.dot`,a.nested', 'delta.columnMapping.mode' = 'name');
```

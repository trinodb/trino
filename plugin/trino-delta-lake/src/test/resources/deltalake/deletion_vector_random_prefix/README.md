Data generated using OSS Delta Lake 3.2.0:

```sql
CREATE TABLE tpch.test_dv_random_prefix 
(a int, b int) 
USING delta 
TBLPROPERTIES(
  'delta.enableDeletionVectors' = true, 
  'delta.randomizeFilePrefixes' = true,
  'delta.randomPrefixLength' = 3
);
```

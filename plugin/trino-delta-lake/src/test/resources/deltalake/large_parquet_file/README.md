Data generated using Delta Lake 4.0.0:

```sql
CREATE TABLE test_large_parquet
(data INT)
USING delta 
LOCATION 's3://test-bucket/test_large_parquet';

INSERT INTO test_large_parquet
SELECT id / 5000 FROM RANGE(0, 50000)
DISTRIBUTE BY 1;
```

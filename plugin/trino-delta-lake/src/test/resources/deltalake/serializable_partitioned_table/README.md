Data generated using OSS Delta Lake 3.0.0 on Spark 3.5.0:


```sql
CREATE TABLE serializable_partitioned_table 
USING DELTA
PARTITIONED BY (part)
TBLPROPERTIES ('delta.isolationLevel' = 'Serializable') 
AS 
    SELECT a, part 
    FROM VALUES (0,10), (33,40) AS data(a, part);
```

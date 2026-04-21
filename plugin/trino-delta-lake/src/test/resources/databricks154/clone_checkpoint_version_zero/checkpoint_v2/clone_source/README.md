Data generated using Databricks 15.4:

```sql
CREATE TABLE source_table (
    id INT,
    name STRING,
    age INT
)
USING DELTA
TBLPROPERTIES (
  'delta.checkpointPolicy' = 'v2'
);

INSERT INTO source_table VALUES
     (1, 'Alice', 25),
     (2, 'Bob', 30),
     (3, 'Charlie', 28);
```

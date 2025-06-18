Data generated using Databricks 15.4:

```sql
CREATE TABLE test_variant_null (
id INT,
x VARIANT
) USING DELTA
LOCATION ?;

INSERT INTO test_variant_null values 
(1, parse_json('{"a":1}')), 
(2, parse_json('{"a":2}')), 
(3, parse_json(NULL)), 
(4, NULL);
```

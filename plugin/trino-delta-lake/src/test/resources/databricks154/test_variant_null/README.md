Data generated using Databricks 15.4:

```sql
CREATE TABLE test_variant_null (
id INT,
x VARIANT,
y MAP<STRING, VARIANT>
) USING DELTA
LOCATION ?;

INSERT INTO test_variant_null values
(1, parse_json('{"a":1}'), map('key1', NULL)),
(2, parse_json('{"a":2}'), map('key1', parse_json('{"key":"value"}'))),
(3, parse_json('null'), NULL),
(4, NULL, NULL),
(5, parse_json('{"a":5}'), NULL);
```

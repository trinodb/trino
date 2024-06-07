Data generated using Databricks 15.3:

```sql
CREATE TABLE default.test_variant
(col_int INT, simple_variant variant, array_variant array<variant>, map_variant map<string, variant>, struct_variant struct<x variant>, col_string string)
USING delta 
LOCATION ?;

INSERT INTO default.test_variant
VALUES (1, parse_json('{"col":1}'), array(parse_json('{"array":2}')), map('key1', parse_json('{"map":3}')), named_struct('x', parse_json('{"struct":4}')), 'test data');
```

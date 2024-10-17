Data generated using Databricks 15.3:

```sql
CREATE TABLE default.test_variant
(col_int INT, simple_variant variant, array_variant array<variant>, map_variant map<string, variant>, struct_variant struct<x variant>, col_string string)
USING delta 
LOCATION ?;

INSERT INTO default.test_variant
VALUES (1, parse_json('{"col":1}'), array(parse_json('{"array":2}')), map('key1', parse_json('{"map":3}')), named_struct('x', parse_json('{"struct":4}')), 'test data');

INSERT INTO default.test_variant
VALUES (2, parse_json('{"col":null}'), array(parse_json('{"array":null}')), map('key1', parse_json('{"map":null}')), named_struct('x', parse_json('{"struct":null}')), 'test null data');

INSERT INTO default.test_variant
VALUES (3, parse_json(NULL), array(NULL), map('key1', NULL), named_struct('x', NULL), 'test null');

INSERT INTO default.test_variant
VALUES (4, parse_json('1'), array(parse_json('2')), map('key1', parse_json('3')), named_struct('x', parse_json('4')), 'test without fields');
```

Data generated using Databricks 15.4:

```sql
CREATE TABLE test_variant_after_optimization (
id INT,
col_variant VARIANT
) USING DELTA
LOCATION ?;

INSERT INTO test_variant_after_optimization VALUES
(1, parse_json('["a", "b", "c"]')),                    -- Array of strings
(2, parse_json('[1, "a", true, null]')),               -- Heterogeneous array
(3, parse_json('{"a": 1, "b": 2}')),                  -- Simple object/map
(4, parse_json('{"a": [1, 2], "b": {"x": true}}')),   -- Nested object with array and object
(5, parse_json('[{"x": 1}, {"y": "z"}]'));            -- Array of objects (list[variant])

INSERT INTO test_variant_after_optimization VALUES
(6, parse_json('{"nested": [{"x": 1}, 2, null]}')),   -- Object with heterogeneous array
(7, parse_json('{"deep": {"deeper_a": {"value": "va"}}}')), -- Deeply nested
(8, parse_json('{"deep": {"deeper_a": {"value": "vaa"}, "deeper_b": {"value": "vbb"}}}')), -- Deeply nested
(9, parse_json('{"deep": {"deeper_c": {"value": "vc"}}}')); -- Deeply nested

OPTIMIZE test_variant_after_optimization
```

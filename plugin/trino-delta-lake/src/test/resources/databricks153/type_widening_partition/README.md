Data generated using Databricks 15.3:

```sql
CREATE TABLE default.test_part_type_widening
(
    col byte,
    part byte
)
    USING DELTA
    LOCATION 's3://starburstdata-test/test-part-type-widening'
    PARTITIONED BY (part)
    TBLPROPERTIES ('delta.enableTypeWidening' = 'true');
INSERT INTO default.test_part_type_widening
VALUES (1, 1);
-- change to supported type widening
ALTER TABLE default.test_part_type_widening ALTER COLUMN part TYPE INT;
INSERT INTO default.test_part_type_widening
VALUES (2, 2);
-- change to unsupported type widening
ALTER TABLE default.test_part_type_widening ALTER COLUMN part TYPE DOUBLE;
INSERT INTO default.test_part_type_widening
VALUES (3, 3);
```

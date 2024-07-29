Data generated using Databricks 15.3:

```sql
CREATE TABLE default.test_type_widening_partition
(
    cOl BYTE,
    Byte_to_Short BYTE, byte_to_int BYTE, byte_to_long BYTE, byte_to_decimal BYTE, Byte_to_Double BYTE,
    short_to_int SHORT, short_to_long SHORT, short_to_decimal SHORT, short_to_double SHORT,
    int_to_long INT, int_to_decimal INT, int_to_double INT,
    long_to_decimal LONG,
    float_to_double FLOAT,
    decimal_to_decimal DECIMAL,
    date_to_timestamp DATE
)
    USING DELTA
    PARTITIONED BY (Byte_to_Short, byte_to_int, byte_to_long, byte_to_decimal, Byte_to_Double, short_to_int, short_to_long, short_to_decimal, short_to_double, int_to_long, int_to_decimal, int_to_double, long_to_decimal, float_to_double, decimal_to_decimal, date_to_timestamp)
    LOCATION 's3://starburstdata-test/test-part-exhaustive-type-widening'
    TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.feature.timestampNtz' = 'supported');

INSERT INTO default.test_type_widening_partition VALUES
    (
        1,
        1, 1, 1, 1, 1,
        1, 1, 1, 1,
        1, 1, 1,
        1,
        1.0,
        1.0,
        DATE '2024-06-19'
    );

ALTER TABLE default.test_type_widening_partition ALTER COLUMN Byte_to_Short TYPE SHORT;
ALTER TABLE default.test_type_widening_partition ALTER COLUMN byte_to_int TYPE INT;
ALTER TABLE default.test_type_widening_partition ALTER COLUMN byte_to_long TYPE LONG;
ALTER TABLE default.test_type_widening_partition ALTER COLUMN byte_to_decimal TYPE DECIMAL;
ALTER TABLE default.test_type_widening_partition ALTER COLUMN Byte_to_Double TYPE DOUBLE;
ALTER TABLE default.test_type_widening_partition ALTER COLUMN short_to_int TYPE INT;
ALTER TABLE default.test_type_widening_partition ALTER COLUMN short_to_long TYPE LONG;
ALTER TABLE default.test_type_widening_partition ALTER COLUMN short_to_decimal TYPE DECIMAL;
ALTER TABLE default.test_type_widening_partition ALTER COLUMN short_to_double TYPE DOUBLE;
ALTER TABLE default.test_type_widening_partition ALTER COLUMN int_to_long TYPE LONG;
ALTER TABLE default.test_type_widening_partition ALTER COLUMN int_to_decimal TYPE DECIMAL;
ALTER TABLE default.test_type_widening_partition ALTER COLUMN int_to_double TYPE DOUBLE;
ALTER TABLE default.test_type_widening_partition ALTER COLUMN long_to_decimal TYPE DECIMAL (20,0);
ALTER TABLE default.test_type_widening_partition ALTER COLUMN float_to_double TYPE DOUBLE;
ALTER TABLE default.test_type_widening_partition ALTER COLUMN decimal_to_decimal TYPE DECIMAL (12,2);
ALTER TABLE default.test_type_widening_partition ALTER COLUMN date_to_timestamp TYPE TIMESTAMP_NTZ;
--     cOl BYTE,
--     Byte_to_Short BYTE, byte_to_int BYTE, byte_to_long BYTE, byte_to_decimal BYTE, Byte_to_Double BYTE,
--     short_to_int SHORT, short_to_long SHORT, short_to_decimal SHORT, short_to_double SHORT,
--     int_to_long INT, int_to_decimal INT, int_to_double INT,
--     long_to_decimal LONG,
--     float_to_double FLOAT,
--     decimal_to_decimal DECIMAL,
--     date_to_timestamp DATE
INSERT INTO default.test_type_widening_partition VALUES
    (
        2,
        256, 35000, 2147483650, 9223372036, 9323372040.456,
        2, 2, 2, 2,
        2, 2, 2,
        2,
        2.0,
        2.22,
        TIMESTAMP_NTZ '2021-7-1T8:43:28.123456'
    );
```

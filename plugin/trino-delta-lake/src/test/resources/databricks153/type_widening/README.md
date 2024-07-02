Data generated using Databricks 15.3:

```sql
--      * byte	    -> short, int, long, decimal, double
--      * short	    -> int, long, decimal, double
--      * int	    -> long, decimal, double
--      * long	    -> decimal
--      * float	    -> double
--      * decimal	-> decimal with greater precision and scale
--      * date	    -> timestampNTZ
--
--  Covering all possible type changes
--  from https://docs.databricks.com/en/delta/type-widening.html#supported-type-changes

CREATE TABLE default.test_db_type_widening
    (
        bts BYTE, bti BYTE, btl BYTE, btd BYTE, btdb BYTE,
        sti SHORT, stl SHORT, std SHORT, stdb SHORT,
        itl INT, itd INT, itdb INT,
        ltd LONG,
        ftdb FLOAT,
        dtd DECIMAL,
        dtt DATE
    )
    USING DELTA
    LOCATION 's3://starburstdata-test/test-db-type-widening'
    TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.feature.timestampNtz' = 'supported');

INSERT INTO default.test_db_type_widening VALUES
    (
        1, 1, 1, 1, 1,
        1, 1, 1, 1,
        1, 1, 1,
        1,
        1.0,
        1.0,
        DATE '2024-06-19'
    );

ALTER TABLE default.test_db_type_widening ALTER COLUMN bts TYPE SHORT;
ALTER TABLE default.test_db_type_widening ALTER COLUMN bti TYPE INT;
ALTER TABLE default.test_db_type_widening ALTER COLUMN btl TYPE LONG;
ALTER TABLE default.test_db_type_widening ALTER COLUMN btd TYPE DECIMAL;
ALTER TABLE default.test_db_type_widening ALTER COLUMN btdb TYPE DOUBLE;
ALTER TABLE default.test_db_type_widening ALTER COLUMN sti TYPE INT;
ALTER TABLE default.test_db_type_widening ALTER COLUMN stl TYPE LONG;
ALTER TABLE default.test_db_type_widening ALTER COLUMN std TYPE DECIMAL;
ALTER TABLE default.test_db_type_widening ALTER COLUMN stdb TYPE DOUBLE;
ALTER TABLE default.test_db_type_widening ALTER COLUMN itl TYPE LONG;
ALTER TABLE default.test_db_type_widening ALTER COLUMN itd TYPE DECIMAL;
ALTER TABLE default.test_db_type_widening ALTER COLUMN itdb TYPE DOUBLE;
ALTER TABLE default.test_db_type_widening ALTER COLUMN ltd TYPE DECIMAL (20,0);
ALTER TABLE default.test_db_type_widening ALTER COLUMN ftdb TYPE DOUBLE;
ALTER TABLE default.test_db_type_widening ALTER COLUMN dtd TYPE DECIMAL (12,2);
ALTER TABLE default.test_db_type_widening ALTER COLUMN dtt TYPE TIMESTAMP_NTZ;

INSERT INTO default.test_db_type_widening VALUES
    (
        2, 2, 2, 2, 2,
        2, 2, 2, 2,
        2, 2, 2,
        2,
        2.0,
        2.22,
        TIMESTAMP_NTZ '2021-7-1T8:43:28.123456'
    );
```

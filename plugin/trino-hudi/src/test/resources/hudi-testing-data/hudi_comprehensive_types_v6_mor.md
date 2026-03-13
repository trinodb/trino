## Create script

Structure of table:
- MOR table with MDT enabled
- Revision: tag release-0.15.0

```scala
test("Create MOR table with comprehensive types") {
    withTempDir { tmp =>
        val tableName = "hudi_type_test_mor"
        spark.sql(
            s"""
               |CREATE TABLE $tableName (
               |  uuid STRING,
               |  precombine_field LONG,
               |
               |  -- Numeric Types
               |  col_boolean BOOLEAN,
               |  col_tinyint TINYINT,
               |  col_smallint SMALLINT,
               |  col_int INT,
               |  col_bigint BIGINT,
               |  col_float FLOAT,
               |  col_double DOUBLE,
               |  col_decimal DECIMAL(10, 2),
               |
               |  -- String Types
               |  col_string STRING,
               |  col_varchar VARCHAR(50),
               |  col_char CHAR(10),
               |
               |  -- Binary Type
               |  col_binary BINARY,
               |
               |  -- Datetime Types
               |  col_date DATE,
               |  col_timestamp TIMESTAMP,
               |  -- col_timestamp_ntz TIMESTAMP_NTZ, (No support on Hudi for now)
               |
               |  -- Complex types
               |  col_array_int ARRAY<INT>,
               |  col_array_string ARRAY<STRING>,
               |  col_map_string_int MAP<STRING, INT>,
               |  col_struct STRUCT<f1: STRING, f2: INT, f3: BOOLEAN>,
               |  col_array_struct ARRAY<STRUCT<nested_f1: DOUBLE, nested_f2: ARRAY<STRING>>>,
               |  col_map_string_struct MAP<STRING, STRUCT<nested_f3: DATE, nested_f4: DECIMAL(5,2)>>,
               |  col_array_struct_with_map ARRAY<STRUCT<f_arr_struct_str: STRING, f_arr_struct_map: MAP<STRING, INT>>>,
               |  col_map_struct_with_array MAP<STRING, STRUCT<f_map_struct_arr: ARRAY<BOOLEAN>, f_map_struct_ts: TIMESTAMP>>,
               |  col_struct_nested_struct STRUCT<outer_f1: INT, nested_struct: STRUCT<inner_f1: STRING, inner_f2: BOOLEAN>>,
               |  col_array_array_int ARRAY<ARRAY<INT>>,
               |  col_map_string_array_double MAP<STRING, ARRAY<DOUBLE>>,
               |  col_map_string_map_string_date MAP<STRING, MAP<STRING, DATE>>,
               |
               |  -- Array of structs with single (inner) fields do not work with parquet.version 1.13.1
               |  col_struct_array_struct STRUCT<outer_f2: STRING, struct_array: ARRAY<STRUCT<inner_f3: TIMESTAMP, inner_f4: STRING>>>,
               |  col_struct_map STRUCT<outer_f3: BOOLEAN, struct_map: MAP<STRING, BIGINT>>,
               |
               |  part_col STRING
               |) USING hudi
               | LOCATION '${tmp.getCanonicalPath}'
               | TBLPROPERTIES (
               |  primaryKey = 'uuid',
               |  type = 'mor',
               |  preCombineField = 'precombine_field'
               | )
               | PARTITIONED BY (part_col)
        """.stripMargin)

        // To not trigger compaction scheduling, and compaction
        spark.sql(s"set hoodie.compact.inline.max.delta.commits=9999")
        spark.sql(s"set hoodie.compact.inline=false")
        
        // Directly write to new parquet file
        spark.sql(s"set hoodie.parquet.small.file.limit=0")
        spark.sql(s"set hoodie.metadata.compact.max.delta.commits=1")
        // Partition stats index is enabled together with column stats index
        spark.sql(s"set hoodie.metadata.index.column.stats.enable=true")
        spark.sql(s"set hoodie.metadata.record.index.enable=true")

        // Insert row 1 into partition 'A'
        spark.sql(
            s"""
               | INSERT INTO $tableName VALUES (
               |  'uuid1', 1000L,
               |  true, cast(1 as tinyint), cast(100 as smallint), 1000, 100000L, 1.1, 10.123, cast(123.45 as decimal(10,2)),
               |  'string val 1', cast('varchar val 1' as varchar(50)), cast('charval1' as char(10)),
               |  cast('binary1' as binary),
               |  cast('2025-01-15' as date), cast('2025-01-15 11:30:00' as timestamp),
               |  -- cast('2025-01-15 11:30:00' as timestamp_ntz),
               |  array(1, 2, 3), array('a', 'b', 'c'), map('key1', 10, 'key2', 20),
               |  struct('struct_str1', 55, false),
               |  array(struct(1.1, array('n1','n2')), struct(2.2, array('n3'))),
               |  map('mapkey1', struct(cast('2024-11-01' as date), cast(9.8 as decimal(5,2)))),
               |  array(struct('arr_struct1', map('map_in_struct_k1', 1)), struct('arr_struct2', map('map_in_struct_k2', 2, 'map_in_struct_k3', 3))),
               |  map('map_struct1', struct(array(true, false), cast('2025-01-01 01:01:01' as timestamp)), 'map_struct2', struct(array(false), cast('2025-02-02 02:02:02' as timestamp))),
               |  struct(101, struct('inner_str_1', true)),
               |  array(array(1, 2), array(3, 4, 5)),
               |  map('arr_key1', array(1.1, 2.2), 'arr_key2', array(3.3)),
               |  map('map_key1', map('mapkey10', cast('2024-01-01' as date), 'mapkey20', cast('2024-02-02' as date))),
               |  struct('outer_str_1', array(struct(cast('2023-11-11 11:11:11' as timestamp), 'inner_str_1'))),
               |  struct(true, map('struct_map_k1', 1000L, 'struct_map_k2', 2000L)),
               |  'A'
               | )
      """.stripMargin)

        // Insert row 2 into partition 'A'
        spark.sql(
            s"""
               | INSERT INTO $tableName VALUES (
               |  'uuid2', 1005L,
               |  false, cast(2 as tinyint), cast(200 as smallint), 2000, 200000L, 2.2, 20.456, cast(234.56 as decimal(10,2)),
               |  'string val 2', cast('varchar val 2' as varchar(50)), cast('charval2' as char(10)),
               |  cast('binary2' as binary),
               |  cast('2025-02-20' as date), cast('2025-02-20 12:45:00' as timestamp),
               |  -- cast('2025-02-20 12:45:00' as timestamp_ntz),
               |  array(4, 5), array('d', 'e', 'f'), map('key3', 30),
               |  struct('struct_str2', 66, true),
               |  null,
               |  map('mapkey2', struct(cast('2024-12-10' as date), cast(7.6 as decimal(5,2)))),
               |  array(struct('arr_struct3', map('map_in_struct_k4', 4)), struct('arr_struct4', null)),
               |  map('map_struct3', struct(null, cast('2025-03-03 03:03:03' as timestamp)), 'map_struct4', struct(array(true), null)),
               |  -- Additional Nested Complex Types (with nulls)
               |  struct(102, null),
               |  array(array(6), array(7, 8)),
               |  map('arr_key3', null),
               |  map('map_key2', map(30, null), 'map_key3', null),
               |  struct('outer_str_2', array(struct(cast('2023-12-12 12:12:12' as timestamp), 'inner_str_2'))),
               |  struct(false, null),
               |  'A'
               | )
      """.stripMargin)

        // Insert row 3 into partition 'B'
        spark.sql(
            s"""
               | INSERT INTO $tableName VALUES (
               |  'uuid3', 1100L,
               |  null, null, null, null, null, null, null, null,
               |  null, null, null,
               |  null,
               |  null, null,
               |  null, null, null,
               |  null,
               |  array(struct(3.3, array('n4'))),
               |  null,
               |  null,
               |  null,
               |  null,
               |  null,
               |  null,
               |  null,
               |  null,
               |  null,
               |  'B'
               | )
          """.stripMargin)
        
        // Generate log files through updates on partition 'A'
        spark.sql(s"UPDATE $tableName SET col_double = col_double + 100, precombine_field = precombine_field + 1 WHERE part_col = 'A'")
        // Generate log files through updates on partition 'B'
        spark.sql(s"UPDATE $tableName SET col_string = 'updated string', precombine_field = precombine_field + 1 WHERE part_col = 'B'")
    }
}
```

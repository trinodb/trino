CREATE TABLE presto_test_sequence (
  n INT
)
COMMENT 'Presto test data'
;

CREATE TABLE presto_test_partition_format (
  t_string STRING,
  t_tinyint TINYINT,
  t_smallint SMALLINT,
  t_int INT,
  t_bigint BIGINT,
  t_float FLOAT,
  t_double DOUBLE,
  t_boolean BOOLEAN
)
COMMENT 'Presto test data'
PARTITIONED BY (ds STRING, file_format STRING, dummy INT)
;

CREATE TABLE presto_test_unpartitioned (
  t_string STRING,
  t_tinyint TINYINT
)
COMMENT 'Presto test data'
STORED AS TEXTFILE
;

CREATE TABLE presto_test_offline_partition (
  t_string STRING
)
COMMENT 'Presto test data'
PARTITIONED BY (ds STRING)
;

CREATE TABLE presto_test_bucketed_by_string_int (
  t_string STRING,
  t_tinyint TINYINT,
  t_smallint SMALLINT,
  t_int INT,
  t_bigint BIGINT,
  t_float FLOAT,
  t_double DOUBLE,
  t_boolean BOOLEAN
)
COMMENT 'Presto test bucketed table'
PARTITIONED BY (ds STRING)
CLUSTERED BY (t_string, t_int) INTO 32 BUCKETS
STORED AS RCFILE
;

CREATE TABLE presto_test_bucketed_by_bigint_boolean (
  t_string STRING,
  t_tinyint TINYINT,
  t_smallint SMALLINT,
  t_int INT,
  t_bigint BIGINT,
  t_float FLOAT,
  t_double DOUBLE,
  t_boolean BOOLEAN
)
COMMENT 'Presto test bucketed table'
PARTITIONED BY (ds STRING)
CLUSTERED BY (t_bigint, t_boolean) INTO 32 BUCKETS
STORED AS RCFILE
;

CREATE TABLE presto_test_bucketed_by_double_float (
  t_string STRING,
  t_tinyint TINYINT,
  t_smallint SMALLINT,
  t_int INT,
  t_bigint BIGINT,
  t_float FLOAT,
  t_double DOUBLE,
  t_boolean BOOLEAN
)
COMMENT 'Presto test bucketed table'
PARTITIONED BY (ds STRING)
CLUSTERED BY (t_double, t_float) INTO 32 BUCKETS
STORED AS RCFILE
;

DROP TABLE IF EXISTS tmp_presto_test;
CREATE TABLE tmp_presto_test (
  t_string STRING,
  t_tinyint TINYINT,
  t_smallint SMALLINT,
  t_int INT,
  t_bigint BIGINT,
  t_float FLOAT,
  t_double DOUBLE,
  t_boolean BOOLEAN
)
;
INSERT INTO TABLE tmp_presto_test
SELECT
  CASE n % 19 WHEN 0 THEN NULL WHEN 1 THEN '' ELSE 'test' END -- t_string
, 1 + n -- t_tinyint
, 2 + n -- t_smallint
, 3 + n -- t_int
, 4 + n + CASE WHEN n % 13 = 0 THEN NULL ELSE 0 END -- t_bigint
, 5.1 + n -- t_float
, 6.2 + n -- t_double
, CASE n % 3 WHEN 0 THEN false WHEN 1 THEN true ELSE NULL END -- t_boolean
FROM presto_test_sequence
LIMIT 100
;

INSERT INTO TABLE presto_test_unpartitioned
SELECT
  CASE n % 19 WHEN 0 THEN NULL WHEN 1 THEN '' ELSE 'unpartitioned' END
, 1 + n
FROM presto_test_sequence LIMIT 100;

DROP TABLE IF EXISTS presto_test_types_orc;
CREATE TABLE presto_test_types_orc (
                                       t_string STRING
    , t_tinyint TINYINT
    , t_smallint SMALLINT
    , t_int INT
    , t_bigint BIGINT
    , t_float FLOAT
    , t_double DOUBLE
    , t_boolean BOOLEAN
    , t_timestamp TIMESTAMP
    , t_binary BINARY
    , t_date DATE
    , t_varchar VARCHAR(50)
    , t_char CHAR(25)
    , t_map MAP<STRING, STRING>
    , t_array_string ARRAY<STRING>
    , t_array_struct ARRAY<STRUCT<s_string: STRING, s_double:DOUBLE>>
    , t_struct STRUCT<s_string: STRING, s_double:DOUBLE>
    , t_complex MAP<INT, ARRAY<STRUCT<s_string: STRING, s_double:DOUBLE>>>
)
    STORED AS ORC
;

INSERT INTO TABLE presto_test_types_orc
SELECT
    CASE n % 19 WHEN 0 THEN NULL WHEN 1 THEN '' ELSE 'test' END
     , 1 + n
     , 2 + n
     , 3 + n
     , 4 + n + CASE WHEN n % 13 = 0 THEN NULL ELSE 0 END
     , 5.1 + n
     , 6.2 + n
     , CASE n % 3 WHEN 0 THEN false WHEN 1 THEN true ELSE NULL END
     , CASE WHEN n % 17 = 0 THEN NULL ELSE '2011-05-06 07:08:09.1234567' END
     , CASE WHEN n % 23 = 0 THEN NULL ELSE CAST('test binary' AS BINARY) END
     , CASE WHEN n % 37 = 0 THEN NULL ELSE '2013-08-09' END
     , CASE n % 39 WHEN 0 THEN NULL WHEN 1 THEN '' ELSE 'test varchar' END
     , CASE n % 41 WHEN 0 THEN NULL WHEN 1 THEN '' ELSE 'test char' END
     , CASE WHEN n % 27 = 0 THEN NULL ELSE map('test key', 'test value') END
     , CASE WHEN n % 29 = 0 THEN NULL ELSE array('abc', 'xyz', 'data') END
     , CASE WHEN n % 31 = 0 THEN NULL ELSE
    array(named_struct('s_string', 'test abc', 's_double', 1e-1),
          named_struct('s_string' , 'test xyz', 's_double', 2e-1)) END
     , CASE WHEN n % 31 = 0 THEN NULL ELSE
    named_struct('s_string', 'test abc', 's_double', 1e-1) END
     , CASE WHEN n % 33 = 0 THEN NULL ELSE
    map(1, array(named_struct('s_string', 'test abc', 's_double', 1e-1),
                 named_struct('s_string' , 'test xyz', 's_double', 2e-1))) END
FROM presto_test_sequence
LIMIT 100
;

CREATE TABLE presto_test_types_sequencefile
    STORED AS SEQUENCEFILE
AS SELECT * FROM presto_test_types_orc;

CREATE TABLE presto_test_types_rctext
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe'
    STORED AS RCFILE
AS SELECT * FROM presto_test_types_orc;

CREATE TABLE presto_test_types_rcbinary
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe'
    STORED AS RCFILE
AS SELECT * FROM presto_test_types_orc;

CREATE TABLE presto_test_types_textfile
    STORED AS TEXTFILE
AS SELECT * FROM presto_test_types_orc;

CREATE TABLE presto_test_types_parquet
    STORED AS PARQUET
AS SELECT * FROM presto_test_types_orc;

ALTER TABLE presto_test_types_textfile ADD COLUMNS (new_column INT);
ALTER TABLE presto_test_types_sequencefile ADD COLUMNS (new_column INT);
ALTER TABLE presto_test_types_rctext ADD COLUMNS (new_column INT);
ALTER TABLE presto_test_types_rcbinary ADD COLUMNS (new_column INT);
ALTER TABLE presto_test_types_orc ADD COLUMNS (new_column INT);
ALTER TABLE presto_test_types_parquet ADD COLUMNS (new_column INT);


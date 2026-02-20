/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.tests.product.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import io.trino.jdbc.Row;
import io.trino.jdbc.TrinoArray;
import io.trino.plugin.hive.HiveTimestampPrecision;
import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.QueryResult;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.hive.HiveTimestampPrecision.NANOSECONDS;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * JUnit 5 port of TestHiveCoercionOnUnpartitionedTable.
 * <p>
 * Tests Hive type coercion on unpartitioned tables with ORC and Parquet storage formats.
 * This tests reading data from Hive after altering column types, verifying
 * that Trino can properly coerce the data.
 */
@ProductTest
@RequiresEnvironment(HiveStorageFormatsEnvironment.class)
@TestGroup.StorageFormats
class TestHiveCoercionOnUnpartitionedTable
{
    private static final String HIVE_VERSION = "3.1";

    @Test
    void testHiveCoercionOrc(HiveStorageFormatsEnvironment env)
    {
        doTestHiveCoercion(env, "ORC");
    }

    @Test
    void testHiveCoercionWithDifferentTimestampPrecisionOrc(HiveStorageFormatsEnvironment env)
    {
        doTestHiveCoercionWithDifferentTimestampPrecision(env, "ORC");
    }

    @Test
    void testHiveCoercionParquet(HiveStorageFormatsEnvironment env)
    {
        doTestHiveCoercion(env, "PARQUET");
    }

    @Test
    void testHiveCoercionWithDifferentTimestampPrecisionParquet(HiveStorageFormatsEnvironment env)
    {
        doTestHiveCoercionWithDifferentTimestampPrecision(env, "PARQUET");
    }

    private void doTestHiveCoercion(HiveStorageFormatsEnvironment env, String fileFormat)
    {
        String tableName = format("%s_hive_coercion_unpartitioned_%s", fileFormat.toLowerCase(ENGLISH), randomNameSuffix());

        try {
            createCoercionTable(env, tableName, fileFormat);

            List<Object> booleanToVarcharVal = tableName.toLowerCase(ENGLISH).contains("parquet") ?
                    ImmutableList.of("true", "false") : ImmutableList.of("TRUE", "FALSE");

            insertTableRows(env, tableName);
            alterTableColumnTypes(env, tableName);
            assertProperAlteredTableSchema(env, tableName);

            List<String> allColumns = ImmutableList.of(
                    "row_to_row", "list_to_list", "map_to_map", "boolean_to_varchar",
                    "string_to_boolean", "special_string_to_boolean", "numeric_string_to_boolean", "varchar_to_boolean",
                    "tinyint_to_smallint", "tinyint_to_int", "tinyint_to_bigint", "tinyint_to_varchar",
                    "tinyint_to_string", "tinyint_to_double", "tinyint_to_shortdecimal", "tinyint_to_longdecimal",
                    "smallint_to_int", "smallint_to_bigint", "smallint_to_varchar", "smallint_to_string",
                    "smallint_to_double", "smallint_to_shortdecimal", "smallint_to_longdecimal",
                    "int_to_bigint", "int_to_varchar", "int_to_string", "int_to_double",
                    "int_to_shortdecimal", "int_to_longdecimal",
                    "bigint_to_double", "bigint_to_varchar", "bigint_to_string",
                    "bigint_to_shortdecimal", "bigint_to_longdecimal",
                    "float_to_double", "float_to_string", "float_to_bounded_varchar", "float_infinity_to_string",
                    "double_to_float", "double_to_string", "double_to_bounded_varchar", "double_infinity_to_string",
                    "shortdecimal_to_shortdecimal", "shortdecimal_to_longdecimal",
                    "longdecimal_to_shortdecimal", "longdecimal_to_longdecimal",
                    "float_to_decimal", "double_to_decimal", "decimal_to_float", "decimal_to_double",
                    "longdecimal_to_tinyint", "shortdecimal_to_tinyint",
                    "longdecimal_to_smallint", "shortdecimal_to_smallint", "too_big_shortdecimal_to_smallint",
                    "longdecimal_to_int", "shortdecimal_to_int", "shortdecimal_with_0_scale_to_int",
                    "longdecimal_to_bigint", "shortdecimal_to_bigint",
                    "short_decimal_to_varchar", "long_decimal_to_varchar",
                    "short_decimal_to_bounded_varchar", "long_decimal_to_bounded_varchar",
                    "varchar_to_tinyint", "string_to_tinyint", "varchar_to_smallint", "string_to_smallint",
                    "varchar_to_integer", "string_to_integer", "varchar_to_bigint", "string_to_bigint",
                    "varchar_to_bigger_varchar", "varchar_to_smaller_varchar",
                    "varchar_to_date", "varchar_to_distant_date",
                    "varchar_to_float", "string_to_float", "varchar_to_float_infinity", "varchar_to_special_float",
                    "varchar_to_double", "string_to_double", "varchar_to_double_infinity", "varchar_to_special_double",
                    "date_to_string", "date_to_bounded_varchar",
                    "char_to_bigger_char", "char_to_smaller_char", "char_to_string",
                    "char_to_bigger_varchar", "char_to_smaller_varchar",
                    "string_to_char", "varchar_to_bigger_char", "varchar_to_smaller_char",
                    "timestamp_millis_to_date", "timestamp_micros_to_date", "timestamp_nanos_to_date",
                    "timestamp_to_string", "timestamp_to_bounded_varchar", "timestamp_to_smaller_varchar",
                    "smaller_varchar_to_timestamp", "varchar_to_timestamp",
                    "binary_to_string", "binary_to_smaller_varchar", "id");

            Function<Engine, Map<String, List<Object>>> expected = engine -> expectedValuesForEngineProvider(engine, tableName, booleanToVarcharVal);

            // For Trino, remove unsupported columns
            List<String> trinoReadColumns = removeUnsupportedColumnsForTrino(allColumns, tableName);
            Map<String, List<Object>> expectedTrinoResults = expected.apply(Engine.TRINO);
            String trinoSelectQuery = format("SELECT %s FROM %s", String.join(", ", trinoReadColumns), tableName);
            assertQueryResults(env, Engine.TRINO, trinoSelectQuery, expectedTrinoResults, trinoReadColumns, 2);

            // Additional assertions for VARBINARY coercion
            if (trinoReadColumns.contains("binary_to_string")) {
                List<Object> hexRepresentedValue = tableName.toLowerCase(ENGLISH).contains("orc") ?
                        ImmutableList.of("3538206637206266206266206266", "3538206637206266206266206266203538") :
                        ImmutableList.of("58EFBFBDEFBFBDEFBFBDEFBFBD", "58EFBFBDEFBFBDEFBFBDEFBFBD58");

                assertQueryResults(env, Engine.TRINO,
                        format("SELECT to_hex(cast(binary_to_string as varbinary)) as hex_representation FROM %s", tableName),
                        ImmutableMap.of("hex_representation", hexRepresentedValue),
                        ImmutableList.of("hex_representation"), 2);
            }

            // For Hive, remove unsupported columns
            List<String> hiveReadColumns = removeUnsupportedColumnsForHive(allColumns, tableName);
            Map<String, List<Object>> expectedHiveResults = expected.apply(Engine.HIVE);
            String hiveSelectQuery = format("SELECT %s FROM %s", String.join(", ", hiveReadColumns), tableName);
            assertQueryResults(env, Engine.HIVE, hiveSelectQuery, expectedHiveResults, hiveReadColumns, 2);

            // Hive hex representation assertion
            if (hiveReadColumns.contains("binary_to_string")) {
                List<Object> hexRepresentedValue = tableName.toLowerCase(ENGLISH).contains("orc") ?
                        ImmutableList.of("3538206637206266206266206266", "3538206637206266206266206266203538") :
                        ImmutableList.of("58F7BFBFBF", "58F7BFBFBF58");

                assertQueryResults(env, Engine.HIVE,
                        format("SELECT hex(binary_to_string) as hex_representation FROM %s", tableName),
                        ImmutableMap.of("hex_representation", hexRepresentedValue),
                        ImmutableList.of("hex_representation"), 2);
            }

            assertNestedSubFields(env, tableName);
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    private void doTestHiveCoercionWithDifferentTimestampPrecision(HiveStorageFormatsEnvironment env, String fileFormat)
    {
        String tableName = format("%s_hive_timestamp_coercion_unpartitioned_%s", fileFormat.toLowerCase(ENGLISH), randomNameSuffix());

        try {
            createTimestampCoercionTable(env, tableName, fileFormat);

            env.executeTrinoInSession(session -> {
                session.executeUpdate("SET SESSION hive.timestamp_precision = 'NANOSECONDS'");
                session.executeUpdate(
                    """
                    INSERT INTO %s
                        SELECT
                            (CAST(ROW (timestamp_value, -1, timestamp_value, CAST(timestamp_value AS VARCHAR), timestamp_value) AS ROW(keep TIMESTAMP(9), si2i SMALLINT, timestamp2string TIMESTAMP(9), string2timestamp VARCHAR, timestamp2date TIMESTAMP(9)))),
                            ARRAY [CAST(ROW (timestamp_value, -1, timestamp_value, CAST(timestamp_value AS VARCHAR), timestamp_value) AS ROW (keep TIMESTAMP(9), si2i SMALLINT, timestamp2string TIMESTAMP(9), string2timestamp VARCHAR, timestamp2date TIMESTAMP(9)))],
                            MAP (ARRAY [2], ARRAY [CAST(ROW (timestamp_value, -1, timestamp_value, CAST(timestamp_value AS VARCHAR), timestamp_value) AS ROW (keep TIMESTAMP(9), si2i SMALLINT, timestamp2string TIMESTAMP(9), string2timestamp VARCHAR, timestamp2date TIMESTAMP(9)))]),
                            timestamp_value,
                            CAST(timestamp_value AS VARCHAR),
                            timestamp_value,
                            1
                        FROM (VALUES
                            (TIMESTAMP '2121-07-15 15:30:12.123499'),
                            (TIMESTAMP '2121-07-15 15:30:12.123500'),
                            (TIMESTAMP '2121-07-15 15:30:12.123501'),
                            (TIMESTAMP '2121-07-15 15:30:12.123499999'),
                            (TIMESTAMP '2121-07-15 15:30:12.123500000'),
                            (TIMESTAMP '2121-07-15 15:30:12.123500001')) AS t (timestamp_value)
                    """.formatted(tableName));
            });

            env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN timestamp_row_to_row timestamp_row_to_row struct<keep:timestamp, si2i:int, timestamp2string:string, string2timestamp:timestamp, timestamp2date:date>", tableName));
            env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN timestamp_list_to_list timestamp_list_to_list array<struct<keep:timestamp, si2i:int, timestamp2string:string, string2timestamp:timestamp, timestamp2date:date>>", tableName));
            env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN timestamp_map_to_map timestamp_map_to_map map<int,struct<keep:timestamp, si2i:int, timestamp2string:string, string2timestamp:timestamp, timestamp2date:date>>", tableName));
            env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN timestamp_to_string timestamp_to_string string", tableName));
            env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN string_to_timestamp string_to_timestamp TIMESTAMP", tableName));
            env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN timestamp_to_date timestamp_to_date DATE", tableName));

            for (HiveTimestampPrecision hiveTimestampPrecision : HiveTimestampPrecision.values()) {
                String timestampType = "timestamp(%d)".formatted(hiveTimestampPrecision.getPrecision());
                assertThat(executeTrinoWithTimestampPrecision(env, hiveTimestampPrecision, "SHOW COLUMNS FROM " + tableName).project(1, 2)).containsExactlyInOrder(
                        row("timestamp_row_to_row", "row(\"keep\" %1$s, \"si2i\" integer, \"timestamp2string\" varchar, \"string2timestamp\" %1$s, \"timestamp2date\" date)".formatted(timestampType)),
                        row("timestamp_list_to_list", "array(row(\"keep\" %1$s, \"si2i\" integer, \"timestamp2string\" varchar, \"string2timestamp\" %1$s, \"timestamp2date\" date))".formatted(timestampType)),
                        row("timestamp_map_to_map", "map(integer, row(\"keep\" %1$s, \"si2i\" integer, \"timestamp2string\" varchar, \"string2timestamp\" %1$s, \"timestamp2date\" date))".formatted(timestampType)),
                        row("timestamp_to_string", "varchar"),
                        row("string_to_timestamp", timestampType),
                        row("timestamp_to_date", "date"),
                        row("id", "bigint"));

                List<String> allColumns = ImmutableList.of(
                        "timestamp_row_to_row", "timestamp_list_to_list", "timestamp_map_to_map",
                        "timestamp_to_string", "string_to_timestamp", "timestamp_to_date", "id");

                List<String> trinoReadColumns = removeUnsupportedColumnsForTrino(allColumns, tableName);
                Map<String, List<Object>> expectedTrinoResults = Maps.filterKeys(
                        expectedRowsForEngineProvider(Engine.TRINO, hiveTimestampPrecision),
                        trinoReadColumns::contains);

                String trinoReadQuery = format("SELECT %s FROM %s", String.join(", ", trinoReadColumns), tableName);
                assertQueryResults(env, Engine.TRINO, trinoReadQuery, expectedTrinoResults, trinoReadColumns, 6, hiveTimestampPrecision);

                List<String> hiveReadColumns = removeUnsupportedColumnsForHive(allColumns, tableName);
                Map<String, List<Object>> expectedHiveResults = Maps.filterKeys(
                        expectedRowsForEngineProvider(Engine.HIVE, hiveTimestampPrecision),
                        hiveReadColumns::contains);

                String hiveSelectQuery = format("SELECT %s FROM %s", String.join(", ", hiveReadColumns), tableName);
                assertQueryResults(env, Engine.HIVE, hiveSelectQuery, expectedHiveResults, hiveReadColumns, 6, hiveTimestampPrecision);
            }
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    private void createCoercionTable(HiveStorageFormatsEnvironment env, String tableName, String fileFormat)
    {
        env.executeHiveUpdate(format("""
                CREATE TABLE %s(
                    row_to_row                         STRUCT<keep: STRING, ti2si: TINYINT, si2int: SMALLINT, int2bi: INT, bi2vc: BIGINT, lower2uppercase: BIGINT>,
                    list_to_list                       ARRAY<STRUCT<ti2int: TINYINT, si2bi: SMALLINT, bi2vc: BIGINT, remove: STRING>>,
                    map_to_map                         MAP<TINYINT, STRUCT<ti2bi: TINYINT, int2bi: INT, float2double: FLOAT>>,
                    boolean_to_varchar                 BOOLEAN,
                    string_to_boolean                  STRING,
                    special_string_to_boolean          STRING,
                    numeric_string_to_boolean          STRING,
                    varchar_to_boolean                 VARCHAR(5),
                    tinyint_to_smallint                TINYINT,
                    tinyint_to_int                     TINYINT,
                    tinyint_to_bigint                  TINYINT,
                    tinyint_to_varchar                 TINYINT,
                    tinyint_to_string                  TINYINT,
                    tinyint_to_double                  TINYINT,
                    tinyint_to_shortdecimal            TINYINT,
                    tinyint_to_longdecimal             TINYINT,
                    smallint_to_int                    SMALLINT,
                    smallint_to_bigint                 SMALLINT,
                    smallint_to_varchar                SMALLINT,
                    smallint_to_string                 SMALLINT,
                    smallint_to_double                 SMALLINT,
                    smallint_to_shortdecimal           SMALLINT,
                    smallint_to_longdecimal            SMALLINT,
                    int_to_bigint                      INT,
                    int_to_varchar                     INT,
                    int_to_string                      INT,
                    int_to_double                      INT,
                    int_to_shortdecimal                INT,
                    int_to_longdecimal                 INT,
                    bigint_to_double                   BIGINT,
                    bigint_to_varchar                  BIGINT,
                    bigint_to_string                   BIGINT,
                    bigint_to_shortdecimal             BIGINT,
                    bigint_to_longdecimal              BIGINT,
                    float_to_double                    FLOAT,
                    float_to_string                    FLOAT,
                    float_to_bounded_varchar           FLOAT,
                    float_infinity_to_string           FLOAT,
                    double_to_float                    DOUBLE,
                    double_to_string                   DOUBLE,
                    double_to_bounded_varchar          DOUBLE,
                    double_infinity_to_string          DOUBLE,
                    shortdecimal_to_shortdecimal       DECIMAL(10,2),
                    shortdecimal_to_longdecimal        DECIMAL(10,2),
                    longdecimal_to_shortdecimal        DECIMAL(20,12),
                    longdecimal_to_longdecimal         DECIMAL(20,12),
                    longdecimal_to_tinyint             DECIMAL(20,12),
                    shortdecimal_to_tinyint            DECIMAL(10,2),
                    longdecimal_to_smallint            DECIMAL(20,12),
                    shortdecimal_to_smallint           DECIMAL(10,2),
                    too_big_shortdecimal_to_smallint   DECIMAL(10,2),
                    longdecimal_to_int                 DECIMAL(20,12),
                    shortdecimal_to_int                DECIMAL(10,2),
                    shortdecimal_with_0_scale_to_int   DECIMAL(10,0),
                    longdecimal_to_bigint              DECIMAL(20,4),
                    shortdecimal_to_bigint             DECIMAL(10,2),
                    float_to_decimal                   FLOAT,
                    double_to_decimal                  DOUBLE,
                    decimal_to_float                   DECIMAL(10,5),
                    decimal_to_double                  DECIMAL(10,5),
                    short_decimal_to_varchar           DECIMAL(10,5),
                    long_decimal_to_varchar            DECIMAL(20,12),
                    short_decimal_to_bounded_varchar   DECIMAL(10,5),
                    long_decimal_to_bounded_varchar    DECIMAL(20,12),
                    varchar_to_tinyint                 VARCHAR(4),
                    string_to_tinyint                  STRING,
                    varchar_to_smallint                VARCHAR(6),
                    string_to_smallint                 STRING,
                    varchar_to_integer                 VARCHAR(11),
                    string_to_integer                  STRING,
                    varchar_to_bigint                  VARCHAR(40),
                    string_to_bigint                   STRING,
                    varchar_to_bigger_varchar          VARCHAR(3),
                    varchar_to_smaller_varchar         VARCHAR(3),
                    varchar_to_date                    VARCHAR(10),
                    varchar_to_distant_date            VARCHAR(12),
                    varchar_to_float                   VARCHAR(40),
                    string_to_float                    STRING,
                    varchar_to_float_infinity          VARCHAR(40),
                    varchar_to_special_float           VARCHAR(40),
                    varchar_to_double                  VARCHAR(40),
                    string_to_double                   STRING,
                    varchar_to_double_infinity         VARCHAR(40),
                    varchar_to_special_double          VARCHAR(40),
                    date_to_string                     DATE,
                    date_to_bounded_varchar            DATE,
                    char_to_bigger_char                CHAR(3),
                    char_to_smaller_char               CHAR(3),
                    char_to_string                     CHAR(3),
                    char_to_bigger_varchar             CHAR(3),
                    char_to_smaller_varchar            CHAR(3),
                    string_to_char                     STRING,
                    varchar_to_bigger_char             VARCHAR(4),
                    varchar_to_smaller_char            VARCHAR(20),
                    timestamp_millis_to_date           TIMESTAMP,
                    timestamp_micros_to_date           TIMESTAMP,
                    timestamp_nanos_to_date            TIMESTAMP,
                    timestamp_to_string                TIMESTAMP,
                    timestamp_to_bounded_varchar       TIMESTAMP,
                    timestamp_to_smaller_varchar       TIMESTAMP,
                    smaller_varchar_to_timestamp       VARCHAR(4),
                    varchar_to_timestamp               STRING,
                    binary_to_string                   BINARY,
                    binary_to_smaller_varchar          BINARY,
                    id                                 BIGINT)
                STORED AS %s
                """, tableName, fileFormat));
    }

    private void createTimestampCoercionTable(HiveStorageFormatsEnvironment env, String tableName, String fileFormat)
    {
        env.executeHiveUpdate(format("""
                CREATE TABLE %s(
                    timestamp_row_to_row       STRUCT<keep: TIMESTAMP, si2i: SMALLINT, timestamp2string: TIMESTAMP, string2timestamp: STRING, timestamp2date: TIMESTAMP>,
                    timestamp_list_to_list     ARRAY<STRUCT<keep: TIMESTAMP, si2i: SMALLINT, timestamp2string: TIMESTAMP, string2timestamp: STRING, timestamp2date: TIMESTAMP>>,
                    timestamp_map_to_map       MAP<SMALLINT, STRUCT<keep: TIMESTAMP, si2i: SMALLINT, timestamp2string: TIMESTAMP, string2timestamp: STRING, timestamp2date: TIMESTAMP>>,
                    timestamp_to_string        TIMESTAMP,
                    string_to_timestamp        STRING,
                    timestamp_to_date          TIMESTAMP,
                    id                         BIGINT)
                STORED AS %s
                """, tableName, fileFormat));
    }

    private void insertTableRows(HiveStorageFormatsEnvironment env, String tableName)
    {
        env.executeTrinoInSession(session -> {
            session.executeUpdate("SET SESSION hive.timestamp_precision = 'NANOSECONDS'");
            session.executeUpdate(format(
                "INSERT INTO %s VALUES " +
                        "(" +
                        "  CAST(ROW ('as is', -1, 100, 2323, 12345, 2) AS ROW(keep VARCHAR, ti2si TINYINT, si2int SMALLINT, int2bi INTEGER, bi2vc BIGINT, lower2uppercase BIGINT)), " +
                        "  ARRAY [CAST(ROW (2, -101, 12345, 'removed') AS ROW (ti2int TINYINT, si2bi SMALLINT, bi2vc BIGINT, remove VARCHAR))], " +
                        "  MAP (ARRAY [TINYINT '2'], ARRAY [CAST(ROW (-3, 2323, REAL '0.5') AS ROW (ti2bi TINYINT, int2bi INTEGER, float2double REAL))]), " +
                        "  TRUE, 'TRUE', ' ', '-123', 'No', " +
                        "  TINYINT '-1', TINYINT '2', TINYINT '-3', TINYINT '0', TINYINT '127', TINYINT '4', TINYINT '5', TINYINT '6', " +
                        "  SMALLINT '100', SMALLINT '-101', SMALLINT '0', SMALLINT '32767', SMALLINT '1024', SMALLINT '2048', SMALLINT '4096', " +
                        "  INTEGER '2323', INTEGER '0', INTEGER '2147483647', INTEGER '16384', INTEGER '16385', INTEGER '16386', " +
                        "  1234567890, 12345, 9223372036854775807, 9223372, 9223372036, " +
                        "  REAL '0.5', REAL '0.5', REAL '0.5', REAL 'Infinity', " +
                        "  DOUBLE '0.5', DOUBLE '12345.12345', DOUBLE '12345.12345', DOUBLE 'Infinity', " +
                        "  DECIMAL '12345678.12', DECIMAL '12345678.12', DECIMAL '12345678.123456123456', DECIMAL '12345678.123456123456', " +
                        "  DECIMAL '13.1313', DECIMAL '11.99', DECIMAL '140.1323', DECIMAL '142.99', DECIMAL '312343.99', " +
                        "  DECIMAL '312345.99', DECIMAL '312347.13433', DECIMAL '123', DECIMAL '123471234567.9989', DECIMAL '12345678.12', " +
                        "  REAL '12345.12345', DOUBLE '12345.12345', DECIMAL '12345.12345', DECIMAL '12345.12345', " +
                        "  DECIMAL '12345.12345', DECIMAL '12345678.123456123456', DECIMAL '12345.12345', DECIMAL '12345678.123456123456', " +
                        "  '-127', '2147483647', '-32768', '2147483647', '-2147483648', '2147483647', '0', '1234567890123', " +
                        "  'abc', 'abc', '2023-09-28', '8000-04-13', '1234.567', '1234.01234', 'Infinity', 'NaN', " +
                        "  '1234.567', '1234.01234', 'Infinity', 'NaN', " +
                        "  DATE '2023-09-28', DATE '2000-04-13', 'abc', 'abc', 'ab', 'cd', 'a', 'Bigger Value', 'Hi  ', 'TrinoDB', " +
                        "  TIMESTAMP '2022-12-31 23:59:59.999', TIMESTAMP '2023-12-31 23:59:59.999999', TIMESTAMP '2024-12-31 23:59:59.999999999', " +
                        "  TIMESTAMP '2121-07-15 15:30:12.123', TIMESTAMP '2121-07-15 15:30:12.123', TIMESTAMP '2121-07-15 15:30:12.123', " +
                        "  '2121', '2019-01-29 23:59:59.123', X'58F7BFBFBF', X'58EDA080', 1), " +
                        "(" +
                        "  CAST(ROW (NULL, 1, -100, -2323, -12345, 2) AS ROW(keep VARCHAR, ti2si TINYINT, si2int SMALLINT, int2bi INTEGER, bi2vc BIGINT, lower2uppercase BIGINT)), " +
                        "  ARRAY [CAST(ROW (-2, 101, -12345, NULL) AS ROW (ti2int TINYINT, si2bi SMALLINT, bi2vc BIGINT, remove VARCHAR))], " +
                        "  MAP (ARRAY [TINYINT '-2'], ARRAY [CAST(ROW (null, -2323, REAL '-1.5') AS ROW (ti2bi TINYINT, int2bi INTEGER, float2double REAL))]), " +
                        "  FALSE, 'FAlSE', 'oFF', '-0', '', " +
                        "  TINYINT '1', TINYINT '-2', NULL, NULL, TINYINT '-128', TINYINT '-4', TINYINT '-5', TINYINT '-6', " +
                        "  SMALLINT '-100', SMALLINT '101', NULL, SMALLINT '-32768', SMALLINT '-1024', SMALLINT '-2048', SMALLINT '-4096', " +
                        "  INTEGER '-2323', NULL, INTEGER '-2147483648', INTEGER '-16384', INTEGER '-16385', INTEGER '-16386', " +
                        "  -1234567890, -12345, -9223372036854775808, -9223372, -9223372036, " +
                        "  REAL '-1.5', REAL '-1.5', REAL 'NaN', REAL '-Infinity', " +
                        "  DOUBLE '-1.5', DOUBLE 'NaN', DOUBLE '-12345.12345', DOUBLE '-Infinity', " +
                        "  DECIMAL '-12345678.12', DECIMAL '-12345678.12', DECIMAL '-12345678.123456123456', DECIMAL '-12345678.123456123456', " +
                        "  DECIMAL '-12.1313', DECIMAL '-10.99', DECIMAL '-141.1323', DECIMAL '-143.99', DECIMAL '-312342.99', " +
                        "  DECIMAL '-312346.99', DECIMAL '-312348.13433', DECIMAL '-124', DECIMAL '-123471234577.9989', DECIMAL '-12345678.12', " +
                        "  REAL '-12345.12345', DOUBLE '-12345.12345', DECIMAL '-12345.12345', DECIMAL '-12345.12345', " +
                        "  DECIMAL '-12345.12345', DECIMAL '-12345678.123456123456', DECIMAL '-12345.12345', DECIMAL '-12345678.123456123456', " +
                        "  '1270', '123e+1', '327680', '123e+1', '21474836480', '123e+1', '-9.223372e+18', 'Hello', " +
                        "  '\uD83D\uDCB0\uD83D\uDCB0\uD83D\uDCB0', '\uD83D\uDCB0\uD83D\uDCB0\uD83D\uDCB0', '2023-09-27', '1900-01-01', '-12345.6789', '0', '-4.4028235e+39f', 'Invalid Double', " +
                        "  '-12345.6789', '0', '-Infinity', 'Invalid Double', " +
                        "  DATE '2123-09-27', DATE '1900-01-01', '\uD83D\uDCB0\uD83D\uDCB0\uD83D\uDCB0', '\uD83D\uDCB0\uD83D\uDCB0\uD83D\uDCB0', '\uD83D\uDCB0\uD83D\uDCB0', '\uD83D\uDCB0\uD83D\uDCB0', '\uD83D\uDCB0', '\uD83D\uDCB0\uD83D\uDCB0\uD83D\uDCB0\uD83D\uDCB0\uD83D\uDCB0', '\uD83D\uDCB0 \uD83D\uDCB0\uD83D\uDCB0', '\uD83D\uDCB0 \uD83D\uDCB0', " +
                        "  TIMESTAMP '1970-01-01 00:00:00.123', TIMESTAMP '1970-01-01 00:00:00.123456', TIMESTAMP '1970-01-01 00:00:00.123456789', " +
                        "  TIMESTAMP '1970-01-01 00:00:00.123', TIMESTAMP '1970-01-01 00:00:00.123', TIMESTAMP '1970-01-01 00:00:00.123', " +
                        "  '1970', '1970-01-01 00:00:00.123', X'58F7BFBFBF58', X'58EDBFBF', 1)",
                tableName));
        });
    }

    private static void alterTableColumnTypes(HiveStorageFormatsEnvironment env, String tableName)
    {
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN row_to_row row_to_row struct<keep:string, ti2si:smallint, si2int:int, int2bi:bigint, bi2vc:string, LOWER2UPPERCASE:bigint>", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN list_to_list list_to_list array<struct<ti2int:int, si2bi:bigint, bi2vc:string>>", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN map_to_map map_to_map map<int,struct<ti2bi:bigint, int2bi:bigint, float2double:double, add:tinyint>>", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN boolean_to_varchar boolean_to_varchar varchar(5)", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN string_to_boolean string_to_boolean boolean", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN special_string_to_boolean special_string_to_boolean boolean", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN numeric_string_to_boolean numeric_string_to_boolean boolean", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN varchar_to_boolean varchar_to_boolean boolean", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN tinyint_to_smallint tinyint_to_smallint smallint", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN tinyint_to_int tinyint_to_int int", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN tinyint_to_bigint tinyint_to_bigint bigint", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN tinyint_to_varchar tinyint_to_varchar varchar(30)", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN tinyint_to_string tinyint_to_string string", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN tinyint_to_double tinyint_to_double double", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN tinyint_to_shortdecimal tinyint_to_shortdecimal decimal(10,2)", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN tinyint_to_longdecimal tinyint_to_longdecimal decimal(20,2)", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN smallint_to_int smallint_to_int int", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN smallint_to_bigint smallint_to_bigint bigint", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN smallint_to_varchar smallint_to_varchar varchar(30)", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN smallint_to_string smallint_to_string string", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN smallint_to_double smallint_to_double double", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN smallint_to_shortdecimal smallint_to_shortdecimal decimal(10,2)", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN smallint_to_longdecimal smallint_to_longdecimal decimal(20,2)", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN int_to_bigint int_to_bigint bigint", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN int_to_varchar int_to_varchar varchar(30)", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN int_to_string int_to_string string", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN int_to_double int_to_double double", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN int_to_shortdecimal int_to_shortdecimal decimal(10,2)", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN int_to_longdecimal int_to_longdecimal decimal(20,2)", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN bigint_to_double bigint_to_double double", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN bigint_to_varchar bigint_to_varchar varchar(30)", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN bigint_to_string bigint_to_string string", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN bigint_to_shortdecimal bigint_to_shortdecimal decimal(10,2)", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN bigint_to_longdecimal bigint_to_longdecimal decimal(20,2)", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN float_to_double float_to_double double", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN float_to_string float_to_string string", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN float_to_bounded_varchar float_to_bounded_varchar varchar(12)", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN float_infinity_to_string float_infinity_to_string string", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN double_to_float double_to_float float", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN double_to_string double_to_string string", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN double_to_bounded_varchar double_to_bounded_varchar varchar(12)", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN double_infinity_to_string double_infinity_to_string string", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN shortdecimal_to_shortdecimal shortdecimal_to_shortdecimal DECIMAL(18,4)", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN shortdecimal_to_longdecimal shortdecimal_to_longdecimal DECIMAL(20,4)", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN longdecimal_to_shortdecimal longdecimal_to_shortdecimal DECIMAL(12,2)", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN longdecimal_to_longdecimal longdecimal_to_longdecimal DECIMAL(38,14)", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN longdecimal_to_tinyint longdecimal_to_tinyint TINYINT", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN shortdecimal_to_tinyint shortdecimal_to_tinyint TINYINT", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN longdecimal_to_smallint longdecimal_to_smallint SMALLINT", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN shortdecimal_to_smallint shortdecimal_to_smallint SMALLINT", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN too_big_shortdecimal_to_smallint too_big_shortdecimal_to_smallint SMALLINT", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN longdecimal_to_int longdecimal_to_int INTEGER", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN shortdecimal_to_int shortdecimal_to_int INTEGER", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN shortdecimal_with_0_scale_to_int shortdecimal_with_0_scale_to_int INTEGER", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN longdecimal_to_bigint longdecimal_to_bigint BIGINT", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN shortdecimal_to_bigint shortdecimal_to_bigint BIGINT", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN float_to_decimal float_to_decimal DECIMAL(10,5)", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN double_to_decimal double_to_decimal DECIMAL(10,5)", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN decimal_to_float decimal_to_float float", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN decimal_to_double decimal_to_double double", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN short_decimal_to_varchar short_decimal_to_varchar string", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN long_decimal_to_varchar long_decimal_to_varchar string", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN short_decimal_to_bounded_varchar short_decimal_to_bounded_varchar varchar(30)", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN long_decimal_to_bounded_varchar long_decimal_to_bounded_varchar varchar(30)", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN varchar_to_tinyint varchar_to_tinyint tinyint", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN string_to_tinyint string_to_tinyint tinyint", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN varchar_to_smallint varchar_to_smallint smallint", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN string_to_smallint string_to_smallint smallint", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN varchar_to_integer varchar_to_integer integer", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN string_to_integer string_to_integer integer", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN varchar_to_bigint varchar_to_bigint bigint", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN string_to_bigint string_to_bigint bigint", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN varchar_to_bigger_varchar varchar_to_bigger_varchar varchar(4)", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN varchar_to_smaller_varchar varchar_to_smaller_varchar varchar(2)", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN varchar_to_date varchar_to_date date", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN date_to_string date_to_string string", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN date_to_bounded_varchar date_to_bounded_varchar varchar(12)", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN varchar_to_distant_date varchar_to_distant_date date", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN varchar_to_float varchar_to_float float", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN string_to_float string_to_float float", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN varchar_to_float_infinity varchar_to_float_infinity float", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN varchar_to_special_float varchar_to_special_float float", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN varchar_to_double varchar_to_double double", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN string_to_double string_to_double double", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN varchar_to_double_infinity varchar_to_double_infinity double", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN varchar_to_special_double varchar_to_special_double double", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN char_to_bigger_char char_to_bigger_char char(4)", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN char_to_smaller_char char_to_smaller_char char(2)", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN char_to_string char_to_string string", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN char_to_bigger_varchar char_to_bigger_varchar varchar(4)", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN char_to_smaller_varchar char_to_smaller_varchar varchar(2)", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN string_to_char string_to_char char(1)", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN varchar_to_bigger_char varchar_to_bigger_char char(6)", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN varchar_to_smaller_char varchar_to_smaller_char char(2)", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN timestamp_millis_to_date timestamp_millis_to_date date", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN timestamp_micros_to_date timestamp_micros_to_date date", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN timestamp_nanos_to_date timestamp_nanos_to_date date", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN timestamp_to_string timestamp_to_string string", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN timestamp_to_bounded_varchar timestamp_to_bounded_varchar varchar(30)", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN timestamp_to_smaller_varchar timestamp_to_smaller_varchar varchar(4)", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN smaller_varchar_to_timestamp smaller_varchar_to_timestamp timestamp", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN varchar_to_timestamp varchar_to_timestamp timestamp", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN binary_to_string binary_to_string string", tableName));
        env.executeHiveUpdate(format("ALTER TABLE %s CHANGE COLUMN binary_to_smaller_varchar binary_to_smaller_varchar varchar(2)", tableName));
    }

    private void assertProperAlteredTableSchema(HiveStorageFormatsEnvironment env, String tableName)
    {
        assertThat(env.executeTrino("SHOW COLUMNS FROM " + tableName).project(1, 2)).containsExactlyInOrder(
                row("row_to_row", "row(\"keep\" varchar, \"ti2si\" smallint, \"si2int\" integer, \"int2bi\" bigint, \"bi2vc\" varchar, \"lower2uppercase\" bigint)"),
                row("list_to_list", "array(row(\"ti2int\" integer, \"si2bi\" bigint, \"bi2vc\" varchar))"),
                row("map_to_map", "map(integer, row(\"ti2bi\" bigint, \"int2bi\" bigint, \"float2double\" double, \"add\" tinyint))"),
                row("boolean_to_varchar", "varchar(5)"),
                row("string_to_boolean", "boolean"),
                row("special_string_to_boolean", "boolean"),
                row("numeric_string_to_boolean", "boolean"),
                row("varchar_to_boolean", "boolean"),
                row("tinyint_to_smallint", "smallint"),
                row("tinyint_to_int", "integer"),
                row("tinyint_to_bigint", "bigint"),
                row("tinyint_to_varchar", "varchar(30)"),
                row("tinyint_to_string", "varchar"),
                row("tinyint_to_double", "double"),
                row("tinyint_to_shortdecimal", "decimal(10,2)"),
                row("tinyint_to_longdecimal", "decimal(20,2)"),
                row("smallint_to_int", "integer"),
                row("smallint_to_bigint", "bigint"),
                row("smallint_to_varchar", "varchar(30)"),
                row("smallint_to_string", "varchar"),
                row("smallint_to_double", "double"),
                row("smallint_to_shortdecimal", "decimal(10,2)"),
                row("smallint_to_longdecimal", "decimal(20,2)"),
                row("int_to_bigint", "bigint"),
                row("int_to_varchar", "varchar(30)"),
                row("int_to_string", "varchar"),
                row("int_to_double", "double"),
                row("int_to_shortdecimal", "decimal(10,2)"),
                row("int_to_longdecimal", "decimal(20,2)"),
                row("bigint_to_double", "double"),
                row("bigint_to_varchar", "varchar(30)"),
                row("bigint_to_string", "varchar"),
                row("bigint_to_shortdecimal", "decimal(10,2)"),
                row("bigint_to_longdecimal", "decimal(20,2)"),
                row("float_to_double", "double"),
                row("float_to_string", "varchar"),
                row("float_to_bounded_varchar", "varchar(12)"),
                row("float_infinity_to_string", "varchar"),
                row("double_to_float", "real"),
                row("double_to_string", "varchar"),
                row("double_to_bounded_varchar", "varchar(12)"),
                row("double_infinity_to_string", "varchar"),
                row("shortdecimal_to_shortdecimal", "decimal(18,4)"),
                row("shortdecimal_to_longdecimal", "decimal(20,4)"),
                row("longdecimal_to_shortdecimal", "decimal(12,2)"),
                row("longdecimal_to_longdecimal", "decimal(38,14)"),
                row("longdecimal_to_tinyint", "tinyint"),
                row("shortdecimal_to_tinyint", "tinyint"),
                row("longdecimal_to_smallint", "smallint"),
                row("shortdecimal_to_smallint", "smallint"),
                row("too_big_shortdecimal_to_smallint", "smallint"),
                row("longdecimal_to_int", "integer"),
                row("shortdecimal_to_int", "integer"),
                row("shortdecimal_with_0_scale_to_int", "integer"),
                row("longdecimal_to_bigint", "bigint"),
                row("shortdecimal_to_bigint", "bigint"),
                row("float_to_decimal", "decimal(10,5)"),
                row("double_to_decimal", "decimal(10,5)"),
                row("decimal_to_float", "real"),
                row("decimal_to_double", "double"),
                row("short_decimal_to_varchar", "varchar"),
                row("long_decimal_to_varchar", "varchar"),
                row("short_decimal_to_bounded_varchar", "varchar(30)"),
                row("long_decimal_to_bounded_varchar", "varchar(30)"),
                row("varchar_to_tinyint", "tinyint"),
                row("string_to_tinyint", "tinyint"),
                row("varchar_to_smallint", "smallint"),
                row("string_to_smallint", "smallint"),
                row("varchar_to_integer", "integer"),
                row("string_to_integer", "integer"),
                row("varchar_to_bigint", "bigint"),
                row("string_to_bigint", "bigint"),
                row("varchar_to_bigger_varchar", "varchar(4)"),
                row("varchar_to_smaller_varchar", "varchar(2)"),
                row("varchar_to_date", "date"),
                row("varchar_to_distant_date", "date"),
                row("varchar_to_float", "real"),
                row("string_to_float", "real"),
                row("varchar_to_float_infinity", "real"),
                row("varchar_to_special_float", "real"),
                row("varchar_to_double", "double"),
                row("string_to_double", "double"),
                row("varchar_to_double_infinity", "double"),
                row("varchar_to_special_double", "double"),
                row("date_to_string", "varchar"),
                row("date_to_bounded_varchar", "varchar(12)"),
                row("char_to_bigger_char", "char(4)"),
                row("char_to_smaller_char", "char(2)"),
                row("char_to_string", "varchar"),
                row("char_to_bigger_varchar", "varchar(4)"),
                row("char_to_smaller_varchar", "varchar(2)"),
                row("string_to_char", "char(1)"),
                row("varchar_to_bigger_char", "char(6)"),
                row("varchar_to_smaller_char", "char(2)"),
                row("timestamp_millis_to_date", "date"),
                row("timestamp_micros_to_date", "date"),
                row("timestamp_nanos_to_date", "date"),
                row("timestamp_to_string", "varchar"),
                row("timestamp_to_bounded_varchar", "varchar(30)"),
                row("timestamp_to_smaller_varchar", "varchar(4)"),
                row("smaller_varchar_to_timestamp", "timestamp(9)"),
                row("varchar_to_timestamp", "timestamp(9)"),
                row("binary_to_string", "varchar"),
                row("binary_to_smaller_varchar", "varchar(2)"),
                row("id", "bigint"));
    }

    // Splitting remainder into Part 2 file due to size - see continuation
    private Map<String, List<Object>> expectedValuesForEngineProvider(Engine engine, String tableName, List<Object> booleanToVarcharVal)
    {
        String hiveValueForCaseChangeField;
        String coercedNaN = "NaN";
        String coercedFloatBoundedVarcharNaN = "NaN";
        String coercedDoubleToStringNaN = "NaN";
        Predicate<String> isFormat = formatName -> tableName.toLowerCase(ENGLISH).contains(formatName);
        boolean hiveOrc = engine == Engine.HIVE && isFormat.test("orc");
        boolean trinoOrc = engine == Engine.TRINO && isFormat.test("orc");
        Map<String, List<Object>> specialCoercion = ImmutableMap.of(
                "string_to_boolean", ImmutableList.of(true, false),
                "special_string_to_boolean", ImmutableList.of(true, false),
                "numeric_string_to_boolean", ImmutableList.of(true, true),
                "varchar_to_boolean", ImmutableList.of(false, false),
                "varchar_to_tinyint", ImmutableList.of(-127, -10),
                "varchar_to_smallint", ImmutableList.of(-32768, 0),
                "binary_to_string", ImmutableList.of("X\uFFFD\uFFFD\uFFFD\uFFFD", "X\uFFFD\uFFFD\uFFFD\uFFFDX"),
                "binary_to_smaller_varchar", ImmutableList.of("X\uFFFD", "X\uFFFD"));

        if (isFormat.test("orc")) {
            hiveValueForCaseChangeField = "\"LOWER2UPPERCASE\":null";
            specialCoercion = ImmutableMap.of(
                    "string_to_boolean", Arrays.asList(null, null),
                    "special_string_to_boolean", Arrays.asList(null, null),
                    "numeric_string_to_boolean", ImmutableList.of(true, false),
                    "varchar_to_boolean", Arrays.asList(null, null),
                    "varchar_to_tinyint", Arrays.asList(-127, null),
                    "varchar_to_smallint", Arrays.asList(-32768, null),
                    "binary_to_string", ImmutableList.of("58 f7 bf bf bf", "58 f7 bf bf bf 58"),
                    "binary_to_smaller_varchar", ImmutableList.of("58", "58"));
        }
        else {
            hiveValueForCaseChangeField = "\"LOWER2UPPERCASE\":2";
        }
        if (isFormat.test("orc")) {
            coercedFloatBoundedVarcharNaN = null;
            coercedDoubleToStringNaN = null;
        }
        if (trinoOrc) {
            coercedNaN = null;
        }

        return ImmutableMap.<String, List<Object>>builder()
                .put("row_to_row", ImmutableList.of(
                        engine == Engine.TRINO ?
                                Row.builder()
                                        .addField("keep", "as is")
                                        .addField("ti2si", (short) -1)
                                        .addField("si2int", 100)
                                        .addField("int2bi", 2323L)
                                        .addField("bi2vc", "12345")
                                        .addField("lower2uppercase", 2L)
                                        .build() :
                                String.format("{\"keep\":\"as is\",\"ti2si\":-1,\"si2int\":100,\"int2bi\":2323,\"bi2vc\":\"12345\",%s}", hiveValueForCaseChangeField),
                        engine == Engine.TRINO ?
                                Row.builder()
                                        .addField("keep", null)
                                        .addField("ti2si", (short) 1)
                                        .addField("si2int", -100)
                                        .addField("int2bi", -2323L)
                                        .addField("bi2vc", "-12345")
                                        .addField("lower2uppercase", 2L)
                                        .build() :
                                String.format("{\"keep\":null,\"ti2si\":1,\"si2int\":-100,\"int2bi\":-2323,\"bi2vc\":\"-12345\",%s}", hiveValueForCaseChangeField)))
                .put("list_to_list", ImmutableList.of(
                        engine == Engine.TRINO ?
                                ImmutableList.of(Row.builder()
                                        .addField("ti2int", 2)
                                        .addField("si2bi", -101L)
                                        .addField("bi2vc", "12345")
                                        .build()) :
                                "[{\"ti2int\":2,\"si2bi\":-101,\"bi2vc\":\"12345\"}]",
                        engine == Engine.TRINO ?
                                ImmutableList.of(Row.builder()
                                        .addField("ti2int", -2)
                                        .addField("si2bi", 101L)
                                        .addField("bi2vc", "-12345")
                                        .build()) :
                                "[{\"ti2int\":-2,\"si2bi\":101,\"bi2vc\":\"-12345\"}]"))
                .put("map_to_map", ImmutableList.of(
                        engine == Engine.TRINO ?
                                ImmutableMap.of(2, Row.builder()
                                        .addField("ti2bi", -3L)
                                        .addField("int2bi", 2323L)
                                        .addField("float2double", 0.5)
                                        .addField("add", null)
                                        .build()) :
                                "{2:{\"ti2bi\":-3,\"int2bi\":2323,\"float2double\":0.5,\"add\":null}}",
                        engine == Engine.TRINO ?
                                ImmutableMap.of(-2, Row.builder()
                                        .addField("ti2bi", null)
                                        .addField("int2bi", -2323L)
                                        .addField("float2double", -1.5)
                                        .addField("add", null)
                                        .build()) :
                                "{-2:{\"ti2bi\":null,\"int2bi\":-2323,\"float2double\":-1.5,\"add\":null}}"))
                .put("boolean_to_varchar", booleanToVarcharVal)
                .putAll(specialCoercion)
                .put("tinyint_to_smallint", ImmutableList.of(-1, 1))
                .put("tinyint_to_int", ImmutableList.of(2, -2))
                .put("tinyint_to_bigint", Arrays.asList(-3L, null))
                .put("tinyint_to_varchar", Arrays.asList("0", null))
                .put("tinyint_to_string", ImmutableList.of("127", "-128"))
                .put("tinyint_to_double", Arrays.asList(-4D, 4D))
                .put("tinyint_to_shortdecimal", Arrays.asList(new BigDecimal(-5), new BigDecimal(5)))
                .put("tinyint_to_longdecimal", Arrays.asList(new BigDecimal(-6), new BigDecimal(6)))
                .put("smallint_to_int", ImmutableList.of(100, -100))
                .put("smallint_to_bigint", ImmutableList.of(-101L, 101L))
                .put("smallint_to_varchar", Arrays.asList("0", null))
                .put("smallint_to_string", ImmutableList.of("32767", "-32768"))
                .put("smallint_to_double", ImmutableList.of(-1024D, 1024D))
                .put("smallint_to_shortdecimal", Arrays.asList(new BigDecimal(-2048), new BigDecimal(2048)))
                .put("smallint_to_longdecimal", Arrays.asList(new BigDecimal(-4096), new BigDecimal(4096)))
                .put("int_to_bigint", ImmutableList.of(2323L, -2323L))
                .put("int_to_varchar", Arrays.asList("0", null))
                .put("int_to_string", ImmutableList.of("2147483647", "-2147483648"))
                .put("int_to_double", ImmutableList.of(-16384D, 16384D))
                .put("int_to_shortdecimal", Arrays.asList(new BigDecimal(-16385), new BigDecimal(16385)))
                .put("int_to_longdecimal", Arrays.asList(new BigDecimal(-16386), new BigDecimal(16386)))
                .put("bigint_to_double", ImmutableList.of(-1234567890D, 1234567890D))
                .put("bigint_to_varchar", ImmutableList.of("12345", "-12345"))
                .put("bigint_to_string", ImmutableList.of("9223372036854775807", "-9223372036854775808"))
                .put("bigint_to_shortdecimal", Arrays.asList(new BigDecimal(-9223372L), new BigDecimal(9223372L)))
                .put("bigint_to_longdecimal", Arrays.asList(new BigDecimal(-9223372036L), new BigDecimal(9223372036L)))
                .put("float_to_double", ImmutableList.of(0.5, -1.5))
                .put("float_to_string", ImmutableList.of("0.5", "-1.5"))
                .put("float_to_bounded_varchar", Arrays.asList("0.5", coercedFloatBoundedVarcharNaN))
                .put("float_infinity_to_string", ImmutableList.of("Infinity", "-Infinity"))
                .put("double_to_float", ImmutableList.of(0.5, -1.5))
                .put("double_to_string", Arrays.asList("12345.12345", coercedDoubleToStringNaN))
                .put("double_to_bounded_varchar", ImmutableList.of("12345.12345", "-12345.12345"))
                .put("double_infinity_to_string", ImmutableList.of("Infinity", "-Infinity"))
                .put("shortdecimal_to_shortdecimal", ImmutableList.of(new BigDecimal("12345678.1200"), new BigDecimal("-12345678.1200")))
                .put("shortdecimal_to_longdecimal", ImmutableList.of(new BigDecimal("12345678.1200"), new BigDecimal("-12345678.1200")))
                .put("longdecimal_to_shortdecimal", ImmutableList.of(new BigDecimal("12345678.12"), new BigDecimal("-12345678.12")))
                .put("longdecimal_to_longdecimal", ImmutableList.of(new BigDecimal("12345678.12345612345600"), new BigDecimal("-12345678.12345612345600")))
                .put("float_to_decimal", ImmutableList.of(new BigDecimal("12345.12300"), new BigDecimal("-12345.12300")))
                .put("double_to_decimal", ImmutableList.of(new BigDecimal("12345.12345"), new BigDecimal("-12345.12345")))
                .put("decimal_to_float", ImmutableList.of(Float.parseFloat("12345.124"), -Float.parseFloat("12345.124")))
                .put("decimal_to_double", ImmutableList.of(12345.12345, -12345.12345))
                .put("longdecimal_to_tinyint", ImmutableList.of(13, -12))
                .put("shortdecimal_to_tinyint", ImmutableList.of(11, -10))
                .put("longdecimal_to_smallint", ImmutableList.of(140, -141))
                .put("shortdecimal_to_smallint", ImmutableList.of(142, -143))
                .put("too_big_shortdecimal_to_smallint", Arrays.asList(null, null))
                .put("longdecimal_to_int", ImmutableList.of(312345, -312346))
                .put("shortdecimal_to_int", ImmutableList.of(312347, -312348))
                .put("shortdecimal_with_0_scale_to_int", ImmutableList.of(123, -124))
                .put("longdecimal_to_bigint", ImmutableList.of(123471234567L, -123471234577L))
                .put("shortdecimal_to_bigint", ImmutableList.of(12345678, -12345678))
                .put("short_decimal_to_varchar", ImmutableList.of("12345.12345", "-12345.12345"))
                .put("long_decimal_to_varchar", ImmutableList.of("12345678.123456123456", "-12345678.123456123456"))
                .put("short_decimal_to_bounded_varchar", ImmutableList.of("12345.12345", "-12345.12345"))
                .put("long_decimal_to_bounded_varchar", ImmutableList.of("12345678.123456123456", "-12345678.123456123456"))
                .put("string_to_tinyint", Arrays.asList(null, null))
                .put("string_to_smallint", Arrays.asList(null, null))
                .put("string_to_integer", Arrays.asList(2147483647, null))
                .put("varchar_to_integer", Arrays.asList(-2147483648, null))
                .put("varchar_to_bigint", Arrays.asList(0, null))
                .put("string_to_bigint", Arrays.asList(1234567890123L, null))
                .put("varchar_to_bigger_varchar", ImmutableList.of("abc", hiveOrc ? "\uFFFD\uFFFD\uFFFD\uFFFD" : "\uD83D\uDCB0\uD83D\uDCB0\uD83D\uDCB0"))
                .put("varchar_to_smaller_varchar", ImmutableList.of("ab", hiveOrc ? "\uFFFD\uFFFD" : "\uD83D\uDCB0\uD83D\uDCB0"))
                .put("string_to_char", ImmutableList.of("B", hiveOrc ? "\uFFFD" : "\uD83D\uDCB0"))
                .put("varchar_to_bigger_char", ImmutableList.of("Hi    ", hiveOrc ? "\uFFFD\uFFFD\uFFFD\uFFFD \uFFFD" : "\uD83D\uDCB0 \uD83D\uDCB0\uD83D\uDCB0  "))
                .put("varchar_to_smaller_char", ImmutableList.of("Tr", hiveOrc ? "\uFFFD\uFFFD" : "\uD83D\uDCB0 "))
                .put("varchar_to_date", ImmutableList.of(Date.valueOf("2023-09-28"), Date.valueOf("2023-09-27")))
                .put("varchar_to_distant_date", ImmutableList.of(Date.valueOf("8000-04-13"), Date.valueOf("1900-01-01")))
                .put("varchar_to_float", ImmutableList.of(1234.567f, -12345.6789f))
                .put("string_to_float", ImmutableList.of(1234.01234f, 0f))
                .put("varchar_to_float_infinity", ImmutableList.of(Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY))
                .put("varchar_to_special_float", Arrays.asList(coercedNaN == null ? null : Float.NaN, null))
                .put("varchar_to_double", ImmutableList.of(1234.567, -12345.6789))
                .put("string_to_double", ImmutableList.of(1234.01234, 0D))
                .put("varchar_to_double_infinity", ImmutableList.of(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY))
                .put("varchar_to_special_double", Arrays.asList(coercedNaN == null ? null : Double.NaN, null))
                .put("date_to_string", ImmutableList.of("2023-09-28", "2123-09-27"))
                .put("date_to_bounded_varchar", ImmutableList.of("2000-04-13", "1900-01-01"))
                .put("char_to_bigger_char", ImmutableList.of("abc ", hiveOrc ? "\uFFFD\uFFFD\uFFFD\uFFFD" : "\uD83D\uDCB0\uD83D\uDCB0\uD83D\uDCB0 "))
                .put("char_to_smaller_char", ImmutableList.of("ab", hiveOrc ? "\uFFFD\uFFFD" : "\uD83D\uDCB0\uD83D\uDCB0"))
                .put("char_to_string", ImmutableList.of("ab", "\uD83D\uDCB0\uD83D\uDCB0"))
                .put("char_to_bigger_varchar", ImmutableList.of("cd", hiveOrc ? "\uFFFD\uFFFD\uFFFD\uFFFD" : "\uD83D\uDCB0\uD83D\uDCB0"))
                .put("char_to_smaller_varchar", ImmutableList.of("a", hiveOrc ? "\uFFFD\uFFFD" : "\uD83D\uDCB0"))
                .put("timestamp_millis_to_date", ImmutableList.of(Date.valueOf("2022-12-31"), Date.valueOf("1970-01-01")))
                .put("timestamp_micros_to_date", ImmutableList.of(Date.valueOf("2023-12-31"), Date.valueOf("1970-01-01")))
                .put("timestamp_nanos_to_date", ImmutableList.of(Date.valueOf("2024-12-31"), Date.valueOf("1970-01-01")))
                .put("timestamp_to_string", ImmutableList.of("2121-07-15 15:30:12.123", "1970-01-01 00:00:00.123"))
                .put("timestamp_to_bounded_varchar", ImmutableList.of("2121-07-15 15:30:12.123", "1970-01-01 00:00:00.123"))
                .put("timestamp_to_smaller_varchar", ImmutableList.of("2121", "1970"))
                .put("smaller_varchar_to_timestamp", Arrays.asList(null, null))
                .put("varchar_to_timestamp", Arrays.asList(Timestamp.valueOf("2019-01-29 23:59:59.123"), Timestamp.valueOf("1970-01-01 00:00:00.123")))
                .put("id", ImmutableList.of(1, 1))
                .buildOrThrow();
    }

    private Map<String, List<Object>> expectedRowsForEngineProvider(Engine engine, HiveTimestampPrecision timestampPrecision)
    {
        List<Object> insertedTimestampAsString = ImmutableList.of(
                "2121-07-15 15:30:12.123499",
                "2121-07-15 15:30:12.1235",
                "2121-07-15 15:30:12.123501",
                "2121-07-15 15:30:12.123499999",
                "2121-07-15 15:30:12.1235",
                "2121-07-15 15:30:12.123500001");

        if (engine == Engine.HIVE) {
            List<Object> baseData = ImmutableList.of(
                    "{\"keep\":\"%s\",\"si2i\":-1,\"timestamp2string\":\"%s\",\"string2timestamp\":\"%s\",\"timestamp2date\":\"2121-07-15\"}".formatted(insertedTimestampAsString.get(0), insertedTimestampAsString.get(0), insertedTimestampAsString.get(0)),
                    "{\"keep\":\"%s\",\"si2i\":-1,\"timestamp2string\":\"%s\",\"string2timestamp\":\"%s\",\"timestamp2date\":\"2121-07-15\"}".formatted(insertedTimestampAsString.get(1), insertedTimestampAsString.get(1), insertedTimestampAsString.get(1)),
                    "{\"keep\":\"%s\",\"si2i\":-1,\"timestamp2string\":\"%s\",\"string2timestamp\":\"%s\",\"timestamp2date\":\"2121-07-15\"}".formatted(insertedTimestampAsString.get(2), insertedTimestampAsString.get(2), insertedTimestampAsString.get(2)),
                    "{\"keep\":\"%s\",\"si2i\":-1,\"timestamp2string\":\"%s\",\"string2timestamp\":\"%s\",\"timestamp2date\":\"2121-07-15\"}".formatted(insertedTimestampAsString.get(3), insertedTimestampAsString.get(3), insertedTimestampAsString.get(3)),
                    "{\"keep\":\"%s\",\"si2i\":-1,\"timestamp2string\":\"%s\",\"string2timestamp\":\"%s\",\"timestamp2date\":\"2121-07-15\"}".formatted(insertedTimestampAsString.get(4), insertedTimestampAsString.get(4), insertedTimestampAsString.get(4)),
                    "{\"keep\":\"%s\",\"si2i\":-1,\"timestamp2string\":\"%s\",\"string2timestamp\":\"%s\",\"timestamp2date\":\"2121-07-15\"}".formatted(insertedTimestampAsString.get(5), insertedTimestampAsString.get(5), insertedTimestampAsString.get(5)));
            return ImmutableMap.<String, List<Object>>builder()
                    .put("timestamp_row_to_row", baseData)
                    .put("timestamp_list_to_list", baseData.stream().map(ImmutableList::of).map(Objects::toString).collect(toImmutableList()))
                    .put("timestamp_map_to_map", baseData.stream().map("{2:%s}"::formatted).collect(toImmutableList()))
                    .put("timestamp_to_string", insertedTimestampAsString)
                    .put("string_to_timestamp", insertedTimestampAsString.stream().map(String.class::cast).map(Timestamp::valueOf).collect(toImmutableList()))
                    .put("timestamp_to_date", nCopies(6, Date.valueOf("2121-07-15")))
                    .put("id", nCopies(6, 1))
                    .buildOrThrow();
        }

        List<Object> timestampValue = timestampValue(timestampPrecision);
        List<Object> nestedTimestampAsString = insertedTimestampAsString;

        List<Object> baseData = Streams.zip(
                timestampValue.stream(),
                nestedTimestampAsString.stream(),
                (timestamp, timestampCoerced) -> Row.builder()
                        .addField("keep", timestamp)
                        .addField("si2i", -1)
                        .addField("timestamp2string", timestampCoerced)
                        .addField("string2timestamp", timestamp)
                        .addField("timestamp2date", Date.valueOf("2121-07-15"))
                        .build())
                .collect(toImmutableList());

        return ImmutableMap.<String, List<Object>>builder()
                .put("timestamp_row_to_row", baseData)
                .put("timestamp_list_to_list", baseData.stream().map(ImmutableList::of).collect(toImmutableList()))
                .put("timestamp_map_to_map", baseData.stream().map(entry -> ImmutableMap.of(2, entry)).collect(toImmutableList()))
                .put("timestamp_to_string", insertedTimestampAsString)
                .put("string_to_timestamp", timestampValue)
                .put("timestamp_to_date", nCopies(6, Date.valueOf("2121-07-15")))
                .put("id", nCopies(6, 1))
                .buildOrThrow();
    }

    private static List<Object> timestampValue(HiveTimestampPrecision timestampPrecision)
    {
        return switch (timestampPrecision) {
            case MILLISECONDS -> ImmutableList.of(
                    Timestamp.valueOf("2121-07-15 15:30:12.123"),
                    Timestamp.valueOf("2121-07-15 15:30:12.124"),
                    Timestamp.valueOf("2121-07-15 15:30:12.124"),
                    Timestamp.valueOf("2121-07-15 15:30:12.123"),
                    Timestamp.valueOf("2121-07-15 15:30:12.124"),
                    Timestamp.valueOf("2121-07-15 15:30:12.124"));
            case MICROSECONDS -> ImmutableList.of(
                    Timestamp.valueOf("2121-07-15 15:30:12.123499"),
                    Timestamp.valueOf("2121-07-15 15:30:12.1235"),
                    Timestamp.valueOf("2121-07-15 15:30:12.123501"),
                    Timestamp.valueOf("2121-07-15 15:30:12.1235"),
                    Timestamp.valueOf("2121-07-15 15:30:12.1235"),
                    Timestamp.valueOf("2121-07-15 15:30:12.1235"));
            case NANOSECONDS -> ImmutableList.of(
                    Timestamp.valueOf("2121-07-15 15:30:12.123499"),
                    Timestamp.valueOf("2121-07-15 15:30:12.1235"),
                    Timestamp.valueOf("2121-07-15 15:30:12.123501"),
                    Timestamp.valueOf("2121-07-15 15:30:12.123499999"),
                    Timestamp.valueOf("2121-07-15 15:30:12.1235"),
                    Timestamp.valueOf("2121-07-15 15:30:12.123500001"));
        };
    }

    private List<String> removeUnsupportedColumnsForHive(List<String> columns, String tableName)
    {
        Map<ColumnContext, String> expectedExceptions = expectedExceptionsWithHiveContext();

        Set<String> unsupportedColumns = expectedExceptions.keySet().stream()
                .filter(context -> context.hiveVersion().orElseThrow().equals(HIVE_VERSION) && tableName.contains(context.format()))
                .map(ColumnContext::column)
                .collect(toImmutableSet());

        return columns.stream()
                .filter(column -> !unsupportedColumns.contains(column))
                .collect(toImmutableList());
    }

    private List<String> removeUnsupportedColumnsForTrino(List<String> columns, String tableName)
    {
        Map<ColumnContext, String> expectedExceptions = expectedExceptionsWithTrinoContext();

        Set<String> unsupportedColumns = expectedExceptions.keySet().stream()
                .filter(context -> tableName.contains(context.format()))
                .map(ColumnContext::column)
                .collect(toImmutableSet());

        return columns.stream()
                .filter(column -> !unsupportedColumns.contains(column))
                .collect(toImmutableList());
    }

    private void assertNestedSubFields(HiveStorageFormatsEnvironment env, String tableName)
    {
        Predicate<String> isFormat = formatName -> tableName.toLowerCase(ENGLISH).contains(formatName);

        Map<String, List<Object>> expectedNestedFieldTrino = ImmutableMap.of("nested_field", ImmutableList.of(2L, 2L));
        Map<String, List<Object>> expectedNestedFieldHive;
        if (isFormat.test("orc")) {
            expectedNestedFieldHive = ImmutableMap.of("nested_field", Arrays.asList(null, null));
        }
        else {
            expectedNestedFieldHive = expectedNestedFieldTrino;
        }
        String subfieldQueryLowerCase = format("SELECT row_to_row.lower2uppercase nested_field FROM %s", tableName);
        String subfieldQueryUpperCase = format("SELECT row_to_row.LOWER2UPPERCASE nested_field FROM %s", tableName);
        List<String> expectedColumns = ImmutableList.of("nested_field");

        // Assert Trino behavior - only if row_to_row is not excluded
        List<String> trinoColumns = removeUnsupportedColumnsForTrino(ImmutableList.of("row_to_row"), tableName);
        if (!trinoColumns.isEmpty()) {
            assertQueryResults(env, Engine.TRINO, subfieldQueryUpperCase, expectedNestedFieldTrino, expectedColumns, 2);
            assertQueryResults(env, Engine.TRINO, subfieldQueryLowerCase, expectedNestedFieldTrino, expectedColumns, 2);
        }

        // Assert Hive behavior
        List<String> hiveColumns = removeUnsupportedColumnsForHive(ImmutableList.of("row_to_row"), tableName);
        if (!hiveColumns.isEmpty()) {
            assertQueryResults(env, Engine.HIVE, subfieldQueryUpperCase, expectedNestedFieldHive, expectedColumns, 2);
            assertQueryResults(env, Engine.HIVE, subfieldQueryLowerCase, expectedNestedFieldHive, expectedColumns, 2);
        }
    }

    private Map<ColumnContext, String> expectedExceptionsWithHiveContext()
    {
        return ImmutableMap.<ColumnContext, String>builder()
                // Base exceptions from parent class
                .put(columnContext("3.1", "parquet", "row_to_row"), "org.apache.hadoop.io.LongWritable cannot be cast to org.apache.hadoop.io.IntWritable")
                .put(columnContext("3.1", "parquet", "list_to_list"), "org.apache.hadoop.io.LongWritable cannot be cast to org.apache.hadoop.hive.serde2.io.ShortWritable")
                .put(columnContext("3.1", "parquet", "map_to_map"), "org.apache.hadoop.io.LongWritable cannot be cast to org.apache.hadoop.hive.serde2.io.ByteWritable")
                .put(columnContext("3.1", "parquet", "tinyint_to_bigint"), "org.apache.hadoop.io.LongWritable cannot be cast to org.apache.hadoop.hive.serde2.io.ByteWritable")
                .put(columnContext("3.1", "parquet", "tinyint_to_double"), "org.apache.hadoop.io.DoubleWritable cannot be cast to org.apache.hadoop.hive.serde2.io.ByteWritable")
                .put(columnContext("3.1", "parquet", "tinyint_to_shortdecimal"), "org.apache.hadoop.hive.serde2.io.HiveDecimalWritable cannot be cast to org.apache.hadoop.io.ByteWritable")
                .put(columnContext("3.1", "parquet", "tinyint_to_longdecimal"), "org.apache.hadoop.hive.serde2.io.HiveDecimalWritable cannot be cast to org.apache.hadoop.io.ByteWritable")
                .put(columnContext("3.1", "parquet", "smallint_to_bigint"), "org.apache.hadoop.io.LongWritable cannot be cast to org.apache.hadoop.hive.serde2.io.ShortWritable")
                .put(columnContext("3.1", "parquet", "smallint_to_double"), "org.apache.hadoop.io.DoubleWritable cannot be cast to org.apache.hadoop.hive.serde2.io.ShortWritable")
                .put(columnContext("3.1", "parquet", "smallint_to_shortdecimal"), "org.apache.hadoop.hive.serde2.io.HiveDecimalWritable cannot be cast to org.apache.hadoop.io.ShortWritable")
                .put(columnContext("3.1", "parquet", "smallint_to_longdecimal"), "org.apache.hadoop.hive.serde2.io.HiveDecimalWritable cannot be cast to org.apache.hadoop.io.ShortWritable")
                .put(columnContext("3.1", "parquet", "int_to_bigint"), "org.apache.hadoop.io.LongWritable cannot be cast to org.apache.hadoop.io.IntWritable")
                .put(columnContext("3.1", "parquet", "int_to_double"), "org.apache.hadoop.io.DoubleWritable cannot be cast to org.apache.hadoop.io.IntWritable")
                .put(columnContext("3.1", "parquet", "int_to_shortdecimal"), "org.apache.hadoop.hive.serde2.io.HiveDecimalWritable cannot be cast to org.apache.hadoop.io.IntWritable")
                .put(columnContext("3.1", "parquet", "int_to_longdecimal"), "org.apache.hadoop.hive.serde2.io.HiveDecimalWritable cannot be cast to org.apache.hadoop.io.IntWritable")
                .put(columnContext("3.1", "parquet", "bigint_to_double"), "org.apache.hadoop.io.DoubleWritable cannot be cast to org.apache.hadoop.io.LongWritable")
                .put(columnContext("3.1", "parquet", "bigint_to_shortdecimal"), "org.apache.hadoop.hive.serde2.io.HiveDecimalWritable cannot be cast to org.apache.hadoop.io.LongWritable")
                .put(columnContext("3.1", "parquet", "bigint_to_longdecimal"), "org.apache.hadoop.hive.serde2.io.HiveDecimalWritable cannot be cast to org.apache.hadoop.io.LongWritable")
                .put(columnContext("3.1", "parquet", "float_to_double"), "org.apache.hadoop.hive.serde2.io.DoubleWritable cannot be cast to org.apache.hadoop.io.FloatWritable")
                // Additional Parquet-specific exceptions for unpartitioned tables
                .put(columnContext("3.1", "parquet", "string_to_boolean"), "org.apache.hadoop.io.Text cannot be cast to org.apache.hadoop.io.BooleanWritable")
                .put(columnContext("3.1", "parquet", "special_string_to_boolean"), "org.apache.hadoop.io.Text cannot be cast to org.apache.hadoop.io.BooleanWritable")
                .put(columnContext("3.1", "parquet", "numeric_string_to_boolean"), "org.apache.hadoop.io.Text cannot be cast to org.apache.hadoop.io.BooleanWritable")
                .put(columnContext("3.1", "parquet", "varchar_to_boolean"), "org.apache.hadoop.io.Text cannot be cast to org.apache.hadoop.io.BooleanWritable")
                .put(columnContext("3.1", "parquet", "float_to_decimal"), "org.apache.hadoop.io.FloatWritable cannot be cast to org.apache.hadoop.hive.serde2.io.HiveDecimalWritable")
                .put(columnContext("3.1", "parquet", "double_to_decimal"), "org.apache.hadoop.io.DoubleWritable cannot be cast to org.apache.hadoop.hive.serde2.io.HiveDecimalWritable")
                .put(columnContext("3.1", "parquet", "decimal_to_float"), "org.apache.hadoop.hive.serde2.io.HiveDecimalWritable cannot be cast to org.apache.hadoop.io.DoubleWritable")
                .put(columnContext("3.1", "parquet", "decimal_to_double"), "org.apache.hadoop.hive.serde2.io.HiveDecimalWritable cannot be cast to org.apache.hadoop.io.DoubleWritable")
                .put(columnContext("3.1", "parquet", "longdecimal_to_tinyint"), "org.apache.hadoop.hive.serde2.io.HiveDecimalWritable cannot be cast to org.apache.hadoop.hive.serde2.io.ByteWritable")
                .put(columnContext("3.1", "parquet", "shortdecimal_to_tinyint"), "org.apache.hadoop.hive.serde2.io.HiveDecimalWritable cannot be cast to org.apache.hadoop.hive.serde2.io.ByteWritable")
                .put(columnContext("3.1", "parquet", "longdecimal_to_smallint"), "org.apache.hadoop.hive.serde2.io.HiveDecimalWritable cannot be cast to org.apache.hadoop.hive.serde2.io.ShortWritable")
                .put(columnContext("3.1", "parquet", "shortdecimal_to_smallint"), "org.apache.hadoop.hive.serde2.io.HiveDecimalWritable cannot be cast to org.apache.hadoop.hive.serde2.io.ShortWritable")
                .put(columnContext("3.1", "parquet", "too_big_shortdecimal_to_smallint"), "org.apache.hadoop.hive.serde2.io.HiveDecimalWritable cannot be cast to org.apache.hadoop.hive.serde2.io.ShortWritable")
                .put(columnContext("3.1", "parquet", "longdecimal_to_int"), "org.apache.hadoop.hive.serde2.io.HiveDecimalWritable cannot be cast to org.apache.hadoop.io.IntWritable")
                .put(columnContext("3.1", "parquet", "shortdecimal_to_int"), "org.apache.hadoop.hive.serde2.io.HiveDecimalWritable cannot be cast to org.apache.hadoop.io.IntWritable")
                .put(columnContext("3.1", "parquet", "shortdecimal_with_0_scale_to_int"), "org.apache.hadoop.hive.serde2.io.HiveDecimalWritable cannot be cast to org.apache.hadoop.io.IntWritable")
                .put(columnContext("3.1", "parquet", "longdecimal_to_bigint"), "org.apache.hadoop.hive.serde2.io.HiveDecimalWritable cannot be cast to org.apache.hadoop.io.LongWritable")
                .put(columnContext("3.1", "parquet", "shortdecimal_to_bigint"), "org.apache.hadoop.hive.serde2.io.HiveDecimalWritable cannot be cast to org.apache.hadoop.io.LongWritable")
                .put(columnContext("3.1", "parquet", "varchar_to_tinyint"), "org.apache.hadoop.io.Text cannot be cast to org.apache.hadoop.hive.serde2.io.ByteWritable")
                .put(columnContext("3.1", "parquet", "string_to_tinyint"), "org.apache.hadoop.io.Text cannot be cast to org.apache.hadoop.hive.serde2.io.ByteWritable")
                .put(columnContext("3.1", "parquet", "varchar_to_smallint"), "org.apache.hadoop.io.Text cannot be cast to org.apache.hadoop.hive.serde2.io.ShortWritable")
                .put(columnContext("3.1", "parquet", "string_to_smallint"), "org.apache.hadoop.io.Text cannot be cast to org.apache.hadoop.hive.serde2.io.ShortWritable")
                .put(columnContext("3.1", "parquet", "varchar_to_integer"), "org.apache.hadoop.io.Text cannot be cast to org.apache.hadoop.io.IntWritable")
                .put(columnContext("3.1", "parquet", "string_to_integer"), "org.apache.hadoop.io.Text cannot be cast to org.apache.hadoop.io.IntWritable")
                .put(columnContext("3.1", "parquet", "varchar_to_bigint"), "org.apache.hadoop.io.Text cannot be cast to org.apache.hadoop.io.LongWritable")
                .put(columnContext("3.1", "parquet", "string_to_bigint"), "org.apache.hadoop.io.Text cannot be cast to org.apache.hadoop.io.LongWritable")
                .put(columnContext("3.1", "parquet", "varchar_to_date"), "org.apache.hadoop.io.Text cannot be cast to org.apache.hadoop.hive.serde2.io.DateWritableV2")
                .put(columnContext("3.1", "parquet", "varchar_to_distant_date"), "org.apache.hadoop.io.Text cannot be cast to org.apache.hadoop.hive.serde2.io.DateWritableV2")
                .put(columnContext("3.1", "parquet", "varchar_to_float"), "org.apache.hadoop.io.Text cannot be cast to org.apache.hadoop.io.DoubleWritable")
                .put(columnContext("3.1", "parquet", "string_to_float"), "org.apache.hadoop.io.Text cannot be cast to org.apache.hadoop.io.DoubleWritable")
                .put(columnContext("3.1", "parquet", "varchar_to_float_infinity"), "org.apache.hadoop.io.Text cannot be cast to org.apache.hadoop.io.DoubleWritable")
                .put(columnContext("3.1", "parquet", "varchar_to_special_float"), "org.apache.hadoop.io.Text cannot be cast to org.apache.hadoop.io.DoubleWritable")
                .put(columnContext("3.1", "parquet", "varchar_to_double"), "org.apache.hadoop.io.Text cannot be cast to org.apache.hadoop.io.DoubleWritable")
                .put(columnContext("3.1", "parquet", "string_to_double"), "org.apache.hadoop.io.Text cannot be cast to org.apache.hadoop.io.DoubleWritable")
                .put(columnContext("3.1", "parquet", "varchar_to_double_infinity"), "org.apache.hadoop.io.Text cannot be cast to org.apache.hadoop.io.DoubleWritable")
                .put(columnContext("3.1", "parquet", "varchar_to_special_double"), "org.apache.hadoop.io.Text cannot be cast to org.apache.hadoop.io.DoubleWritable")
                .put(columnContext("3.1", "parquet", "date_to_string"), "Cannot inspect org.apache.hadoop.hive.serde2.io.DateWritableV2")
                .put(columnContext("3.1", "parquet", "date_to_bounded_varchar"), "Cannot inspect org.apache.hadoop.hive.serde2.io.DateWritableV2")
                .put(columnContext("3.1", "parquet", "timestamp_millis_to_date"), "org.apache.hadoop.hive.serde2.io.TimestampWritableV2 cannot be cast to org.apache.hadoop.hive.serde2.io.DateWritableV2")
                .put(columnContext("3.1", "parquet", "timestamp_micros_to_date"), "org.apache.hadoop.hive.serde2.io.TimestampWritableV2 cannot be cast to org.apache.hadoop.hive.serde2.io.DateWritableV2")
                .put(columnContext("3.1", "parquet", "timestamp_nanos_to_date"), "org.apache.hadoop.hive.serde2.io.TimestampWritableV2 cannot be cast to org.apache.hadoop.hive.serde2.io.DateWritableV2")
                .put(columnContext("3.1", "parquet", "smaller_varchar_to_timestamp"), "org.apache.hadoop.io.Text cannot be cast to org.apache.hadoop.hive.serde2.io.TimestampWritableV2")
                .put(columnContext("3.1", "parquet", "varchar_to_timestamp"), "org.apache.hadoop.io.Text cannot be cast to org.apache.hadoop.hive.serde2.io.TimestampWritableV2")
                .put(columnContext("3.1", "parquet", "timestamp_row_to_row"), "org.apache.hadoop.io.Text cannot be cast to org.apache.hadoop.hive.serde2.io.TimestampWritableV2")
                .put(columnContext("3.1", "parquet", "timestamp_list_to_list"), "org.apache.hadoop.io.Text cannot be cast to org.apache.hadoop.hive.serde2.io.TimestampWritableV2")
                .put(columnContext("3.1", "parquet", "timestamp_map_to_map"), "org.apache.hadoop.io.Text cannot be cast to org.apache.hadoop.hive.serde2.io.TimestampWritableV2")
                .put(columnContext("3.1", "parquet", "string_to_timestamp"), "org.apache.hadoop.io.Text cannot be cast to org.apache.hadoop.hive.serde2.io.TimestampWritableV2")
                .put(columnContext("3.1", "parquet", "timestamp_to_date"), "org.apache.hadoop.io.Text cannot be cast to org.apache.hadoop.hive.serde2.io.TimestampWritableV2")
                .put(columnContext("3.1", "parquet", "double_to_float"), "org.apache.hadoop.hive.serde2.io.DoubleWritable cannot be cast to org.apache.hadoop.io.FloatWritable")
                .put(columnContext("3.1", "parquet", "binary_to_string"), "org.apache.hadoop.io.BytesWritable cannot be cast to org.apache.hadoop.hive.serde2.io.HiveVarcharWritable")
                .put(columnContext("3.1", "parquet", "binary_to_smaller_varchar"), "org.apache.hadoop.io.BytesWritable cannot be cast to org.apache.hadoop.hive.serde2.io.HiveVarcharWritable")
                .buildOrThrow();
    }

    private Map<ColumnContext, String> expectedExceptionsWithTrinoContext()
    {
        // These are Parquet coercion limitations for unpartitioned tables in Trino
        return ImmutableMap.<ColumnContext, String>builder()
                .put(columnContext("parquet", "row_to_row"), "Unsupported Trino column type (varchar) for Parquet column")
                .put(columnContext("parquet", "list_to_list"), "Unsupported Trino column type (varchar) for Parquet column")
                .put(columnContext("parquet", "boolean_to_varchar"), "Unsupported Trino column type (varchar(5)) for Parquet column")
                .put(columnContext("parquet", "string_to_boolean"), "Unsupported Trino column type (boolean) for Parquet column")
                .put(columnContext("parquet", "special_string_to_boolean"), "Unsupported Trino column type (boolean) for Parquet column")
                .put(columnContext("parquet", "numeric_string_to_boolean"), "Unsupported Trino column type (boolean) for Parquet column")
                .put(columnContext("parquet", "varchar_to_boolean"), "Unsupported Trino column type (boolean) for Parquet column")
                .put(columnContext("parquet", "tinyint_to_longdecimal"), "Unsupported Trino column type (decimal(20,2)) for Parquet column")
                .put(columnContext("parquet", "smallint_to_longdecimal"), "Unsupported Trino column type (decimal(20,2)) for Parquet column")
                .put(columnContext("parquet", "int_to_longdecimal"), "Unsupported Trino column type (decimal(20,2)) for Parquet column")
                .put(columnContext("parquet", "bigint_to_shortdecimal"), "Unsupported Trino column type (decimal(10,2)) for Parquet column")
                .put(columnContext("parquet", "bigint_to_longdecimal"), "Unsupported Trino column type (decimal(20,2)) for Parquet column")
                .put(columnContext("parquet", "longdecimal_to_tinyint"), "Unsupported Trino column type (tinyint) for Parquet column")
                .put(columnContext("parquet", "shortdecimal_to_tinyint"), "Unsupported Trino column type (tinyint) for Parquet column")
                .put(columnContext("parquet", "longdecimal_to_smallint"), "Unsupported Trino column type (smallint) for Parquet column")
                .put(columnContext("parquet", "shortdecimal_to_smallint"), "Unsupported Trino column type (smallint) for Parquet column")
                .put(columnContext("parquet", "too_big_shortdecimal_to_smallint"), "Unsupported Trino column type (smallint) for Parquet column")
                .put(columnContext("parquet", "longdecimal_to_int"), "Unsupported Trino column type (integer) for Parquet column")
                .put(columnContext("parquet", "shortdecimal_to_int"), "Unsupported Trino column type (integer) for Parquet column")
                .put(columnContext("parquet", "longdecimal_to_bigint"), "Unsupported Trino column type (bigint) for Parquet column")
                .put(columnContext("parquet", "shortdecimal_to_bigint"), "Unsupported Trino column type (bigint) for Parquet column")
                .put(columnContext("parquet", "float_to_decimal"), "Unsupported Trino column type (decimal(10,5)) for Parquet column")
                .put(columnContext("parquet", "double_to_float"), "Unsupported Trino column type (real) for Parquet column")
                .put(columnContext("parquet", "double_to_decimal"), "Unsupported Trino column type (decimal(10,5)) for Parquet column")
                .put(columnContext("parquet", "decimal_to_float"), "Unsupported Trino column type (double) for Parquet column")
                .put(columnContext("parquet", "decimal_to_double"), "Unsupported Trino column type (double) for Parquet column")
                .put(columnContext("parquet", "short_decimal_to_bounded_varchar"), "Unsupported Trino column type (varchar(30)) for Parquet column")
                .put(columnContext("parquet", "long_decimal_to_bounded_varchar"), "Unsupported Trino column type (varchar(30)) for Parquet column")
                .put(columnContext("parquet", "varchar_to_tinyint"), "Unsupported Trino column type (tinyint) for Parquet column")
                .put(columnContext("parquet", "string_to_tinyint"), "Unsupported Trino column type (tinyint) for Parquet column")
                .put(columnContext("parquet", "varchar_to_smallint"), "Unsupported Trino column type (smallint) for Parquet column")
                .put(columnContext("parquet", "string_to_smallint"), "Unsupported Trino column type (smallint) for Parquet column")
                .put(columnContext("parquet", "varchar_to_integer"), "Unsupported Trino column type (integer) for Parquet column")
                .put(columnContext("parquet", "string_to_integer"), "Unsupported Trino column type (integer) for Parquet column")
                .put(columnContext("parquet", "varchar_to_bigint"), "Unsupported Trino column type (bigint) for Parquet column")
                .put(columnContext("parquet", "string_to_bigint"), "Unsupported Trino column type (bigint) for Parquet column")
                .put(columnContext("parquet", "varchar_to_date"), "Unsupported Trino column type (date) for Parquet column")
                .put(columnContext("parquet", "varchar_to_distant_date"), "Unsupported Trino column type (date) for Parquet column")
                .put(columnContext("parquet", "varchar_to_float"), "Unsupported Trino column type (double) for Parquet column")
                .put(columnContext("parquet", "string_to_float"), "Unsupported Trino column type (double) for Parquet column")
                .put(columnContext("parquet", "varchar_to_float_infinity"), "Unsupported Trino column type (double) for Parquet column")
                .put(columnContext("parquet", "varchar_to_special_float"), "Unsupported Trino column type (double) for Parquet column")
                .put(columnContext("parquet", "varchar_to_double"), "Unsupported Trino column type (double) for Parquet column")
                .put(columnContext("parquet", "string_to_double"), "Unsupported Trino column type (double) for Parquet column")
                .put(columnContext("parquet", "varchar_to_double_infinity"), "Unsupported Trino column type (double) for Parquet column")
                .put(columnContext("parquet", "varchar_to_special_double"), "Unsupported Trino column type (double) for Parquet column")
                .put(columnContext("parquet", "date_to_string"), "Unsupported Trino column type (varchar) for Parquet column")
                .put(columnContext("parquet", "date_to_bounded_varchar"), "Unsupported Trino column type (varchar(12)) for Parquet column")
                .put(columnContext("parquet", "timestamp_millis_to_date"), "Unsupported Trino column type (date) for Parquet column")
                .put(columnContext("parquet", "timestamp_micros_to_date"), "Unsupported Trino column type (date) for Parquet column")
                .put(columnContext("parquet", "timestamp_nanos_to_date"), "Unsupported Trino column type (date) for Parquet column")
                .put(columnContext("parquet", "smaller_varchar_to_timestamp"), "Unsupported Trino column type (timestamp(3)) for Parquet column")
                .put(columnContext("parquet", "varchar_to_timestamp"), "Unsupported Trino column type (timestamp(3)) for Parquet column")
                .put(columnContext("parquet", "timestamp_row_to_row"), "Unsupported Trino column type (varchar) for Parquet column")
                .put(columnContext("parquet", "timestamp_list_to_list"), "Unsupported Trino column type (varchar) for Parquet column")
                .put(columnContext("parquet", "timestamp_map_to_map"), "Unsupported Trino column type (varchar) for Parquet column")
                .put(columnContext("parquet", "string_to_timestamp"), "Unsupported Trino column type (timestamp(3)) for Parquet column")
                .put(columnContext("parquet", "timestamp_to_date"), "Unsupported Trino column type (date) for Parquet column")
                .put(columnContext("parquet", "binary_to_string"), "Varbinary to Varchar coercion not supported")
                .put(columnContext("parquet", "binary_to_smaller_varchar"), "Varbinary to Varchar coercion not supported")
                .buildOrThrow();
    }

    private void assertQueryResults(
            HiveStorageFormatsEnvironment env,
            Engine engine,
            String query,
            Map<String, List<Object>> expected,
            List<String> columns,
            int rowCount)
    {
        assertQueryResults(env, engine, query, expected, columns, rowCount, null);
    }

    private void assertQueryResults(
            HiveStorageFormatsEnvironment env,
            Engine engine,
            String query,
            Map<String, List<Object>> expected,
            List<String> columns,
            int rowCount,
            HiveTimestampPrecision hiveTimestampPrecision)
    {
        QueryResult actual = execute(env, engine, query, hiveTimestampPrecision);

        ImmutableList.Builder<io.trino.testing.containers.environment.Row> rowsBuilder = ImmutableList.builder();
        for (int row = 0; row < rowCount; row++) {
            List<Object> currentRow = new ArrayList<>();

            for (int column = 0; column < columns.size(); column++) {
                String columnName = columns.get(column);
                checkArgument(expected.containsKey(columnName), "columnName should be present in expected results");
                currentRow.add(expected.get(columnName).get(row));
            }

            rowsBuilder.add(io.trino.testing.containers.environment.Row.fromList(currentRow));
        }

        List<io.trino.testing.containers.environment.Row> expectedRows = rowsBuilder.build();

        for (int sqlIndex = 1; sqlIndex <= columns.size(); sqlIndex++) {
            String column = columns.get(sqlIndex - 1);
            int columnIndex = sqlIndex - 1;

            if (column.contains("row_to_row") || column.contains("map_to_map")) {
                List<Object> actualColumn = actual.column(sqlIndex);
                List<Object> expectedColumn = expectedRows.stream()
                        .map(row -> row.getValue(columnIndex))
                        .collect(toList());
                assertThat(actualColumn)
                        .as("%s field is not equal", column)
                        .containsExactlyInAnyOrderElementsOf(expectedColumn);
                continue;
            }

            if (column.contains("list_to_list")) {
                List<Object> listToListResult = engine == Engine.TRINO ? extract(actual.column(sqlIndex)) : actual.column(sqlIndex);
                List<Object> expectedColumn = expectedRows.stream()
                        .map(row -> row.getValue(columnIndex))
                        .collect(toList());
                assertThat(listToListResult)
                        .as("list_to_list field is not equal")
                        .containsExactlyInAnyOrderElementsOf(expectedColumn);
                continue;
            }

            List<io.trino.testing.containers.environment.Row> expectedColumnRows = expectedRows.stream()
                    .map(row -> io.trino.testing.containers.environment.Row.row(row.getValue(columnIndex)))
                    .collect(toList());
            assertThat(actual.project(sqlIndex))
                    .as("%s field is not equal", column)
                    .containsOnly(expectedColumnRows);
        }
    }

    private static QueryResult execute(HiveStorageFormatsEnvironment env, Engine engine, String sql, HiveTimestampPrecision hiveTimestampPrecision)
    {
        if (engine == Engine.TRINO && hiveTimestampPrecision != null) {
            return executeTrinoWithTimestampPrecision(env, hiveTimestampPrecision, sql);
        }
        return engine == Engine.TRINO ? env.executeTrino(sql) : env.executeHive(sql);
    }

    private static QueryResult executeTrinoWithTimestampPrecision(HiveStorageFormatsEnvironment env, HiveTimestampPrecision hiveTimestampPrecision, String sql)
    {
        AtomicReference<QueryResult> result = new AtomicReference<>();
        env.executeTrinoInSession(session -> {
            session.executeUpdate("SET SESSION hive.timestamp_precision = '" + hiveTimestampPrecision.name() + "'");
            result.set(session.executeQuery(sql));
        });
        return result.get();
    }

    private static List<Object> extract(List<Object> arrays)
    {
        return arrays.stream()
                .map(obj -> {
                    if (obj instanceof TrinoArray trinoArray) {
                        return ImmutableList.copyOf((Object[]) trinoArray.getArray());
                    }
                    return obj;
                })
                .collect(toImmutableList());
    }

    private static void setHiveTimestampPrecision(HiveStorageFormatsEnvironment env, HiveTimestampPrecision hiveTimestampPrecision)
    {
        try (Connection conn = env.createTrinoConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute("SET SESSION hive.timestamp_precision = '" + hiveTimestampPrecision.name() + "'");
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void resetHiveTimestampPrecision(HiveStorageFormatsEnvironment env)
    {
        try (Connection conn = env.createTrinoConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute("RESET SESSION hive.timestamp_precision");
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static io.trino.testing.containers.environment.Row row(Object... values)
    {
        return io.trino.testing.containers.environment.Row.row(values);
    }

    private static ColumnContext columnContext(String version, String format, String column)
    {
        return new ColumnContext(Optional.of(version), format, column);
    }

    private static ColumnContext columnContext(String format, String column)
    {
        return new ColumnContext(Optional.empty(), format, column);
    }

    private record ColumnContext(Optional<String> hiveVersion, String format, String column) {}

    private enum Engine
    {
        TRINO,
        HIVE
    }
}

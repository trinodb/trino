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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import io.trino.jdbc.TrinoArray;
import io.trino.plugin.hive.HiveTimestampPrecision;
import io.trino.tempto.assertions.QueryAssert.Row;
import io.trino.tempto.fulfillment.table.MutableTablesState;
import io.trino.tempto.fulfillment.table.TableDefinition;
import io.trino.tempto.fulfillment.table.TableHandle;
import io.trino.tempto.fulfillment.table.TableInstance;
import io.trino.tempto.fulfillment.table.hive.HiveTableDefinition;
import io.trino.tempto.query.QueryExecutor;
import io.trino.tempto.query.QueryResult;

import java.math.BigDecimal;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.hive.HiveTimestampPrecision.NANOSECONDS;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.context.ThreadLocalTestContextHolder.testContext;
import static io.trino.tempto.fulfillment.table.TableHandle.tableHandle;
import static io.trino.tests.product.utils.JdbcDriverUtils.resetSessionProperty;
import static io.trino.tests.product.utils.JdbcDriverUtils.setSessionProperty;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static java.sql.JDBCType.ARRAY;
import static java.sql.JDBCType.BIGINT;
import static java.sql.JDBCType.BOOLEAN;
import static java.sql.JDBCType.CHAR;
import static java.sql.JDBCType.DATE;
import static java.sql.JDBCType.DECIMAL;
import static java.sql.JDBCType.DOUBLE;
import static java.sql.JDBCType.FLOAT;
import static java.sql.JDBCType.INTEGER;
import static java.sql.JDBCType.JAVA_OBJECT;
import static java.sql.JDBCType.REAL;
import static java.sql.JDBCType.SMALLINT;
import static java.sql.JDBCType.STRUCT;
import static java.sql.JDBCType.TIMESTAMP;
import static java.sql.JDBCType.TINYINT;
import static java.sql.JDBCType.VARCHAR;
import static java.util.Collections.nCopies;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class BaseTestHiveCoercion
        extends HiveProductTest
{
    protected void doTestHiveCoercion(HiveTableDefinition tableDefinition)
    {
        String tableName = mutableTableInstanceOf(tableDefinition).getNameInDatabase();

        List<Object> booleanToVarcharVal = tableName.toLowerCase(ENGLISH).contains("parquet") && tableName.toLowerCase(ENGLISH).contains("_unpartitioned") ? ImmutableList.of("true", "false") : ImmutableList.of("TRUE", "FALSE");

        insertTableRows(tableName);

        alterTableColumnTypes(tableName);
        assertProperAlteredTableSchema(tableName);

        List<String> allColumns = ImmutableList.of(
                "row_to_row",
                "list_to_list",
                "map_to_map",
                "boolean_to_varchar",
                "string_to_boolean",
                "special_string_to_boolean",
                "numeric_string_to_boolean",
                "varchar_to_boolean",
                "tinyint_to_smallint",
                "tinyint_to_int",
                "tinyint_to_bigint",
                "tinyint_to_varchar",
                "tinyint_to_string",
                "tinyint_to_double",
                "tinyint_to_shortdecimal",
                "tinyint_to_longdecimal",
                "smallint_to_int",
                "smallint_to_bigint",
                "smallint_to_varchar",
                "smallint_to_string",
                "smallint_to_double",
                "smallint_to_shortdecimal",
                "smallint_to_longdecimal",
                "int_to_bigint",
                "int_to_varchar",
                "int_to_string",
                "int_to_double",
                "int_to_shortdecimal",
                "int_to_longdecimal",
                "bigint_to_double",
                "bigint_to_varchar",
                "bigint_to_string",
                "bigint_to_shortdecimal",
                "bigint_to_longdecimal",
                "float_to_double",
                "float_to_string",
                "float_to_bounded_varchar",
                "float_infinity_to_string",
                "double_to_float",
                "double_to_string",
                "double_to_bounded_varchar",
                "double_infinity_to_string",
                "shortdecimal_to_shortdecimal",
                "shortdecimal_to_longdecimal",
                "longdecimal_to_shortdecimal",
                "longdecimal_to_longdecimal",
                "float_to_decimal",
                "double_to_decimal",
                "decimal_to_float",
                "decimal_to_double",
                "longdecimal_to_tinyint",
                "shortdecimal_to_tinyint",
                "longdecimal_to_smallint",
                "shortdecimal_to_smallint",
                "too_big_shortdecimal_to_smallint",
                "longdecimal_to_int",
                "shortdecimal_to_int",
                "shortdecimal_with_0_scale_to_int",
                "longdecimal_to_bigint",
                "shortdecimal_to_bigint",
                "short_decimal_to_varchar",
                "long_decimal_to_varchar",
                "short_decimal_to_bounded_varchar",
                "long_decimal_to_bounded_varchar",
                "varchar_to_tinyint",
                "string_to_tinyint",
                "varchar_to_smallint",
                "string_to_smallint",
                "varchar_to_integer",
                "string_to_integer",
                "varchar_to_bigint",
                "string_to_bigint",
                "varchar_to_bigger_varchar",
                "varchar_to_smaller_varchar",
                "varchar_to_date",
                "varchar_to_distant_date",
                "varchar_to_float",
                "string_to_float",
                "varchar_to_float_infinity",
                "varchar_to_special_float",
                "varchar_to_double",
                "string_to_double",
                "varchar_to_double_infinity",
                "varchar_to_special_double",
                "date_to_string",
                "date_to_bounded_varchar",
                "char_to_bigger_char",
                "char_to_smaller_char",
                "char_to_string",
                "char_to_bigger_varchar",
                "char_to_smaller_varchar",
                "string_to_char",
                "varchar_to_bigger_char",
                "varchar_to_smaller_char",
                "timestamp_millis_to_date",
                "timestamp_micros_to_date",
                "timestamp_nanos_to_date",
                "timestamp_to_string",
                "timestamp_to_bounded_varchar",
                "timestamp_to_smaller_varchar",
                "smaller_varchar_to_timestamp",
                "varchar_to_timestamp",
                "binary_to_string",
                "binary_to_smaller_varchar",
                "id");

        Function<Engine, Map<String, List<Object>>> expected = engine -> expectedValuesForEngineProvider(engine, tableName, booleanToVarcharVal);

        // For Trino, remove unsupported columns
        List<String> trinoReadColumns = removeUnsupportedColumnsForTrino(allColumns, tableName);
        Map<String, List<Object>> expectedTrinoResults = expected.apply(Engine.TRINO);
        // In case of unpartitioned tables we don't support all the column coercion thereby making this assertion conditional
        if (expectedExceptionsWithTrinoContext().isEmpty()) {
            assertThat(ImmutableSet.copyOf(trinoReadColumns)).isEqualTo(expectedTrinoResults.keySet());
        }
        String trinoSelectQuery = format("SELECT %s FROM %s", String.join(", ", trinoReadColumns), tableName);
        assertQueryResults(Engine.TRINO, trinoSelectQuery, expectedTrinoResults, trinoReadColumns, 2);

        // Additional assertions for VARBINARY coercion
        if (trinoReadColumns.contains("binary_to_string")) {
            List<Object> hexRepresentedValue = ImmutableList.of("58EFBFBDEFBFBDEFBFBDEFBFBD", "58EFBFBDEFBFBDEFBFBDEFBFBD58");

            if (tableName.toLowerCase(ENGLISH).contains("orc")) {
                hexRepresentedValue = ImmutableList.of("3538206637206266206266206266", "3538206637206266206266206266203538");
            }

            assertQueryResults(
                    Engine.TRINO,
                    format("SELECT to_hex(cast(binary_to_string as varbinary)) as hex_representation FROM %s", tableName),
                    ImmutableMap.of("hex_representation", hexRepresentedValue),
                    ImmutableList.of("hex_representation"),
                    2);
        }

        // For Hive, remove unsupported columns for the current file format and hive version
        List<String> hiveReadColumns = removeUnsupportedColumnsForHive(allColumns, tableName);
        Map<String, List<Object>> expectedHiveResults = expected.apply(Engine.HIVE);
        String hiveSelectQuery = format("SELECT %s FROM %s", String.join(", ", hiveReadColumns), tableName);
        assertQueryResults(Engine.HIVE, hiveSelectQuery, expectedHiveResults, hiveReadColumns, 2);

        List<Object> hexRepresentedValue = ImmutableList.of("58F7BFBFBF", "58F7BFBFBF58");

        // TODO: Translate internal byte sequence of Varchar in sync with Hive when coercing from Varbinary column
        if (tableName.toLowerCase(ENGLISH).contains("parquet")) {
            hexRepresentedValue = ImmutableList.of("58EFBFBDEFBFBDEFBFBDEFBFBD", "58EFBFBDEFBFBDEFBFBDEFBFBD58");
        }
        else if (tableName.toLowerCase(ENGLISH).contains("orc")) {
            hexRepresentedValue = ImmutableList.of("3538206637206266206266206266", "3538206637206266206266206266203538");
        }

        // Additional assertions for VARBINARY coercion
        assertQueryResults(
                Engine.HIVE,
                format("SELECT hex(binary_to_string) as hex_representation FROM %s", tableName),
                ImmutableMap.of("hex_representation", hexRepresentedValue),
                ImmutableList.of("hex_representation"),
                2);

        assertNestedSubFields(tableName);
    }

    protected void insertTableRows(String tableName)
    {
        // Insert all the data with nanoseconds precision
        setHiveTimestampPrecision(NANOSECONDS);
        onTrino().executeQuery(format(
                "INSERT INTO %s VALUES " +
                        "(" +
                        "  CAST(ROW ('as is', -1, 100, 2323, 12345, 2) AS ROW(keep VARCHAR, ti2si TINYINT, si2int SMALLINT, int2bi INTEGER, bi2vc BIGINT, lower2uppercase BIGINT)), " +
                        "  ARRAY [CAST(ROW (2, -101, 12345, 'removed') AS ROW (ti2int TINYINT, si2bi SMALLINT, bi2vc BIGINT, remove VARCHAR))], " +
                        "  MAP (ARRAY [TINYINT '2'], ARRAY [CAST(ROW (-3, 2323, REAL '0.5') AS ROW (ti2bi TINYINT, int2bi INTEGER, float2double REAL))]), " +
                        "  TRUE, " +
                        "  'TRUE', " +
                        "  ' ', " +
                        "  '-123', " +
                        "  'No', " +
                        "  TINYINT '-1', " +
                        "  TINYINT '2', " +
                        "  TINYINT '-3', " +
                        "  TINYINT '0', " +
                        "  TINYINT '127', " +
                        "  TINYINT '4', " +
                        "  TINYINT '5', " +
                        "  TINYINT '6', " +
                        "  SMALLINT '100', " +
                        "  SMALLINT '-101', " +
                        "  SMALLINT '0', " +
                        "  SMALLINT '32767', " +
                        "  SMALLINT '1024', " +
                        "  SMALLINT '2048', " +
                        "  SMALLINT '4096', " +
                        "  INTEGER '2323', " +
                        "  INTEGER '0', " +
                        "  INTEGER '2147483647', " +
                        "  INTEGER '16384', " +
                        "  INTEGER '16385', " +
                        "  INTEGER '16386', " +
                        "  1234567890, " +
                        "  12345, " +
                        "  9223372036854775807, " +
                        "  9223372, " +
                        "  9223372036, " +
                        "  REAL '0.5', " +
                        "  REAL '0.5', " +
                        "  REAL '0.5', " +
                        "  REAL 'Infinity', " +
                        "  DOUBLE '0.5', " +
                        "  DOUBLE '12345.12345', " +
                        "  DOUBLE '12345.12345', " +
                        "  DOUBLE 'Infinity' ," +
                        "  DECIMAL '12345678.12', " +
                        "  DECIMAL '12345678.12', " +
                        "  DECIMAL '12345678.123456123456', " +
                        "  DECIMAL '12345678.123456123456', " +
                        "  DECIMAL '13.1313', " +
                        "  DECIMAL '11.99', " +
                        "  DECIMAL '140.1323', " +
                        "  DECIMAL '142.99', " +
                        "  DECIMAL '312343.99', " +
                        "  DECIMAL '312345.99', " +
                        "  DECIMAL '312347.13433', " +
                        "  DECIMAL '123', " +
                        "  DECIMAL '123471234567.9989', " +
                        "  DECIMAL '12345678.12', " +
                        "  REAL '12345.12345', " +
                        "  DOUBLE '12345.12345', " +
                        "  DECIMAL '12345.12345', " +
                        "  DECIMAL '12345.12345', " +
                        "  DECIMAL '12345.12345', " +
                        "  DECIMAL '12345678.123456123456', " +
                        "  DECIMAL '12345.12345', " +
                        "  DECIMAL '12345678.123456123456', " +
                        "  '-127', " +
                        "  '2147483647', " +
                        "  '-32768', " +
                        "  '2147483647', " +
                        "  '-2147483648', " +
                        "  '2147483647', " +
                        "  '0', " +
                        "  '1234567890123', " +
                        "  'abc', " +
                        "  'abc', " +
                        "  '2023-09-28', " +
                        "  '8000-04-13', " +
                        "  '1234.567', " +
                        "  '1234.01234', " +
                        "  'Infinity'," +
                        "  'NaN'," +
                        "  '1234.567', " +
                        "  '1234.01234', " +
                        "  'Infinity'," +
                        "  'NaN'," +
                        "  DATE '2023-09-28', " +
                        "  DATE '2000-04-13', " +
                        "  'abc', " +
                        "  'abc', " +
                        "  'ab', " +
                        "  'cd', " +
                        "  'a', " +
                        "  'Bigger Value', " +
                        "  'Hi  ', " +
                        "  'TrinoDB', " +
                        "  TIMESTAMP '2022-12-31 23:59:59.999', " +
                        "  TIMESTAMP '2023-12-31 23:59:59.999999', " +
                        "  TIMESTAMP '2024-12-31 23:59:59.999999999', " +
                        "  TIMESTAMP '2121-07-15 15:30:12.123', " +
                        "  TIMESTAMP '2121-07-15 15:30:12.123', " +
                        "  TIMESTAMP '2121-07-15 15:30:12.123', " +
                        "  '2121', " +
                        "  '2019-01-29 23:59:59.123', " +
                        "  X'58F7BFBFBF', " +
                        "  X'58EDA080', " +
                        "  1), " +
                        "(" +
                        "  CAST(ROW (NULL, 1, -100, -2323, -12345, 2) AS ROW(keep VARCHAR, ti2si TINYINT, si2int SMALLINT, int2bi INTEGER, bi2vc BIGINT, lower2uppercase BIGINT)), " +
                        "  ARRAY [CAST(ROW (-2, 101, -12345, NULL) AS ROW (ti2int TINYINT, si2bi SMALLINT, bi2vc BIGINT, remove VARCHAR))], " +
                        "  MAP (ARRAY [TINYINT '-2'], ARRAY [CAST(ROW (null, -2323, REAL '-1.5') AS ROW (ti2bi TINYINT, int2bi INTEGER, float2double REAL))]), " +
                        "  FALSE, " +
                        "  'FAlSE', " +
                        "  'oFF', " +
                        "  '-0', " +
                        "  '', " +
                        "  TINYINT '1', " +
                        "  TINYINT '-2', " +
                        "  NULL, " +
                        "  NULL, " +
                        "  TINYINT '-128', " +
                        "  TINYINT '-4', " +
                        "  TINYINT '-5', " +
                        "  TINYINT '-6', " +
                        "  SMALLINT '-100', " +
                        "  SMALLINT '101', " +
                        "  NULL, " +
                        "  SMALLINT '-32768', " +
                        "  SMALLINT '-1024', " +
                        "  SMALLINT '-2048', " +
                        "  SMALLINT '-4096', " +
                        "  INTEGER '-2323', " +
                        "  NULL, " +
                        "  INTEGER '-2147483648', " +
                        "  INTEGER '-16384', " +
                        "  INTEGER '-16385', " +
                        "  INTEGER '-16386', " +
                        "  -1234567890, " +
                        "  -12345, " +
                        "  -9223372036854775808, " +
                        "  -9223372, " +
                        "  -9223372036, " +
                        "  REAL '-1.5', " +
                        "  REAL '-1.5', " +
                        "  REAL 'NaN', " +
                        "  REAL '-Infinity', " +
                        "  DOUBLE '-1.5', " +
                        "  DOUBLE 'NaN', " +
                        "  DOUBLE '-12345.12345', " +
                        "  DOUBLE '-Infinity' ," +
                        "  DECIMAL '-12345678.12', " +
                        "  DECIMAL '-12345678.12', " +
                        "  DECIMAL '-12345678.123456123456', " +
                        "  DECIMAL '-12345678.123456123456', " +
                        "  DECIMAL '-12.1313', " +
                        "  DECIMAL '-10.99', " +
                        "  DECIMAL '-141.1323', " +
                        "  DECIMAL '-143.99', " +
                        "  DECIMAL '-312342.99', " +
                        "  DECIMAL '-312346.99', " +
                        "  DECIMAL '-312348.13433', " +
                        "  DECIMAL '-124', " +
                        "  DECIMAL '-123471234577.9989', " +
                        "  DECIMAL '-12345678.12', " +
                        "  REAL '-12345.12345', " +
                        "  DOUBLE '-12345.12345', " +
                        "  DECIMAL '-12345.12345', " +
                        "  DECIMAL '-12345.12345', " +
                        "  DECIMAL '-12345.12345', " +
                        "  DECIMAL '-12345678.123456123456', " +
                        "  DECIMAL '-12345.12345', " +
                        "  DECIMAL '-12345678.123456123456', " +
                        "  '1270', " +
                        "  '123e+1', " +
                        "  '327680', " +
                        "  '123e+1', " +
                        "  '21474836480', " +
                        "  '123e+1', " +
                        "  '-9.223372e+18', " +
                        "  'Hello', " +
                        "  '\uD83D\uDCB0\uD83D\uDCB0\uD83D\uDCB0', " +
                        "  '\uD83D\uDCB0\uD83D\uDCB0\uD83D\uDCB0', " +
                        "  '2023-09-27', " +
                        "  '1900-01-01', " +
                        "  '-12345.6789', " +
                        "  '0', " +
                        "  '-4.4028235e+39f'," +
                        "  'Invalid Double'," +
                        "  '-12345.6789', " +
                        "  '0', " +
                        "  '-Infinity'," +
                        "  'Invalid Double'," +
                        "  DATE '2123-09-27', " +
                        "  DATE '1900-01-01', " +
                        "  '\uD83D\uDCB0\uD83D\uDCB0\uD83D\uDCB0', " +
                        "  '\uD83D\uDCB0\uD83D\uDCB0\uD83D\uDCB0', " +
                        "  '\uD83D\uDCB0\uD83D\uDCB0', " +
                        "  '\uD83D\uDCB0\uD83D\uDCB0', " +
                        "  '\uD83D\uDCB0', " +
                        "  '\uD83D\uDCB0\uD83D\uDCB0\uD83D\uDCB0\uD83D\uDCB0\uD83D\uDCB0', " +
                        "  '\uD83D\uDCB0 \uD83D\uDCB0\uD83D\uDCB0', " +
                        "  '\uD83D\uDCB0 \uD83D\uDCB0', " +
                        "  TIMESTAMP '1970-01-01 00:00:00.123', " +
                        "  TIMESTAMP '1970-01-01 00:00:00.123456', " +
                        "  TIMESTAMP '1970-01-01 00:00:00.123456789', " +
                        "  TIMESTAMP '1970-01-01 00:00:00.123', " +
                        "  TIMESTAMP '1970-01-01 00:00:00.123', " +
                        "  TIMESTAMP '1970-01-01 00:00:00.123', " +
                        "  '1970', " +
                        "  '1970-01-01 00:00:00.123', " +
                        "  X'58F7BFBFBF58', " +
                        "  X'58EDBFBF', " +
                        "  1)",
                tableName));
        resetHiveTimestampPrecision();
    }

    protected Map<String, List<Object>> expectedValuesForEngineProvider(Engine engine, String tableName, List<Object> booleanToVarcharVal)
    {
        String hiveValueForCaseChangeField;
        String coercedNaN = "NaN";
        Predicate<String> isFormat = formatName -> tableName.toLowerCase(ENGLISH).contains(formatName);
        Map<String, List<Object>> specialCoercion = ImmutableMap.of(
                "string_to_boolean", ImmutableList.of(true, false),
                "special_string_to_boolean", ImmutableList.of(true, false),
                "numeric_string_to_boolean", ImmutableList.of(true, true),
                "varchar_to_boolean", ImmutableList.of(false, false),
                "varchar_to_tinyint", ImmutableList.of(-127, -10),
                "varchar_to_smallint", ImmutableList.of(-32768, 0),
                "binary_to_string", ImmutableList.of("X\uFFFD\uFFFD\uFFFD\uFFFD", "X\uFFFD\uFFFD\uFFFD\uFFFDX"),
                "binary_to_smaller_varchar", ImmutableList.of("X\uFFFD", "X\uFFFD"));
        if (Stream.of("rctext", "textfile", "sequencefile").anyMatch(isFormat)) {
            hiveValueForCaseChangeField = "\"lower2uppercase\":2";
        }
        else if (isFormat.test("orc")) {
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

        // Apache Hive reads Double.NaN as null when coerced to varchar for ORC file format
        if (isFormat.test("orc")) {
            coercedNaN = null;
        }

        return ImmutableMap.<String, List<Object>>builder()
                .put("row_to_row", ImmutableList.of(
                        engine == Engine.TRINO ?
                                rowBuilder()
                                        .addField("keep", "as is")
                                        .addField("ti2si", (short) -1)
                                        .addField("si2int", 100)
                                        .addField("int2bi", 2323L)
                                        .addField("bi2vc", "12345")
                                        .addField("lower2uppercase", 2L)
                                        .build() :
                                // TODO: Compare structures for hive executor instead of serialized representation
                                String.format("{\"keep\":\"as is\",\"ti2si\":-1,\"si2int\":100,\"int2bi\":2323,\"bi2vc\":\"12345\",%s}", hiveValueForCaseChangeField),
                        engine == Engine.TRINO ?
                                rowBuilder()
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
                                ImmutableList.of(rowBuilder()
                                        .addField("ti2int", 2)
                                        .addField("si2bi", -101L)
                                        .addField("bi2vc", "12345")
                                        .build()) :
                                "[{\"ti2int\":2,\"si2bi\":-101,\"bi2vc\":\"12345\"}]",
                        engine == Engine.TRINO ?
                                ImmutableList.of(rowBuilder()
                                        .addField("ti2int", -2)
                                        .addField("si2bi", 101L)
                                        .addField("bi2vc", "-12345")
                                        .build()) :
                                "[{\"ti2int\":-2,\"si2bi\":101,\"bi2vc\":\"-12345\"}]"))
                .put("map_to_map", ImmutableList.of(
                        engine == Engine.TRINO ?
                                ImmutableMap.of(2, rowBuilder()
                                        .addField("ti2bi", -3L)
                                        .addField("int2bi", 2323L)
                                        .addField("float2double", 0.5)
                                        .addField("add", null)
                                        .build()) :
                                "{2:{\"ti2bi\":-3,\"int2bi\":2323,\"float2double\":0.5,\"add\":null}}",
                        engine == Engine.TRINO ?
                                ImmutableMap.of(-2, rowBuilder()
                                        .addField("ti2bi", null)
                                        .addField("int2bi", -2323L)
                                        .addField("float2double", -1.5)
                                        .addField("add", null)
                                        .build()) :
                                "{-2:{\"ti2bi\":null,\"int2bi\":-2323,\"float2double\":-1.5,\"add\":null}}"))
                .put("boolean_to_varchar", booleanToVarcharVal)
                .putAll(specialCoercion)
                .put("tinyint_to_smallint", ImmutableList.of(
                        -1,
                        1))
                .put("tinyint_to_int", ImmutableList.of(
                        2,
                        -2))
                .put("tinyint_to_bigint", Arrays.asList(
                        -3L,
                        null))
                .put("tinyint_to_varchar", Arrays.asList(
                        "0",
                        null))
                .put("tinyint_to_string", ImmutableList.of(
                        "127",
                        "-128"))
                .put("tinyint_to_double", Arrays.asList(
                        -4D,
                        4D))
                .put("tinyint_to_shortdecimal", Arrays.asList(
                        new BigDecimal(-5),
                        new BigDecimal(5)))
                .put("tinyint_to_longdecimal", Arrays.asList(
                        new BigDecimal(-6),
                        new BigDecimal(6)))
                .put("smallint_to_int", ImmutableList.of(
                        100,
                        -100))
                .put("smallint_to_bigint", ImmutableList.of(
                        -101L,
                        101L))
                .put("smallint_to_varchar", Arrays.asList(
                        "0",
                        null))
                .put("smallint_to_string", ImmutableList.of(
                        "32767",
                        "-32768"))
                .put("smallint_to_double", ImmutableList.of(
                        -1024D,
                        1024D))
                .put("smallint_to_shortdecimal", Arrays.asList(
                        new BigDecimal(-2048),
                        new BigDecimal(-2048)))
                .put("smallint_to_longdecimal", Arrays.asList(
                        new BigDecimal(-4096),
                        new BigDecimal(4096)))
                .put("int_to_bigint", ImmutableList.of(
                        2323L,
                        -2323L))
                .put("int_to_varchar", Arrays.asList(
                        "0",
                        null))
                .put("int_to_string", ImmutableList.of(
                        "2147483647",
                        "-2147483648"))
                .put("int_to_double", ImmutableList.of(
                        -16384D,
                        16384D))
                .put("int_to_shortdecimal", Arrays.asList(
                        new BigDecimal(-16385),
                        new BigDecimal(16385)))
                .put("int_to_longdecimal", Arrays.asList(
                        new BigDecimal(-16386),
                        new BigDecimal(16386)))
                .put("bigint_to_double", ImmutableList.of(
                        -1234567890D,
                        1234567890D))
                .put("bigint_to_varchar", ImmutableList.of(
                        "12345",
                        "-12345"))
                .put("bigint_to_string", ImmutableList.of(
                        "9223372036854775807",
                        "-9223372036854775808"))
                .put("bigint_to_shortdecimal", Arrays.asList(
                        new BigDecimal(-9223372L),
                        new BigDecimal(9223372L)))
                .put("bigint_to_longdecimal", Arrays.asList(
                        new BigDecimal(-9223372036L),
                        new BigDecimal(9223372036L)))
                .put("float_to_double", ImmutableList.of(
                        0.5,
                        -1.5))
                .put("float_to_string", ImmutableList.of("0.5", "-1.5"))
                .put("float_to_bounded_varchar", Arrays.asList("0.5", coercedNaN))
                .put("float_infinity_to_string", ImmutableList.of("Infinity", "-Infinity"))
                .put("double_to_float", ImmutableList.of(0.5, -1.5))
                .put("double_to_string", Arrays.asList("12345.12345", coercedNaN))
                .put("double_to_bounded_varchar", ImmutableList.of("12345.12345", "-12345.12345"))
                .put("double_infinity_to_string", ImmutableList.of("Infinity", "-Infinity"))
                .put("shortdecimal_to_shortdecimal", ImmutableList.of(
                        new BigDecimal("12345678.1200"),
                        new BigDecimal("-12345678.1200")))
                .put("shortdecimal_to_longdecimal", ImmutableList.of(
                        new BigDecimal("12345678.1200"),
                        new BigDecimal("-12345678.1200")))
                .put("longdecimal_to_shortdecimal", ImmutableList.of(
                        new BigDecimal("12345678.12"),
                        new BigDecimal("-12345678.12")))
                .put("longdecimal_to_longdecimal", ImmutableList.of(
                        new BigDecimal("12345678.12345612345600"),
                        new BigDecimal("-12345678.12345612345600")))
                .put("float_to_decimal", ImmutableList.of(new BigDecimal("12345.12300"), new BigDecimal("-12345.12300")))
                .put("double_to_decimal", ImmutableList.of(new BigDecimal("12345.12345"), new BigDecimal("-12345.12345")))
                .put("decimal_to_float", ImmutableList.of(
                        Float.parseFloat("12345.124"),
                        -Float.parseFloat("12345.124")))
                .put("decimal_to_double", ImmutableList.of(
                        12345.12345,
                        -12345.12345))
                .put("longdecimal_to_tinyint", ImmutableList.of(
                        13,
                        -12))
                .put("shortdecimal_to_tinyint", ImmutableList.of(
                        11,
                        -10))
                .put("longdecimal_to_smallint", ImmutableList.of(
                        140,
                        -141))
                .put("shortdecimal_to_smallint", ImmutableList.of(
                        142,
                        -143))
                .put("too_big_shortdecimal_to_smallint", Arrays.asList(
                        null,
                        null))
                .put("longdecimal_to_int", ImmutableList.of(
                        312345,
                        -312346))
                .put("shortdecimal_to_int", ImmutableList.of(
                        312347,
                        -312348))
                .put("shortdecimal_with_0_scale_to_int", ImmutableList.of(
                        123,
                        -124))
                .put("longdecimal_to_bigint", ImmutableList.of(
                        123471234567L,
                        -123471234577L))
                .put("shortdecimal_to_bigint", ImmutableList.of(
                        12345678,
                        -12345678))
                .put("short_decimal_to_varchar", ImmutableList.of(
                        "12345.12345",
                        "-12345.12345"))
                .put("long_decimal_to_varchar", ImmutableList.of(
                        "12345678.123456123456",
                        "-12345678.123456123456"))
                .put("short_decimal_to_bounded_varchar", ImmutableList.of(
                        "12345.12345",
                        "12345.12345"))
                .put("long_decimal_to_bounded_varchar", ImmutableList.of(
                        "12345678.123456123456",
                        "-12345678.123456123456"))
                .put("string_to_tinyint", Arrays.asList(null, null))
                .put("string_to_smallint", Arrays.asList(null, null))
                .put("string_to_integer", Arrays.asList(null, null))
                .put("varchar_to_integer", Arrays.asList(-2147483648, null))
                .put("varchar_to_bigint", Arrays.asList(0, null))
                .put("string_to_bigint", Arrays.asList(1234567890123L, null))
                .put("varchar_to_bigger_varchar", ImmutableList.of(
                        "abc",
                        "\uD83D\uDCB0\uD83D\uDCB0\uD83D\uDCB0"))
                .put("varchar_to_smaller_varchar", ImmutableList.of(
                        "ab",
                        "\uD83D\uDCB0\uD83D\uDCB0"))
                .put("string_to_char", ImmutableList.of(
                        "B",
                        "\uD83D\uDCB0"))
                .put("varchar_to_bigger_char", ImmutableList.of(
                        "Hi    ",
                        "\uD83D\uDCB0 \uD83D\uDCB0\uD83D\uDCB0  "))
                .put("varchar_to_smaller_char", ImmutableList.of(
                        "Tr",
                        "\uD83D\uDCB0 "))
                .put("varchar_to_date", ImmutableList.of(
                        java.sql.Date.valueOf("2023-09-28"),
                        java.sql.Date.valueOf("2023-09-27")))
                .put("varchar_to_distant_date", ImmutableList.of(
                        java.sql.Date.valueOf("8000-04-13"),
                        java.sql.Date.valueOf("1900-01-01")))
                .put("varchar_to_float", ImmutableList.of(
                        1234.567f,
                        -12345.6789f))
                .put("string_to_float", ImmutableList.of(
                        1234.01234f,
                        0f))
                .put("varchar_to_float_infinity", ImmutableList.of(
                        Float.POSITIVE_INFINITY,
                        Float.NEGATIVE_INFINITY))
                .put("varchar_to_special_float", Arrays.asList(
                        coercedNaN == null ? null : Float.NaN,
                        null))
                .put("varchar_to_double", ImmutableList.of(
                        1234.567,
                        -12345.6789))
                .put("string_to_double", ImmutableList.of(
                        1234.01234,
                        0D))
                .put("varchar_to_double_infinity", ImmutableList.of(
                        Double.POSITIVE_INFINITY,
                        Double.NEGATIVE_INFINITY))
                .put("varchar_to_special_double", Arrays.asList(
                        coercedNaN == null ? null : Double.NaN,
                        null))
                .put("date_to_string", ImmutableList.of(
                        "2023-09-28",
                        "2123-09-27"))
                .put("date_to_bounded_varchar", ImmutableList.of(
                        "2000-04-13",
                        "1900-01-01"))
                .put("char_to_bigger_char", ImmutableList.of(
                        "abc ",
                        "\uD83D\uDCB0\uD83D\uDCB0\uD83D\uDCB0 "))
                .put("char_to_smaller_char", ImmutableList.of(
                        "ab",
                        "\uD83D\uDCB0\uD83D\uDCB0"))
                .put("char_to_string", ImmutableList.of(
                        "ab",
                        "\uD83D\uDCB0\uD83D\uDCB0"))
                .put("char_to_bigger_varchar", ImmutableList.of(
                        "cd",
                        "\uD83D\uDCB0\uD83D\uDCB0"))
                .put("char_to_smaller_varchar", ImmutableList.of(
                        "a",
                        "\uD83D\uDCB0"))
                .put("timestamp_millis_to_date", ImmutableList.of(
                        java.sql.Date.valueOf("2022-12-31"),
                        java.sql.Date.valueOf("1970-01-01")))
                .put("timestamp_micros_to_date", ImmutableList.of(
                        java.sql.Date.valueOf("2023-12-31"),
                        java.sql.Date.valueOf("1970-01-01")))
                .put("timestamp_nanos_to_date", ImmutableList.of(
                        java.sql.Date.valueOf("2024-12-31"),
                        java.sql.Date.valueOf("1970-01-01")))
                .put("timestamp_to_string", ImmutableList.of(
                        "2121-07-15 15:30:12.123",
                        "1970-01-01 00:00:00.123"))
                .put("timestamp_to_bounded_varchar", ImmutableList.of(
                        "2121-07-15 15:30:12.123",
                        "1970-01-01 00:00:00.123"))
                .put("timestamp_to_smaller_varchar", ImmutableList.of(
                        "2121",
                        "1970"))
                .put("smaller_varchar_to_timestamp", Arrays.asList(
                        null,
                        null))
                .put("varchar_to_timestamp", Arrays.asList(
                        Timestamp.valueOf("2019-01-29 23:59:59.123"),
                        Timestamp.valueOf("1970-01-01 00:00:00.123")))
                .put("id", ImmutableList.of(
                        1,
                        1))
                .buildOrThrow();
    }

    protected void doTestHiveCoercionWithDifferentTimestampPrecision(HiveTableDefinition tableDefinition)
    {
        String tableName = mutableTableInstanceOf(tableDefinition).getNameInDatabase();

        // Insert all the data with nanoseconds precision
        setHiveTimestampPrecision(NANOSECONDS);
        onTrino().executeQuery(
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

        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN timestamp_row_to_row timestamp_row_to_row struct<keep:timestamp, si2i:int, timestamp2string:string, string2timestamp:timestamp, timestamp2date:date>", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN timestamp_list_to_list timestamp_list_to_list array<struct<keep:timestamp, si2i:int, timestamp2string:string, string2timestamp:timestamp, timestamp2date:date>>", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN timestamp_map_to_map timestamp_map_to_map map<int,struct<keep:timestamp, si2i:int, timestamp2string:string, string2timestamp:timestamp, timestamp2date:date>>", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN timestamp_to_string timestamp_to_string string", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN string_to_timestamp string_to_timestamp TIMESTAMP", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN timestamp_to_date timestamp_to_date DATE", tableName));

        for (HiveTimestampPrecision hiveTimestampPrecision : HiveTimestampPrecision.values()) {
            String timestampType = "timestamp(%d)".formatted(hiveTimestampPrecision.getPrecision());
            setHiveTimestampPrecision(hiveTimestampPrecision);
            assertThat(onTrino().executeQuery("SHOW COLUMNS FROM " + tableName).project(1, 2)).containsExactlyInOrder(
                    row("timestamp_row_to_row", "row(keep %1$s, si2i integer, timestamp2string varchar, string2timestamp %1$s, timestamp2date date)".formatted(timestampType)),
                    row("timestamp_list_to_list", "array(row(keep %1$s, si2i integer, timestamp2string varchar, string2timestamp %1$s, timestamp2date date))".formatted(timestampType)),
                    row("timestamp_map_to_map", "map(integer, row(keep %1$s, si2i integer, timestamp2string varchar, string2timestamp %1$s, timestamp2date date))".formatted(timestampType)),
                    row("timestamp_to_string", "varchar"),
                    row("string_to_timestamp", timestampType),
                    row("timestamp_to_date", "date"),
                    row("id", "bigint"));

            List<String> allColumns = ImmutableList.of(
                    "timestamp_row_to_row",
                    "timestamp_list_to_list",
                    "timestamp_map_to_map",
                    "timestamp_to_string",
                    "string_to_timestamp",
                    "timestamp_to_date",
                    "id");

            // For Trino, remove unsupported columns
            List<String> trinoReadColumns = removeUnsupportedColumnsForTrino(allColumns, tableName);
            Map<String, List<Object>> expectedTinoResults = Maps.filterKeys(
                    expectedRowsForEngineProvider(Engine.TRINO, hiveTimestampPrecision),
                    trinoReadColumns::contains);

            String trinoReadQuery = format("SELECT %s FROM %s", String.join(", ", trinoReadColumns), tableName);
            assertQueryResults(Engine.TRINO, trinoReadQuery, expectedTinoResults, trinoReadColumns, 6);

            List<String> hiveReadColumns = removeUnsupportedColumnsForHive(allColumns, tableName);
            Map<String, List<Object>> expectedHiveResults = Maps.filterKeys(
                    expectedRowsForEngineProvider(Engine.HIVE, hiveTimestampPrecision),
                    hiveReadColumns::contains);

            String hiveSelectQuery = format("SELECT %s FROM %s", String.join(", ", hiveReadColumns), tableName);
            assertQueryResults(Engine.HIVE, hiveSelectQuery, expectedHiveResults, hiveReadColumns, 6);
        }
    }

    protected Map<String, List<Object>> expectedRowsForEngineProvider(Engine engine, HiveTimestampPrecision timestampPrecision)
    {
        List<Object> timestampAsString = ImmutableList.of(
                "2121-07-15 15:30:12.123499",
                "2121-07-15 15:30:12.1235",
                "2121-07-15 15:30:12.123501",
                "2121-07-15 15:30:12.123499999",
                "2121-07-15 15:30:12.1235",
                "2121-07-15 15:30:12.123500001");

        if (engine == Engine.HIVE) {
            List<Object> baseData = ImmutableList.of(
                    "{\"keep\":\"2121-07-15 15:30:12.123499\",\"si2i\":-1,\"timestamp2string\":\"2121-07-15 15:30:12.123499\",\"string2timestamp\":\"2121-07-15 15:30:12.123499\",\"timestamp2date\":\"2121-07-15\"}",
                    "{\"keep\":\"2121-07-15 15:30:12.1235\",\"si2i\":-1,\"timestamp2string\":\"2121-07-15 15:30:12.1235\",\"string2timestamp\":\"2121-07-15 15:30:12.1235\",\"timestamp2date\":\"2121-07-15\"}",
                    "{\"keep\":\"2121-07-15 15:30:12.123501\",\"si2i\":-1,\"timestamp2string\":\"2121-07-15 15:30:12.123501\",\"string2timestamp\":\"2121-07-15 15:30:12.123501\",\"timestamp2date\":\"2121-07-15\"}",
                    "{\"keep\":\"2121-07-15 15:30:12.123499999\",\"si2i\":-1,\"timestamp2string\":\"2121-07-15 15:30:12.123499999\",\"string2timestamp\":\"2121-07-15 15:30:12.123499999\",\"timestamp2date\":\"2121-07-15\"}",
                    "{\"keep\":\"2121-07-15 15:30:12.1235\",\"si2i\":-1,\"timestamp2string\":\"2121-07-15 15:30:12.1235\",\"string2timestamp\":\"2121-07-15 15:30:12.1235\",\"timestamp2date\":\"2121-07-15\"}",
                    "{\"keep\":\"2121-07-15 15:30:12.123500001\",\"si2i\":-1,\"timestamp2string\":\"2121-07-15 15:30:12.123500001\",\"string2timestamp\":\"2121-07-15 15:30:12.123500001\",\"timestamp2date\":\"2121-07-15\"}");
            return ImmutableMap.<String, List<Object>>builder()
                    .put("timestamp_row_to_row", baseData)
                    .put("timestamp_list_to_list", baseData.stream()
                            .map(ImmutableList::of)
                            .map(Objects::toString)
                            .collect(toImmutableList()))
                    .put("timestamp_map_to_map", baseData.stream()
                            .map("{2:%s}"::formatted)
                            .collect(toImmutableList()))
                    .put("timestamp_to_string", timestampAsString)
                    .put("string_to_timestamp", timestampAsString.stream()
                            .map(String.class::cast)
                            .map(Timestamp::valueOf)
                            .collect(toImmutableList()))
                    .put("timestamp_to_date", nCopies(6, java.sql.Date.valueOf("2121-07-15")))
                    .put("id", nCopies(6, 1))
                    .buildOrThrow();
        }

        List<Object> timestampValue = switch (timestampPrecision) {
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

        List<Object> baseData = Streams.zip(
                timestampValue.stream(),
                timestampAsString.stream(),
                (timestamp, timestampCoerced) -> rowBuilder()
                        .addField("keep", timestamp)
                        .addField("si2i", -1)
                        .addField("timestamp2string", timestampCoerced)
                        .addField("string2timestamp", timestamp)
                        .addField("timestamp2date", java.sql.Date.valueOf("2121-07-15"))
                        .build())
                .collect(toImmutableList());

        return ImmutableMap.<String, List<Object>>builder()
                .put("timestamp_row_to_row", baseData)
                .put("timestamp_list_to_list", baseData.stream()
                        .map(ImmutableList::of)
                        .collect(toImmutableList()))
                .put("timestamp_map_to_map", baseData.stream()
                        .map(entry -> ImmutableMap.of(2, entry))
                        .collect(toImmutableList()))
                .put("timestamp_to_string", timestampAsString)
                .put("string_to_timestamp", timestampValue)
                .put("timestamp_to_date", nCopies(6, java.sql.Date.valueOf("2121-07-15")))
                .put("id", nCopies(6, 1))
                .buildOrThrow();
    }

    protected List<String> removeUnsupportedColumnsForHive(List<String> columns, String tableName)
    {
        // TODO: assert exceptions being thrown for each column
        Map<ColumnContext, String> expectedExceptions = expectedExceptionsWithHiveContext();

        String hiveVersion = getHiveVersionMajor() + "." + getHiveVersionMinor();
        Set<String> unsupportedColumns = expectedExceptions.keySet().stream()
                .filter(context -> context.hiveVersion().orElseThrow().equals(hiveVersion) && tableName.contains(context.format()))
                .map(ColumnContext::column)
                .collect(toImmutableSet());

        return columns.stream()
                .filter(column -> !unsupportedColumns.contains(column))
                .collect(toImmutableList());
    }

    protected List<String> removeUnsupportedColumnsForTrino(List<String> columns, String tableName)
    {
        // TODO: assert exceptions being thrown for each column
        Map<ColumnContext, String> expectedExceptions = expectedExceptionsWithTrinoContext();

        Set<String> unsupportedColumns = expectedExceptions.keySet().stream()
                .filter(context -> tableName.contains(context.format()))
                .map(ColumnContext::column)
                .collect(toImmutableSet());

        return columns.stream()
                .filter(column -> !unsupportedColumns.contains(column))
                .collect(toImmutableList());
    }

    private void assertNestedSubFields(String tableName)
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

        // Assert Trino behavior
        assertQueryResults(Engine.TRINO, subfieldQueryUpperCase, expectedNestedFieldTrino, expectedColumns, 2);
        assertQueryResults(Engine.TRINO, subfieldQueryLowerCase, expectedNestedFieldTrino, expectedColumns, 2);

        // Assert Hive behavior
        if (isFormat.test("rcbinary")) {
            assertThatThrownBy(() -> assertQueryResults(Engine.HIVE, subfieldQueryUpperCase, expectedNestedFieldTrino, expectedColumns, 2))
                    .hasMessageContaining("org.apache.hadoop.hive.ql.metadata.HiveException");
            assertThatThrownBy(() -> assertQueryResults(Engine.HIVE, subfieldQueryLowerCase, expectedNestedFieldTrino, expectedColumns, 2))
                    .hasMessageContaining("org.apache.hadoop.hive.ql.metadata.HiveException");
        }
        else {
            assertQueryResults(Engine.HIVE, subfieldQueryUpperCase, expectedNestedFieldHive, expectedColumns, 2);
            assertQueryResults(Engine.HIVE, subfieldQueryLowerCase, expectedNestedFieldHive, expectedColumns, 2);
        }
    }

    protected Map<ColumnContext, String> expectedExceptionsWithHiveContext()
    {
        return ImmutableMap.<ColumnContext, String>builder()
                // 3.1
                // Parquet
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
                // Rcbinary
                .put(columnContext("3.1", "rcbinary", "row_to_row"), "java.util.ArrayList cannot be cast to org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryStruct")
                .put(columnContext("3.1", "rcbinary", "list_to_list"), "java.util.ArrayList cannot be cast to org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryArray")
                .put(columnContext("3.1", "rcbinary", "map_to_map"), "java.util.LinkedHashMap cannot be cast to org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryMap")
                .put(columnContext("3.1", "rcbinary", "timestamp_row_to_row"), "java.util.ArrayList cannot be cast to org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryStruct")
                .put(columnContext("3.1", "rcbinary", "timestamp_list_to_list"), "java.util.ArrayList cannot be cast to org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryArray")
                .put(columnContext("3.1", "rcbinary", "timestamp_map_to_map"), "java.util.LinkedHashMap cannot be cast to org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryMap")
                .buildOrThrow();
    }

    protected Map<ColumnContext, String> expectedExceptionsWithTrinoContext()
    {
        return ImmutableMap.of();
    }

    private void assertQueryResults(
            Engine engine,
            String query,
            Map<String, List<Object>> expected,
            List<String> columns,
            int rowCount)
    {
        QueryResult actual = execute(engine, query);

        ImmutableList.Builder<Row> rowsBuilder = ImmutableList.builder();
        for (int row = 0; row < rowCount; row++) {
            List<Object> currentRow = new ArrayList<>();    // Avoid ImmutableList to allow nulls

            for (int column = 0; column < columns.size(); column++) {
                String columnName = columns.get(column);
                checkArgument(expected.containsKey(columnName), "columnName should be present in expected results");
                currentRow.add(expected.get(columnName).get(row));
            }

            rowsBuilder.add(new Row(currentRow));
        }

        List<Row> expectedRows = rowsBuilder.build();
        assertColumnTypes(actual, engine, columns);

        for (int sqlIndex = 1; sqlIndex <= columns.size(); sqlIndex++) {
            String column = columns.get(sqlIndex - 1);

            if (column.contains("row_to_row") || column.contains("map_to_map")) {
                assertThat(actual.column(sqlIndex))
                        .as("%s field is not equal", column)
                        .containsExactlyInAnyOrderElementsOf(column(expectedRows, sqlIndex));
                continue;
            }

            if (column.contains("list_to_list")) {
                List<Object> listToListResult = engine == Engine.TRINO ? extract(actual.column(sqlIndex)) : actual.column(sqlIndex);
                assertThat(listToListResult)
                        .as("list_to_list field is not equal")
                        .containsExactlyInAnyOrderElementsOf(column(expectedRows, sqlIndex));
                continue;
            }

            // test primitive values
            assertThat(actual.project(sqlIndex)).containsOnly(project(expectedRows, sqlIndex));
        }
    }

    private void assertProperAlteredTableSchema(String tableName)
    {
        assertThat(onTrino().executeQuery("SHOW COLUMNS FROM " + tableName).project(1, 2)).containsExactlyInOrder(
                // The field lower2uppercase in the row is recorded in upper case in hive, but Trino converts it to lower case
                row("row_to_row", "row(keep varchar, ti2si smallint, si2int integer, int2bi bigint, bi2vc varchar, lower2uppercase bigint)"),
                row("list_to_list", "array(row(ti2int integer, si2bi bigint, bi2vc varchar))"),
                row("map_to_map", "map(integer, row(ti2bi bigint, int2bi bigint, float2double double, add tinyint))"),
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
                row("smaller_varchar_to_timestamp", "timestamp(3)"),
                row("varchar_to_timestamp", "timestamp(3)"),
                row("binary_to_string", "varchar"),
                row("binary_to_smaller_varchar", "varchar(2)"),
                row("id", "bigint"));
    }

    private void assertColumnTypes(
            QueryResult queryResult,
            Engine engine,
            List<String> columns)
    {
        JDBCType floatType = engine == Engine.TRINO ? REAL : FLOAT;
        Map<String, JDBCType> expectedTypes = ImmutableMap.<String, JDBCType>builder()
                .put("row_to_row", engine == Engine.TRINO ? JAVA_OBJECT : STRUCT)   // row
                .put("list_to_list", ARRAY) // list
                .put("map_to_map", JAVA_OBJECT) // map
                .put("boolean_to_varchar", VARCHAR)
                .put("string_to_boolean", BOOLEAN)
                .put("special_string_to_boolean", BOOLEAN)
                .put("numeric_string_to_boolean", BOOLEAN)
                .put("varchar_to_boolean", BOOLEAN)
                .put("tinyint_to_smallint", SMALLINT)
                .put("tinyint_to_int", INTEGER)
                .put("tinyint_to_bigint", BIGINT)
                .put("tinyint_to_varchar", VARCHAR)
                .put("tinyint_to_string", VARCHAR)
                .put("tinyint_to_double", DOUBLE)
                .put("tinyint_to_shortdecimal", DECIMAL)
                .put("tinyint_to_longdecimal", DECIMAL)
                .put("smallint_to_int", INTEGER)
                .put("smallint_to_bigint", BIGINT)
                .put("smallint_to_varchar", VARCHAR)
                .put("smallint_to_string", VARCHAR)
                .put("smallint_to_double", DOUBLE)
                .put("smallint_to_shortdecimal", DECIMAL)
                .put("smallint_to_longdecimal", DECIMAL)
                .put("int_to_bigint", BIGINT)
                .put("int_to_varchar", VARCHAR)
                .put("int_to_string", VARCHAR)
                .put("int_to_double", DOUBLE)
                .put("int_to_shortdecimal", DECIMAL)
                .put("int_to_longdecimal", DECIMAL)
                .put("bigint_to_double", DOUBLE)
                .put("bigint_to_varchar", VARCHAR)
                .put("bigint_to_string", VARCHAR)
                .put("bigint_to_shortdecimal", DECIMAL)
                .put("bigint_to_longdecimal", DECIMAL)
                .put("float_to_double", DOUBLE)
                .put("float_to_string", VARCHAR)
                .put("float_to_bounded_varchar", VARCHAR)
                .put("float_infinity_to_string", VARCHAR)
                .put("double_to_float", floatType)
                .put("double_to_string", VARCHAR)
                .put("double_to_bounded_varchar", VARCHAR)
                .put("double_infinity_to_string", VARCHAR)
                .put("shortdecimal_to_shortdecimal", DECIMAL)
                .put("shortdecimal_to_longdecimal", DECIMAL)
                .put("longdecimal_to_shortdecimal", DECIMAL)
                .put("longdecimal_to_longdecimal", DECIMAL)
                .put("longdecimal_to_tinyint", TINYINT)
                .put("shortdecimal_to_tinyint", TINYINT)
                .put("longdecimal_to_smallint", SMALLINT)
                .put("shortdecimal_to_smallint", SMALLINT)
                .put("too_big_shortdecimal_to_smallint", SMALLINT)
                .put("longdecimal_to_int", INTEGER)
                .put("shortdecimal_to_int", INTEGER)
                .put("shortdecimal_with_0_scale_to_int", INTEGER)
                .put("longdecimal_to_bigint", BIGINT)
                .put("shortdecimal_to_bigint", BIGINT)
                .put("float_to_decimal", DECIMAL)
                .put("double_to_decimal", DECIMAL)
                .put("decimal_to_float", floatType)
                .put("decimal_to_double", DOUBLE)
                .put("short_decimal_to_varchar", VARCHAR)
                .put("long_decimal_to_varchar", VARCHAR)
                .put("short_decimal_to_bounded_varchar", VARCHAR)
                .put("long_decimal_to_bounded_varchar", VARCHAR)
                .put("varchar_to_tinyint", TINYINT)
                .put("string_to_tinyint", TINYINT)
                .put("varchar_to_smallint", SMALLINT)
                .put("string_to_smallint", SMALLINT)
                .put("varchar_to_integer", INTEGER)
                .put("string_to_integer", INTEGER)
                .put("varchar_to_bigint", BIGINT)
                .put("string_to_bigint", BIGINT)
                .put("varchar_to_bigger_varchar", VARCHAR)
                .put("varchar_to_smaller_varchar", VARCHAR)
                .put("varchar_to_date", DATE)
                .put("varchar_to_distant_date", DATE)
                .put("varchar_to_float", floatType)
                .put("string_to_float", floatType)
                .put("varchar_to_float_infinity", floatType)
                .put("varchar_to_special_float", floatType)
                .put("varchar_to_double", DOUBLE)
                .put("string_to_double", DOUBLE)
                .put("varchar_to_double_infinity", DOUBLE)
                .put("varchar_to_special_double", DOUBLE)
                .put("date_to_string", VARCHAR)
                .put("date_to_bounded_varchar", VARCHAR)
                .put("char_to_bigger_char", CHAR)
                .put("char_to_smaller_char", CHAR)
                .put("char_to_string", VARCHAR)
                .put("char_to_bigger_varchar", VARCHAR)
                .put("char_to_smaller_varchar", VARCHAR)
                .put("string_to_char", CHAR)
                .put("varchar_to_bigger_char", CHAR)
                .put("varchar_to_smaller_char", CHAR)
                .put("id", BIGINT)
                .put("nested_field", BIGINT)
                .put("timestamp_to_string", VARCHAR)
                .put("timestamp_to_bounded_varchar", VARCHAR)
                .put("timestamp_to_smaller_varchar", VARCHAR)
                .put("smaller_varchar_to_timestamp", TIMESTAMP)
                .put("varchar_to_timestamp", TIMESTAMP)
                .put("timestamp_to_date", DATE)
                .put("timestamp_millis_to_date", DATE)
                .put("timestamp_micros_to_date", DATE)
                .put("timestamp_nanos_to_date", DATE)
                .put("timestamp_to_varchar", VARCHAR)
                .put("timestamp_row_to_row", engine == Engine.TRINO ? JAVA_OBJECT : STRUCT)   // row
                .put("timestamp_list_to_list", ARRAY) // list
                .put("timestamp_map_to_map", JAVA_OBJECT) // map
                .put("string_to_timestamp", TIMESTAMP)
                .put("binary_to_string", VARCHAR)
                .put("binary_to_smaller_varchar", VARCHAR)
                .put("hex_representation", VARCHAR)
                .buildOrThrow();

        assertThat(queryResult)
                .hasColumns(columns.stream().map(expectedTypes::get).collect(toImmutableList()));
    }

    private static void alterTableColumnTypes(String tableName)
    {
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN row_to_row row_to_row struct<keep:string, ti2si:smallint, si2int:int, int2bi:bigint, bi2vc:string, LOWER2UPPERCASE:bigint>", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN list_to_list list_to_list array<struct<ti2int:int, si2bi:bigint, bi2vc:string>>", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN map_to_map map_to_map map<int,struct<ti2bi:bigint, int2bi:bigint, float2double:double, add:tinyint>>", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN boolean_to_varchar boolean_to_varchar varchar(5)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN string_to_boolean string_to_boolean boolean", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN special_string_to_boolean special_string_to_boolean boolean", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN numeric_string_to_boolean numeric_string_to_boolean boolean", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN varchar_to_boolean varchar_to_boolean boolean", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN tinyint_to_smallint tinyint_to_smallint smallint", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN tinyint_to_int tinyint_to_int int", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN tinyint_to_bigint tinyint_to_bigint bigint", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN tinyint_to_varchar tinyint_to_varchar varchar(30)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN tinyint_to_string tinyint_to_string string", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN tinyint_to_double tinyint_to_double double", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN tinyint_to_shortdecimal tinyint_to_shortdecimal decimal(10,2)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN tinyint_to_longdecimal tinyint_to_longdecimal decimal(20,2)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN smallint_to_int smallint_to_int int", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN smallint_to_bigint smallint_to_bigint bigint", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN smallint_to_varchar smallint_to_varchar varchar(30)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN smallint_to_string smallint_to_string string", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN smallint_to_double smallint_to_double double", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN smallint_to_shortdecimal smallint_to_shortdecimal decimal(10,2)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN smallint_to_longdecimal smallint_to_longdecimal decimal(20,2)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN int_to_bigint int_to_bigint bigint", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN int_to_varchar int_to_varchar varchar(30)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN int_to_string int_to_string string", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN int_to_double int_to_double double", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN int_to_shortdecimal int_to_shortdecimal decimal(10,2)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN int_to_longdecimal int_to_longdecimal decimal(20,2)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN bigint_to_double bigint_to_double double", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN bigint_to_varchar bigint_to_varchar varchar(30)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN bigint_to_string bigint_to_string string", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN bigint_to_shortdecimal bigint_to_shortdecimal decimal(10,2)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN bigint_to_longdecimal bigint_to_longdecimal decimal(20,2)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN float_to_double float_to_double double", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN float_to_string float_to_string string", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN float_to_bounded_varchar float_to_bounded_varchar varchar(12)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN float_infinity_to_string float_infinity_to_string string", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN double_to_float double_to_float float", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN double_to_string double_to_string string", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN double_to_bounded_varchar double_to_bounded_varchar varchar(12)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN double_infinity_to_string double_infinity_to_string string", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN shortdecimal_to_shortdecimal shortdecimal_to_shortdecimal DECIMAL(18,4)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN shortdecimal_to_longdecimal shortdecimal_to_longdecimal DECIMAL(20,4)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN longdecimal_to_shortdecimal longdecimal_to_shortdecimal DECIMAL(12,2)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN longdecimal_to_longdecimal longdecimal_to_longdecimal DECIMAL(38,14)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN longdecimal_to_tinyint longdecimal_to_tinyint TINYINT", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN shortdecimal_to_tinyint shortdecimal_to_tinyint TINYINT", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN longdecimal_to_smallint longdecimal_to_smallint SMALLINT", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN shortdecimal_to_smallint shortdecimal_to_smallint SMALLINT", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN too_big_shortdecimal_to_smallint too_big_shortdecimal_to_smallint SMALLINT", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN longdecimal_to_int longdecimal_to_int INTEGER", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN shortdecimal_to_int shortdecimal_to_int INTEGER", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN shortdecimal_with_0_scale_to_int shortdecimal_with_0_scale_to_int INTEGER", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN longdecimal_to_bigint longdecimal_to_bigint BIGINT", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN shortdecimal_to_bigint shortdecimal_to_bigint BIGINT", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN float_to_decimal float_to_decimal DECIMAL(10,5)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN double_to_decimal double_to_decimal DECIMAL(10,5)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN decimal_to_float decimal_to_float float", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN decimal_to_double decimal_to_double double", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN short_decimal_to_varchar short_decimal_to_varchar string", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN long_decimal_to_varchar long_decimal_to_varchar string", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN short_decimal_to_bounded_varchar short_decimal_to_bounded_varchar varchar(30)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN long_decimal_to_bounded_varchar long_decimal_to_bounded_varchar varchar(30)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN varchar_to_tinyint varchar_to_tinyint tinyint", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN string_to_tinyint string_to_tinyint tinyint", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN varchar_to_smallint varchar_to_smallint smallint", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN string_to_smallint string_to_smallint smallint", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN varchar_to_integer varchar_to_integer integer", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN string_to_integer string_to_integer integer", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN varchar_to_bigint varchar_to_bigint bigint", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN string_to_bigint string_to_bigint bigint", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN varchar_to_bigger_varchar varchar_to_bigger_varchar varchar(4)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN varchar_to_smaller_varchar varchar_to_smaller_varchar varchar(2)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN varchar_to_date varchar_to_date date", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN date_to_string date_to_string string", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN date_to_bounded_varchar date_to_bounded_varchar varchar(12)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN varchar_to_distant_date varchar_to_distant_date date", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN varchar_to_float varchar_to_float float", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN string_to_float string_to_float float", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN varchar_to_float_infinity varchar_to_float_infinity float", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN varchar_to_special_float varchar_to_special_float float", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN varchar_to_double varchar_to_double double", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN string_to_double string_to_double double", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN varchar_to_double_infinity varchar_to_double_infinity double", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN varchar_to_special_double varchar_to_special_double double", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN char_to_bigger_char char_to_bigger_char char(4)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN char_to_smaller_char char_to_smaller_char char(2)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN char_to_string char_to_string string", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN char_to_bigger_varchar char_to_bigger_varchar varchar(4)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN char_to_smaller_varchar char_to_smaller_varchar varchar(2)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN string_to_char string_to_char char(1)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN varchar_to_bigger_char varchar_to_bigger_char char(6)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN varchar_to_smaller_char varchar_to_smaller_char char(2)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN timestamp_millis_to_date timestamp_millis_to_date date", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN timestamp_micros_to_date timestamp_micros_to_date date", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN timestamp_nanos_to_date timestamp_nanos_to_date date", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN timestamp_to_string timestamp_to_string string", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN timestamp_to_bounded_varchar timestamp_to_bounded_varchar varchar(30)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN timestamp_to_smaller_varchar timestamp_to_smaller_varchar varchar(4)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN smaller_varchar_to_timestamp smaller_varchar_to_timestamp timestamp", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN varchar_to_timestamp varchar_to_timestamp timestamp", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN binary_to_string binary_to_string string", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN binary_to_smaller_varchar binary_to_smaller_varchar varchar(2)", tableName));
    }

    protected static TableInstance<?> mutableTableInstanceOf(TableDefinition tableDefinition)
    {
        if (tableDefinition.getDatabase().isPresent()) {
            return mutableTableInstanceOf(tableDefinition, tableDefinition.getDatabase().get());
        }
        return mutableTableInstanceOf(tableHandleInSchema(tableDefinition));
    }

    private static TableInstance<?> mutableTableInstanceOf(TableDefinition tableDefinition, String database)
    {
        return mutableTableInstanceOf(tableHandleInSchema(tableDefinition).inDatabase(database));
    }

    private static TableInstance<?> mutableTableInstanceOf(TableHandle tableHandle)
    {
        return testContext().getDependency(MutableTablesState.class).get(tableHandle);
    }

    private static TableHandle tableHandleInSchema(TableDefinition tableDefinition)
    {
        TableHandle tableHandle = tableHandle(tableDefinition.getName());
        if (tableDefinition.getSchema().isPresent()) {
            tableHandle = tableHandle.inSchema(tableDefinition.getSchema().get());
        }
        return tableHandle;
    }

    private io.trino.jdbc.Row.Builder rowBuilder()
    {
        return io.trino.jdbc.Row.builder();
    }

    private static Row project(Row row, int... columns)
    {
        return new Row(Arrays.stream(columns)
                .mapToObj(column -> row.getValues().get(column - 1))
                .collect(toList())); // to allow nulls
    }

    private static List<Row> project(List<Row> rows, int... columns)
    {
        return rows.stream()
                .map(row -> project(row, columns))
                .collect(toImmutableList());
    }

    private static List<?> column(List<Row> rows, int sqlColumnIndex)
    {
        return rows.stream()
                .map(row -> project(row, sqlColumnIndex).getValues().get(0))
                .collect(toList()); // to allow nulls
    }

    private static List<Object> extract(List<TrinoArray> arrays)
    {
        return arrays.stream()
                .map(trinoArray -> ImmutableList.copyOf((Object[]) trinoArray.getArray()))
                .collect(toImmutableList());
    }

    public static ColumnContext columnContext(String version, String format, String column)
    {
        return new ColumnContext(Optional.of(version), format, column);
    }

    public static ColumnContext columnContext(String format, String column)
    {
        return new ColumnContext(Optional.empty(), format, column);
    }

    public record ColumnContext(Optional<String> hiveVersion, String format, String column)
    {
        public ColumnContext
        {
            requireNonNull(hiveVersion, "hiveVersion is null");
            requireNonNull(format, "format is null");
            requireNonNull(column, "column is null");
        }
    }

    private static QueryResult execute(Engine engine, String sql, QueryExecutor.QueryParam... params)
    {
        return engine.queryExecutor().executeQuery(sql, params);
    }

    private static void setHiveTimestampPrecision(HiveTimestampPrecision hiveTimestampPrecision)
    {
        try {
            setSessionProperty(onTrino().getConnection(), "hive.timestamp_precision", hiveTimestampPrecision.name());
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void resetHiveTimestampPrecision()
    {
        try {
            resetSessionProperty(onTrino().getConnection(), "hive.timestamp_precision");
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}

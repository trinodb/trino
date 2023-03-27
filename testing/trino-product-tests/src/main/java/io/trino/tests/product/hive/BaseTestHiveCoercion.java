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
import io.trino.jdbc.TrinoArray;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tempto.context.ThreadLocalTestContextHolder.testContext;
import static io.trino.tempto.fulfillment.table.TableHandle.tableHandle;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static java.sql.JDBCType.ARRAY;
import static java.sql.JDBCType.BIGINT;
import static java.sql.JDBCType.CHAR;
import static java.sql.JDBCType.DECIMAL;
import static java.sql.JDBCType.DOUBLE;
import static java.sql.JDBCType.FLOAT;
import static java.sql.JDBCType.INTEGER;
import static java.sql.JDBCType.JAVA_OBJECT;
import static java.sql.JDBCType.REAL;
import static java.sql.JDBCType.SMALLINT;
import static java.sql.JDBCType.STRUCT;
import static java.sql.JDBCType.VARCHAR;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public abstract class BaseTestHiveCoercion
        extends HiveProductTest
{
    protected void doTestHiveCoercion(HiveTableDefinition tableDefinition)
    {
        String tableName = mutableTableInstanceOf(tableDefinition).getNameInDatabase();

        String floatToDoubleType = tableName.toLowerCase(ENGLISH).contains("parquet") ? "DOUBLE" : "REAL";
        String floatToDecimalVal = tableName.toLowerCase(ENGLISH).contains("parquet") ? "12345.12345" : "12345.12300";
        String decimalToFloatVal = tableName.toLowerCase(ENGLISH).contains("parquet") ? "12345.12345" : "12345.124";

        insertTableRows(tableName, floatToDoubleType);

        alterTableColumnTypes(tableName);
        assertProperAlteredTableSchema(tableName);

        List<String> allColumns = ImmutableList.of(
                "row_to_row",
                "list_to_list",
                "map_to_map",
                "tinyint_to_smallint",
                "tinyint_to_int",
                "tinyint_to_bigint",
                "smallint_to_int",
                "smallint_to_bigint",
                "int_to_bigint",
                "bigint_to_varchar",
                "float_to_double",
                "double_to_float",
                "shortdecimal_to_shortdecimal",
                "shortdecimal_to_longdecimal",
                "longdecimal_to_shortdecimal",
                "longdecimal_to_longdecimal",
                "float_to_decimal",
                "double_to_decimal",
                "decimal_to_float",
                "decimal_to_double",
                "short_decimal_to_varchar",
                "long_decimal_to_varchar",
                "short_decimal_to_bounded_varchar",
                "long_decimal_to_bounded_varchar",
                "varchar_to_bigger_varchar",
                "varchar_to_smaller_varchar",
                "char_to_bigger_char",
                "char_to_smaller_char",
                "id");

        Function<Engine, Map<String, List<Object>>> expected = engine -> expectedValuesForEngineProvider(engine, tableName, decimalToFloatVal, floatToDecimalVal);

        // For Trino, remove unsupported columns
        List<String> prestoReadColumns = removeUnsupportedColumnsForTrino(allColumns, tableName);
        Map<String, List<Object>> expectedPrestoResults = expected.apply(Engine.TRINO);
        // In case of unpartitioned tables we don't support all the column coercion thereby making this assertion conditional
        if (expectedExceptionsWithTrinoContext().isEmpty()) {
            assertEquals(ImmutableSet.copyOf(prestoReadColumns), expectedPrestoResults.keySet());
        }
        String prestoSelectQuery = format("SELECT %s FROM %s", String.join(", ", prestoReadColumns), tableName);
        assertQueryResults(Engine.TRINO, prestoSelectQuery, expectedPrestoResults, prestoReadColumns, 2, tableName);

        // For Hive, remove unsupported columns for the current file format and hive version
        List<String> hiveReadColumns = removeUnsupportedColumnsForHive(allColumns, tableName);
        Map<String, List<Object>> expectedHiveResults = expected.apply(Engine.HIVE);
        String hiveSelectQuery = format("SELECT %s FROM %s", String.join(", ", hiveReadColumns), tableName);
        assertQueryResults(Engine.HIVE, hiveSelectQuery, expectedHiveResults, hiveReadColumns, 2, tableName);

        assertNestedSubFields(tableName);
    }

    protected void insertTableRows(String tableName, String floatToDoubleType)
    {
        onTrino().executeQuery(format(
                "INSERT INTO %1$s VALUES " +
                        "(" +
                        "  CAST(ROW ('as is', -1, 100, 2323, 12345, 2) AS ROW(keep VARCHAR, ti2si TINYINT, si2int SMALLINT, int2bi INTEGER, bi2vc BIGINT, lower2uppercase BIGINT)), " +
                        "  ARRAY [CAST(ROW (2, -101, 12345, 'removed') AS ROW (ti2int TINYINT, si2bi SMALLINT, bi2vc BIGINT, remove VARCHAR))], " +
                        "  MAP (ARRAY [TINYINT '2'], ARRAY [CAST(ROW (-3, 2323, REAL '0.5') AS ROW (ti2bi TINYINT, int2bi INTEGER, float2double %2$s))]), " +
                        "  TINYINT '-1', " +
                        "  TINYINT '2', " +
                        "  TINYINT '-3', " +
                        "  SMALLINT '100', " +
                        "  SMALLINT '-101', " +
                        "  INTEGER '2323', " +
                        "  12345, " +
                        "  REAL '0.5', " +
                        "  DOUBLE '0.5', " +
                        "  DECIMAL '12345678.12', " +
                        "  DECIMAL '12345678.12', " +
                        "  DECIMAL '12345678.123456123456', " +
                        "  DECIMAL '12345678.123456123456', " +
                        "  %2$s '12345.12345', " +
                        "  DOUBLE '12345.12345', " +
                        "  DECIMAL '12345.12345', " +
                        "  DECIMAL '12345.12345', " +
                        "  DECIMAL '12345.12345', " +
                        "  DECIMAL '12345678.123456123456', " +
                        "  DECIMAL '12345.12345', " +
                        "  DECIMAL '12345678.123456123456', " +
                        "  'abc', " +
                        "  'abc', " +
                        "  'abc', " +
                        "  'abc', " +
                        "  1), " +
                        "(" +
                        "  CAST(ROW (NULL, 1, -100, -2323, -12345, 2) AS ROW(keep VARCHAR, ti2si TINYINT, si2int SMALLINT, int2bi INTEGER, bi2vc BIGINT, lower2uppercase BIGINT)), " +
                        "  ARRAY [CAST(ROW (-2, 101, -12345, NULL) AS ROW (ti2int TINYINT, si2bi SMALLINT, bi2vc BIGINT, remove VARCHAR))], " +
                        "  MAP (ARRAY [TINYINT '-2'], ARRAY [CAST(ROW (null, -2323, REAL '-1.5') AS ROW (ti2bi TINYINT, int2bi INTEGER, float2double %2$s))]), " +
                        "  TINYINT '1', " +
                        "  TINYINT '-2', " +
                        "  NULL, " +
                        "  SMALLINT '-100', " +
                        "  SMALLINT '101', " +
                        "  INTEGER '-2323', " +
                        "  -12345, " +
                        "  REAL '-1.5', " +
                        "  DOUBLE '-1.5', " +
                        "  DECIMAL '-12345678.12', " +
                        "  DECIMAL '-12345678.12', " +
                        "  DECIMAL '-12345678.123456123456', " +
                        "  DECIMAL '-12345678.123456123456', " +
                        "  %2$s '-12345.12345', " +
                        "  DOUBLE '-12345.12345', " +
                        "  DECIMAL '-12345.12345', " +
                        "  DECIMAL '-12345.12345', " +
                        "  DECIMAL '-12345.12345', " +
                        "  DECIMAL '-12345678.123456123456', " +
                        "  DECIMAL '-12345.12345', " +
                        "  DECIMAL '-12345678.123456123456', " +
                        "  '\uD83D\uDCB0\uD83D\uDCB0\uD83D\uDCB0', " +
                        "  '\uD83D\uDCB0\uD83D\uDCB0\uD83D\uDCB0', " +
                        "  '\uD83D\uDCB0\uD83D\uDCB0\uD83D\uDCB0', " +
                        "  '\uD83D\uDCB0\uD83D\uDCB0\uD83D\uDCB0', " +
                        "  1)",
                tableName,
                floatToDoubleType));
    }

    protected Map<String, List<Object>> expectedValuesForEngineProvider(Engine engine, String tableName, String decimalToFloatVal, String floatToDecimalVal)
    {
        String hiveValueForCaseChangeField;
        Predicate<String> isFormat = formatName -> tableName.toLowerCase(ENGLISH).contains(formatName);
        if (isFormat.test("rctext") || isFormat.test("textfile")) {
            hiveValueForCaseChangeField = "\"lower2uppercase\":2";
        }
        else if (getHiveVersionMajor() == 3 && isFormat.test("orc")) {
            hiveValueForCaseChangeField = "\"LOWER2UPPERCASE\":null";
        }
        else {
            hiveValueForCaseChangeField = "\"LOWER2UPPERCASE\":2";
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
                .put("tinyint_to_smallint", ImmutableList.of(
                        -1,
                        1))
                .put("tinyint_to_int", ImmutableList.of(
                        2,
                        -2))
                .put("tinyint_to_bigint", Arrays.asList(
                        -3L,
                        null))
                .put("smallint_to_int", ImmutableList.of(
                        100,
                        -100))
                .put("smallint_to_bigint", ImmutableList.of(
                        -101L,
                        101L))
                .put("int_to_bigint", ImmutableList.of(
                        2323L,
                        -2323L))
                .put("bigint_to_varchar", ImmutableList.of(
                        "12345",
                        "-12345"))
                .put("float_to_double", ImmutableList.of(
                        0.5,
                        -1.5))
                .put("double_to_float", ImmutableList.of(0.5, -1.5))
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
                .put("float_to_decimal", ImmutableList.of(new BigDecimal(floatToDecimalVal), new BigDecimal("-" + floatToDecimalVal)))
                .put("double_to_decimal", ImmutableList.of(new BigDecimal("12345.12345"), new BigDecimal("-12345.12345")))
                .put("decimal_to_float", ImmutableList.of(
                        Float.parseFloat(decimalToFloatVal),
                        -Float.parseFloat(decimalToFloatVal)))
                .put("decimal_to_double", ImmutableList.of(
                        12345.12345,
                        -12345.12345))
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
                .put("varchar_to_bigger_varchar", ImmutableList.of(
                        "abc",
                        "\uD83D\uDCB0\uD83D\uDCB0\uD83D\uDCB0"))
                .put("varchar_to_smaller_varchar", ImmutableList.of(
                        "ab",
                        "\uD83D\uDCB0\uD83D\uDCB0"))
                .put("char_to_bigger_char", ImmutableList.of(
                        "abc ",
                        "\uD83D\uDCB0\uD83D\uDCB0\uD83D\uDCB0 "))
                .put("char_to_smaller_char", ImmutableList.of(
                        "ab",
                        "\uD83D\uDCB0\uD83D\uDCB0"))
                .put("id", ImmutableList.of(
                        1,
                        1))
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
        if (getHiveVersionMajor() == 3 && isFormat.test("orc")) {
            expectedNestedFieldHive = ImmutableMap.of("nested_field", Arrays.asList(null, null));
        }
        else {
            expectedNestedFieldHive = expectedNestedFieldTrino;
        }
        String subfieldQueryLowerCase = format("SELECT row_to_row.lower2uppercase nested_field FROM %s", tableName);
        String subfieldQueryUpperCase = format("SELECT row_to_row.LOWER2UPPERCASE nested_field FROM %s", tableName);
        List<String> expectedColumns = ImmutableList.of("nested_field");

        // Assert Trino behavior
        assertQueryResults(Engine.TRINO, subfieldQueryUpperCase, expectedNestedFieldTrino, expectedColumns, 2, tableName);
        assertQueryResults(Engine.TRINO, subfieldQueryLowerCase, expectedNestedFieldTrino, expectedColumns, 2, tableName);

        // Assert Hive behavior
        if (isFormat.test("rcbinary")) {
            assertThatThrownBy(() -> assertQueryResults(Engine.HIVE, subfieldQueryUpperCase, expectedNestedFieldTrino, expectedColumns, 2, tableName))
                    .hasMessageContaining("org.apache.hadoop.hive.ql.metadata.HiveException");
            assertThatThrownBy(() -> assertQueryResults(Engine.HIVE, subfieldQueryLowerCase, expectedNestedFieldTrino, expectedColumns, 2, tableName))
                    .hasMessageContaining("org.apache.hadoop.hive.ql.metadata.HiveException");
        }
        else if (isFormat.test("parquet")) {
            assertQueryResults(Engine.HIVE, subfieldQueryUpperCase, expectedNestedFieldHive, expectedColumns, 2, tableName);

            if (getHiveVersionMajor() == 1) {
                assertThatThrownBy(() -> assertQueryResults(Engine.HIVE, subfieldQueryLowerCase, expectedNestedFieldHive, expectedColumns, 2, tableName))
                        .hasMessageContaining("java.sql.SQLException");
            }
            else {
                assertQueryResults(Engine.HIVE, subfieldQueryLowerCase, expectedNestedFieldHive, expectedColumns, 2, tableName);
            }
        }
        else {
            assertQueryResults(Engine.HIVE, subfieldQueryUpperCase, expectedNestedFieldHive, expectedColumns, 2, tableName);
            assertQueryResults(Engine.HIVE, subfieldQueryLowerCase, expectedNestedFieldHive, expectedColumns, 2, tableName);
        }
    }

    protected Map<ColumnContext, String> expectedExceptionsWithHiveContext()
    {
        return ImmutableMap.<ColumnContext, String>builder()
                // 1.1
                // Parquet
                .put(columnContext("1.1", "parquet", "row_to_row"), "org.apache.hadoop.io.LongWritable cannot be cast to org.apache.hadoop.io.IntWritable")
                .put(columnContext("1.1", "parquet", "list_to_list"), "org.apache.hadoop.io.LongWritable cannot be cast to org.apache.hadoop.hive.serde2.io.ShortWritable")
                .put(columnContext("1.1", "parquet", "map_to_map"), "org.apache.hadoop.io.LongWritable cannot be cast to org.apache.hadoop.hive.serde2.io.ByteWritable")
                .put(columnContext("1.1", "parquet", "tinyint_to_bigint"), "org.apache.hadoop.io.LongWritable cannot be cast to org.apache.hadoop.hive.serde2.io.ByteWritable")
                .put(columnContext("1.1", "parquet", "smallint_to_bigint"), "org.apache.hadoop.io.LongWritable cannot be cast to org.apache.hadoop.hive.serde2.io.ShortWritable")
                .put(columnContext("1.1", "parquet", "int_to_bigint"), "org.apache.hadoop.io.LongWritable cannot be cast to org.apache.hadoop.io.IntWritable")
                // Rcbinary
                .put(columnContext("1.1", "rcbinary", "row_to_row"), "java.util.ArrayList cannot be cast to org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryStruct")
                .put(columnContext("1.1", "rcbinary", "list_to_list"), "java.util.ArrayList cannot be cast to org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryArray")
                .put(columnContext("1.1", "rcbinary", "map_to_map"), "java.util.HashMap cannot be cast to org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryMap")
                //
                // 1.2
                // Orc
                .put(columnContext("1.2", "orc", "map_to_map"), "Unknown encoding kind: DIRECT_V2")
                // Parquet
                .put(columnContext("1.2", "parquet", "list_to_list"), "java.lang.UnsupportedOperationException: Cannot inspect java.util.ArrayList")
                // Rcbinary
                .put(columnContext("1.2", "rcbinary", "row_to_row"), "java.util.ArrayList cannot be cast to org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryStruct")
                .put(columnContext("1.2", "rcbinary", "list_to_list"), "java.util.ArrayList cannot be cast to org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryArray")
                .put(columnContext("1.2", "rcbinary", "map_to_map"), "java.util.HashMap cannot be cast to org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryMap")
                //
                // 2.1
                // Parquet
                .put(columnContext("2.1", "parquet", "row_to_row"), "org.apache.hadoop.io.LongWritable cannot be cast to org.apache.hadoop.io.IntWritable")
                .put(columnContext("2.1", "parquet", "list_to_list"), "org.apache.hadoop.io.LongWritable cannot be cast to org.apache.hadoop.hive.serde2.io.ShortWritable")
                .put(columnContext("2.1", "parquet", "map_to_map"), "org.apache.hadoop.io.LongWritable cannot be cast to org.apache.hadoop.hive.serde2.io.ByteWritable")
                .put(columnContext("2.1", "parquet", "tinyint_to_bigint"), "org.apache.hadoop.io.LongWritable cannot be cast to org.apache.hadoop.hive.serde2.io.ByteWritable")
                .put(columnContext("2.1", "parquet", "smallint_to_bigint"), "org.apache.hadoop.io.LongWritable cannot be cast to org.apache.hadoop.hive.serde2.io.ShortWritable")
                .put(columnContext("2.1", "parquet", "int_to_bigint"), "org.apache.hadoop.io.LongWritable cannot be cast to org.apache.hadoop.io.IntWritable")
                // Rcbinary
                .put(columnContext("2.1", "rcbinary", "row_to_row"), "java.util.ArrayList cannot be cast to org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryStruct")
                .put(columnContext("2.1", "rcbinary", "list_to_list"), "java.util.ArrayList cannot be cast to org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryArray")
                .put(columnContext("2.1", "rcbinary", "map_to_map"), "java.util.HashMap cannot be cast to org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryMap")
                //
                // 3.1
                // Parquet
                .put(columnContext("3.1", "parquet", "row_to_row"), "org.apache.hadoop.io.LongWritable cannot be cast to org.apache.hadoop.io.IntWritable")
                .put(columnContext("3.1", "parquet", "list_to_list"), "org.apache.hadoop.io.LongWritable cannot be cast to org.apache.hadoop.hive.serde2.io.ShortWritable")
                .put(columnContext("3.1", "parquet", "map_to_map"), "org.apache.hadoop.io.LongWritable cannot be cast to org.apache.hadoop.hive.serde2.io.ByteWritable")
                .put(columnContext("3.1", "parquet", "tinyint_to_bigint"), "org.apache.hadoop.io.LongWritable cannot be cast to org.apache.hadoop.hive.serde2.io.ByteWritable")
                .put(columnContext("3.1", "parquet", "smallint_to_bigint"), "org.apache.hadoop.io.LongWritable cannot be cast to org.apache.hadoop.hive.serde2.io.ShortWritable")
                .put(columnContext("3.1", "parquet", "int_to_bigint"), "org.apache.hadoop.io.LongWritable cannot be cast to org.apache.hadoop.io.IntWritable")
                // Rcbinary
                .put(columnContext("3.1", "rcbinary", "row_to_row"), "java.util.ArrayList cannot be cast to org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryStruct")
                .put(columnContext("3.1", "rcbinary", "list_to_list"), "java.util.ArrayList cannot be cast to org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryArray")
                .put(columnContext("3.1", "rcbinary", "map_to_map"), "java.util.LinkedHashMap cannot be cast to org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryMap")
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
            int rowCount,
            String tableName)
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
        assertColumnTypes(actual, tableName, engine, columns);

        for (int sqlIndex = 1; sqlIndex <= columns.size(); sqlIndex++) {
            String column = columns.get(sqlIndex - 1);

            if (column.equals("row_to_row") || column.equals("map_to_map")) {
                assertEqualsIgnoreOrder(
                        actual.column(sqlIndex),
                        column(expectedRows, sqlIndex),
                        format("%s field is not equal", column));
                continue;
            }

            if (column.equals("list_to_list")) {
                assertEqualsIgnoreOrder(
                        engine == Engine.TRINO ? extract(actual.column(sqlIndex)) : actual.column(sqlIndex),
                        column(expectedRows, sqlIndex),
                        "list_to_list field is not equal");
                continue;
            }

            // test primitive values
            assertThat(actual.project(sqlIndex)).containsOnly(project(expectedRows, sqlIndex));
        }
    }

    private void assertProperAlteredTableSchema(String tableName)
    {
        String floatType = tableName.toLowerCase(ENGLISH).contains("parquet") ? "double" : "real";

        assertThat(onTrino().executeQuery("SHOW COLUMNS FROM " + tableName).project(1, 2)).containsExactlyInOrder(
                // The field lower2uppercase in the row is recorded in upper case in hive, but Trino converts it to lower case
                row("row_to_row", "row(keep varchar, ti2si smallint, si2int integer, int2bi bigint, bi2vc varchar, lower2uppercase bigint)"),
                row("list_to_list", "array(row(ti2int integer, si2bi bigint, bi2vc varchar))"),
                row("map_to_map", "map(integer, row(ti2bi bigint, int2bi bigint, float2double double, add tinyint))"),
                row("tinyint_to_smallint", "smallint"),
                row("tinyint_to_int", "integer"),
                row("tinyint_to_bigint", "bigint"),
                row("smallint_to_int", "integer"),
                row("smallint_to_bigint", "bigint"),
                row("int_to_bigint", "bigint"),
                row("bigint_to_varchar", "varchar"),
                row("float_to_double", "double"),
                row("double_to_float", floatType),
                row("shortdecimal_to_shortdecimal", "decimal(18,4)"),
                row("shortdecimal_to_longdecimal", "decimal(20,4)"),
                row("longdecimal_to_shortdecimal", "decimal(12,2)"),
                row("longdecimal_to_longdecimal", "decimal(38,14)"),
                row("float_to_decimal", "decimal(10,5)"),
                row("double_to_decimal", "decimal(10,5)"),
                row("decimal_to_float", floatType),
                row("decimal_to_double", "double"),
                row("short_decimal_to_varchar", "varchar"),
                row("long_decimal_to_varchar", "varchar"),
                row("short_decimal_to_bounded_varchar", "varchar(30)"),
                row("long_decimal_to_bounded_varchar", "varchar(30)"),
                row("varchar_to_bigger_varchar", "varchar(4)"),
                row("varchar_to_smaller_varchar", "varchar(2)"),
                row("char_to_bigger_char", "char(4)"),
                row("char_to_smaller_char", "char(2)"),
                row("id", "bigint"));
    }

    private void assertColumnTypes(
            QueryResult queryResult,
            String tableName,
            Engine engine,
            List<String> columns)
    {
        JDBCType floatType;
        if (engine == Engine.TRINO) {
            floatType = tableName.toLowerCase(ENGLISH).contains("parquet") ? DOUBLE : REAL;
        }
        else {
            floatType = tableName.toLowerCase(ENGLISH).contains("parquet") ? DOUBLE : FLOAT;
        }

        Map<String, JDBCType> expectedTypes = ImmutableMap.<String, JDBCType>builder()
                .put("row_to_row", engine == Engine.TRINO ? JAVA_OBJECT : STRUCT)   // row
                .put("list_to_list", ARRAY) // list
                .put("map_to_map", JAVA_OBJECT) // map
                .put("tinyint_to_smallint", SMALLINT)
                .put("tinyint_to_int", INTEGER)
                .put("tinyint_to_bigint", BIGINT)
                .put("smallint_to_int", INTEGER)
                .put("smallint_to_bigint", BIGINT)
                .put("int_to_bigint", BIGINT)
                .put("bigint_to_varchar", VARCHAR)
                .put("float_to_double", DOUBLE)
                .put("double_to_float", floatType)
                .put("shortdecimal_to_shortdecimal", DECIMAL)
                .put("shortdecimal_to_longdecimal", DECIMAL)
                .put("longdecimal_to_shortdecimal", DECIMAL)
                .put("longdecimal_to_longdecimal", DECIMAL)
                .put("float_to_decimal", DECIMAL)
                .put("double_to_decimal", DECIMAL)
                .put("decimal_to_float", floatType)
                .put("decimal_to_double", DOUBLE)
                .put("short_decimal_to_varchar", VARCHAR)
                .put("long_decimal_to_varchar", VARCHAR)
                .put("short_decimal_to_bounded_varchar", VARCHAR)
                .put("long_decimal_to_bounded_varchar", VARCHAR)
                .put("varchar_to_bigger_varchar", VARCHAR)
                .put("varchar_to_smaller_varchar", VARCHAR)
                .put("char_to_bigger_char", CHAR)
                .put("char_to_smaller_char", CHAR)
                .put("id", BIGINT)
                .put("nested_field", BIGINT)
                .buildOrThrow();

        assertThat(queryResult)
                .hasColumns(columns.stream().map(expectedTypes::get).collect(toImmutableList()));
    }

    private static void alterTableColumnTypes(String tableName)
    {
        String floatType = tableName.toLowerCase(ENGLISH).contains("parquet") ? "double" : "float";

        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN row_to_row row_to_row struct<keep:string, ti2si:smallint, si2int:int, int2bi:bigint, bi2vc:string, LOWER2UPPERCASE:bigint>", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN list_to_list list_to_list array<struct<ti2int:int, si2bi:bigint, bi2vc:string>>", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN map_to_map map_to_map map<int,struct<ti2bi:bigint, int2bi:bigint, float2double:double, add:tinyint>>", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN tinyint_to_smallint tinyint_to_smallint smallint", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN tinyint_to_int tinyint_to_int int", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN tinyint_to_bigint tinyint_to_bigint bigint", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN smallint_to_int smallint_to_int int", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN smallint_to_bigint smallint_to_bigint bigint", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN int_to_bigint int_to_bigint bigint", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN bigint_to_varchar bigint_to_varchar string", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN float_to_double float_to_double double", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN double_to_float double_to_float %s", tableName, floatType));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN shortdecimal_to_shortdecimal shortdecimal_to_shortdecimal DECIMAL(18,4)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN shortdecimal_to_longdecimal shortdecimal_to_longdecimal DECIMAL(20,4)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN longdecimal_to_shortdecimal longdecimal_to_shortdecimal DECIMAL(12,2)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN longdecimal_to_longdecimal longdecimal_to_longdecimal DECIMAL(38,14)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN float_to_decimal float_to_decimal DECIMAL(10,5)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN double_to_decimal double_to_decimal DECIMAL(10,5)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN decimal_to_float decimal_to_float %s", tableName, floatType));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN decimal_to_double decimal_to_double double", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN short_decimal_to_varchar short_decimal_to_varchar string", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN long_decimal_to_varchar long_decimal_to_varchar string", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN short_decimal_to_bounded_varchar short_decimal_to_bounded_varchar varchar(30)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN long_decimal_to_bounded_varchar long_decimal_to_bounded_varchar varchar(30)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN varchar_to_bigger_varchar varchar_to_bigger_varchar varchar(4)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN varchar_to_smaller_varchar varchar_to_smaller_varchar varchar(2)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN char_to_bigger_char char_to_bigger_char char(4)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN char_to_smaller_char char_to_smaller_char char(2)", tableName));
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

    private static List<List<?>> extract(List<TrinoArray> arrays)
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
}

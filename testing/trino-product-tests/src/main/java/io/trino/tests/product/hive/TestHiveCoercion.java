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
import io.trino.tempto.Requirement;
import io.trino.tempto.RequirementsProvider;
import io.trino.tempto.Requires;
import io.trino.tempto.assertions.QueryAssert.Row;
import io.trino.tempto.configuration.Configuration;
import io.trino.tempto.fulfillment.table.MutableTableRequirement;
import io.trino.tempto.fulfillment.table.MutableTablesState;
import io.trino.tempto.fulfillment.table.TableDefinition;
import io.trino.tempto.fulfillment.table.TableHandle;
import io.trino.tempto.fulfillment.table.TableInstance;
import io.trino.tempto.fulfillment.table.hive.HiveTableDefinition;
import io.trino.tempto.query.QueryExecutor;
import io.trino.tempto.query.QueryResult;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tempto.context.ThreadLocalTestContextHolder.testContext;
import static io.trino.tempto.fulfillment.table.MutableTableRequirement.State.CREATED;
import static io.trino.tempto.fulfillment.table.TableHandle.tableHandle;
import static io.trino.tests.product.TestGroups.HIVE_COERCION;
import static io.trino.tests.product.TestGroups.JDBC;
import static io.trino.tests.product.hive.TestHiveCoercion.ColumnContext.columnContext;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static java.sql.JDBCType.ARRAY;
import static java.sql.JDBCType.BIGINT;
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
import static org.testng.Assert.assertEquals;

public class TestHiveCoercion
        extends HiveProductTest
{
    public static final HiveTableDefinition HIVE_COERCION_TEXTFILE = tableDefinitionBuilder("TEXTFILE", Optional.empty(), Optional.of("DELIMITED FIELDS TERMINATED BY '|'"))
            .setNoData()
            .build();

    public static final HiveTableDefinition HIVE_COERCION_PARQUET = tableDefinitionBuilder("PARQUET", Optional.empty(), Optional.empty())
            .setNoData()
            .build();

    public static final HiveTableDefinition HIVE_COERCION_AVRO = avroTableDefinitionBuilder()
            .setNoData()
            .build();

    public static final HiveTableDefinition HIVE_COERCION_ORC = tableDefinitionBuilder("ORC", Optional.empty(), Optional.empty())
            .setNoData()
            .build();

    public static final HiveTableDefinition HIVE_COERCION_RCTEXT = tableDefinitionBuilder("RCFILE", Optional.of("RCTEXT"), Optional.of("SERDE 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe'"))
            .setNoData()
            .build();

    public static final HiveTableDefinition HIVE_COERCION_RCBINARY = tableDefinitionBuilder("RCFILE", Optional.of("RCBINARY"), Optional.of("SERDE 'org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe'"))
            .setNoData()
            .build();

    private static HiveTableDefinition.HiveTableDefinitionBuilder tableDefinitionBuilder(String fileFormat, Optional<String> recommendTableName, Optional<String> rowFormat)
    {
        String tableName = format("%s_hive_coercion", recommendTableName.orElse(fileFormat).toLowerCase(ENGLISH));
        String floatType = fileFormat.toLowerCase(ENGLISH).contains("parquet") ? "DOUBLE" : "FLOAT";
        return HiveTableDefinition.builder(tableName)
                .setCreateTableDDLTemplate("" +
                        "CREATE TABLE %NAME%(" +
                        // all nested primitive/varchar coercions and adding/removing tailing nested fields are covered across row_to_row, list_to_list, and map_to_map
                        "    row_to_row                 STRUCT<keep: STRING, ti2si: TINYINT, si2int: SMALLINT, int2bi: INT, bi2vc: BIGINT>, " +
                        "    list_to_list               ARRAY<STRUCT<ti2int: TINYINT, si2bi: SMALLINT, bi2vc: BIGINT, remove: STRING>>, " +
                        "    map_to_map                 MAP<TINYINT, STRUCT<ti2bi: TINYINT, int2bi: INT, float2double: " + floatType + ">>, " +
                        "    tinyint_to_smallint        TINYINT," +
                        "    tinyint_to_int             TINYINT," +
                        "    tinyint_to_bigint          TINYINT," +
                        "    smallint_to_int            SMALLINT," +
                        "    smallint_to_bigint         SMALLINT," +
                        "    int_to_bigint              INT," +
                        "    bigint_to_varchar          BIGINT," +
                        "    float_to_double            " + floatType + "," +
                        //"    double_to_float            DOUBLE," + // this coercion is not permitted in Hive 3. TODO test this on Hive < 3.
                        "    shortdecimal_to_shortdecimal          DECIMAL(10,2)," +
                        "    shortdecimal_to_longdecimal           DECIMAL(10,2)," +
                        "    longdecimal_to_shortdecimal           DECIMAL(20,12)," +
                        "    longdecimal_to_longdecimal            DECIMAL(20,12)," +
                        //"    float_to_decimal           " + floatType + "," + // this coercion is not permitted in Hive 3. TODO test this on Hive < 3.
                        //"    double_to_decimal          DOUBLE," + // this coercion is not permitted in Hive 3. TODO test this on Hive < 3.
                        "    decimal_to_float           DECIMAL(10,5)," +
                        "    decimal_to_double          DECIMAL(10,5)," +
                        "    varchar_to_bigger_varchar  VARCHAR(3)," +
                        "    varchar_to_smaller_varchar VARCHAR(3)" +
                        ") " +
                        "PARTITIONED BY (id BIGINT) " +
                        rowFormat.map(s -> format("ROW FORMAT %s ", s)).orElse("") +
                        "STORED AS " + fileFormat);
    }

    private static HiveTableDefinition.HiveTableDefinitionBuilder avroTableDefinitionBuilder()
    {
        return HiveTableDefinition.builder("avro_hive_coercion")
                .setCreateTableDDLTemplate("" +
                        "CREATE TABLE %NAME%(" +
                        "    int_to_bigint              INT," +
                        "    float_to_double            DOUBLE" +
                        ") " +
                        "PARTITIONED BY (id BIGINT) " +
                        "STORED AS AVRO");
    }

    public static final class TextRequirements
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return MutableTableRequirement.builder(HIVE_COERCION_TEXTFILE).withState(CREATED).build();
        }
    }

    public static final class OrcRequirements
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return MutableTableRequirement.builder(HIVE_COERCION_ORC).withState(CREATED).build();
        }
    }

    public static final class RcTextRequirements
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return MutableTableRequirement.builder(HIVE_COERCION_RCTEXT).withState(CREATED).build();
        }
    }

    public static final class RcBinaryRequirements
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return MutableTableRequirement.builder(HIVE_COERCION_RCBINARY).withState(CREATED).build();
        }
    }

    public static final class ParquetRequirements
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return MutableTableRequirement.builder(HIVE_COERCION_PARQUET).withState(CREATED).build();
        }
    }

    public static final class AvroRequirements
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return MutableTableRequirement.builder(HIVE_COERCION_AVRO).withState(CREATED).build();
        }
    }

    @Requires(TextRequirements.class)
    @Test(groups = {HIVE_COERCION, JDBC})
    public void testHiveCoercionTextFile()
    {
        doTestHiveCoercion(HIVE_COERCION_TEXTFILE);
    }

    @Requires(OrcRequirements.class)
    @Test(groups = {HIVE_COERCION, JDBC})
    public void testHiveCoercionOrc()
    {
        doTestHiveCoercion(HIVE_COERCION_ORC);
    }

    @Requires(RcTextRequirements.class)
    @Test(groups = {HIVE_COERCION, JDBC})
    public void testHiveCoercionRcText()
    {
        doTestHiveCoercion(HIVE_COERCION_RCTEXT);
    }

    @Requires(RcBinaryRequirements.class)
    @Test(groups = {HIVE_COERCION, JDBC})
    public void testHiveCoercionRcBinary()
    {
        doTestHiveCoercion(HIVE_COERCION_RCBINARY);
    }

    @Requires(ParquetRequirements.class)
    @Test(groups = {HIVE_COERCION, JDBC})
    public void testHiveCoercionParquet()
    {
        doTestHiveCoercion(HIVE_COERCION_PARQUET);
    }

    @Requires(AvroRequirements.class)
    @Test(groups = {HIVE_COERCION, JDBC})
    public void testHiveCoercionAvro()
    {
        String tableName = mutableTableInstanceOf(HIVE_COERCION_AVRO).getNameInDatabase();

        onHive().executeQuery(format("INSERT INTO TABLE %s " +
                        "PARTITION (id=1) " +
                        "VALUES" +
                        "(2323, 0.5)," +
                        "(-2323, -1.5)",
                tableName));

        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN int_to_bigint int_to_bigint bigint", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN float_to_double float_to_double double", tableName));

        assertThat(onTrino().executeQuery("SHOW COLUMNS FROM " + tableName).project(1, 2)).containsExactlyInOrder(
                row("int_to_bigint", "bigint"),
                row("float_to_double", "double"),
                row("id", "bigint"));

        QueryResult queryResult = onTrino().executeQuery("SELECT * FROM " + tableName);
        assertThat(queryResult).hasColumns(BIGINT, DOUBLE, BIGINT);

        assertThat(queryResult).containsOnly(
                row(2323L, 0.5, 1),
                row(-2323L, -1.5, 1));
    }

    private void doTestHiveCoercion(HiveTableDefinition tableDefinition)
    {
        String tableName = mutableTableInstanceOf(tableDefinition).getNameInDatabase();

        String floatToDoubleType = tableName.toLowerCase(ENGLISH).contains("parquet") ? "DOUBLE" : "REAL";
        String decimalToFloatVal = tableName.toLowerCase(ENGLISH).contains("parquet") ? "12345.12345" : "12345.124";

        insertTableRows(tableName, floatToDoubleType);

        alterTableColumnTypes(tableName);
        assertProperAlteredTableSchema(tableName);

        List<String> prestoReadColumns = ImmutableList.of(
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
                // "double_to_float",
                "shortdecimal_to_shortdecimal",
                "shortdecimal_to_longdecimal",
                "longdecimal_to_shortdecimal",
                "longdecimal_to_longdecimal",
                // "float_to_decimal",
                // "double_to_decimal",
                "decimal_to_float",
                "decimal_to_double",
                "varchar_to_bigger_varchar",
                "varchar_to_smaller_varchar",
                "id");

        Function<Engine, Map<String, List<Object>>> expected = engine -> expectedValuesForEngineProvider(engine, tableName, decimalToFloatVal);

        Map<String, List<Object>> expectedPrestoResults = expected.apply(Engine.TRINO);
        assertEquals(ImmutableSet.copyOf(prestoReadColumns), expectedPrestoResults.keySet());
        String prestoSelectQuery = format("SELECT %s FROM %s", String.join(", ", prestoReadColumns), tableName);
        assertQueryResults(Engine.TRINO, prestoSelectQuery, expectedPrestoResults, prestoReadColumns, 2, tableName);

        // For Hive, remove unsupported columns for the current file format and hive version
        List<String> hiveReadColumns = removeUnsupportedColumnsForHive(prestoReadColumns, tableName);
        Map<String, List<Object>> expectedHiveResults = expected.apply(Engine.HIVE);
        String hiveSelectQuery = format("SELECT %s FROM %s", String.join(", ", hiveReadColumns), tableName);
        assertQueryResults(Engine.HIVE, hiveSelectQuery, expectedHiveResults, hiveReadColumns, 2, tableName);
    }

    protected void insertTableRows(String tableName, String floatToDoubleType)
    {
        onTrino().executeQuery(format(
                "INSERT INTO %1$s VALUES " +
                        "(" +
                        "  CAST(ROW ('as is', -1, 100, 2323, 12345) AS ROW(keep VARCHAR, ti2si TINYINT, si2int SMALLINT, int2bi INTEGER, bi2vc BIGINT)), " +
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
                        //"  DOUBLE '0.5', " +
                        "  DECIMAL '12345678.12', " +
                        "  DECIMAL '12345678.12', " +
                        "  DECIMAL '12345678.123456123456', " +
                        "  DECIMAL '12345678.123456123456', " +
                        //"  %2$s '12345.12345', " +
                        //"  DOUBLE '12345.12345', " +
                        "  DECIMAL '12345.12345', " +
                        "  DECIMAL '12345.12345', " +
                        "  'abc', " +
                        "  'abc', " +
                        "  1), " +
                        "(" +
                        "  CAST(ROW (NULL, 1, -100, -2323, -12345) AS ROW(keep VARCHAR, ti2si TINYINT, si2int SMALLINT, int2bi INTEGER, bi2vc BIGINT)), " +
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
                        //"  DOUBLE '-1.5', " +
                        "  DECIMAL '-12345678.12', " +
                        "  DECIMAL '-12345678.12', " +
                        "  DECIMAL '-12345678.123456123456', " +
                        "  DECIMAL '-12345678.123456123456', " +
                        //"  %2$s '-12345.12345', " +
                        //"  DOUBLE '-12345.12345', " +
                        "  DECIMAL '-12345.12345', " +
                        "  DECIMAL '-12345.12345', " +
                        "  '\uD83D\uDCB0\uD83D\uDCB0\uD83D\uDCB0', " +
                        "  '\uD83D\uDCB0\uD83D\uDCB0\uD83D\uDCB0', " +
                        "  1)",
                tableName,
                floatToDoubleType));
    }

    protected Map<String, List<Object>> expectedValuesForEngineProvider(Engine engine, String tableName, String decimalToFloatVal)
    {
        return ImmutableMap.<String, List<Object>>builder()
                .put("row_to_row", Arrays.asList(
                        engine == Engine.TRINO ?
                                rowBuilder()
                                        .addField("keep", "as is")
                                        .addField("ti2si", (short) -1)
                                        .addField("si2int", 100)
                                        .addField("int2bi", 2323L)
                                        .addField("bi2vc", "12345")
                                        .build() :
                                // TODO: Compare structures for hive executor instead of serialized representation
                                "{\"keep\":\"as is\",\"ti2si\":-1,\"si2int\":100,\"int2bi\":2323,\"bi2vc\":\"12345\"}",
                        engine == Engine.TRINO ?
                                rowBuilder()
                                        .addField("keep", null)
                                        .addField("ti2si", (short) 1)
                                        .addField("si2int", -100)
                                        .addField("int2bi", -2323L)
                                        .addField("bi2vc", "-12345")
                                        .build() :
                                "{\"keep\":null,\"ti2si\":1,\"si2int\":-100,\"int2bi\":-2323,\"bi2vc\":\"-12345\"}"))
                .put("list_to_list", Arrays.asList(
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
                .put("map_to_map", Arrays.asList(
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
                .put("tinyint_to_smallint", Arrays.asList(
                        -1,
                        1))
                .put("tinyint_to_int", Arrays.asList(
                        2,
                        -2))
                .put("tinyint_to_bigint", Arrays.asList(
                        -3L,
                        null))
                .put("smallint_to_int", Arrays.asList(
                        100,
                        -100))
                .put("smallint_to_bigint", Arrays.asList(
                        -101L,
                        101L))
                .put("int_to_bigint", Arrays.asList(
                        2323L,
                        -2323L))
                .put("bigint_to_varchar", Arrays.asList(
                        "12345",
                        "-12345"))
                .put("float_to_double", Arrays.asList(
                        0.5,
                        -1.5))
                // .put("double_to_float", Arrays.asList(0.5, -1.5))
                .put("shortdecimal_to_shortdecimal", Arrays.asList(
                        new BigDecimal("12345678.1200"),
                        new BigDecimal("-12345678.1200")))
                .put("shortdecimal_to_longdecimal", Arrays.asList(
                        new BigDecimal("12345678.1200"),
                        new BigDecimal("-12345678.1200")))
                .put("longdecimal_to_shortdecimal", Arrays.asList(
                        new BigDecimal("12345678.12"),
                        new BigDecimal("-12345678.12")))
                .put("longdecimal_to_longdecimal", Arrays.asList(
                        new BigDecimal("12345678.12345612345600"),
                        new BigDecimal("-12345678.12345612345600")))
                // .put("float_to_decimal", Arrays.asList(new BigDecimal(floatToDecimalVal), new BigDecimal("-" + floatToDecimalVal)))
                // .put("double_to_decimal", Arrays.asList(new BigDecimal("12345.12345"), new BigDecimal("-12345.12345")))
                .put("decimal_to_float", Arrays.asList(
                        Float.parseFloat(decimalToFloatVal),
                        -Float.parseFloat(decimalToFloatVal)))
                .put("decimal_to_double", Arrays.asList(
                        12345.12345,
                        -12345.12345))
                .put("varchar_to_bigger_varchar", Arrays.asList(
                        "abc",
                        "\uD83D\uDCB0\uD83D\uDCB0\uD83D\uDCB0"))
                .put("varchar_to_smaller_varchar", Arrays.asList(
                        "ab",
                        "\uD83D\uDCB0\uD83D\uDCB0"))
                .put("id", Arrays.asList(
                        1,
                        1))
                .build();
    }

    private List<String> removeUnsupportedColumnsForHive(List<String> columns, String tableName)
    {
        // TODO: assert exceptions being thrown for each column
        Map<ColumnContext, String> expectedExceptions = expectedExceptionsWithContext();

        String hiveVersion = getHiveVersionMajor() + "." + getHiveVersionMinor();
        Set<String> unsupportedColumns = expectedExceptions.keySet().stream()
                .filter(context -> context.getHiveVersion().equals(hiveVersion) && tableName.contains(context.getFormat()))
                .map(ColumnContext::getColumn)
                .collect(toImmutableSet());

        return columns.stream()
                .filter(column -> !unsupportedColumns.contains(column))
                .collect(toImmutableList());
    }

    protected Map<ColumnContext, String> expectedExceptionsWithContext()
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

                .build();
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
                row("row_to_row", "row(keep varchar, ti2si smallint, si2int integer, int2bi bigint, bi2vc varchar)"),
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
                //row("double_to_float", floatType),
                row("shortdecimal_to_shortdecimal", "decimal(18,4)"),
                row("shortdecimal_to_longdecimal", "decimal(20,4)"),
                row("longdecimal_to_shortdecimal", "decimal(12,2)"),
                row("longdecimal_to_longdecimal", "decimal(38,14)"),
                //row("float_to_decimal", "decimal(10,5)"),
                //row("double_to_decimal", "decimal(10,5)"),
                row("decimal_to_float", floatType),
                row("decimal_to_double", "double"),
                row("varchar_to_bigger_varchar", "varchar(4)"),
                row("varchar_to_smaller_varchar", "varchar(2)"),
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
                //.put( "double_to_float", floatType)
                .put("shortdecimal_to_shortdecimal", DECIMAL)
                .put("shortdecimal_to_longdecimal", DECIMAL)
                .put("longdecimal_to_shortdecimal", DECIMAL)
                .put("longdecimal_to_longdecimal", DECIMAL)
                //.put("float_to_decimal", DECIMAL)
                //.put("double_to_decimal", DECIMAL)
                .put("decimal_to_float", floatType)
                .put("decimal_to_double", DOUBLE)
                .put("varchar_to_bigger_varchar", VARCHAR)
                .put("varchar_to_smaller_varchar", VARCHAR)
                .put("id", BIGINT)
                .build();

        assertThat(queryResult)
                .hasColumns(columns.stream().map(expectedTypes::get).collect(toImmutableList()));
    }

    private static void alterTableColumnTypes(String tableName)
    {
        String floatType = tableName.toLowerCase(ENGLISH).contains("parquet") ? "double" : "float";

        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN row_to_row row_to_row struct<keep:string, ti2si:smallint, si2int:int, int2bi:bigint, bi2vc:string>", tableName));
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
        //onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN double_to_float double_to_float %s", tableName, floatType));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN shortdecimal_to_shortdecimal shortdecimal_to_shortdecimal DECIMAL(18,4)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN shortdecimal_to_longdecimal shortdecimal_to_longdecimal DECIMAL(20,4)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN longdecimal_to_shortdecimal longdecimal_to_shortdecimal DECIMAL(12,2)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN longdecimal_to_longdecimal longdecimal_to_longdecimal DECIMAL(38,14)", tableName));
        //onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN float_to_decimal float_to_decimal DECIMAL(10,5)", tableName));
        //onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN double_to_decimal double_to_decimal DECIMAL(10,5)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN decimal_to_float decimal_to_float %s", tableName, floatType));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN decimal_to_double decimal_to_double double", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN varchar_to_bigger_varchar varchar_to_bigger_varchar varchar(4)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN varchar_to_smaller_varchar varchar_to_smaller_varchar varchar(2)", tableName));
    }

    private static TableInstance<?> mutableTableInstanceOf(TableDefinition tableDefinition)
    {
        if (tableDefinition.getDatabase().isPresent()) {
            return mutableTableInstanceOf(tableDefinition, tableDefinition.getDatabase().get());
        }
        else {
            return mutableTableInstanceOf(tableHandleInSchema(tableDefinition));
        }
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
                .map(trinoArray -> Arrays.asList((Object[]) trinoArray.getArray()))
                .collect(toImmutableList());
    }

    public static class ColumnContext
    {
        private final String hiveVersion;
        private final String format;
        private final String column;

        public ColumnContext(String hiveVersion, String format, String column)
        {
            this.hiveVersion = requireNonNull(hiveVersion, "hiveVersion is null");
            this.format = requireNonNull(format, "format is null");
            this.column = requireNonNull(column, "column is null");
        }

        public static ColumnContext columnContext(String version, String format, String column)
        {
            return new ColumnContext(version, format, column);
        }

        public String getHiveVersion()
        {
            return hiveVersion;
        }

        public String getFormat()
        {
            return format;
        }

        public String getColumn()
        {
            return column;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ColumnContext that = (ColumnContext) o;
            return Objects.equals(hiveVersion, that.hiveVersion) &&
                    Objects.equals(format, that.format) &&
                    Objects.equals(column, that.column);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(hiveVersion, format, column);
        }
    }

    private static QueryResult execute(Engine engine, String sql, QueryExecutor.QueryParam... params)
    {
        return engine.queryExecutor().executeQuery(sql, params);
    }
}

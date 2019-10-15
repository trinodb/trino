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
package io.prestosql.tests.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.jdbc.PrestoArray;
import io.prestosql.tempto.Requirement;
import io.prestosql.tempto.RequirementsProvider;
import io.prestosql.tempto.Requires;
import io.prestosql.tempto.assertions.QueryAssert.Row;
import io.prestosql.tempto.configuration.Configuration;
import io.prestosql.tempto.fulfillment.table.MutableTableRequirement;
import io.prestosql.tempto.fulfillment.table.MutableTablesState;
import io.prestosql.tempto.fulfillment.table.TableDefinition;
import io.prestosql.tempto.fulfillment.table.TableHandle;
import io.prestosql.tempto.fulfillment.table.TableInstance;
import io.prestosql.tempto.fulfillment.table.hive.HiveTableDefinition;
import io.prestosql.tempto.query.QueryResult;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.JDBCType;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static io.prestosql.tempto.assertions.QueryAssert.Row.row;
import static io.prestosql.tempto.assertions.QueryAssert.assertThat;
import static io.prestosql.tempto.context.ThreadLocalTestContextHolder.testContext;
import static io.prestosql.tempto.fulfillment.table.MutableTableRequirement.State.CREATED;
import static io.prestosql.tempto.fulfillment.table.TableHandle.tableHandle;
import static io.prestosql.tempto.query.QueryExecutor.defaultQueryExecutor;
import static io.prestosql.tempto.query.QueryExecutor.query;
import static io.prestosql.tests.TestGroups.HIVE_COERCION;
import static io.prestosql.tests.TestGroups.JDBC;
import static io.prestosql.tests.utils.JdbcDriverUtils.usingPrestoJdbcDriver;
import static io.prestosql.tests.utils.JdbcDriverUtils.usingSimbaJdbcDriver;
import static io.prestosql.tests.utils.QueryExecutors.onHive;
import static java.lang.String.format;
import static java.sql.JDBCType.ARRAY;
import static java.sql.JDBCType.BIGINT;
import static java.sql.JDBCType.DECIMAL;
import static java.sql.JDBCType.DOUBLE;
import static java.sql.JDBCType.INTEGER;
import static java.sql.JDBCType.JAVA_OBJECT;
import static java.sql.JDBCType.REAL;
import static java.sql.JDBCType.SMALLINT;
import static java.sql.JDBCType.VARCHAR;
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
        String tableName = format("%s_hive_coercion", recommendTableName.orElse(fileFormat).toLowerCase(Locale.ENGLISH));
        String floatType = fileFormat.toLowerCase(Locale.ENGLISH).contains("parquet") ? "DOUBLE" : "FLOAT";
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
                        "    double_to_float            DOUBLE," +
                        "    shortdecimal_to_shortdecimal          DECIMAL(10,2)," +
                        "    shortdecimal_to_longdecimal           DECIMAL(10,2)," +
                        "    longdecimal_to_shortdecimal           DECIMAL(20,12)," +
                        "    longdecimal_to_longdecimal            DECIMAL(20,12)," +
                        "    float_to_decimal           " + floatType + "," +
                        "    double_to_decimal          DOUBLE," +
                        "    decimal_to_float           DECIMAL(10,5)," +
                        "    decimal_to_double          DECIMAL(10,5)" +
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
        if (getHiveVersionMajor() >= 3) {
            throw new SkipException("Hive 3 forbids certain coercions, we should still test remaining"); // TODO (https://github.com/prestosql/presto/issues/1218)
        }
        doTestHiveCoercion(HIVE_COERCION_TEXTFILE);
    }

    @Requires(OrcRequirements.class)
    @Test(groups = {HIVE_COERCION, JDBC})
    public void testHiveCoercionOrc()
    {
        if (getHiveVersionMajor() >= 3) {
            throw new SkipException("Hive 3 forbids certain coercions, we should still test remaining"); // TODO (https://github.com/prestosql/presto/issues/1218)
        }
        doTestHiveCoercion(HIVE_COERCION_ORC);
    }

    @Requires(RcTextRequirements.class)
    @Test(groups = {HIVE_COERCION, JDBC})
    public void testHiveCoercionRcText()
    {
        if (getHiveVersionMajor() >= 3) {
            throw new SkipException("Hive 3 forbids certain coercions, we should still test remaining"); // TODO (https://github.com/prestosql/presto/issues/1218)
        }
        doTestHiveCoercion(HIVE_COERCION_RCTEXT);
    }

    @Requires(RcBinaryRequirements.class)
    @Test(groups = {HIVE_COERCION, JDBC})
    public void testHiveCoercionRcBinary()
    {
        if (getHiveVersionMajor() >= 3) {
            throw new SkipException("Hive 3 forbids certain coercions, we should still test remaining"); // TODO (https://github.com/prestosql/presto/issues/1218)
        }
        doTestHiveCoercion(HIVE_COERCION_RCBINARY);
    }

    @Requires(ParquetRequirements.class)
    @Test(groups = {HIVE_COERCION, JDBC})
    public void testHiveCoercionParquet()
    {
        if (getHiveVersionMajor() >= 3) {
            throw new SkipException("Hive 3 forbids certain coercions, we should still test remaining"); // TODO (https://github.com/prestosql/presto/issues/1218)
        }
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

        assertThat(query("SHOW COLUMNS FROM " + tableName).project(1, 2)).containsExactly(
                row("int_to_bigint", "bigint"),
                row("float_to_double", "double"),
                row("id", "bigint"));

        QueryResult queryResult = query("SELECT * FROM " + tableName);
        assertThat(queryResult).hasColumns(BIGINT, DOUBLE, BIGINT);

        assertThat(queryResult).containsOnly(
                row(2323L, 0.5, 1),
                row(-2323L, -1.5, 1));
    }

    private void doTestHiveCoercion(HiveTableDefinition tableDefinition)
    {
        String tableName = mutableTableInstanceOf(tableDefinition).getNameInDatabase();

        String floatToDoubleType = tableName.toLowerCase(Locale.ENGLISH).contains("parquet") ? "DOUBLE" : "REAL";
        String floatToDecimalVal = tableName.toLowerCase(Locale.ENGLISH).contains("parquet") ? "12345.12345" : "12345.12300";
        String decimalToFloatVal = tableName.toLowerCase(Locale.ENGLISH).contains("parquet") ? "12345.12345" : "12345.124";

        query(format(
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
                        "  DOUBLE '0.5', " +
                        "  DECIMAL '12345678.12', " +
                        "  DECIMAL '12345678.12', " +
                        "  DECIMAL '12345678.123456123456', " +
                        "  DECIMAL '12345678.123456123456', " +
                        "  %2$s '12345.12345', " +
                        "  DOUBLE '12345.12345', " +
                        "  DECIMAL '12345.12345', " +
                        "  DECIMAL '12345.12345', " +
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
                        "  DOUBLE '-1.5', " +
                        "  DECIMAL '-12345678.12', " +
                        "  DECIMAL '-12345678.12', " +
                        "  DECIMAL '-12345678.123456123456', " +
                        "  DECIMAL '-12345678.123456123456', " +
                        "  %2$s '-12345.12345', " +
                        "  DOUBLE '-12345.12345', " +
                        "  DECIMAL '-12345.12345', " +
                        "  DECIMAL '-12345.12345', " +
                        "  1)",
                tableName,
                floatToDoubleType));

        alterTableColumnTypes(tableName);
        assertProperAlteredTableSchema(tableName);

        QueryResult queryResult = query("SELECT * FROM " + tableName);
        assertColumnTypes(queryResult, tableName);
        List<Row> expectedRows;
        Connection connection = defaultQueryExecutor().getConnection();
        if (usingPrestoJdbcDriver(connection)) {
            expectedRows = ImmutableList.of(
                    row(
                            asMap("keep", "as is", "ti2si", (short) -1, "si2int", 100, "int2bi", 2323L, "bi2vc", "12345"),
                            ImmutableList.of(asMap("ti2int", 2, "si2bi", -101L, "bi2vc", "12345")),
                            asMap(2, asMap("ti2bi", -3L, "int2bi", 2323L, "float2double", 0.5, "add", null)),
                            -1,
                            2,
                            -3L,
                            100,
                            -101L,
                            2323L,
                            "12345",
                            0.5,
                            0.5,
                            new BigDecimal("12345678.1200"),
                            new BigDecimal("12345678.1200"),
                            new BigDecimal("12345678.12"),
                            new BigDecimal("12345678.12345612345600"),
                            new BigDecimal(floatToDecimalVal),
                            new BigDecimal("12345.12345"),
                            Float.parseFloat(decimalToFloatVal),
                            12345.12345,
                            1),
                    row(
                            asMap("keep", null, "ti2si", (short) 1, "si2int", -100, "int2bi", -2323L, "bi2vc", "-12345"),
                            ImmutableList.of(asMap("ti2int", -2, "si2bi", 101L, "bi2vc", "-12345")),
                            ImmutableMap.of(-2, asMap("ti2bi", null, "int2bi", -2323L, "float2double", -1.5, "add", null)),
                            1,
                            -2,
                            null,
                            -100,
                            101L,
                            -2323L,
                            "-12345",
                            -1.5,
                            -1.5,
                            new BigDecimal("-12345678.1200"),
                            new BigDecimal("-12345678.1200"),
                            new BigDecimal("-12345678.12"),
                            new BigDecimal("-12345678.12345612345600"),
                            new BigDecimal("-" + floatToDecimalVal),
                            new BigDecimal("-12345.12345"),
                            -Float.parseFloat(decimalToFloatVal),
                            -12345.12345,
                            1));
        }
        else if (usingSimbaJdbcDriver(connection)) {
            expectedRows = ImmutableList.of(
                    row(
                            "[\"as is\",-1,100,2323,\"12345\"]",
                            "[[2,-101,\"12345\"]]",
                            "{\"2\":[-3,2323,0.5,null]}",
                            -1,
                            2,
                            -3L,
                            100,
                            -101L,
                            2323L,
                            "12345",
                            0.5,
                            0.5,
                            new BigDecimal("12345678.1200"),
                            new BigDecimal("12345678.1200"),
                            new BigDecimal("12345678.12"),
                            new BigDecimal("12345678.12345612345600"),
                            new BigDecimal(floatToDecimalVal),
                            new BigDecimal("12345.12345"),
                            Float.parseFloat(decimalToFloatVal),
                            12345.12345,
                            1),
                    row(
                            "[null,1,-100,-2323,\"-12345\"]",
                            "[[-2,101,\"-12345\"]]",
                            "{\"-2\":[null,-2323,-1.5,null]}",
                            1,
                            -2,
                            null,
                            -100,
                            101L,
                            -2323L,
                            "-12345",
                            -1.5,
                            -1.5,
                            new BigDecimal("-12345678.1200"),
                            new BigDecimal("-12345678.1200"),
                            new BigDecimal("-12345678.12"),
                            new BigDecimal("-12345678.12345612345600"),
                            new BigDecimal("-" + floatToDecimalVal),
                            new BigDecimal("-12345.12345"),
                            -Float.parseFloat(decimalToFloatVal),
                            -12345.12345,
                            1));
        }
        else {
            throw new IllegalStateException();
        }

        // test structural values (tempto can't handle map and row)
        assertEqualsIgnoreOrder(queryResult.column(1), column(expectedRows, 1), "row_to_row field is not equal");
        if (usingPrestoJdbcDriver(connection)) {
            assertEqualsIgnoreOrder(extract(queryResult.column(2)), column(expectedRows, 2), "list_to_list field is not equal");
        }
        else if (usingSimbaJdbcDriver(connection)) {
            assertEqualsIgnoreOrder(queryResult.column(2), column(expectedRows, 2), "list_to_list field is not equal");
        }
        else {
            throw new IllegalStateException();
        }
        assertEqualsIgnoreOrder(queryResult.column(3), column(expectedRows, 3), "map_to_map field is not equal");

        // test primitive values
        int[] indexesOfPrimitives = IntStream.rangeClosed(4, expectedRows.get(0).getValues().size()).toArray();
        assertThat(queryResult.project(indexesOfPrimitives)).containsOnly(project(expectedRows, indexesOfPrimitives));
    }

    private void assertProperAlteredTableSchema(String tableName)
    {
        String floatType = tableName.toLowerCase(Locale.ENGLISH).contains("parquet") ? "double" : "real";

        assertThat(query("SHOW COLUMNS FROM " + tableName).project(1, 2)).containsExactly(
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
                row("double_to_float", floatType),
                row("shortdecimal_to_shortdecimal", "decimal(18,4)"),
                row("shortdecimal_to_longdecimal", "decimal(20,4)"),
                row("longdecimal_to_shortdecimal", "decimal(12,2)"),
                row("longdecimal_to_longdecimal", "decimal(38,14)"),
                row("float_to_decimal", "decimal(10,5)"),
                row("double_to_decimal", "decimal(10,5)"),
                row("decimal_to_float", floatType),
                row("decimal_to_double", "double"),
                row("id", "bigint"));
    }

    private void assertColumnTypes(QueryResult queryResult, String tableName)
    {
        JDBCType floatType = tableName.toLowerCase(Locale.ENGLISH).contains("parquet") ? DOUBLE : REAL;

        Connection connection = defaultQueryExecutor().getConnection();
        if (usingPrestoJdbcDriver(connection)) {
            assertThat(queryResult).hasColumns(
                    JAVA_OBJECT, // row
                    ARRAY, // list
                    JAVA_OBJECT, // map
                    SMALLINT,
                    INTEGER,
                    BIGINT,
                    INTEGER,
                    BIGINT,
                    BIGINT,
                    VARCHAR,
                    DOUBLE,
                    floatType,
                    DECIMAL,
                    DECIMAL,
                    DECIMAL,
                    DECIMAL,
                    DECIMAL,
                    DECIMAL,
                    floatType,
                    DOUBLE,
                    BIGINT);
        }
        else if (usingSimbaJdbcDriver(connection)) {
            assertThat(queryResult).hasColumns(
                    VARCHAR, // row
                    VARCHAR, // list
                    VARCHAR, // map
                    SMALLINT,
                    INTEGER,
                    BIGINT,
                    INTEGER,
                    BIGINT,
                    BIGINT,
                    VARCHAR,
                    DOUBLE,
                    floatType,
                    DECIMAL,
                    DECIMAL,
                    DECIMAL,
                    DECIMAL,
                    DECIMAL,
                    DECIMAL,
                    floatType,
                    DOUBLE,
                    BIGINT);
        }
        else {
            throw new IllegalStateException();
        }
    }

    private static void alterTableColumnTypes(String tableName)
    {
        String floatType = tableName.toLowerCase(Locale.ENGLISH).contains("parquet") ? "double" : "float";

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
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN double_to_float double_to_float %s", tableName, floatType));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN shortdecimal_to_shortdecimal shortdecimal_to_shortdecimal DECIMAL(18,4)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN shortdecimal_to_longdecimal shortdecimal_to_longdecimal DECIMAL(20,4)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN longdecimal_to_shortdecimal longdecimal_to_shortdecimal DECIMAL(12,2)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN longdecimal_to_longdecimal longdecimal_to_longdecimal DECIMAL(38,14)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN float_to_decimal float_to_decimal DECIMAL(10,5)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN double_to_decimal double_to_decimal DECIMAL(10,5)", tableName));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN decimal_to_float decimal_to_float %s", tableName, floatType));
        onHive().executeQuery(format("ALTER TABLE %s CHANGE COLUMN decimal_to_double decimal_to_double double", tableName));
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

    // This help function should only be used when the map contains null value.
    // Otherwise, use ImmutableMap.
    private static Map<Object, Object> asMap(Object... objects)
    {
        assertEquals(objects.length % 2, 0, "number of objects must be even");
        Map<Object, Object> struct = new HashMap<>();
        for (int i = 0; i < objects.length; i += 2) {
            struct.put(objects[i], objects[i + 1]);
        }
        return struct;
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

    private static List<List<?>> extract(List<PrestoArray> arrays)
    {
        return arrays.stream()
                .map(prestoArray -> Arrays.asList((Object[]) prestoArray.getArray()))
                .collect(toImmutableList());
    }
}

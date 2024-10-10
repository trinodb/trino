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
package io.trino.plugin.oracle;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.jdbc.BaseJdbcCastPushdownTest;
import io.trino.plugin.jdbc.CastDataTypeTestTable;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestOracleCastPushdown
        extends BaseJdbcCastPushdownTest
{
    private CastDataTypeTestTable left;
    private CastDataTypeTestTable right;
    private TestingOracleServer oracleServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        oracleServer = closeAfterClass(new TestingOracleServer());
        return OracleQueryRunner.builder(oracleServer)
                .addConnectorProperties(ImmutableMap.<String, String>builder()
                        .put("jdbc-types-mapped-to-varchar", "interval year(2) to month, timestamp(6) with local time zone")
                        .put("join-pushdown.enabled", "true")
                        .buildOrThrow())
                .build();
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return new SqlExecutor()
        {
            @Override
            public boolean supportsMultiRowInsert()
            {
                return false;
            }

            @Override
            public void execute(String sql)
            {
                oracleServer.execute(sql);
            }
        };
    }

    @BeforeAll
    public void setup()
    {
        left = closeAfterClass(CastDataTypeTestTable.create(3)
                .addColumn("id", "number(10)", asList(11, 12, 13))
                .addColumn("c_number_3", "number(3)", asList(1, 2, null)) // tinyint in trino
                .addColumn("c_number_5", "number(5)", asList(1, 2, null)) // smallint in trino
                .addColumn("c_number_10", "number(10)", asList(1, 2, null)) // integer in trino
                .addColumn("c_number_19", "number(19)", asList(1, 2, null)) // bigint in trino
                .addColumn("c_float", "float", asList(1.23, 2.67, null)) // double in trino
                .addColumn("c_float_5", "float(5)", asList(1.23, 2.67, null)) // double in trino
                .addColumn("c_binary_float", "binary_float", asList(1.23, 2.67, null))
                .addColumn("c_binary_double", "binary_double", asList(1.23, 2.67, null))
                .addColumn("c_nan", "binary_double", asList("BINARY_DOUBLE_NAN", "BINARY_FLOAT_NAN", null))
                .addColumn("c_infinity", "binary_double", asList("BINARY_DOUBLE_INFINITY", "-BINARY_DOUBLE_INFINITY", null))
                .addColumn("c_number_15", "decimal(15)", asList(1, 2, null))
                .addColumn("c_number_10_2", "decimal(10, 2)", asList(1.23, 2.67, null))
                .addColumn("c_number_30_2", "decimal(30, 2)", asList(1.23, 2.67, null))
                .addColumn("c_char_10", "char(10)", asList("'India'", "'Poland'", null))
                .addColumn("c_char_50", "char(50)", asList("'India'", "'Poland'", null))
                .addColumn("c_char_501", "char(501)", asList("'India'", "'Poland'", null)) // greater than ORACLE_CHAR_MAX_CHARS
                .addColumn("c_char_520", "char(520)", asList("'India'", "'Poland'", null)) // greater than ORACLE_CHAR_MAX_CHARS
                .addColumn("c_nchar_10", "nchar(10)", asList("N'India'", "N'Poland'", null))
                .addColumn("c_varchar_10", "varchar2(10)", asList("'India'", "'Poland'", null))
                .addColumn("c_varchar_10_byte", "varchar2(10 byte)", asList("'India'", "'Poland'", null))
                .addColumn("c_varchar_50", "varchar2(50)", asList("'India'", "'Poland'", null))
                .addColumn("c_varchar_1001", "varchar2(1001)", asList("'India'", "'Poland'", null)) // greater than ORACLE_VARCHAR2_MAX_CHARS
                .addColumn("c_varchar_1020", "varchar2(1020)", asList("'India'", "'Poland'", null)) // greater than ORACLE_VARCHAR2_MAX_CHARS
                .addColumn("c_varchar_numeric", "varchar2(50)", asList("'123'", "'456'", null))
                .addColumn("c_varchar_decimal", "varchar2(50)", asList("'1.23'", "'2.67'", null))
                .addColumn("c_varchar_alpha_numeric", "varchar2(50)", asList("'H311o'", "'123Hey'", null))
                .addColumn("c_varchar_date", "varchar2(50)", asList("'2024-09-08'", "'2019-08-15'", null))
                .addColumn("c_varchar_timestamp", "varchar2(50)", asList("'2024-09-08 01:02:03.666'", "'2019-08-15 09:08:07.333'", null))
                .addColumn("c_varchar_timestamptz", "varchar2(50)", asList("'2024-09-08 01:02:03.666 +05:30'", "'2019-08-15 09:08:07.333 +05:30'", null))
                .addColumn("c_nvarchar_100", "nvarchar2(100)", asList("N'India'", "N'Poland'", null)) // varchar(p) in trino
                .addColumn("c_clob", "clob", asList("'India'", "'Poland'", null)) // varchar in trino
                .addColumn("c_nclob", "nclob", asList("N'India'", "N'Poland'", null)) // varchar in trino
                .addColumn("c_blob", "blob", asList("HEXTORAW('496E646961')", "HEXTORAW('506F6C616E64')", null)) // varbinary in trino
                .addColumn("c_raw_200", "raw(200)", asList("HEXTORAW('496E646961')", "HEXTORAW('506F6C616E64')", null)) // varbinary in trino
                .addColumn("c_date", "date", asList("DATE '2024-09-08'", "DATE '2019-08-15'", null))
                .addColumn("c_timestamp", "timestamp", asList("TIMESTAMP '2024-09-08 01:02:03.666'", "TIMESTAMP '2019-08-15 09:08:07.333'", null))
                .addColumn("c_timestamptz", "timestamp with time zone", asList("TIMESTAMP '2024-09-08 01:02:03.666 +05:30'", "TIMESTAMP '2019-08-15 09:08:07.333 +05:30'", null))
                // the number of Unicode code points in 攻殻機動隊 is 5, and in 😂 is 1.
                .addColumn("c_char_unicode", "char(20)", asList("'攻殻機動隊'", "'😂'", null))
                .addColumn("c_nchar_unicode", "nchar(20)", asList("'攻殻機動隊'", "'😂'", null))
                .addColumn("c_varchar_unicode", "varchar2(20)", asList("'攻殻機動隊'", "'😂'", null))
                .addColumn("c_nvarchar_unicode", "nvarchar2(20)", asList("'攻殻機動隊'", "'😂'", null))
                .addColumn("c_clob_unicode", "clob", asList("'攻殻機動隊'", "'😂'", null))
                .addColumn("c_nclob_unicode", "nclob", asList("N'攻殻機動隊'", "N'😂'", null))

                // unsupported in trino
                .addColumn("c_number", "number", asList(1, 2, null))
                .addColumn("c_timestamp_ltz", "timestamp with local time zone", asList("TIMESTAMP '2024-09-08 01:02:03.666'", "TIMESTAMP '2019-08-15 09:08:07.333'", null))
                .addColumn("c_interval_ym", "interval year to month", asList("INTERVAL '1-2' YEAR TO MONTH", "INTERVAL '3-4' YEAR TO MONTH", null))
                .addColumn("c_interval_ds", "interval day to second", asList("INTERVAL '10 11:12:13.456' DAY TO SECOND", "INTERVAL '10 12:13:14.456' DAY TO SECOND", null))
                .addColumn("c_long", "long", asList("'India'", "'Poland'", null))
                .addColumn("c_xmltype", "xmltype", asList("XMLTYPE('<root><element>Sample XML-1</element></root>')", "XMLTYPE('<root><element>Sample XML-2</element></root>')", null))
                .execute(onRemoteDatabase(), "left_table_"));

        // 2nd row value is different in right table than left table
        right = closeAfterClass(CastDataTypeTestTable.create(3)
                .addColumn("id", "number(10)", asList(21, 22, 23))
                .addColumn("c_number_3", "number(3)", asList(1, 22, null)) // tinyint in trino
                .addColumn("c_number_5", "number(5)", asList(1, 22, null)) // smallint in trino
                .addColumn("c_number_10", "number(10)", asList(1, 22, null)) // integer in trino
                .addColumn("c_number_19", "number(19)", asList(1, 22, null)) // bigint in trino
                .addColumn("c_float", "float", asList(1.23, 22.67, null)) // double in trino
                .addColumn("c_float_5", "float(5)", asList(1.23, 22.67, null)) // double in trino
                .addColumn("c_binary_float", "binary_float", asList(1.23, 22.67, null))
                .addColumn("c_binary_double", "binary_double", asList(1.23, 22.67, null))
                .addColumn("c_nan", "binary_double", asList("BINARY_DOUBLE_NAN", "BINARY_DOUBLE_NAN", null))
                .addColumn("c_infinity", "binary_double", asList("BINARY_DOUBLE_INFINITY", "BINARY_DOUBLE_INFINITY", null))
                .addColumn("c_number_15", "decimal(15)", asList(1, 22, null))
                .addColumn("c_number_10_2", "decimal(10, 2)", asList(1.23, 22.67, null))
                .addColumn("c_number_30_2", "decimal(30, 2)", asList(1.23, 22.67, null))
                .addColumn("c_char_10", "char(10)", asList("'India'", "'France'", null))
                .addColumn("c_char_50", "char(50)", asList("'India'", "'France'", null))
                .addColumn("c_char_501", "char(501)", asList("'India'", "'France'", null)) // greater than ORACLE_CHAR_MAX_CHARS
                .addColumn("c_char_520", "char(520)", asList("'India'", "'France'", null)) // greater than ORACLE_CHAR_MAX_CHARS
                .addColumn("c_nchar_10", "nchar(10)", asList("N'India'", "N'France'", null))
                .addColumn("c_varchar_10", "varchar2(10)", asList("'India'", "'France'", null))
                .addColumn("c_varchar_10_byte", "varchar2(10 byte)", asList("'India'", "'France'", null))
                .addColumn("c_varchar_50", "varchar2(50)", asList("'India'", "'France'", null))
                .addColumn("c_varchar_1001", "varchar2(1001)", asList("'India'", "'France'", null)) // greater than ORACLE_VARCHAR2_MAX_CHARS
                .addColumn("c_varchar_1020", "varchar2(1020)", asList("'India'", "'France'", null)) // greater than ORACLE_VARCHAR2_MAX_CHARS
                .addColumn("c_varchar_numeric", "varchar2(50)", asList("'123'", "'234'", null))
                .addColumn("c_varchar_decimal", "varchar2(50)", asList("'1.23'", "'22.67'", null))
                .addColumn("c_varchar_alpha_numeric", "varchar2(50)", asList("'H311o'", "'123Bye'", null))
                .addColumn("c_varchar_date", "varchar2(50)", asList("'2024-09-08'", "'2020-08-15'", null))
                .addColumn("c_varchar_timestamp", "varchar2(50)", asList("'2024-09-08 01:02:03.666'", "'2020-08-15 09:08:07.333'", null))
                .addColumn("c_varchar_timestamptz", "varchar2(50)", asList("'2024-09-08 01:02:03.666 +05:30'", "'2020-08-15 09:08:07.333 +05:30'", null))
                .addColumn("c_nvarchar_100", "nvarchar2(100)", asList("N'India'", "N'France'", null)) // varchar(p) in trino
                .addColumn("c_clob", "clob", asList("'India'", "'France'", null)) // varchar in trino
                .addColumn("c_nclob", "nclob", asList("N'India'", "N'France'", null)) // varchar in trino
                .addColumn("c_blob", "blob", asList("HEXTORAW('496E646961')", "HEXTORAW('4672616E6365')", null)) // varbinary in trino
                .addColumn("c_raw_200", "raw(200)", asList("HEXTORAW('496E646961')", "HEXTORAW('4672616E6365')", null)) // varbinary in trino
                .addColumn("c_date", "date", asList("DATE '2024-09-08'", "DATE '2020-08-15'", null))
                .addColumn("c_timestamp", "timestamp", asList("TIMESTAMP '2024-09-08 01:02:03.666'", "TIMESTAMP '2020-08-15 09:08:07.333'", null))
                .addColumn("c_timestamptz", "timestamp with time zone", asList("TIMESTAMP '2024-09-08 01:02:03.666 +05:30'", "TIMESTAMP '2020-08-15 09:08:07.333 +05:30'", null))
                // the number of Unicode code points in 攻殻機動隊 is 5, and in 😇 is 1.
                .addColumn("c_char_unicode", "char(20)", asList("'攻殻機動隊'", "'😇'", null))
                .addColumn("c_nchar_unicode", "nchar(20)", asList("'攻殻機動隊'", "'😇'", null))
                .addColumn("c_varchar_unicode", "varchar2(20)", asList("'攻殻機動隊'", "'😇'", null))
                .addColumn("c_nvarchar_unicode", "nvarchar2(20)", asList("'攻殻機動隊'", "'😇'", null))
                .addColumn("c_clob_unicode", "clob", asList("'攻殻機動隊'", "'😇'", null))
                .addColumn("c_nclob_unicode", "nclob", asList("N'攻殻機動隊'", "N'😇'", null))

                // unsupported in trino
                .addColumn("c_number", "number", asList(1, 22, null))
                .addColumn("c_timestamp_ltz", "timestamp with local time zone", asList("TIMESTAMP '2024-09-08 01:02:03.666'", "TIMESTAMP '2020-08-15 09:08:07.333'", null))
                .addColumn("c_interval_ym", "interval year to month", asList("INTERVAL '1-2' YEAR TO MONTH", "INTERVAL '4-5' YEAR TO MONTH", null))
                .addColumn("c_interval_ds", "interval day to second", asList("INTERVAL '10 11:12:13.456' DAY TO SECOND", "INTERVAL '11 12:13:14.456' DAY TO SECOND", null))
                .addColumn("c_long", "long", asList("'India'", "'France'", null))
                .addColumn("c_xmltype", "xmltype", asList("XMLTYPE('<root><element>Sample XML-1</element></root>')", "XMLTYPE('<root><element>Sample XML-3</element></root>')", null))
                .execute(onRemoteDatabase(), "right_table_"));
    }

    @Override
    protected String leftTable()
    {
        return left.getName();
    }

    @Override
    protected String rightTable()
    {
        return right.getName();
    }

    @Test
    public void testCastPushdownSpecialCase()
    {
        for (CastTestCase testCase : specialCaseNClob()) {
            // Projection pushdown is supported, because trino converts the clob type to nclob thus cast is not required
            assertThat(query("SELECT CAST(%s AS %s) FROM %s".formatted(testCase.sourceColumn(), testCase.castType(), leftTable())))
                    .isFullyPushedDown();
            // join pushdown is not supported, because comparison between nclob is not pushdown
            assertThat(query("SELECT l.id FROM %s l JOIN %s r ON CAST(l.%s AS %s) = r.%s".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                    .joinIsNotFullyPushedDown();
        }
    }

    @Test
    public void testJoinPushdownWithNestedCast()
    {
        CastTestCase testCase = new CastTestCase("c_varchar_10", "varchar(100)", "c_varchar_50");
        assertThat(query("SELECT l.id FROM %s l JOIN %s r ON CAST(l.%s AS %s) = r.%s".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                .isFullyPushedDown();
    }

    @Test
    public void testInsertWithImplicitCharCast()
    {
        try (TestTable sourceTable = new TestTable(
                onRemoteDatabase(),
                "source_",
                "(id int, c_char_2 char(2))",
                asList("1, 'x'", "2, 'y'", "3, null"));
                TestTable targetTable = new TestTable(
                        onRemoteDatabase(),
                        "target_",
                        "(id int, c_char_4 char(4))")) {
            assertUpdate("INSERT INTO %s SELECT * FROM %s".formatted(targetTable.getName(), sourceTable.getName()), 3);
            assertThat(query("SELECT * FROM " + targetTable.getName()))
                    .matches("VALUES " +
                            "(CAST(1 AS DECIMAL(38, 0)), CAST('x   ' AS char(4))), " +
                            "(CAST(2 AS DECIMAL(38, 0)), CAST('y   ' AS char(4))), " +
                            "(CAST(3 AS DECIMAL(38, 0)), CAST(null AS char(4)))");
        }
    }

    @Test
    public void testAllJoinPushdownWithCast()
    {
        CastTestCase testCase = new CastTestCase("c_varchar_10", "varchar(50)", "c_varchar_50");
        assertThat(query("SELECT l.id FROM %s l LEFT JOIN %s r ON CAST(l.%s AS %s) = r.%s".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l RIGHT JOIN %s r ON CAST(l.%s AS %s) = r.%s".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l INNER JOIN %s r ON CAST(l.%s AS %s) = r.%s".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l FULL JOIN %s r ON CAST(l.%s AS %s) = r.%s".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                .isFullyPushedDown();

        testCase = new CastTestCase("c_varchar_10", "varchar(10)", "c_varchar_50");
        assertThat(query("SELECT l.id FROM %s l LEFT JOIN %s r ON l.%s = CAST(r.%s AS %s)".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.targetColumn(), testCase.castType())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l RIGHT JOIN %s r ON l.%s = CAST(r.%s AS %s)".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.targetColumn(), testCase.castType())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l INNER JOIN %s r ON l.%s = CAST(r.%s AS %s)".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.targetColumn(), testCase.castType())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l FULL JOIN %s r ON l.%s = CAST(r.%s AS %s)".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.targetColumn(), testCase.castType())))
                .isFullyPushedDown();

        testCase = new CastTestCase("c_varchar_10", "varchar(200)", "c_varchar_50");
        assertThat(query("SELECT l.id FROM %s l LEFT JOIN %s r ON CAST(l.%3$s AS %4$s) = CAST(r.%5$s AS %4$s)".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l RIGHT JOIN %s r ON CAST(l.%3$s AS %4$s) = CAST(r.%5$s AS %4$s)".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l INNER JOIN %s r ON CAST(l.%3$s AS %4$s) = CAST(r.%5$s AS %4$s)".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l FULL JOIN %s r ON CAST(l.%3$s AS %4$s) = CAST(r.%5$s AS %4$s)".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                .isFullyPushedDown();
    }

    @Test
    public void testCastPushdownClobSensitivity()
    {
        // Verify that clob/nclob join condition is not applied in a case-insensitive way
        try (TestTable leftTable = new TestTable(
                onRemoteDatabase(),
                "l_clob_sensitivity_",
                "(id int, c_varchar_50 varchar(50), c_clob clob, c_nclob nclob)",
                asList("11, 'India', 'India', 'India'", "12, 'Poland', 'Poland', 'Poland'"));
                TestTable rightTable = new TestTable(
                        onRemoteDatabase(),
                        "r_clob_sensitivity_",
                        "(id int, c_varchar_50 varchar(50), c_clob clob, c_nclob nclob)",
                        asList("21, 'INDIA', 'INDIA', 'INDIA'", "22, 'POLAND', 'POLAND', 'POLAND'", "23, 'India', 'India', 'India'"))) {
            assertThat(query("SELECT r.id, r.c_nclob FROM %s l JOIN %s r ON CAST(l.c_nclob AS VARCHAR(50)) = r.c_varchar_50".formatted(leftTable.getName(), rightTable.getName())))
                    .matches("VALUES (CAST(23 AS DECIMAL(38, 0)), VARCHAR 'India')")
                    .isFullyPushedDown();

            assertThat(query("SELECT r.id, r.c_clob FROM %s l JOIN %s r ON CAST(l.c_clob AS VARCHAR(50)) = r.c_varchar_50".formatted(leftTable.getName(), rightTable.getName())))
                    .matches("VALUES (CAST(23 AS DECIMAL(38, 0)), VARCHAR 'India')")
                    .isFullyPushedDown();
        }
    }

    @Test
    public void testCastPushdownDisabled()
    {
        Session sessionWithoutComplexExpressionPushdown = Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "complex_expression_pushdown", "false")
                .build();
        assertThat(query(sessionWithoutComplexExpressionPushdown, "SELECT CAST (c_varchar_10 AS VARCHAR(100)) FROM %s".formatted(leftTable())))
                .isNotFullyPushedDown(ProjectNode.class);
    }

    @Test
    public void testCastPushdownWithForcedTypedToVarchar()
    {
        // These column types are not supported by default by trino. These types are forced mapped to varchar.
        assertThat(query("SELECT CAST(c_interval_ym AS VARCHAR(100)) FROM %s".formatted(leftTable())))
                .isNotFullyPushedDown(ProjectNode.class);
        assertThat(query("SELECT CAST(c_timestamp_ltz AS VARCHAR(100)) FROM %s".formatted(leftTable())))
                .isNotFullyPushedDown(ProjectNode.class);
    }

    @Override
    protected List<CastTestCase> supportedCastTypePushdown()
    {
        return ImmutableList.of(
                new CastTestCase("c_char_10", "char(50)", "c_char_50"),
                new CastTestCase("c_char_50", "char(50)", "c_char_50"),
                new CastTestCase("c_char_501", "char(50)", "c_char_50"),
                new CastTestCase("c_char_520", "char(50)", "c_char_50"),
                new CastTestCase("c_nchar_10", "char(50)", "c_char_50"),
                new CastTestCase("c_varchar_10", "char(50)", "c_char_50"),
                new CastTestCase("c_varchar_10_byte", "char(50)", "c_char_50"),
                new CastTestCase("c_varchar_1001", "char(50)", "c_char_50"),
                new CastTestCase("c_varchar_1020", "char(50)", "c_char_50"),
                new CastTestCase("c_nvarchar_100", "char(50)", "c_char_50"),
                new CastTestCase("c_clob", "char(50)", "c_char_50"),
                new CastTestCase("c_nclob", "char(50)", "c_char_50"),
                new CastTestCase("c_char_unicode", "char(50)", "c_char_50"),
                new CastTestCase("c_nchar_unicode", "char(50)", "c_char_50"),
                new CastTestCase("c_varchar_unicode", "char(50)", "c_char_50"),
                new CastTestCase("c_nvarchar_unicode", "char(50)", "c_char_50"),
                new CastTestCase("c_clob_unicode", "char(50)", "c_char_50"),
                new CastTestCase("c_nclob_unicode", "char(50)", "c_char_50"),

                new CastTestCase("c_varchar_10", "varchar(50)", "c_varchar_50"),
                new CastTestCase("c_varchar_10_byte", "varchar(50)", "c_varchar_50"),
                new CastTestCase("c_varchar_1001", "varchar(50)", "c_varchar_50"),
                new CastTestCase("c_varchar_1020", "varchar(50)", "c_varchar_50"),
                new CastTestCase("c_nvarchar_100", "varchar(50)", "c_varchar_50"),
                new CastTestCase("c_clob", "varchar(50)", "c_varchar_50"),
                new CastTestCase("c_nclob", "varchar(50)", "c_varchar_50"),
                new CastTestCase("c_number_3", "varchar(50)", "c_varchar_50"),
                new CastTestCase("c_number_5", "varchar(50)", "c_varchar_50"),
                new CastTestCase("c_number_10", "varchar(50)", "c_varchar_50"),
                new CastTestCase("c_number_19", "varchar(50)", "c_varchar_50"),
                new CastTestCase("c_number_15", "varchar(50)", "c_varchar_50"),
                new CastTestCase("c_number_10_2", "varchar(50)", "c_varchar_50"),
                new CastTestCase("c_number_30_2", "varchar(50)", "c_varchar_50"),
                new CastTestCase("c_varchar_unicode", "varchar(50)", "c_varchar_50"),
                new CastTestCase("c_nvarchar_unicode", "varchar(50)", "c_varchar_50"),
                new CastTestCase("c_clob_unicode", "varchar(50)", "c_varchar_50"),
                new CastTestCase("c_nclob_unicode", "varchar(50)", "c_varchar_50"));
    }

    @Override
    protected List<CastTestCase> unsupportedCastTypePushdown()
    {
        return ImmutableList.of(
                new CastTestCase("c_char_10", "char(501)", "c_char_501"),
                new CastTestCase("c_char_501", "char(520)", "c_char_520"),
                new CastTestCase("c_varchar_10", "char(501)", "c_char_501"),
                new CastTestCase("c_varchar_1001", "char(501)", "c_char_501"),
                new CastTestCase("c_clob", "char(501)", "c_char_501"),
                new CastTestCase("c_nclob", "char(501)", "c_char_501"),

                // Issue with padding in the result data
                new CastTestCase("c_char_10", "varchar(50)", "c_varchar_50"),
                new CastTestCase("c_char_50", "varchar(50)", "c_varchar_50"),
                new CastTestCase("c_char_501", "varchar(50)", "c_varchar_50"),
                new CastTestCase("c_char_520", "varchar(50)", "c_varchar_50"),
                new CastTestCase("c_nchar_10", "varchar(50)", "c_varchar_50"),
                new CastTestCase("c_char_unicode", "varchar(50)", "c_varchar_50"),
                new CastTestCase("c_nchar_unicode", "varchar(50)", "c_varchar_50"),

                new CastTestCase("c_char_10", "varchar(1001)", "c_varchar_1001"),
                new CastTestCase("c_char_501", "varchar(1001)", "c_varchar_1001"),
                new CastTestCase("c_varchar_10", "varchar(1001)", "c_varchar_1001"),
                new CastTestCase("c_varchar_1001", "varchar(1020)", "c_varchar_1020"),
                new CastTestCase("c_clob", "varchar(1001)", "c_varchar_1001"),
                new CastTestCase("c_nclob", "varchar(1001)", "c_varchar_1001"),
                new CastTestCase("c_number_3", "varchar(1001)", "c_varchar_1001"),
                new CastTestCase("c_number_5", "varchar(1001)", "c_varchar_1001"),
                new CastTestCase("c_number_10", "varchar(1001)", "c_varchar_1001"),
                new CastTestCase("c_number_19", "varchar(1001)", "c_varchar_1001"),
                new CastTestCase("c_float", "varchar(1001)", "c_varchar_1001"),
                new CastTestCase("c_float_5", "varchar(1001)", "c_varchar_1001"),
                new CastTestCase("c_binary_float", "varchar(1001)", "c_varchar_1001"),
                new CastTestCase("c_binary_double", "varchar(1001)", "c_varchar_1001"),
                new CastTestCase("c_nan", "varchar(1001)", "c_varchar_1001"),
                new CastTestCase("c_infinity", "varchar(1001)", "c_varchar_1001"),
                new CastTestCase("c_number_15", "varchar(1001)", "c_varchar_1001"),
                new CastTestCase("c_number_10_2", "varchar(1001)", "c_varchar_1001"),
                new CastTestCase("c_number_30_2", "varchar(1001)", "c_varchar_1001"),

                new CastTestCase("c_char_10", "varchar", "c_clob"),
                new CastTestCase("c_char_501", "varchar", "c_clob"),
                new CastTestCase("c_varchar_10", "varchar", "c_clob"),
                new CastTestCase("c_varchar_1001", "varchar", "c_clob"),
                new CastTestCase("c_number_3", "varchar", "c_clob"),
                new CastTestCase("c_number_5", "varchar", "c_clob"),
                new CastTestCase("c_number_10", "varchar", "c_clob"),
                new CastTestCase("c_number_19", "varchar", "c_clob"),
                new CastTestCase("c_float", "varchar", "c_clob"),
                new CastTestCase("c_float_5", "varchar", "c_clob"),
                new CastTestCase("c_binary_float", "varchar", "c_clob"),
                new CastTestCase("c_binary_double", "varchar", "c_clob"),
                new CastTestCase("c_nan", "varchar", "c_clob"),
                new CastTestCase("c_infinity", "varchar", "c_clob"),
                new CastTestCase("c_number_15", "varchar", "c_clob"),
                new CastTestCase("c_number_10_2", "varchar", "c_clob"),
                new CastTestCase("c_number_30_2", "varchar", "c_clob"),

                new CastTestCase("c_char_10", "varchar", "c_nclob"),
                new CastTestCase("c_char_501", "varchar", "c_nclob"),
                new CastTestCase("c_varchar_10", "varchar", "c_nclob"),
                new CastTestCase("c_varchar_1001", "varchar", "c_nclob"),
                new CastTestCase("c_number_3", "varchar", "c_nclob"),
                new CastTestCase("c_number_5", "varchar", "c_nclob"),
                new CastTestCase("c_number_10", "varchar", "c_nclob"),
                new CastTestCase("c_number_19", "varchar", "c_nclob"),
                new CastTestCase("c_float", "varchar", "c_nclob"),
                new CastTestCase("c_float_5", "varchar", "c_nclob"),
                new CastTestCase("c_binary_float", "varchar", "c_nclob"),
                new CastTestCase("c_binary_double", "varchar", "c_nclob"),
                new CastTestCase("c_nan", "varchar", "c_nclob"),
                new CastTestCase("c_infinity", "varchar", "c_nclob"),
                new CastTestCase("c_number_15", "varchar", "c_nclob"),
                new CastTestCase("c_number_10_2", "varchar", "c_nclob"),
                new CastTestCase("c_number_30_2", "varchar", "c_nclob"),

                new CastTestCase("c_number_3", "tinyint", "c_number_5"),
                new CastTestCase("c_number_3", "smallint", "c_number_10"),
                new CastTestCase("c_number_3", "integer", "c_number_19"),
                new CastTestCase("c_number_3", "bigint", "c_float"),
                new CastTestCase("c_number_3", "real", "c_float_5"),
                new CastTestCase("c_number_3", "double", "c_binary_float"),
                new CastTestCase("c_number_3", "double", "c_binary_double"),
                new CastTestCase("c_number_3", "decimal(15)", "c_number_15"),
                new CastTestCase("c_number_3", "decimal(10, 2)", "c_number_10_2"),
                new CastTestCase("c_number_3", "decimal(30, 2)", "c_number_30_2"),
                new CastTestCase("c_varchar_10", "varbinary", "c_blob"),
                new CastTestCase("c_varchar_10", "varbinary", "c_raw_200"),
                new CastTestCase("c_timestamp", "date", "c_date"),
                new CastTestCase("c_timestamptz", "timestamp", "c_timestamp"),
                new CastTestCase("c_timestamp", "timestamp with time zone", "c_timestamptz"),

                // When data inserted from Trino, below cases give mismatched value between pushdown
                // and without pushdown, So not supporting cast pushdown for these cases
                new CastTestCase("c_float", "varchar(50)", "c_varchar_50"),
                new CastTestCase("c_float_5", "varchar(50)", "c_varchar_50"),
                new CastTestCase("c_binary_float", "varchar(50)", "c_varchar_50"),
                new CastTestCase("c_binary_double", "varchar(50)", "c_varchar_50"),
                new CastTestCase("c_nan", "varchar(50)", "c_varchar_50"),
                new CastTestCase("c_infinity", "varchar(50)", "c_varchar_50"),
                new CastTestCase("c_date", "varchar(50)", "c_varchar_50"),
                new CastTestCase("c_timestamp", "varchar(50)", "c_varchar_50"),
                new CastTestCase("c_timestamptz", "varchar(50)", "c_varchar_50"));
    }

    @Override
    protected List<InvalidCastTestCase> invalidCast()
    {
        return ImmutableList.of(
                new InvalidCastTestCase("c_number_3", "char(50)"),
                new InvalidCastTestCase("c_number_5", "char(50)"),
                new InvalidCastTestCase("c_number_10", "char(50)"),
                new InvalidCastTestCase("c_number_19", "char(50)"),
                new InvalidCastTestCase("c_float", "char(50)"),
                new InvalidCastTestCase("c_float_5", "char(50)"),
                new InvalidCastTestCase("c_binary_float", "char(50)"),
                new InvalidCastTestCase("c_binary_double", "char(50)"),
                new InvalidCastTestCase("c_number_15", "char(50)"),
                new InvalidCastTestCase("c_number_10_2", "char(50)"),
                new InvalidCastTestCase("c_number_30_2", "char(50)"),
                new InvalidCastTestCase("c_date", "char(50)"),
                new InvalidCastTestCase("c_timestamp", "char(50)"),
                new InvalidCastTestCase("c_timestamptz", "char(50)"),
                new InvalidCastTestCase("c_blob", "char(50)"),
                new InvalidCastTestCase("c_raw_200", "char(50)"),

                new InvalidCastTestCase("c_number_3", "char(501)"),
                new InvalidCastTestCase("c_number_5", "char(501)"),
                new InvalidCastTestCase("c_number_10", "char(501)"),
                new InvalidCastTestCase("c_number_19", "char(501)"),
                new InvalidCastTestCase("c_float", "char(501)"),
                new InvalidCastTestCase("c_float_5", "char(501)"),
                new InvalidCastTestCase("c_binary_float", "char(501)"),
                new InvalidCastTestCase("c_binary_double", "char(501)"),
                new InvalidCastTestCase("c_number_15", "char(501)"),
                new InvalidCastTestCase("c_number_10_2", "char(501)"),
                new InvalidCastTestCase("c_number_30_2", "char(501)"),
                new InvalidCastTestCase("c_date", "char(501)"),
                new InvalidCastTestCase("c_timestamp", "char(501)"),
                new InvalidCastTestCase("c_timestamptz", "char(501)"),
                new InvalidCastTestCase("c_blob", "char(501)"),
                new InvalidCastTestCase("c_raw_200", "char(501)"),

                new InvalidCastTestCase("c_blob", "varchar(50)"),
                new InvalidCastTestCase("c_raw_200", "varchar(50)"));
    }

    private static List<CastTestCase> specialCaseNClob()
    {
        // Trino converts clob type to nclob
        return ImmutableList.of(
                new CastTestCase("c_clob", "varchar", "c_clob"),
                new CastTestCase("c_nclob", "varchar", "c_clob"),
                new CastTestCase("c_clob", "varchar", "c_nclob"),
                new CastTestCase("c_nclob", "varchar", "c_nclob"));
    }
}

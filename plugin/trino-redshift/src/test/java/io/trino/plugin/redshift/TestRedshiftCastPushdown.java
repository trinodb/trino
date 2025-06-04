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
package io.trino.plugin.redshift;

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

import java.util.List;

import static io.trino.plugin.redshift.TestingRedshiftServer.TEST_SCHEMA;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

final class TestRedshiftCastPushdown
        extends BaseJdbcCastPushdownTest
{
    private CastDataTypeTestTable left;
    private CastDataTypeTestTable right;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return RedshiftQueryRunner.builder()
                .setConnectorProperties(ImmutableMap.<String, String>builder()
                        .put("unsupported-type-handling", "CONVERT_TO_VARCHAR")
                        .put("join-pushdown.enabled", "true")
                        .put("join-pushdown.strategy", "EAGER")
                        .buildOrThrow())
                .build();
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return TestingRedshiftServer::executeInRedshift;
    }

    @BeforeAll
    void setupTable()
    {
        left = closeAfterClass(CastDataTypeTestTable.create(3)
                .addColumn("id", "int", asList(11, 12, 13))
                .addColumn("c_boolean", "boolean", asList(true, false, null))
                .addColumn("c_smallint", "smallint", asList(1, 2, null))
                .addColumn("c_int2", "int2", asList(1, 2, null))
                .addColumn("c_integer", "integer", asList(1, 2, null))
                .addColumn("c_int", "int", asList(1, 2, null))
                .addColumn("c_int4", "int4", asList(1, 2, null))
                .addColumn("c_bigint", "bigint", asList(1, 2, null))
                .addColumn("c_int8", "int8", asList(1, 2, null))
                .addColumn("c_real", "real", asList(1.23, 2.67, null))
                .addColumn("c_float4", "float4", asList(1.23, 2.67, null))
                .addColumn("c_double_precision", "double precision", asList(1.23, 2.67, null))
                .addColumn("c_float", "float", asList(1.23, 2.67, null))
                .addColumn("c_float8", "float8", asList(1.23, 2.67, null))
                .addColumn("c_decimal_10_2", "decimal(10, 2)", asList(1.23, 2.67, null))
                .addColumn("c_decimal_19_2", "decimal(19, 2)", asList(1.23, 2.67, null)) // Equal to REDSHIFT_DECIMAL_CUTOFF_PRECISION
                .addColumn("c_decimal_30_2", "decimal(30, 2)", asList(1.23, 2.67, null))
                .addColumn("c_numeric_10_2", "numeric(10, 2)", asList(1.23, 2.67, null))
                .addColumn("c_numeric_19_2", "numeric(19, 2)", asList(1.23, 2.67, null)) // Equal to REDSHIFT_DECIMAL_CUTOFF_PRECISION
                .addColumn("c_numeric_30_2", "numeric(30, 2)", asList(1.23, 2.67, null))
                .addColumn("c_char", "char", asList("'I'", "'P'", null))
                .addColumn("c_char_10", "char(10)", asList("'India'", "'Poland'", null))
                .addColumn("c_char_50", "char(50)", asList("'India'", "'Poland'", null))
                .addColumn("c_char_4096", "char(4096)", asList("'India'", "'Poland'", null)) // Equal to REDSHIFT_MAX_CHAR

                // the number of Unicode code points in 攻殻機動隊 is 5, and in 😂 is 1.
                .addColumn("c_varchar_15_unicode", "varchar(15)", asList("'攻殻機動隊'", "'😂'", null))
                .addColumn("c_nvarchar_15_unicode", "nvarchar(15)", asList("'攻殻機動隊'", "'😂'", null))

                .addColumn("c_nchar", "nchar", asList("'I'", "'P'", null))
                .addColumn("c_nchar_10", "nchar(10)", asList("'India'", "'Poland'", null))
                .addColumn("c_nchar_50", "nchar(50)", asList("'India'", "'Poland'", null))
                .addColumn("c_nchar_4096", "nchar(4096)", asList("'India'", "'Poland'", null)) // Equal to REDSHIFT_MAX_CHAR
                .addColumn("c_bpchar", "bpchar", asList("'India'", "'Poland'", null))
                .addColumn("c_varchar_10", "varchar(10)", asList("'India'", "'Poland'", null))
                .addColumn("c_varchar_50", "varchar(50)", asList("'India'", "'Poland'", null))
                .addColumn("c_varchar_65535", "varchar(65535)", asList("'India'", "'Poland'", null)) // Equal to REDSHIFT_MAX_VARCHAR
                .addColumn("c_nvarchar_10", "nvarchar(10)", asList("'India'", "'Poland'", null))
                .addColumn("c_nvarchar_50", "nvarchar(50)", asList("'India'", "'Poland'", null))
                .addColumn("c_nvarchar", "nvarchar", asList("'India'", "'Poland'", null))
                .addColumn("c_nvarchar_65535", "nvarchar(65535)", asList("'India'", "'Poland'", null)) // Greater than REDSHIFT_MAX_VARCHAR
                .addColumn("c_text", "text", asList("'India'", "'Poland'", null))
                .addColumn("c_varbinary", "varbinary", asList("'\\x66696E6465706920726F636B7321'", "'\\x000102f0feee'", null))
                .addColumn("c_date", "date", asList("DATE '2024-09-08'", "DATE '2019-08-15'", null))
                .addColumn("c_time", "time", asList("TIME '00:13:42.000000'", "TIME '10:01:17.100000'", null))
                .addColumn("c_timestamp", "timestamp", asList("TIMESTAMP '2024-09-08 01:02:03.666'", "TIMESTAMP '2019-08-15 09:08:07.333'", null))
                .addColumn("c_timestamptz", "timestamptz", asList("TIMESTAMP '2024-09-08 01:02:03.666+05:30'", "TIMESTAMP '2019-08-15 09:08:07.333+05:30'", null))

                .addColumn("c_nan_real", "real", asList("'Nan'", "'-Nan'", null))
                .addColumn("c_nan_double", "double precision", asList("'Nan'", "'-Nan'", null))
                .addColumn("c_infinity_real", "real", asList("'Infinity'", "'-Infinity'", null))
                .addColumn("c_infinity_double", "double precision", asList("'Infinity'", "'-Infinity'", null))
                .addColumn("c_decimal_negative", "decimal(19, 2)", asList(-1.23, -2.67, null))
                .addColumn("c_varchar_numeric", "varchar(50)", asList("'123'", "'124'", null))
                .addColumn("c_char_numeric", "char(50)", asList("'123'", "'124'", null))
                .addColumn("c_bpchar_numeric", "bpchar", asList("'123'", "'124'", null))
                .addColumn("c_text_numeric", "text", asList("'123'", "'124'", null))
                .addColumn("c_nvarchar_numeric", "nvarchar(50)", asList("'123'", "'124'", null))
                .addColumn("c_varchar_numeric_sign", "varchar(50)", asList("'+123'", "'-124'", null))
                .addColumn("c_varchar_decimal", "varchar(50)", asList("'1.23'", "'2.34'", null))
                .addColumn("c_varchar_decimal_sign", "varchar(50)", asList("'+1.23'", "'-2.34'", null))
                .addColumn("c_varchar_alpha_numeric", "varchar(50)", asList("'H311o'", "'123Hey'", null))
                .addColumn("c_varchar_date", "varchar(50)", asList("'2024-09-08'", "'2019-08-15'", null))
                .addColumn("c_varchar_timestamp", "varchar(50)", asList("'2024-09-08 01:02:03.666'", "'2019-08-15 09:08:07.333'", null))
                .addColumn("c_varchar_timestamptz", "varchar(50)", asList("'2024-09-08 01:02:03.666+05:30'", "'2019-08-15 09:08:07.333+05:30'", null))

                // unsupported in trino
                .addColumn("c_timetz", "timetz", asList("TIME '00:13:42.000000+05:30'", "TIME '10:01:17.100000+05:30'", null))
                .addColumn("c_super", "super", asList(1, 2, null))
                .execute(onRemoteDatabase(), TEST_SCHEMA + "." + "left_table_"));

        // 2nd row value is different in right than left
        right = closeAfterClass(CastDataTypeTestTable.create(3)
                .addColumn("id", "int", asList(21, 22, 23))
                .addColumn("c_boolean", "boolean", asList(true, true, null))
                .addColumn("c_smallint", "smallint", asList(1, 22, null))
                .addColumn("c_int2", "int2", asList(1, 22, null))
                .addColumn("c_integer", "integer", asList(1, 22, null))
                .addColumn("c_int", "int", asList(1, 22, null))
                .addColumn("c_int4", "int4", asList(1, 22, null))
                .addColumn("c_bigint", "bigint", asList(1, 22, null))
                .addColumn("c_int8", "int8", asList(1, 22, null))
                .addColumn("c_real", "real", asList(1.23, 22.67, null))
                .addColumn("c_float4", "float4", asList(1.23, 22.67, null))
                .addColumn("c_double_precision", "double precision", asList(1.23, 22.67, null))
                .addColumn("c_float", "float", asList(1.23, 22.67, null))
                .addColumn("c_float8", "float8", asList(1.23, 22.67, null))
                .addColumn("c_decimal_10_2", "decimal(10, 2)", asList(1.23, 22.67, null))
                .addColumn("c_decimal_19_2", "decimal(19, 2)", asList(1.23, 22.67, null)) // Equal to REDSHIFT_DECIMAL_CUTOFF_PRECISION
                .addColumn("c_decimal_30_2", "decimal(30, 2)", asList(1.23, 22.67, null))
                .addColumn("c_numeric_10_2", "numeric(10, 2)", asList(1.23, 22.67, null))
                .addColumn("c_numeric_19_2", "numeric(19, 2)", asList(1.23, 22.67, null)) // Equal to REDSHIFT_DECIMAL_CUTOFF_PRECISION
                .addColumn("c_numeric_30_2", "numeric(30, 2)", asList(1.23, 22.67, null))
                .addColumn("c_char", "char", asList("'I'", "'F'", null))
                .addColumn("c_char_10", "char(10)", asList("'India'", "'France'", null))
                .addColumn("c_char_50", "char(50)", asList("'India'", "'France'", null))
                .addColumn("c_char_4096", "char(4096)", asList("'India'", "'France'", null)) // Equal to REDSHIFT_MAX_CHAR

                // the number of Unicode code points in 攻殻機動隊 is 5, and in 😂 is 1.
                .addColumn("c_varchar_15_unicode", "varchar(15)", asList("'攻殻機動隊'", "'😂'", null))
                .addColumn("c_nvarchar_15_unicode", "nvarchar(15)", asList("'攻殻機動隊'", "'😂'", null))

                .addColumn("c_nchar", "nchar", asList("'I'", "'F'", null))
                .addColumn("c_nchar_10", "nchar(10)", asList("'India'", "'France'", null))
                .addColumn("c_nchar_50", "nchar(50)", asList("'India'", "'France'", null))
                .addColumn("c_nchar_4096", "nchar(4096)", asList("'India'", "'France'", null)) // Equal to REDSHIFT_MAX_CHAR
                .addColumn("c_bpchar", "bpchar", asList("'India'", "'France'", null))
                .addColumn("c_varchar_10", "varchar(10)", asList("'India'", "'France'", null))
                .addColumn("c_varchar_50", "varchar(50)", asList("'India'", "'France'", null))
                .addColumn("c_varchar_65535", "varchar(65535)", asList("'India'", "'France'", null)) // Equal to REDSHIFT_MAX_VARCHAR
                .addColumn("c_nvarchar_10", "nvarchar(10)", asList("'India'", "'France'", null))
                .addColumn("c_nvarchar_50", "nvarchar(50)", asList("'India'", "'France'", null))
                .addColumn("c_nvarchar", "nvarchar", asList("'India'", "'France'", null))
                .addColumn("c_nvarchar_65535", "nvarchar(65535)", asList("'India'", "'France'", null)) // Equal to REDSHIFT_MAX_VARCHAR
                .addColumn("c_text", "text", asList("'India'", "'France'", null))
                .addColumn("c_varbinary", "varbinary", asList("'\\x66696E6465706920726F636B7321'", "'\\x4672616E6365'", null))
                .addColumn("c_date", "date", asList("DATE '2024-09-08'", "DATE '2020-08-15'", null))
                .addColumn("c_time", "time", asList("TIME '00:13:42.000000'", "TIME '11:01:17.100000'", null))
                .addColumn("c_timestamp", "timestamp", asList("TIMESTAMP '2024-09-08 01:02:03.666'", "TIMESTAMP '2020-08-15 09:08:07.333'", null))
                .addColumn("c_timestamptz", "timestamptz", asList("TIMESTAMP '2024-09-08 01:02:03.666+05:30'", "TIMESTAMP '2020-08-15 09:08:07.333+05:30'", null))

                .addColumn("c_nan_real", "real", asList("'Nan'", "'-Nan'", null))
                .addColumn("c_nan_double", "double precision", asList("'Nan'", "'-Nan'", null))
                .addColumn("c_infinity_real", "real", asList("'Infinity'", "'-Infinity'", null))
                .addColumn("c_infinity_double", "double precision", asList("'Infinity'", "'-Infinity'", null))
                .addColumn("c_decimal_negative", "decimal(19, 2)", asList(-1.23, -22.67, null))
                .addColumn("c_varchar_numeric", "varchar(50)", asList("'123'", "'228'", null))
                .addColumn("c_char_numeric", "char(50)", asList("'123'", "'125'", null))
                .addColumn("c_bpchar_numeric", "bpchar", asList("'123'", "'125'", null))
                .addColumn("c_text_numeric", "text", asList("'123'", "'125'", null))
                .addColumn("c_nvarchar_numeric", "nvarchar(50)", asList("'123'", "'125'", null))
                .addColumn("c_varchar_numeric_sign", "varchar(50)", asList("'+123'", "'-125'", null))
                .addColumn("c_varchar_decimal", "varchar(50)", asList("'1.23'", "'22.34'", null))
                .addColumn("c_varchar_decimal_sign", "varchar(50)", asList("'+1.23'", "'-22.34'", null))
                .addColumn("c_varchar_alpha_numeric", "varchar(50)", asList("'H311o'", "'123Bye'", null))
                .addColumn("c_varchar_date", "varchar(50)", asList("'2024-09-08'", "'2020-08-15'", null))
                .addColumn("c_varchar_timestamp", "varchar(50)", asList("'2024-09-08 01:02:03.666'", "'2020-08-15 09:08:07.333'", null))
                .addColumn("c_varchar_timestamptz", "varchar(50)", asList("'2024-09-08 01:02:03.666+05:30'", "'2020-08-15 09:08:07.333+05:30'", null))

                // unsupported in trino
                .addColumn("c_timetz", "timetz", asList("TIME '00:13:42.000000+05:30'", "TIME '11:01:17.100000+05:30'", null))
                .addColumn("c_super", "super", asList(1, 22, null))
                .execute(onRemoteDatabase(), TEST_SCHEMA + "." + "right_table_"));
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
    void testJoinPushdownWithNestedCast()
    {
        CastTestCase testCase = new CastTestCase("c_decimal_10_2", "bigint", "c_bigint");
        assertThat(query("SELECT l.id FROM %s l LEFT JOIN %s r ON CAST(CAST(l.%s AS %s) AS integer) = r.%s".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                .isFullyPushedDown();
    }

    @Test
    void testAllJoinPushdownWithCast()
    {
        CastTestCase testCase = new CastTestCase("c_int", "bigint", "c_bigint");
        assertThat(query("SELECT l.id FROM %s l LEFT JOIN %s r ON CAST(l.%s AS %s) = r.%s".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l RIGHT JOIN %s r ON CAST(l.%s AS %s) = r.%s".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l INNER JOIN %s r ON CAST(l.%s AS %s) = r.%s".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                .isFullyPushedDown();
        // Full Join pushdown is not supported
        assertThat(query("SELECT l.id FROM %s l FULL JOIN %s r ON CAST(l.%s AS %s) = r.%s".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                .joinIsNotFullyPushedDown();

        testCase = new CastTestCase("c_bigint", "integer", "c_int");
        assertThat(query("SELECT l.id FROM %s l LEFT JOIN %s r ON l.%s = CAST(r.%s AS %s)".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.targetColumn(), testCase.castType())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l RIGHT JOIN %s r ON l.%s = CAST(r.%s AS %s)".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.targetColumn(), testCase.castType())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l INNER JOIN %s r ON l.%s = CAST(r.%s AS %s)".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.targetColumn(), testCase.castType())))
                .isFullyPushedDown();
        // Full Join pushdown is not supported
        assertThat(query("SELECT l.id FROM %s l FULL JOIN %s r ON CAST(l.%s AS %s) = r.%s".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                .joinIsNotFullyPushedDown();

        testCase = new CastTestCase("c_bigint", "smallint", "c_int");
        assertThat(query("SELECT l.id FROM %s l LEFT JOIN %s r ON CAST(l.%3$s AS %4$s) = CAST(r.%5$s AS %4$s)".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l RIGHT JOIN %s r ON CAST(l.%3$s AS %4$s) = CAST(r.%5$s AS %4$s)".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l INNER JOIN %s r ON CAST(l.%3$s AS %4$s) = CAST(r.%5$s AS %4$s)".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                .isFullyPushedDown();
        // Full Join pushdown is not supported
        assertThat(query("SELECT l.id FROM %s l FULL JOIN %s r ON CAST(l.%s AS %s) = r.%s".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                .joinIsNotFullyPushedDown();

        testCase = new CastTestCase("c_varchar_10", "varchar(200)", "c_varchar_50");
        assertThat(query("SELECT l.id FROM %s l LEFT JOIN %s r ON CAST(l.%3$s AS %4$s) = CAST(r.%5$s AS %4$s)".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l RIGHT JOIN %s r ON CAST(l.%3$s AS %4$s) = CAST(r.%5$s AS %4$s)".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l INNER JOIN %s r ON CAST(l.%3$s AS %4$s) = CAST(r.%5$s AS %4$s)".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                .isFullyPushedDown();
        // Full Join pushdown is not supported
        assertThat(query("SELECT l.id FROM %s l FULL JOIN %s r ON CAST(l.%3$s AS %4$s) = CAST(r.%5$s AS %4$s)".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                .joinIsNotFullyPushedDown();
    }

    @Test
    void testCastPushdownDisabled()
    {
        Session sessionWithoutPushdown = Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "complex_expression_pushdown", "false")
                .build();
        assertThat(query(sessionWithoutPushdown, "SELECT CAST (c_int AS bigint) FROM %s".formatted(leftTable())))
                .isNotFullyPushedDown(ProjectNode.class);
    }

    @Test
    void testCastPushdownOutOfRangeValue()
    {
        CastDataTypeTestTable table = closeAfterClass(CastDataTypeTestTable.create(1)
                .addColumn("id", "int", List.of(1))
                .addColumn("c_smallint_1", "smallint", List.of("-129"))
                .addColumn("c_smallint_2", "smallint", List.of("128"))
                .addColumn("c_int_1", "integer", List.of("-65537"))
                .addColumn("c_int_2", "integer", List.of("65536"))
                .addColumn("c_bigint_1", "bigint", List.of("-2147483649"))
                .addColumn("c_bigint_2", "bigint", List.of("2147483648"))

                .addColumn("c_decimal_1", "decimal(25, 2)", List.of("-129.49"))
                .addColumn("c_decimal_2", "decimal(25, 2)", List.of("-128.94"))
                .addColumn("c_decimal_3", "decimal(25, 2)", List.of("127.94"))
                .addColumn("c_decimal_4", "decimal(25, 2)", List.of("128.49"))
                .addColumn("c_decimal_5", "decimal(25, 2)", List.of("-65537.49"))
                .addColumn("c_decimal_6", "decimal(25, 2)", List.of("-65536.94"))
                .addColumn("c_decimal_7", "decimal(25, 2)", List.of("65535.94"))
                .addColumn("c_decimal_8", "decimal(25, 2)", List.of("65536.49"))
                .addColumn("c_decimal_9", "decimal(25, 2)", List.of("-2147483649.49"))
                .addColumn("c_decimal_10", "decimal(25, 2)", List.of("-2147483648.94"))
                .addColumn("c_decimal_11", "decimal(25, 2)", List.of("2147483647.94"))
                .addColumn("c_decimal_12", "decimal(25, 2)", List.of("2147483648.49"))
                .addColumn("c_decimal_13", "decimal(25, 2)", List.of("-9223372036854775809.49"))
                .addColumn("c_decimal_14", "decimal(25, 2)", List.of("-9223372036854775808.94"))
                .addColumn("c_decimal_15", "decimal(25, 2)", List.of("9223372036854775807.94"))
                .addColumn("c_decimal_16", "decimal(25, 2)", List.of("9223372036854775808.49"))

                .addColumn("c_infinity_real_1", "real", List.of("'Infinity'"))
                .addColumn("c_infinity_real_2", "real", List.of("'-Infinity'"))
                .addColumn("c_infinity_double_1", "double precision", List.of("'Infinity'"))
                .addColumn("c_infinity_double_2", "double precision", List.of("'-Infinity'"))
                .execute(onRemoteDatabase(), TEST_SCHEMA + "." + "test_decimal_overflow_"));

        assertInvalidCast(
                table.getName(),
                ImmutableList.<InvalidCastTestCase>builder()
                        // Not pushdown for tinyint type
                        .add(new InvalidCastTestCase("c_smallint_1", "tinyint", "Out of range for tinyint: -129"))
                        .add(new InvalidCastTestCase("c_smallint_2", "tinyint", "Out of range for tinyint: 128"))
                        .add(new InvalidCastTestCase("c_int_1", "tinyint", "Out of range for tinyint: -65537"))
                        .add(new InvalidCastTestCase("c_int_2", "tinyint", "Out of range for tinyint: 65536"))
                        .add(new InvalidCastTestCase("c_bigint_1", "tinyint", "Out of range for tinyint: -2147483649"))
                        .add(new InvalidCastTestCase("c_bigint_2", "tinyint", "Out of range for tinyint: 2147483648"))

                        .add(new InvalidCastTestCase("c_int_1", "smallint", "Out of range for smallint: -65537", "(?s).*ERROR: Value out of range for 2 bytes(?s).*"))
                        .add(new InvalidCastTestCase("c_int_2", "smallint", "Out of range for smallint: 65536", "(?s).*Value out of range for 2 bytes(?s).*"))
                        .add(new InvalidCastTestCase("c_bigint_1", "smallint", "Out of range for smallint: -2147483649", "(?s).*Value out of range for 2 bytes(?s).*"))
                        .add(new InvalidCastTestCase("c_bigint_2", "smallint", "Out of range for smallint: 2147483648", "(?s).*Value out of range for 2 bytes(?s).*"))
                        .add(new InvalidCastTestCase("c_bigint_1", "integer", "Out of range for integer: -2147483649", "(?s).*Value out of range for 4 bytes(?s).*"))
                        .add(new InvalidCastTestCase("c_bigint_2", "integer", "Out of range for integer: 2147483648", "(?s).*Value out of range for 4 bytes(?s).*"))

                        // Not pushdown for tinyint type
                        .add(new InvalidCastTestCase("c_decimal_1", "tinyint", "Cannot cast '-129.49' to TINYINT"))
                        .add(new InvalidCastTestCase("c_decimal_2", "tinyint", "Cannot cast '-128.94' to TINYINT"))
                        .add(new InvalidCastTestCase("c_decimal_3", "tinyint", "Cannot cast '127.94' to TINYINT"))
                        .add(new InvalidCastTestCase("c_decimal_4", "tinyint", "Cannot cast '128.49' to TINYINT"))

                        .add(new InvalidCastTestCase("c_decimal_5", "smallint", "Cannot cast '-65537.49' to SMALLINT", "(?s).*Value out of range for 2 bytes(?s).*"))
                        .add(new InvalidCastTestCase("c_decimal_6", "smallint", "Cannot cast '-65536.94' to SMALLINT", "(?s).*Value out of range for 2 bytes(?s).*"))
                        .add(new InvalidCastTestCase("c_decimal_7", "smallint", "Cannot cast '65535.94' to SMALLINT", "(?s).*Value out of range for 2 bytes(?s).*"))
                        .add(new InvalidCastTestCase("c_decimal_8", "smallint", "Cannot cast '65536.49' to SMALLINT", "(?s).*Value out of range for 2 bytes(?s).*"))
                        .add(new InvalidCastTestCase("c_decimal_9", "integer", "Cannot cast '-2147483649.49' to INTEGER", "(?s).*Value out of range for 4 bytes(?s).*"))
                        .add(new InvalidCastTestCase("c_decimal_10", "integer", "Cannot cast '-2147483648.94' to INTEGER", "(?s).*Value out of range for 4 bytes(?s).*"))
                        .add(new InvalidCastTestCase("c_decimal_11", "integer", "Cannot cast '2147483647.94' to INTEGER", "(?s).*Value out of range for 4 bytes(?s).*"))
                        .add(new InvalidCastTestCase("c_decimal_12", "integer", "Cannot cast '2147483648.49' to INTEGER", "(?s).*Value out of range for 4 bytes(?s).*"))
                        .add(new InvalidCastTestCase("c_decimal_13", "bigint", "Cannot cast '-9223372036854775809.49' to BIGINT", "(?s).*Value out of range for 8 bytes(?s).*"))
                        .add(new InvalidCastTestCase("c_decimal_14", "bigint", "Cannot cast '-9223372036854775808.94' to BIGINT", "(?s).*Value out of range for 8 bytes(?s).*"))
                        .add(new InvalidCastTestCase("c_decimal_15", "bigint", "Cannot cast '9223372036854775807.94' to BIGINT", "(?s).*Value out of range for 8 bytes(?s).*"))
                        .add(new InvalidCastTestCase("c_decimal_16", "bigint", "Cannot cast '9223372036854775808.49' to BIGINT", "(?s).*Value out of range for 8 bytes(?s).*"))

                        // No pushdown for real datatype to integral types
                        .add(new InvalidCastTestCase("c_infinity_real_1", "tinyint", "Out of range for tinyint: Infinity"))
                        .add(new InvalidCastTestCase("c_infinity_real_1", "smallint", "Out of range for smallint: Infinity"))
                        .add(new InvalidCastTestCase("c_infinity_real_1", "integer", "Out of range for integer: Infinity"))
                        .add(new InvalidCastTestCase("c_infinity_real_2", "tinyint", "Out of range for tinyint: -Infinity"))
                        .add(new InvalidCastTestCase("c_infinity_real_2", "smallint", "Out of range for smallint: -Infinity"))
                        .add(new InvalidCastTestCase("c_infinity_real_2", "integer", "Out of range for integer: -Infinity"))

                        // No pushdown for double precision datatype to integral types
                        .add(new InvalidCastTestCase("c_infinity_double_1", "tinyint", "Out of range for tinyint: Infinity"))
                        .add(new InvalidCastTestCase("c_infinity_double_1", "smallint", "Out of range for smallint: Infinity"))
                        .add(new InvalidCastTestCase("c_infinity_double_1", "integer", "Out of range for integer: Infinity"))
                        .add(new InvalidCastTestCase("c_infinity_double_1", "bigint", "Unable to cast Infinity to bigint"))
                        .add(new InvalidCastTestCase("c_infinity_double_2", "tinyint", "Out of range for tinyint: -Infinity"))
                        .add(new InvalidCastTestCase("c_infinity_double_2", "smallint", "Out of range for smallint: -Infinity"))
                        .add(new InvalidCastTestCase("c_infinity_double_2", "integer", "Out of range for integer: -Infinity"))
                        .add(new InvalidCastTestCase("c_infinity_double_2", "bigint", "Unable to cast -Infinity to bigint"))
                        .build());
    }

    @Test
    void testCastRealInfinityValueToBigint()
    {
        assertThat(query("SELECT CAST(c_Infinity_real AS BIGINT) FROM %s".formatted(leftTable())))
                .matches("VALUES (BIGINT '9223372036854775807'), (BIGINT '-9223372036854775808'), (null)")
                .isNotFullyPushedDown(ProjectNode.class);
    }

    @Test
    void testCastPushdownWithForcedTypedToInteger()
    {
        // These column types are not supported by default by trino. These types are forced mapped to varchar.
        assertThat(query("SELECT CAST(c_super AS INTEGER) FROM %s".formatted(leftTable())))
                .isNotFullyPushedDown(ProjectNode.class);
        assertThat(query("SELECT CAST(c_super AS BIGINT) FROM %s".formatted(leftTable())))
                .isNotFullyPushedDown(ProjectNode.class);
    }

    @Test
    void testCastPushdownWithForcedTypedToVarchar()
    {
        // These column types are not supported by default by trino. These types are forced mapped to varchar.
        assertThat(query("SELECT CAST(c_timetz AS VARCHAR(100)) FROM " + leftTable()))
                .isNotFullyPushedDown(ProjectNode.class);
        assertThat(query("SELECT CAST(c_super AS VARCHAR(100)) FROM " + leftTable()))
                .isNotFullyPushedDown(ProjectNode.class);
    }

    @Override
    protected List<CastTestCase> supportedCastTypePushdown()
    {
        return ImmutableList.<CastTestCase>builder()
                .add(new CastTestCase("c_boolean", "smallint", "c_smallint"))
                .add(new CastTestCase("c_smallint", "smallint", "c_smallint"))
                .add(new CastTestCase("c_integer", "smallint", "c_smallint"))
                .add(new CastTestCase("c_bigint", "smallint", "c_smallint"))
                .add(new CastTestCase("c_decimal_10_2", "smallint", "c_smallint"))
                .add(new CastTestCase("c_decimal_19_2", "smallint", "c_smallint"))
                .add(new CastTestCase("c_decimal_30_2", "smallint", "c_smallint"))
                .add(new CastTestCase("c_decimal_negative", "smallint", "c_smallint"))

                .add(new CastTestCase("c_boolean", "integer", "c_integer"))
                .add(new CastTestCase("c_smallint", "integer", "c_integer"))
                .add(new CastTestCase("c_int2", "integer", "c_integer"))
                .add(new CastTestCase("c_integer", "integer", "c_integer"))
                .add(new CastTestCase("c_int", "integer", "c_integer"))
                .add(new CastTestCase("c_int4", "integer", "c_integer"))
                .add(new CastTestCase("c_bigint", "integer", "c_integer"))
                .add(new CastTestCase("c_int8", "integer", "c_integer"))
                .add(new CastTestCase("c_decimal_10_2", "integer", "c_integer"))
                .add(new CastTestCase("c_decimal_19_2", "integer", "c_integer"))
                .add(new CastTestCase("c_decimal_30_2", "integer", "c_integer"))
                .add(new CastTestCase("c_numeric_10_2", "integer", "c_integer"))
                .add(new CastTestCase("c_numeric_19_2", "integer", "c_integer"))
                .add(new CastTestCase("c_numeric_30_2", "integer", "c_integer"))
                .add(new CastTestCase("c_decimal_negative", "integer", "c_integer"))

                .add(new CastTestCase("c_boolean", "bigint", "c_bigint"))
                .add(new CastTestCase("c_smallint", "bigint", "c_bigint"))
                .add(new CastTestCase("c_integer", "bigint", "c_bigint"))
                .add(new CastTestCase("c_bigint", "bigint", "c_bigint"))
                .add(new CastTestCase("c_decimal_10_2", "bigint", "c_bigint"))
                .add(new CastTestCase("c_decimal_19_2", "bigint", "c_bigint"))
                .add(new CastTestCase("c_decimal_30_2", "bigint", "c_bigint"))
                .add(new CastTestCase("c_decimal_negative", "bigint", "c_bigint"))

                .add(new CastTestCase("c_char", "char(1)", "c_nchar"))
                .add(new CastTestCase("c_char_50", "char(10)", "c_char_10"))
                .add(new CastTestCase("c_char_50", "char(10)", "c_nchar_10"))
                .add(new CastTestCase("c_bpchar", "char(10)", "c_char_10"))
                .add(new CastTestCase("c_char_10", "char(50)", "c_char_50"))
                .add(new CastTestCase("c_char_10", "char(256)", "c_bpchar"))
                .add(new CastTestCase("c_char", "char(4096)", "c_char_4096"))

                .add(new CastTestCase("c_varchar_10", "varchar(10)", "c_varchar_10"))
                .add(new CastTestCase("c_varchar_15_unicode", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_nvarchar_15_unicode", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_varchar_50", "varchar(10)", "c_varchar_10"))
                .add(new CastTestCase("c_nvarchar_50", "varchar(10)", "c_nvarchar_10"))
                .add(new CastTestCase("c_text", "varchar(10)", "c_varchar_10"))
                .add(new CastTestCase("c_varchar_10", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_nvarchar_10", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_nvarchar", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_text", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_varchar_10", "varchar(256)", "c_text"))
                .add(new CastTestCase("c_varchar_65535", "varchar(256)", "c_text"))
                .add(new CastTestCase("c_varchar_10", "varchar(65535)", "c_varchar_65535"))
                .add(new CastTestCase("c_varchar_10", "varchar(65535)", "c_nvarchar_65535"))
                .build();
    }

    @Override
    protected List<CastTestCase> unsupportedCastTypePushdown()
    {
        return ImmutableList.<CastTestCase>builder()
                .add(new CastTestCase("c_boolean", "tinyint", "c_smallint"))
                .add(new CastTestCase("c_smallint", "tinyint", "c_smallint"))
                .add(new CastTestCase("c_integer", "tinyint", "c_smallint"))
                .add(new CastTestCase("c_bigint", "tinyint", "c_smallint"))
                .add(new CastTestCase("c_decimal_10_2", "tinyint", "c_smallint"))
                .add(new CastTestCase("c_decimal_19_2", "tinyint", "c_smallint"))
                .add(new CastTestCase("c_decimal_30_2", "tinyint", "c_smallint"))
                .add(new CastTestCase("c_decimal_negative", "tinyint", "c_smallint"))

                .add(new CastTestCase("c_real", "tinyint", "c_smallint"))
                .add(new CastTestCase("c_float4", "tinyint", "c_smallint"))
                .add(new CastTestCase("c_double_precision", "tinyint", "c_smallint"))
                .add(new CastTestCase("c_float", "tinyint", "c_smallint"))
                .add(new CastTestCase("c_float8", "tinyint", "c_smallint"))
                .add(new CastTestCase("c_varchar_numeric", "tinyint", "c_smallint"))
                .add(new CastTestCase("c_text_numeric", "tinyint", "c_smallint"))
                .add(new CastTestCase("c_nvarchar_numeric", "tinyint", "c_smallint"))
                .add(new CastTestCase("c_varchar_numeric_sign", "tinyint", "c_smallint"))

                .add(new CastTestCase("c_real", "smallint", "c_smallint"))
                .add(new CastTestCase("c_float4", "smallint", "c_smallint"))
                .add(new CastTestCase("c_double_precision", "smallint", "c_smallint"))
                .add(new CastTestCase("c_float", "smallint", "c_smallint"))
                .add(new CastTestCase("c_float8", "smallint", "c_smallint"))
                .add(new CastTestCase("c_varchar_numeric", "smallint", "c_smallint"))
                .add(new CastTestCase("c_text_numeric", "smallint", "c_smallint"))
                .add(new CastTestCase("c_nvarchar_numeric", "smallint", "c_smallint"))
                .add(new CastTestCase("c_varchar_numeric_sign", "smallint", "c_smallint"))

                .add(new CastTestCase("c_real", "integer", "c_integer"))
                .add(new CastTestCase("c_float4", "integer", "c_integer"))
                .add(new CastTestCase("c_double_precision", "integer", "c_integer"))
                .add(new CastTestCase("c_float", "integer", "c_integer"))
                .add(new CastTestCase("c_float8", "integer", "c_integer"))
                .add(new CastTestCase("c_varchar_numeric", "integer", "c_integer"))
                .add(new CastTestCase("c_text_numeric", "integer", "c_integer"))
                .add(new CastTestCase("c_nvarchar_numeric", "integer", "c_integer"))
                .add(new CastTestCase("c_varchar_numeric_sign", "integer", "c_integer"))

                .add(new CastTestCase("c_real", "bigint", "c_bigint"))
                .add(new CastTestCase("c_float4", "bigint", "c_bigint"))
                .add(new CastTestCase("c_double_precision", "bigint", "c_bigint"))
                .add(new CastTestCase("c_float", "bigint", "c_bigint"))
                .add(new CastTestCase("c_float8", "bigint", "c_bigint"))
                .add(new CastTestCase("c_varchar_numeric", "bigint", "c_bigint"))
                .add(new CastTestCase("c_text_numeric", "bigint", "c_bigint"))
                .add(new CastTestCase("c_nvarchar_numeric", "bigint", "c_bigint"))
                .add(new CastTestCase("c_varchar_numeric_sign", "bigint", "c_bigint"))

                .add(new CastTestCase("c_smallint", "boolean", "c_boolean"))
                .add(new CastTestCase("c_real", "double", "c_double_precision"))
                .add(new CastTestCase("c_double_precision", "real", "c_real"))
                .add(new CastTestCase("c_double_precision", "decimal(10,2)", "c_decimal_10_2"))

                .add(new CastTestCase("c_varchar_15_unicode", "char(50)", "c_char_50"))
                .add(new CastTestCase("c_nvarchar_15_unicode", "char(50)", "c_char_50"))
                .add(new CastTestCase("c_varchar_50", "char(50)", "c_char_50"))

                .add(new CastTestCase("c_char_50", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_boolean", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_smallint", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_int2", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_integer", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_int", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_int4", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_bigint", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_int8", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_real", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_float4", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_float", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_float8", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_double_precision", "varchar(50)", "c_varchar_50"))

                .add(new CastTestCase("c_varchar_10", "varchar", "c_varchar_65535"))
                .add(new CastTestCase("c_varchar_15_unicode", "varchar", "c_varchar_65535"))
                .add(new CastTestCase("c_nvarchar_15_unicode", "varchar", "c_varchar_65535"))
                .add(new CastTestCase("c_text", "varchar", "c_varchar_65535"))

                .add(new CastTestCase("c_timestamp", "varchar(50)", "c_varchar_50"))
                .add(new CastTestCase("c_date", "varchar(50)", "c_varchar_50"))

                .add(new CastTestCase("c_timestamp", "date", "c_date"))
                .add(new CastTestCase("c_timestamp", "time", "c_time"))
                .add(new CastTestCase("c_date", "timestamp", "c_timestamp"))
                .add(new CastTestCase("c_varchar_timestamp", "timestamp", "c_timestamp"))
                .add(new CastTestCase("c_varchar_timestamptz", "timestamp", "c_timestamp"))
                .build();
    }

    @Override
    protected List<InvalidCastTestCase> invalidCast()
    {
        return ImmutableList.<InvalidCastTestCase>builder()
                .add(new InvalidCastTestCase("c_varchar_decimal", "integer"))
                .add(new InvalidCastTestCase("c_varchar_decimal_sign", "integer"))
                .add(new InvalidCastTestCase("c_varchar_alpha_numeric", "integer"))
                .add(new InvalidCastTestCase("c_char_50", "integer"))
                .add(new InvalidCastTestCase("c_char_numeric", "integer"))
                .add(new InvalidCastTestCase("c_bpchar_numeric", "integer"))
                .add(new InvalidCastTestCase("c_nan_real", "integer"))
                .add(new InvalidCastTestCase("c_nan_double", "integer"))

                // c_timetz is not supported by default by trino. This is forced mapped to varchar.
                .add(new InvalidCastTestCase("c_timetz", "tinyint"))
                .add(new InvalidCastTestCase("c_timetz", "smallint"))
                .add(new InvalidCastTestCase("c_timetz", "int"))
                .add(new InvalidCastTestCase("c_timetz", "bigint"))
                .build();
    }

    @Test
    void testCastPushdownWithCharConvertedToVarchar()
    {
        try (TestTable table = newTrinoTable(
                TEST_SCHEMA + "." + "char_converted_to_varchar_",
                "(a char(4097))", // char(REDSHIFT_MAX_CHAR` + 1) in Trino is mapped to varchar(REDSHIFT_MAX_CHAR` + 1) in Redshift
                ImmutableList.of("'hello'"))) {
            assertThat(query("SELECT cast(a AS varchar(50)) FROM " + table.getName()))
                    .isFullyPushedDown();
            assertThat(query("SELECT cast(a AS char(50)) FROM " + table.getName()))
                    .isNotFullyPushedDown(ProjectNode.class);
        }
    }
}

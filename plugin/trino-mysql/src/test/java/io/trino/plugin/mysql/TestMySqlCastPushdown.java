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
package io.trino.plugin.mysql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.jdbc.BaseJdbcCastPushdownTest;
import io.trino.plugin.jdbc.CastDataTypeTestTable;
import io.trino.spi.type.DecimalType;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static io.trino.testing.MaterializedResult.resultBuilder;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

final class TestMySqlCastPushdown
        extends BaseJdbcCastPushdownTest
{
    private TestingMySqlServer mySqlServer;

    private CastDataTypeTestTable left;
    private CastDataTypeTestTable right;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        mySqlServer = closeAfterClass(new TestingMySqlServer());
        return MySqlQueryRunner.builder(mySqlServer)
                .addConnectorProperties(ImmutableMap.<String, String>builder()
                        .put("unsupported-type-handling", "CONVERT_TO_VARCHAR")
                        .put("join-pushdown.enabled", "true")
                        .put("join-pushdown.strategy", "EAGER")
                        .buildOrThrow())
                .build();
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return mySqlServer::execute;
    }

    @BeforeAll
    void setupTable()
    {
        left = closeAfterClass(CastDataTypeTestTable.create(3)
                .addColumn("id", "int", asList(11, 12, 13))
                .addColumn("c_boolean", "boolean", asList(true, false, null))
                .addColumn("c_int", "int", asList(1, 2, null))
                .addColumn("c_tinyint", "tinyint", asList(1, 2, null))
                .addColumn("c_smallint", "smallint", asList(1, 2, null))
                .addColumn("c_integer", "integer", asList(1, 2, null))
                .addColumn("c_bigint", "bigint", asList(1, 2, null))
                .addColumn("c_float", "float", asList(1.23, 2.67, null))
                .addColumn("c_real", "real", asList(1.23, 2.67, null))
                .addColumn("c_double_precision", "double precision", asList(1.23, 2.67, null))
                .addColumn("c_double_precision_negative", "double precision", asList(-1.23, -2.67, null))
                .addColumn("c_decimal_10_2", "decimal(10, 2)", asList(1.23, 2.67, null))
                .addColumn("c_numeric_30_2", "numeric(30, 2)", asList(1.23, 2.67, null))

                .addColumn("c_char_numeric", "char(50)", asList("'123'", "'124'", null))
                .addColumn("c_varchar_decimal", "varchar(50)", asList("'1.23'", "'2.34'", null))
                .addColumn("c_varchar_decimal_sign", "varchar(50)", asList("'+1.23'", "'-2.34'", null))
                .addColumn("c_varchar_alpha_numeric", "varchar(50)", asList("'H311o'", "'123Hey'", null))

                .execute(onRemoteDatabase(), "left_table_"));

        // 2nd row value is different in right than left
        right = closeAfterClass(CastDataTypeTestTable.create(3)
                .addColumn("id", "int", asList(21, 22, 23))
                .addColumn("c_boolean", "boolean", asList(true, true, null))
                .addColumn("c_int", "int", asList(1, 2, null))
                .addColumn("c_tinyint", "tinyint", asList(1, 2, null))
                .addColumn("c_smallint", "smallint", asList(1, 22, null))
                .addColumn("c_integer", "integer", asList(1, 22, null))
                .addColumn("c_bigint", "bigint", asList(1, 22, null))
                .addColumn("c_float", "float", asList(1.23, 22.67, null))
                .addColumn("c_real", "real", asList(1.23, 22.67, null))
                .addColumn("c_double_precision", "double precision", asList(1.23, 22.67, null))
                .addColumn("c_double_precision_negative", "double precision", asList(-1.23, -2.67, null))
                .addColumn("c_decimal_10_2", "decimal(10, 2)", asList(1.23, 22.67, null))
                .addColumn("c_numeric_30_2", "numeric(30, 2)", asList(1.23, 22.67, null))

                .addColumn("c_char_numeric", "char(50)", asList("'123'", "'125'", null))
                .addColumn("c_varchar_decimal", "varchar(50)", asList("'1.23'", "'22.34'", null))
                .addColumn("c_varchar_decimal_sign", "varchar(50)", asList("'+1.23'", "'-22.34'", null))
                .addColumn("c_varchar_alpha_numeric", "varchar(50)", asList("'H311o'", "'123Bye'", null))

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
    void testJoinPushdownWithNestedCast()
    {
        CastTestCase testCase = new CastTestCase("c_smallint", "double", "c_double_precision");
        assertThat(query("SELECT l.id FROM %s l LEFT JOIN %s r ON CAST(CAST(l.%s AS %s) AS double) = r.%s".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                .isFullyPushedDown();
    }

    @Test
    void testAllJoinPushdownWithCast()
    {
        CastTestCase testCase = new CastTestCase("c_tinyint", "double", "c_double_precision");
        assertThat(query("SELECT l.id FROM %s l LEFT JOIN %s r ON CAST(l.%s AS %s) = r.%s".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l RIGHT JOIN %s r ON CAST(l.%s AS %s) = r.%s".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l INNER JOIN %s r ON CAST(l.%s AS %s) = r.%s".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                .isFullyPushedDown();
        // Full Join pushdown is not supported
        assertThat(query("SELECT l.id FROM %s l FULL JOIN %s r ON CAST(l.%s AS %s) = r.%s".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                .joinIsNotFullyPushedDown();

        testCase = new CastTestCase("c_smallint", "double", "c_double_precision");
        assertThat(query("SELECT l.id FROM %s l LEFT JOIN %s r ON l.%s = CAST(r.%s AS %s)".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.targetColumn(), testCase.castType())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l RIGHT JOIN %s r ON l.%s = CAST(r.%s AS %s)".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.targetColumn(), testCase.castType())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l INNER JOIN %s r ON l.%s = CAST(r.%s AS %s)".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.targetColumn(), testCase.castType())))
                .isFullyPushedDown();
        // Full Join pushdown is not supported
        assertThat(query("SELECT l.id FROM %s l FULL JOIN %s r ON CAST(l.%s AS %s) = r.%s".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                .joinIsNotFullyPushedDown();

        testCase = new CastTestCase("c_integer", "double", "c_double_precision");
        assertThat(query("SELECT l.id FROM %s l LEFT JOIN %s r ON CAST(l.%3$s AS %4$s) = CAST(r.%5$s AS %4$s)".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l RIGHT JOIN %s r ON CAST(l.%3$s AS %4$s) = CAST(r.%5$s AS %4$s)".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l INNER JOIN %s r ON CAST(l.%3$s AS %4$s) = CAST(r.%5$s AS %4$s)".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                .isFullyPushedDown();
        // Full Join pushdown is not supported
        assertThat(query("SELECT l.id FROM %s l FULL JOIN %s r ON CAST(l.%s AS %s) = r.%s".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn())))
                .joinIsNotFullyPushedDown();
    }

    @Test
    void testCastPushdownDisabled()
    {
        Session sessionWithoutPushdown = Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "complex_expression_pushdown", "false")
                .build();
        assertThat(query(sessionWithoutPushdown, "SELECT CAST (c_int AS double) FROM %s".formatted(leftTable())))
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

                .addColumn("c_double_1", "double precision", List.of("-129.67"))
                .addColumn("c_double_2", "double precision", List.of("129.67"))
                .addColumn("c_double_3", "double precision", List.of("-65537.49"))
                .addColumn("c_double_4", "double precision", List.of("65537.49"))
                .addColumn("c_double_5", "double precision", List.of("-2147483649.49"))
                .addColumn("c_double_6", "double precision", List.of("2147483647.94"))
                .addColumn("c_double_7", "double precision", List.of("-92233720368547758810.94"))
                .addColumn("c_double_8", "double precision", List.of("92233720368547758810.49"))

                .execute(onRemoteDatabase(), "test_overflow_"));

        assertInvalidCast(
                table.getName(),
                ImmutableList.<InvalidCastTestCase>builder()
                        .add(new InvalidCastTestCase("c_smallint_1", "tinyint", "Out of range for tinyint: -129"))
                        .add(new InvalidCastTestCase("c_smallint_2", "tinyint", "Out of range for tinyint: 128"))
                        .add(new InvalidCastTestCase("c_int_1", "tinyint", "Out of range for tinyint: -65537"))
                        .add(new InvalidCastTestCase("c_int_2", "tinyint", "Out of range for tinyint: 65536"))
                        .add(new InvalidCastTestCase("c_bigint_1", "tinyint", "Out of range for tinyint: -2147483649"))
                        .add(new InvalidCastTestCase("c_bigint_2", "tinyint", "Out of range for tinyint: 2147483648"))

                        .add(new InvalidCastTestCase("c_int_1", "smallint", "Out of range for smallint: -65537"))
                        .add(new InvalidCastTestCase("c_int_2", "smallint", "Out of range for smallint: 65536"))
                        .add(new InvalidCastTestCase("c_bigint_1", "smallint", "Out of range for smallint: -2147483649"))
                        .add(new InvalidCastTestCase("c_bigint_2", "smallint", "Out of range for smallint: 2147483648"))
                        .add(new InvalidCastTestCase("c_bigint_1", "integer", "Out of range for integer: -2147483649"))
                        .add(new InvalidCastTestCase("c_bigint_2", "integer", "Out of range for integer: 2147483648"))

                        // No pushdown for tinyint type
                        .add(new InvalidCastTestCase("c_decimal_1", "tinyint", "Cannot cast '-129.49' to TINYINT"))
                        .add(new InvalidCastTestCase("c_decimal_2", "tinyint", "Cannot cast '-128.94' to TINYINT"))
                        .add(new InvalidCastTestCase("c_decimal_3", "tinyint", "Cannot cast '127.94' to TINYINT"))
                        .add(new InvalidCastTestCase("c_decimal_4", "tinyint", "Cannot cast '128.49' to TINYINT"))

                        .add(new InvalidCastTestCase("c_decimal_5", "smallint", "Cannot cast '-65537.49' to SMALLINT"))
                        .add(new InvalidCastTestCase("c_decimal_6", "smallint", "Cannot cast '-65536.94' to SMALLINT"))
                        .add(new InvalidCastTestCase("c_decimal_7", "smallint", "Cannot cast '65535.94' to SMALLINT"))
                        .add(new InvalidCastTestCase("c_decimal_8", "smallint", "Cannot cast '65536.49' to SMALLINT"))
                        .add(new InvalidCastTestCase("c_decimal_9", "integer", "Cannot cast '-2147483649.49' to INTEGER"))
                        .add(new InvalidCastTestCase("c_decimal_10", "integer", "Cannot cast '-2147483648.94' to INTEGER"))
                        .add(new InvalidCastTestCase("c_decimal_11", "integer", "Cannot cast '2147483647.94' to INTEGER"))
                        .add(new InvalidCastTestCase("c_decimal_12", "integer", "Cannot cast '2147483648.49' to INTEGER"))
                        .add(new InvalidCastTestCase("c_decimal_13", "bigint", "Cannot cast '-9223372036854775809.49' to BIGINT"))
                        .add(new InvalidCastTestCase("c_decimal_14", "bigint", "Cannot cast '-9223372036854775808.94' to BIGINT"))
                        .add(new InvalidCastTestCase("c_decimal_15", "bigint", "Cannot cast '9223372036854775807.94' to BIGINT"))
                        .add(new InvalidCastTestCase("c_decimal_16", "bigint", "Cannot cast '9223372036854775808.49' to BIGINT"))

                        .add(new InvalidCastTestCase("c_double_1", "tinyint", "Out of range for tinyint: -129.67"))
                        .add(new InvalidCastTestCase("c_double_2", "tinyint", "Out of range for tinyint: 129.67"))
                        .add(new InvalidCastTestCase("c_double_3", "smallint", "Out of range for smallint: -65537.49"))
                        .add(new InvalidCastTestCase("c_double_4", "smallint", "Out of range for smallint: 65537.49"))
                        .add(new InvalidCastTestCase("c_double_5", "integer", "Out of range for integer: -2.14748364949E9"))
                        .add(new InvalidCastTestCase("c_double_6", "integer", "Out of range for integer: 2.14748364794E9"))
                        .add(new InvalidCastTestCase("c_double_7", "bigint", "Unable to cast -9.223372036854776E19 to bigint"))
                        .add(new InvalidCastTestCase("c_double_8", "bigint", "Unable to cast 9.223372036854776E19 to bigint"))
                        .build());
    }

    @Test
    void testCastsWithRoundingTruncationAndLowerPrecisionScale()
    {
        CastDataTypeTestTable table = closeAfterClass(CastDataTypeTestTable.create(1)
                .addColumn("id", "int", List.of(1))
                .addColumn("c_decimal_7_2", "decimal(7, 2)", List.of("12345.67"))
                .addColumn("c_decimal_7_4", "decimal(7, 4)", List.of("123.2337"))
                .execute(onRemoteDatabase(), "test_casts_rounding_truncation_precision_"));

        // Mysql allows casts from higher precision and scale to lower precision and scale
        // Mysql performs rounding and truncation as part of casting
        List<CastTestCase> castsWithLowerPrecisionAndScale = ImmutableList.<CastTestCase>builder()
                .add(new CastTestCase("c_decimal_7_4", "decimal(7,1)", "c_decimal_7_1"))
                .add(new CastTestCase("c_decimal_7_4", "decimal(6,4)", "c_decimal_6_4"))
                .add(new CastTestCase("c_decimal_7_4", "decimal(6,3)", "c_decimal_6_3"))
                .add(new CastTestCase("c_decimal_7_4", "decimal(2,2)", "c_decimal_2_2"))
                .add(new CastTestCase("c_decimal_7_4", "decimal(1,1)", "c_decimal_1_1"))
                .add(new CastTestCase("c_decimal_7_4", "decimal(1,0)", "c_decimal_1_0"))
                .build();

        for (CastTestCase testCase : castsWithLowerPrecisionAndScale) {
            assertThat(query("SELECT CAST(%s AS %s) FROM %s".formatted(testCase.sourceColumn(), testCase.castType(), table.getName())))
                    .succeeds();
        }

        // Rounding
        // Casting decimal(7, 4) column with value 123.2337 to decimal(6,3), MySQL rounds up to 123.234
        assertThat(query("SELECT CAST(c_decimal_7_4 AS decimal(6,3)) FROM " + table.getName()))
                .result()
                .matches(resultBuilder(getSession(), DecimalType.createDecimalType(6, 3))
                        .row(new BigDecimal(new char[] {'1', '2', '3', '.', '2', '3', '4'}))
                        .build());

        // Casting decimal(7, 4) column with value 123.2337 to decimal(5,2), MySQL rounds to 123.23
        assertThat(query("SELECT CAST(c_decimal_7_4 AS decimal(5, 2)) FROM " + table.getName()))
                .result()
                .matches(resultBuilder(getSession(), DecimalType.createDecimalType(5, 2))
                        .row(new BigDecimal(new char[] {'1', '2', '3', '.', '2', '3'}))
                        .build());

        // Tests showing truncation and loss of precision and scale during casting in MySQL
        // Casting decimal(7, 4) column with value 123.2337 to decimal(7,0), the truncated result is 123
        assertThat(query("SELECT CAST(c_decimal_7_4 AS decimal(7,0)) FROM " + table.getName()))
                .result()
                .matches(resultBuilder(getSession(), DecimalType.createDecimalType(7, 0))
                        .row(new BigDecimal(new char[] {'1', '2', '3'}))
                        .build());

        // Casting decimal(7, 2) column with value 12345.67 to decimal(5,2), the truncated result is 999.99
        // since 999.99 is the closest decimal(5, 2) value to 12345.67
        assertThat(query("SELECT CAST(c_decimal_7_2 AS decimal(5,2)) FROM " + table.getName()))
                .result()
                .matches(resultBuilder(getSession(), DecimalType.createDecimalType(5, 2))
                        .row(new BigDecimal(new char[] {'9', '9', '9', '.', '9', '9'}))
                        .build());

        // Casting decimal(7, 4) column with value 123.2337 to decimal(2,2), the result is 0.99
        assertThat(query("SELECT CAST(c_decimal_7_4 AS decimal(2, 2)) FROM " + table.getName()))
                .result()
                .matches(resultBuilder(getSession(), DecimalType.createDecimalType(2, 2))
                        .row(new BigDecimal(new char[] {'0', '.', '9', '9'}))
                        .build());

        // When casting to decimal(M,D), M must be >= D
        assertThat(query("SELECT CAST(c_decimal_7_4 AS decimal(2,4)) FROM " + table.getName()))
                .failure()
                .hasMessageContaining("DECIMAL scale must be in range");
    }

    @Override
    protected List<CastTestCase> supportedCastTypePushdown()
    {
        return ImmutableList.<CastTestCase>builder()
                // Cast from all types (boolean, tinyint, smallint, integer, bigint, float, real, double, decimal, numeric) to (tinyint) using cast type 'double'
                .add(new CastTestCase("c_boolean", "double", "c_tinyint"))
                .add(new CastTestCase("c_tinyint", "double", "c_tinyint"))
                .add(new CastTestCase("c_smallint", "double", "c_tinyint"))
                .add(new CastTestCase("c_integer", "double", "c_tinyint"))
                .add(new CastTestCase("c_bigint", "double", "c_tinyint"))
                .add(new CastTestCase("c_float", "double", "c_tinyint"))
                .add(new CastTestCase("c_real", "double", "c_tinyint"))
                .add(new CastTestCase("c_double_precision", "double", "c_tinyint"))
                .add(new CastTestCase("c_double_precision_negative", "double", "c_tinyint"))
                .add(new CastTestCase("c_decimal_10_2", "double", "c_tinyint"))
                .add(new CastTestCase("c_numeric_30_2", "double", "c_tinyint"))

                // Cast from all types (boolean, tinyint, smallint, integer, bigint, float, real, double, decimal, numeric) to (smallint) using cast type 'decimal'
                .add(new CastTestCase("c_boolean", "decimal", "c_smallint"))
                .add(new CastTestCase("c_tinyint", "decimal", "c_smallint"))
                .add(new CastTestCase("c_smallint", "decimal", "c_smallint"))
                .add(new CastTestCase("c_integer", "decimal", "c_smallint"))
                .add(new CastTestCase("c_bigint", "decimal", "c_smallint"))
                .add(new CastTestCase("c_float", "decimal", "c_smallint"))
                .add(new CastTestCase("c_real", "decimal", "c_smallint"))
                .add(new CastTestCase("c_double_precision", "decimal", "c_smallint"))
                .add(new CastTestCase("c_double_precision_negative", "decimal", "c_smallint"))
                .add(new CastTestCase("c_decimal_10_2", "decimal", "c_smallint"))
                .add(new CastTestCase("c_numeric_30_2", "decimal", "c_smallint"))

                // Cast from all types (boolean, tinyint, smallint, integer, bigint, float, real, double, decimal, numeric) to (integer) using cast type 'decimal'
                .add(new CastTestCase("c_boolean", "decimal", "c_integer"))
                .add(new CastTestCase("c_tinyint", "decimal", "c_integer"))
                .add(new CastTestCase("c_smallint", "decimal", "c_integer"))
                .add(new CastTestCase("c_integer", "decimal", "c_integer"))
                .add(new CastTestCase("c_bigint", "decimal", "c_integer"))
                .add(new CastTestCase("c_float", "decimal", "c_integer"))
                .add(new CastTestCase("c_real", "decimal", "c_integer"))
                .add(new CastTestCase("c_double_precision", "decimal", "c_integer"))
                .add(new CastTestCase("c_double_precision_negative", "decimal", "c_integer"))
                .add(new CastTestCase("c_decimal_10_2", "decimal", "c_integer"))
                .add(new CastTestCase("c_numeric_30_2", "decimal", "c_integer"))

                // Cast from all types (boolean, tinyint, smallint, integer, bigint, float, real, double, decimal, numeric) to (bigint) using cast type 'double'
                .add(new CastTestCase("c_boolean", "double", "c_bigint"))
                .add(new CastTestCase("c_tinyint", "double", "c_bigint"))
                .add(new CastTestCase("c_smallint", "double", "c_bigint"))
                .add(new CastTestCase("c_integer", "double", "c_bigint"))
                .add(new CastTestCase("c_bigint", "double", "c_bigint"))
                .add(new CastTestCase("c_float", "double", "c_bigint"))
                .add(new CastTestCase("c_real", "double", "c_bigint"))
                .add(new CastTestCase("c_double_precision", "double", "c_bigint"))
                .add(new CastTestCase("c_double_precision_negative", "double", "c_bigint"))
                .add(new CastTestCase("c_decimal_10_2", "double", "c_bigint"))
                .add(new CastTestCase("c_numeric_30_2", "double", "c_bigint"))

                // Cast from all types (boolean, tinyint, smallint, integer, bigint, float, real, double, decimal, numeric) to (float) using cast type 'double'
                .add(new CastTestCase("c_boolean", "double", "c_float"))
                .add(new CastTestCase("c_tinyint", "double", "c_float"))
                .add(new CastTestCase("c_smallint", "double", "c_float"))
                .add(new CastTestCase("c_integer", "double", "c_float"))
                .add(new CastTestCase("c_bigint", "double", "c_float"))
                .add(new CastTestCase("c_float", "double", "c_float"))
                .add(new CastTestCase("c_real", "double", "c_float"))
                .add(new CastTestCase("c_double_precision", "double", "c_float"))
                .add(new CastTestCase("c_double_precision_negative", "double", "c_float"))
                .add(new CastTestCase("c_decimal_10_2", "double", "c_float"))
                .add(new CastTestCase("c_numeric_30_2", "double", "c_float"))

                // Cast from all types (boolean, tinyint, smallint, integer, bigint, float, real, double, decimal, numeric) to (double) using cast type 'decimal'
                .add(new CastTestCase("c_boolean", "decimal", "c_double_precision"))
                .add(new CastTestCase("c_tinyint", "decimal", "c_double_precision"))
                .add(new CastTestCase("c_smallint", "decimal", "c_double_precision"))
                .add(new CastTestCase("c_integer", "decimal", "c_double_precision"))
                .add(new CastTestCase("c_bigint", "decimal", "c_double_precision"))
                .add(new CastTestCase("c_float", "decimal", "c_double_precision"))
                .add(new CastTestCase("c_real", "decimal", "c_double_precision"))
                .add(new CastTestCase("c_double_precision", "decimal", "c_double_precision"))
                .add(new CastTestCase("c_double_precision_negative", "decimal", "c_double_precision"))
                .add(new CastTestCase("c_decimal_10_2", "decimal", "c_double_precision"))
                .add(new CastTestCase("c_numeric_30_2", "decimal", "c_double_precision"))

                // Cast from all types (boolean, tinyint, smallint, integer, bigint, float, real, double, decimal, numeric) to (numeric) using cast type 'decimal'
                .add(new CastTestCase("c_boolean", "decimal", "c_numeric_30_2"))
                .add(new CastTestCase("c_tinyint", "decimal", "c_numeric_30_2"))
                .add(new CastTestCase("c_smallint", "decimal", "c_numeric_30_2"))
                .add(new CastTestCase("c_integer", "decimal", "c_numeric_30_2"))
                .add(new CastTestCase("c_bigint", "decimal", "c_numeric_30_2"))
                .add(new CastTestCase("c_float", "decimal", "c_numeric_30_2"))
                .add(new CastTestCase("c_real", "decimal", "c_numeric_30_2"))
                .add(new CastTestCase("c_double_precision", "decimal", "c_numeric_30_2"))
                .add(new CastTestCase("c_double_precision_negative", "decimal", "c_numeric_30_2"))
                .add(new CastTestCase("c_decimal_10_2", "decimal", "c_numeric_30_2"))
                .add(new CastTestCase("c_numeric_30_2", "decimal", "c_numeric_30_2"))

                // Cast from all types (boolean, tinyint, smallint, integer, bigint, float, real, double, decimal, numeric) to (decimal) using cast type 'double'
                .add(new CastTestCase("c_boolean", "double", "c_decimal_10_2"))
                .add(new CastTestCase("c_tinyint", "double", "c_decimal_10_2"))
                .add(new CastTestCase("c_smallint", "double", "c_decimal_10_2"))
                .add(new CastTestCase("c_integer", "double", "c_decimal_10_2"))
                .add(new CastTestCase("c_bigint", "double", "c_decimal_10_2"))
                .add(new CastTestCase("c_float", "double", "c_decimal_10_2"))
                .add(new CastTestCase("c_real", "double", "c_decimal_10_2"))
                .add(new CastTestCase("c_double_precision", "double", "c_decimal_10_2"))
                .add(new CastTestCase("c_double_precision_negative", "double", "c_decimal_10_2"))
                .add(new CastTestCase("c_decimal_10_2", "double", "c_decimal_10_2"))
                .add(new CastTestCase("c_numeric_30_2", "double", "c_decimal_10_2"))

                // Cast from all types (boolean, tinyint, smallint, integer, bigint, float, real, double, decimal, numeric) to (real) using cast type 'decimal'
                .add(new CastTestCase("c_boolean", "decimal", "c_real"))
                .add(new CastTestCase("c_tinyint", "decimal", "c_real"))
                .add(new CastTestCase("c_smallint", "decimal", "c_real"))
                .add(new CastTestCase("c_integer", "decimal", "c_real"))
                .add(new CastTestCase("c_bigint", "decimal", "c_real"))
                .add(new CastTestCase("c_float", "decimal", "c_real"))
                .add(new CastTestCase("c_real", "decimal", "c_real"))
                .add(new CastTestCase("c_double_precision", "decimal", "c_real"))
                .add(new CastTestCase("c_double_precision_negative", "decimal", "c_real"))
                .add(new CastTestCase("c_decimal_10_2", "decimal", "c_real"))
                .add(new CastTestCase("c_numeric_30_2", "decimal", "c_real"))

                // Self-casts are allowed with cast types tinyint, smallint, integer, bigint, real
                .add(new CastTestCase("c_tinyint", "tinyint", "c_tinyint"))
                .add(new CastTestCase("c_smallint", "smallint", "c_smallint"))
                .add(new CastTestCase("c_integer", "integer", "c_integer"))
                .add(new CastTestCase("c_bigint", "bigint", "c_bigint"))
                .add(new CastTestCase("c_real", "real", "c_real"))
                .add(new CastTestCase("c_double_precision", "double", "c_double_precision"))
                .add(new CastTestCase("c_double_precision_negative", "double", "c_double_precision_negative"))
                .add(new CastTestCase("c_decimal_10_2", "decimal", "c_decimal_10_2"))
                .build();
    }

    @Override
    protected List<CastTestCase> unsupportedCastTypePushdown()
    {
        return ImmutableList.<CastTestCase>builder()
                // Using cast type 'tinyint' is not supported in mysql, irrespective of source and target columns
                .add(new CastTestCase("c_smallint", "tinyint", "c_tinyint"))
                .add(new CastTestCase("c_integer", "tinyint", "c_tinyint"))
                .add(new CastTestCase("c_bigint", "tinyint", "c_tinyint"))
                .add(new CastTestCase("c_float", "tinyint", "c_double_precision"))
                .add(new CastTestCase("c_real", "tinyint", "c_double_precision"))
                .add(new CastTestCase("c_double_precision", "tinyint", "c_double_precision"))
                .add(new CastTestCase("c_double_precision_negative", "tinyint", "c_double_precision"))
                .add(new CastTestCase("c_decimal_10_2", "tinyint", "c_tinyint"))
                .add(new CastTestCase("c_numeric_30_2", "tinyint", "c_tinyint"))

                // Using cast type 'smallint' is not supported in mysql, irrespective of source and target columns
                .add(new CastTestCase("c_boolean", "smallint", "c_smallint"))
                .add(new CastTestCase("c_tinyint", "smallint", "c_smallint"))
                .add(new CastTestCase("c_integer", "smallint", "c_smallint"))
                .add(new CastTestCase("c_bigint", "smallint", "c_smallint"))
                .add(new CastTestCase("c_float", "smallint", "c_double_precision"))
                .add(new CastTestCase("c_real", "smallint", "c_double_precision"))
                .add(new CastTestCase("c_double_precision", "smallint", "c_double_precision"))
                .add(new CastTestCase("c_double_precision_negative", "smallint", "c_double_precision"))
                .add(new CastTestCase("c_decimal_10_2", "smallint", "c_smallint"))
                .add(new CastTestCase("c_numeric_30_2", "smallint", "c_smallint"))

                // Using cast type 'integer' is not supported in mysql, irrespective of source and target columns
                .add(new CastTestCase("c_boolean", "integer", "c_integer"))
                .add(new CastTestCase("c_tinyint", "integer", "c_integer"))
                .add(new CastTestCase("c_smallint", "integer", "c_integer"))
                .add(new CastTestCase("c_bigint", "integer", "c_integer"))
                .add(new CastTestCase("c_float", "integer", "c_double_precision"))
                .add(new CastTestCase("c_real", "integer", "c_double_precision"))
                .add(new CastTestCase("c_double_precision", "integer", "c_double_precision"))
                .add(new CastTestCase("c_double_precision_negative", "integer", "c_double_precision"))
                .add(new CastTestCase("c_decimal_10_2", "integer", "c_integer"))
                .add(new CastTestCase("c_numeric_30_2", "integer", "c_integer"))

                // Using cast type 'bigint' is not supported in mysql, irrespective of source and target columns
                .add(new CastTestCase("c_boolean", "bigint", "c_bigint"))
                .add(new CastTestCase("c_tinyint", "bigint", "c_bigint"))
                .add(new CastTestCase("c_smallint", "bigint", "c_bigint"))
                .add(new CastTestCase("c_integer", "bigint", "c_bigint"))
                .add(new CastTestCase("c_float", "bigint", "c_double_precision"))
                .add(new CastTestCase("c_real", "bigint", "c_double_precision"))
                .add(new CastTestCase("c_double_precision", "bigint", "c_double_precision"))
                .add(new CastTestCase("c_double_precision_negative", "bigint", "c_double_precision"))
                .add(new CastTestCase("c_decimal_10_2", "bigint", "c_bigint"))
                .add(new CastTestCase("c_numeric_30_2", "bigint", "c_bigint"))
                .build();
    }

    @Override
    protected List<InvalidCastTestCase> invalidCast()
    {
        return ImmutableList.<InvalidCastTestCase>builder()
                .add(new InvalidCastTestCase("c_varchar_decimal", "integer"))
                .add(new InvalidCastTestCase("c_varchar_decimal_sign", "integer"))
                .add(new InvalidCastTestCase("c_varchar_alpha_numeric", "integer"))
                .add(new InvalidCastTestCase("c_char_numeric", "integer"))
                .build();
    }
}

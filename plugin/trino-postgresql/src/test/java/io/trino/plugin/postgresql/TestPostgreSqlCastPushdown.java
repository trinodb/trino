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
package io.trino.plugin.postgresql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.jdbc.BaseJdbcCastPushdownTest;
import io.trino.plugin.jdbc.CastDataTypeTestTable;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

final class TestPostgreSqlCastPushdown
        extends BaseJdbcCastPushdownTest
{
    private TestingPostgreSqlServer postgreSqlServer;

    private CastDataTypeTestTable left;
    private CastDataTypeTestTable right;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        postgreSqlServer = closeAfterClass(new TestingPostgreSqlServer());
        return PostgreSqlQueryRunner.builder(postgreSqlServer)
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
        return postgreSqlServer::execute;
    }

    @BeforeAll
    void setupTable()
    {
        left = closeAfterClass(CastDataTypeTestTable.create(3)
                .addColumn("id", "int", asList(11, 12, 13))
                .addColumn("c_boolean", "boolean", asList(true, false, null))
                .addColumn("c_smallint", "smallint", asList(1, 2, null))
                .addColumn("c_smallint_negative", "smallint", asList(-1, -2, null))
                .addColumn("c_int2", "int2", asList(1, 2, null))
                .addColumn("c_integer", "integer", asList(1, 2, null))
                .addColumn("c_integer_negative", "integer", asList(1, 2, null))
                .addColumn("c_int", "int", asList(1, 2, null))
                .addColumn("c_int4", "int4", asList(1, 2, null))
                .addColumn("c_bigint", "bigint", asList(1, 2, null))
                .addColumn("c_bigint_negative", "bigint", asList(-1, -2, null))
                .addColumn("c_int8", "int8", asList(1, 2, null))
                .addColumn("c_real", "real", asList(1.23, 2.67, null))
                .addColumn("c_float4", "float4", asList(1.23, 2.67, null))
                .addColumn("c_double_precision", "double precision", asList(1.23, 2.67, null))
                .addColumn("c_double_precision_negative", "double precision", asList(-1.23, -2.67, null))
                .addColumn("c_float", "float", asList(1.23, 2.67, null))
                .addColumn("c_float8", "float8", asList(1.23, 2.67, null))
                .addColumn("c_decimal_10_2", "decimal(10, 2)", asList(1.23, 2.67, null))
                .addColumn("c_decimal_19_2", "decimal(19, 2)", asList(1.23, 2.67, null))
                .addColumn("c_decimal_30_2", "decimal(30, 2)", asList(1.23, 2.67, null))
                .addColumn("c_numeric_10_2", "numeric(10, 2)", asList(1.23, 2.67, null))
                .addColumn("c_numeric_19_2", "numeric(19, 2)", asList(1.23, 2.67, null))
                .addColumn("c_numeric_30_2", "numeric(30, 2)", asList(1.23, 2.67, null))

                .addColumn("c_nan_real", "real", asList("'Nan'", "'-Nan'", null))
                .addColumn("c_nan_double", "double precision", asList("'Nan'", "'-Nan'", null))
                .addColumn("c_infinity_real", "real", asList("'Infinity'", "'-Infinity'", null))
                .addColumn("c_infinity_double", "double precision", asList("'Infinity'", "'-Infinity'", null))
                .addColumn("c_decimal_negative", "decimal(19, 2)", asList(-1.23, -2.67, null))
                .addColumn("c_numeric_negative", "numeric(19, 2)", asList(-1.23, -2.67, null))
                .addColumn("c_varchar_numeric", "varchar(50)", asList("'123'", "'124'", null))
                .addColumn("c_char_numeric", "char(50)", asList("'123'", "'124'", null))
                .addColumn("c_bpchar_numeric", "bpchar", asList("'123'", "'124'", null))
                .addColumn("c_text_numeric", "text", asList("'123'", "'124'", null))
                .addColumn("c_varchar_numeric_sign", "varchar(50)", asList("'+123'", "'-124'", null))
                .addColumn("c_varchar_decimal", "varchar(50)", asList("'1.23'", "'2.34'", null))
                .addColumn("c_varchar_decimal_sign", "varchar(50)", asList("'+1.23'", "'-2.34'", null))
                .addColumn("c_varchar_alpha_numeric", "varchar(50)", asList("'H311o'", "'123Hey'", null))

                .addColumn("c_small_serial", "smallserial", asList("SMALLINT '-1'", "SMALLINT '1'", "SMALLINT '2'")) // smallserial don't accept NULLS
                .addColumn("c_serial", "serial", asList("-1", "1", "2")) // serial don't accept NULLS
                .addColumn("c_big_serial", "bigserial", asList("-1", "1", "2")) // bigserial don't accept NULLS

                // unsupported in trino
                .addColumn("c_xml", "xml", asList("'<order><id>321</id><customer>John Doe</customer></order>'", "'<key>111</key>'", null))
                .execute(onRemoteDatabase(), "left_table_"));

        // 2nd row value is different in right than left
        right = closeAfterClass(CastDataTypeTestTable.create(3)
                .addColumn("id", "int", asList(21, 22, 23))
                .addColumn("c_boolean", "boolean", asList(true, true, null))
                .addColumn("c_smallint", "smallint", asList(1, 22, null))
                .addColumn("c_smallint_negative", "smallint", asList(-1, -2, null))
                .addColumn("c_int2", "int2", asList(1, 22, null))
                .addColumn("c_integer", "integer", asList(1, 22, null))
                .addColumn("c_integer_negative", "integer", asList(1, 2, null))
                .addColumn("c_int", "int", asList(1, 22, null))
                .addColumn("c_int4", "int4", asList(1, 22, null))
                .addColumn("c_bigint", "bigint", asList(1, 22, null))
                .addColumn("c_bigint_negative", "bigint", asList(-1, -2, null))
                .addColumn("c_int8", "int8", asList(1, 22, null))
                .addColumn("c_real", "real", asList(1.23, 22.67, null))
                .addColumn("c_float4", "float4", asList(1.23, 22.67, null))
                .addColumn("c_double_precision", "double precision", asList(1.23, 22.67, null))
                .addColumn("c_double_precision_negative", "double precision", asList(-1.23, -2.67, null))
                .addColumn("c_float", "float", asList(1.23, 22.67, null))
                .addColumn("c_float8", "float8", asList(1.23, 22.67, null))
                .addColumn("c_decimal_10_2", "decimal(10, 2)", asList(1.23, 22.67, null))
                .addColumn("c_decimal_19_2", "decimal(19, 2)", asList(1.23, 22.67, null))
                .addColumn("c_decimal_30_2", "decimal(30, 2)", asList(1.23, 22.67, null))
                .addColumn("c_numeric_10_2", "numeric(10, 2)", asList(1.23, 22.67, null))
                .addColumn("c_numeric_19_2", "numeric(19, 2)", asList(1.23, 22.67, null))
                .addColumn("c_numeric_30_2", "numeric(30, 2)", asList(1.23, 22.67, null))

                .addColumn("c_nan_real", "real", asList("'Nan'", "'-Nan'", null))
                .addColumn("c_nan_double", "double precision", asList("'Nan'", "'-Nan'", null))
                .addColumn("c_infinity_real", "real", asList("'Infinity'", "'-Infinity'", null))
                .addColumn("c_infinity_double", "double precision", asList("'Infinity'", "'-Infinity'", null))
                .addColumn("c_decimal_negative", "decimal(19, 2)", asList(-1.23, -22.67, null))
                .addColumn("c_numeric_negative", "numeric(19, 2)", asList(-1.23, -2.67, null))
                .addColumn("c_varchar_numeric", "varchar(50)", asList("'123'", "'228'", null))
                .addColumn("c_char_numeric", "char(50)", asList("'123'", "'125'", null))
                .addColumn("c_bpchar_numeric", "bpchar", asList("'123'", "'125'", null))
                .addColumn("c_text_numeric", "text", asList("'123'", "'125'", null))
                .addColumn("c_varchar_numeric_sign", "varchar(50)", asList("'+123'", "'-125'", null))
                .addColumn("c_varchar_decimal", "varchar(50)", asList("'1.23'", "'22.34'", null))
                .addColumn("c_varchar_decimal_sign", "varchar(50)", asList("'+1.23'", "'-22.34'", null))
                .addColumn("c_varchar_alpha_numeric", "varchar(50)", asList("'H311o'", "'123Bye'", null))

                .addColumn("c_small_serial", "smallserial", asList("SMALLINT '-1'", "SMALLINT '1'", "SMALLINT '2'")) // smallserial don't accept NULLS
                .addColumn("c_serial", "serial", asList("-1", "1", "2")) // serial don't accept NULLS
                .addColumn("c_big_serial", "bigserial", asList("-1", "1", "2")) // bigserial don't accept NULLS

                // unsupported in trino
                .addColumn("c_xml", "xml", asList("'<order><id>123</id><customer>John Doe</customer></order>'", "'<key>111</key>'", null))
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

                .addColumn("c_small_serial_1", "smallserial", List.of("SMALLINT '-32768'"))
                .addColumn("c_small_serial_2", "smallserial", List.of("SMALLINT '32767'"))
                .addColumn("c_serial_1", "serial", List.of("-2147483648"))
                .addColumn("c_serial_2", "serial", List.of("2147483647"))
                .addColumn("c_big_serial_1", "bigserial", List.of("-9223372036854775808"))
                .addColumn("c_big_serial_2", "bigserial", List.of("9223372036854775807"))
                .execute(onRemoteDatabase(), "test_overflow_"));

        assertInvalidCast(
                table.getName(),
                ImmutableList.<InvalidCastTestCase>builder()
                        // Not pushdown for tinyint type
                        .add(new InvalidCastTestCase("c_smallint_1", "tinyint", "Out of range for tinyint: -129"))
                        .add(new InvalidCastTestCase("c_smallint_2", "tinyint", "Out of range for tinyint: 128"))
                        .add(new InvalidCastTestCase("c_small_serial_1", "tinyint", "Out of range for tinyint: -32768"))
                        .add(new InvalidCastTestCase("c_small_serial_2", "tinyint", "Out of range for tinyint: 32767"))
                        .add(new InvalidCastTestCase("c_int_1", "tinyint", "Out of range for tinyint: -65537"))
                        .add(new InvalidCastTestCase("c_int_2", "tinyint", "Out of range for tinyint: 65536"))
                        .add(new InvalidCastTestCase("c_serial_1", "tinyint", "Out of range for tinyint: -2147483648"))
                        .add(new InvalidCastTestCase("c_serial_2", "tinyint", "Out of range for tinyint: 2147483647"))
                        .add(new InvalidCastTestCase("c_bigint_1", "tinyint", "Out of range for tinyint: -2147483649"))
                        .add(new InvalidCastTestCase("c_bigint_2", "tinyint", "Out of range for tinyint: 2147483648"))
                        .add(new InvalidCastTestCase("c_big_serial_1", "tinyint", "Out of range for tinyint: -9223372036854775808"))
                        .add(new InvalidCastTestCase("c_big_serial_2", "tinyint", "Out of range for tinyint: 9223372036854775807"))

                        .add(new InvalidCastTestCase("c_int_1", "smallint", "Out of range for smallint: -65537", "ERROR: smallint out of range"))
                        .add(new InvalidCastTestCase("c_int_2", "smallint", "Out of range for smallint: 65536", "ERROR: smallint out of range"))
                        .add(new InvalidCastTestCase("c_serial_1", "smallint", "Out of range for smallint: -2147483648", "ERROR: smallint out of range"))
                        .add(new InvalidCastTestCase("c_serial_2", "smallint", "Out of range for smallint: 2147483647", "ERROR: smallint out of range"))
                        .add(new InvalidCastTestCase("c_bigint_1", "smallint", "Out of range for smallint: -2147483649", "ERROR: smallint out of range"))
                        .add(new InvalidCastTestCase("c_bigint_2", "smallint", "Out of range for smallint: 2147483648", "ERROR: smallint out of range"))
                        .add(new InvalidCastTestCase("c_bigint_1", "integer", "Out of range for integer: -2147483649", "ERROR: integer out of range"))
                        .add(new InvalidCastTestCase("c_bigint_2", "integer", "Out of range for integer: 2147483648", "ERROR: integer out of range"))
                        .add(new InvalidCastTestCase("c_big_serial_1", "integer", "Out of range for integer: -9223372036854775808", "ERROR: integer out of range"))
                        .add(new InvalidCastTestCase("c_big_serial_2", "integer", "Out of range for integer: 9223372036854775807", "ERROR: integer out of range"))

                        // Not pushdown for tinyint type
                        .add(new InvalidCastTestCase("c_decimal_1", "tinyint", "Cannot cast '-129.49' to TINYINT"))
                        .add(new InvalidCastTestCase("c_decimal_2", "tinyint", "Cannot cast '-128.94' to TINYINT"))
                        .add(new InvalidCastTestCase("c_decimal_3", "tinyint", "Cannot cast '127.94' to TINYINT"))
                        .add(new InvalidCastTestCase("c_decimal_4", "tinyint", "Cannot cast '128.49' to TINYINT"))

                        .add(new InvalidCastTestCase("c_decimal_5", "smallint", "Cannot cast '-65537.49' to SMALLINT", "ERROR: smallint out of range"))
                        .add(new InvalidCastTestCase("c_decimal_6", "smallint", "Cannot cast '-65536.94' to SMALLINT", "ERROR: smallint out of range"))
                        .add(new InvalidCastTestCase("c_decimal_7", "smallint", "Cannot cast '65535.94' to SMALLINT", "ERROR: smallint out of range"))
                        .add(new InvalidCastTestCase("c_decimal_8", "smallint", "Cannot cast '65536.49' to SMALLINT", "ERROR: smallint out of range"))
                        .add(new InvalidCastTestCase("c_decimal_9", "integer", "Cannot cast '-2147483649.49' to INTEGER", "ERROR: integer out of range"))
                        .add(new InvalidCastTestCase("c_decimal_10", "integer", "Cannot cast '-2147483648.94' to INTEGER", "ERROR: integer out of range"))
                        .add(new InvalidCastTestCase("c_decimal_11", "integer", "Cannot cast '2147483647.94' to INTEGER", "ERROR: integer out of range"))
                        .add(new InvalidCastTestCase("c_decimal_12", "integer", "Cannot cast '2147483648.49' to INTEGER", "ERROR: integer out of range"))
                        .add(new InvalidCastTestCase("c_decimal_13", "bigint", "Cannot cast '-9223372036854775809.49' to BIGINT", "ERROR: bigint out of range"))
                        .add(new InvalidCastTestCase("c_decimal_14", "bigint", "Cannot cast '-9223372036854775808.94' to BIGINT", "ERROR: bigint out of range"))
                        .add(new InvalidCastTestCase("c_decimal_15", "bigint", "Cannot cast '9223372036854775807.94' to BIGINT", "ERROR: bigint out of range"))
                        .add(new InvalidCastTestCase("c_decimal_16", "bigint", "Cannot cast '9223372036854775808.49' to BIGINT", "ERROR: bigint out of range"))

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
        assertThat(query("SELECT CAST(c_xml AS VARCHAR(20)) FROM %s".formatted(leftTable())))
                .isNotFullyPushedDown(ProjectNode.class);
        assertThat(query("SELECT CAST(c_xml AS VARCHAR(20)) FROM %s".formatted(leftTable())))
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
                .add(new CastTestCase("c_small_serial", "smallint", "c_smallint"))

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
                .add(new CastTestCase("c_serial", "integer", "c_integer"))

                .add(new CastTestCase("c_boolean", "bigint", "c_bigint"))
                .add(new CastTestCase("c_smallint", "bigint", "c_bigint"))
                .add(new CastTestCase("c_integer", "bigint", "c_bigint"))
                .add(new CastTestCase("c_bigint", "bigint", "c_bigint"))
                .add(new CastTestCase("c_decimal_10_2", "bigint", "c_bigint"))
                .add(new CastTestCase("c_decimal_19_2", "bigint", "c_bigint"))
                .add(new CastTestCase("c_decimal_30_2", "bigint", "c_bigint"))
                .add(new CastTestCase("c_decimal_negative", "bigint", "c_bigint"))
                .add(new CastTestCase("c_big_serial", "bigint", "c_bigint"))
                .build();
    }

    @Override
    protected List<CastTestCase> unsupportedCastTypePushdown()
    {
        return ImmutableList.<CastTestCase>builder()
                .add(new CastTestCase("c_boolean", "tinyint", "c_smallint"))
                .add(new CastTestCase("c_smallint", "tinyint", "c_smallint"))
                .add(new CastTestCase("c_smallint_negative", "tinyint", "c_smallint"))
                .add(new CastTestCase("c_integer", "tinyint", "c_smallint"))
                .add(new CastTestCase("c_integer_negative", "tinyint", "c_smallint"))
                .add(new CastTestCase("c_bigint", "tinyint", "c_smallint"))
                .add(new CastTestCase("c_bigint_negative", "tinyint", "c_smallint"))
                .add(new CastTestCase("c_decimal_10_2", "tinyint", "c_smallint"))
                .add(new CastTestCase("c_decimal_19_2", "tinyint", "c_smallint"))
                .add(new CastTestCase("c_decimal_30_2", "tinyint", "c_smallint"))
                .add(new CastTestCase("c_decimal_negative", "tinyint", "c_smallint"))
                .add(new CastTestCase("c_numeric_negative", "tinyint", "c_smallint"))

                .add(new CastTestCase("c_real", "tinyint", "c_smallint"))
                .add(new CastTestCase("c_float4", "tinyint", "c_smallint"))
                .add(new CastTestCase("c_double_precision", "tinyint", "c_smallint"))
                .add(new CastTestCase("c_double_precision_negative", "tinyint", "c_smallint"))
                .add(new CastTestCase("c_float", "tinyint", "c_smallint"))
                .add(new CastTestCase("c_float8", "tinyint", "c_smallint"))
                .add(new CastTestCase("c_varchar_numeric", "tinyint", "c_smallint"))
                .add(new CastTestCase("c_text_numeric", "tinyint", "c_smallint"))
                .add(new CastTestCase("c_varchar_numeric_sign", "tinyint", "c_smallint"))

                .add(new CastTestCase("c_real", "smallint", "c_smallint"))
                .add(new CastTestCase("c_float4", "smallint", "c_smallint"))
                .add(new CastTestCase("c_double_precision", "smallint", "c_smallint"))
                .add(new CastTestCase("c_double_precision_negative", "smallint", "c_smallint"))
                .add(new CastTestCase("c_float", "smallint", "c_smallint"))
                .add(new CastTestCase("c_float8", "smallint", "c_smallint"))
                .add(new CastTestCase("c_varchar_numeric", "smallint", "c_smallint"))
                .add(new CastTestCase("c_text_numeric", "smallint", "c_smallint"))
                .add(new CastTestCase("c_varchar_numeric_sign", "smallint", "c_smallint"))

                .add(new CastTestCase("c_real", "integer", "c_integer"))
                .add(new CastTestCase("c_float4", "integer", "c_integer"))
                .add(new CastTestCase("c_double_precision", "integer", "c_integer"))
                .add(new CastTestCase("c_double_precision_negative", "integer", "c_integer"))
                .add(new CastTestCase("c_float", "integer", "c_integer"))
                .add(new CastTestCase("c_float8", "integer", "c_integer"))
                .add(new CastTestCase("c_varchar_numeric", "integer", "c_integer"))
                .add(new CastTestCase("c_text_numeric", "integer", "c_integer"))
                .add(new CastTestCase("c_varchar_numeric_sign", "integer", "c_integer"))

                .add(new CastTestCase("c_real", "bigint", "c_bigint"))
                .add(new CastTestCase("c_float4", "bigint", "c_bigint"))
                .add(new CastTestCase("c_double_precision", "bigint", "c_bigint"))
                .add(new CastTestCase("c_double_precision_negative", "bigint", "c_bigint"))
                .add(new CastTestCase("c_float", "bigint", "c_bigint"))
                .add(new CastTestCase("c_float8", "bigint", "c_bigint"))
                .add(new CastTestCase("c_varchar_numeric", "bigint", "c_bigint"))
                .add(new CastTestCase("c_text_numeric", "bigint", "c_bigint"))
                .add(new CastTestCase("c_varchar_numeric_sign", "bigint", "c_bigint"))

                .add(new CastTestCase("c_smallint", "boolean", "c_boolean"))
                .add(new CastTestCase("c_smallint_negative", "boolean", "c_boolean"))
                .add(new CastTestCase("c_real", "double", "c_double_precision"))
                .add(new CastTestCase("c_double_precision", "real", "c_real"))
                .add(new CastTestCase("c_double_precision_negative", "real", "c_real"))
                .add(new CastTestCase("c_double_precision", "decimal(10,2)", "c_decimal_10_2"))
                .add(new CastTestCase("c_double_precision_negative", "decimal(10,2)", "c_decimal_10_2"))
                .add(new CastTestCase("c_decimal_negative", "decimal(10,2)", "c_decimal_10_2"))
                .add(new CastTestCase("c_numeric_negative", "decimal(10,2)", "c_decimal_10_2"))
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
                .add(new InvalidCastTestCase("c_nan_real", "integer"))
                .add(new InvalidCastTestCase("c_nan_double", "integer"))

                // c_xml is not supported by default by trino. This is forced mapped to varchar.
                .add(new InvalidCastTestCase("c_xml", "tinyint"))
                .add(new InvalidCastTestCase("c_xml", "smallint"))
                .add(new InvalidCastTestCase("c_xml", "int"))
                .add(new InvalidCastTestCase("c_xml", "bigint"))
                .build();
    }
}

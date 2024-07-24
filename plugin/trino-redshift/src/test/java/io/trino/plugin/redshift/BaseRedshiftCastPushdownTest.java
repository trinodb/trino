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
import io.trino.Session;
import io.trino.plugin.jdbc.BaseJdbcCastPushdownTest;
import io.trino.plugin.jdbc.CastDataTypeTestTable;
import io.trino.sql.planner.plan.ProjectNode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.redshift.TestingRedshiftServer.TEST_SCHEMA;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class BaseRedshiftCastPushdownTest
        extends BaseJdbcCastPushdownTest
{
    private CastDataTypeTestTable left;
    private CastDataTypeTestTable right;

    @BeforeAll
    public void setupTable()
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
                .addColumn("c_char_10", "char(10)", asList("'India'", "'Poland'", null))
                .addColumn("c_char_50", "char(50)", asList("'India'", "'Poland'", null))
                .addColumn("c_char_4096", "char(4096)", asList("'India'", "'Poland'", null)) // Equal to REDSHIFT_MAX_CHAR
                .addColumn("c_nchar_10", "nchar(10)", asList("'India'", "'Poland'", null))
                .addColumn("c_nchar_50", "nchar(50)", asList("'India'", "'Poland'", null))
                .addColumn("c_nchar_4096", "nchar(4096)", asList("'India'", "'Poland'", null)) // Equal to REDSHIFT_MAX_CHAR
                .addColumn("c_bpchar", "bpchar", asList("'India'", "'Poland'", null))
                .addColumn("c_varchar_10", "varchar(10)", asList("'India'", "'Poland'", null))
                .addColumn("c_varchar_50", "varchar(50)", asList("'India'", "'Poland'", null))
                .addColumn("c_varchar_65535", "varchar(65535)", asList("'India'", "'Poland'", null)) // Equal to REDSHIFT_MAX_VARCHAR
                .addColumn("c_nvarchar_10", "nvarchar(10)", asList("'India'", "'Poland'", null))
                .addColumn("c_nvarchar_50", "nvarchar(50)", asList("'India'", "'Poland'", null))
                .addColumn("c_nvarchar_65535", "nvarchar(65535)", asList("'India'", "'Poland'", null)) // Greater than REDSHIFT_MAX_VARCHAR
                .addColumn("c_text", "text", asList("'India'", "'Poland'", null))
                .addColumn("c_varbinary", "varbinary", asList("'\\x66696E6465706920726F636B7321'", "'\\x000102f0feee'", null))
                .addColumn("c_date", "date", asList("DATE '2024-09-08'", "DATE '2019-08-15'", null))
                .addColumn("c_time", "time", asList("TIME '00:13:42.000000'", "TIME '10:01:17.100000'", null))
                .addColumn("c_timestamp", "timestamp", asList("TIMESTAMP '2024-09-08 01:02:03.666'", "TIMESTAMP '2019-08-15 09:08:07.333'", null))
                .addColumn("c_timestamptz", "timestamptz", asList("TIMESTAMP '2024-09-08 01:02:03.666+05:30'", "TIMESTAMP '2019-08-15 09:08:07.333+05:30'", null))

                .addColumn("c_nan", "double precision", asList("'Nan'", "'-Nan'", null))
                .addColumn("c_infinity", "double precision", asList("'Infinity'", "'-Infinity'", null))
                .addColumn("c_big_number", "bigint", asList("-9223372036854775808", "9223372036854775807", null))
                .addColumn("c_decimal_negative", "decimal(19, 2)", asList(-1.23, -2.67, null))
                .addColumn("c_varchar_numeric", "varchar(50)", asList("'127'", "'128'", null))
                .addColumn("c_char_numeric", "char(50)", asList("'127'", "'128'", null))
                .addColumn("c_bpchar_numeric", "bpchar", asList("'127'", "'128'", null))
                .addColumn("c_text_numeric", "text", asList("'127'", "'128'", null))
                .addColumn("c_nvarchar_numeric", "nvarchar(50)", asList("'127'", "'128'", null))
                .addColumn("c_varchar_numeric_sign", "varchar(50)", asList("'+127'", "'-127'", null))
                .addColumn("c_varchar_big_numeric", "varchar(50)", asList("'2147483647'", "'2147483644'", null))
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
                .addColumn("c_char_10", "char(10)", asList("'India'", "'France'", null))
                .addColumn("c_char_50", "char(50)", asList("'India'", "'France'", null))
                .addColumn("c_char_4096", "char(4096)", asList("'India'", "'France'", null)) // Equal to REDSHIFT_MAX_CHAR
                .addColumn("c_nchar_10", "nchar(10)", asList("'India'", "'France'", null))
                .addColumn("c_nchar_50", "nchar(50)", asList("'India'", "'France'", null))
                .addColumn("c_nchar_4096", "nchar(4096)", asList("'India'", "'France'", null)) // Equal to REDSHIFT_MAX_CHAR
                .addColumn("c_bpchar", "bpchar", asList("'India'", "'France'", null))
                .addColumn("c_varchar_10", "varchar(10)", asList("'India'", "'France'", null))
                .addColumn("c_varchar_50", "varchar(50)", asList("'India'", "'France'", null))
                .addColumn("c_varchar_65535", "varchar(65535)", asList("'India'", "'France'", null)) // Equal to REDSHIFT_MAX_VARCHAR
                .addColumn("c_nvarchar_10", "nvarchar(10)", asList("'India'", "'France'", null))
                .addColumn("c_nvarchar_50", "nvarchar(50)", asList("'India'", "'France'", null))
                .addColumn("c_nvarchar_65535", "nvarchar(65535)", asList("'India'", "'France'", null)) // Equal to REDSHIFT_MAX_VARCHAR
                .addColumn("c_text", "text", asList("'India'", "'France'", null))
                .addColumn("c_varbinary", "varbinary", asList("'\\x66696E6465706920726F636B7321'", "'\\x4672616E6365'", null))
                .addColumn("c_date", "date", asList("DATE '2024-09-08'", "DATE '2020-08-15'", null))
                .addColumn("c_time", "time", asList("TIME '00:13:42.000000'", "TIME '11:01:17.100000'", null))
                .addColumn("c_timestamp", "timestamp", asList("TIMESTAMP '2024-09-08 01:02:03.666'", "TIMESTAMP '2020-08-15 09:08:07.333'", null))
                .addColumn("c_timestamptz", "timestamptz", asList("TIMESTAMP '2024-09-08 01:02:03.666+05:30'", "TIMESTAMP '2020-08-15 09:08:07.333+05:30'", null))

                .addColumn("c_nan", "double precision", asList("'Nan'", "'Nan'", null))
                .addColumn("c_infinity", "double precision", asList("'Infinity'", "'Infinity'", null))
                .addColumn("c_big_number", "bigint", asList("-9223372036854775808", "9223372036854775806", null))
                .addColumn("c_decimal_negative", "decimal(19, 2)", asList(-1.23, -22.67, null))
                .addColumn("c_varchar_numeric", "varchar(50)", asList("'127'", "'228'", null))
                .addColumn("c_char_numeric", "char(50)", asList("'127'", "'228'", null))
                .addColumn("c_bpchar_numeric", "bpchar", asList("'127'", "'228'", null))
                .addColumn("c_text_numeric", "text", asList("'127'", "'228'", null))
                .addColumn("c_nvarchar_numeric", "nvarchar(50)", asList("'127'", "'128'", null))
                .addColumn("c_varchar_numeric_sign", "varchar(50)", asList("'+127'", "'-227'", null))
                .addColumn("c_varchar_big_numeric", "varchar(50)", asList("'2147483647'", "'2147483645'", null))
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
    public void testJoinPushdownWithNestedCast()
    {
        CastTestCase testCase = new CastTestCase("c_real", "bigint", Optional.of("c_bigint"));
//        assertThat(query("SELECT l.id FROM %s l JOIN %s r ON CAST(l.%s AS %s) = r.%s".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn().orElseThrow())))
//                .isFullyPushedDown();
    }

    @Test
    public void testAllJoinPushdownWithCast()
    {
        CastTestCase testCase = new CastTestCase("c_int", "bigint", Optional.of("c_bigint"));
        assertThat(query("SELECT l.id FROM %s l LEFT JOIN %s r ON CAST(l.%s AS %s) = r.%s".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn().orElseThrow())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l RIGHT JOIN %s r ON CAST(l.%s AS %s) = r.%s".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn().orElseThrow())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l INNER JOIN %s r ON CAST(l.%s AS %s) = r.%s".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn().orElseThrow())))
                .isFullyPushedDown();

        testCase = new CastTestCase("c_varchar_10", "varchar(10)", Optional.of("c_varchar_50"));
        assertThat(query("SELECT l.id FROM %s l LEFT JOIN %s r ON l.%s = CAST(r.%s AS %s)".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.targetColumn().orElseThrow(), testCase.castType())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l RIGHT JOIN %s r ON l.%s = CAST(r.%s AS %s)".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.targetColumn().orElseThrow(), testCase.castType())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l INNER JOIN %s r ON l.%s = CAST(r.%s AS %s)".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.targetColumn().orElseThrow(), testCase.castType())))
                .isFullyPushedDown();

        testCase = new CastTestCase("c_varchar_10", "varchar(200)", Optional.of("c_varchar_50"));
        assertThat(query("SELECT l.id FROM %s l LEFT JOIN %s r ON CAST(l.%3$s AS %4$s) = CAST(r.%5$s AS %4$s)".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn().orElseThrow())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l RIGHT JOIN %s r ON CAST(l.%3$s AS %4$s) = CAST(r.%5$s AS %4$s)".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn().orElseThrow())))
                .isFullyPushedDown();
        assertThat(query("SELECT l.id FROM %s l INNER JOIN %s r ON CAST(l.%3$s AS %4$s) = CAST(r.%5$s AS %4$s)".formatted(leftTable(), rightTable(), testCase.sourceColumn(), testCase.castType(), testCase.targetColumn().orElseThrow())))
                .isFullyPushedDown();
    }

    @Test
    public void testCastPushdownDisabled()
    {
        Session sessionWithoutPushdown = Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "complex_expression_pushdown", "false")
                .build();
        assertThat(query(sessionWithoutPushdown, "SELECT CAST (c_int AS bigint) FROM %s".formatted(leftTable())))
                .isNotFullyPushedDown(ProjectNode.class);
    }

    @Test
    public void testCastPushdownWithForcedTypedToIntegralType()
    {
        // These column types are not supported by default by trino. These types are forced mapped to varchar.
        assertThat(query("SELECT CAST(c_super AS INTEGER) FROM %s".formatted(leftTable())))
                .isNotFullyPushedDown(ProjectNode.class);
        assertThat(query("SELECT CAST(c_super AS BIGINT) FROM %s".formatted(leftTable())))
                .isNotFullyPushedDown(ProjectNode.class);
    }

    @Test
    public void testCastPushdownOverflow()
    {
        Session sessionWithoutPushdown = Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "complex_expression_pushdown", "false")
                .build();
        assertThatThrownBy(() -> getQueryRunner().execute(sessionWithoutPushdown, "SELECT CAST(c_big_number AS TINYINT) FROM %s".formatted(leftTable())))
                .hasMessageContaining("Out of range for tinyint: -9223372036854775808");
        assertThatThrownBy(() -> getQueryRunner().execute(sessionWithoutPushdown, "SELECT CAST(c_big_number AS SMALLINT) FROM %s".formatted(leftTable())))
                .hasMessageContaining("Out of range for smallint: -9223372036854775808");
        assertThatThrownBy(() -> getQueryRunner().execute(sessionWithoutPushdown, "SELECT CAST(c_big_number AS INTEGER) FROM %s".formatted(leftTable())))
                .hasMessageContaining("Out of range for integer: -9223372036854775808");
        assertThat(query(sessionWithoutPushdown, "SELECT CAST(c_big_number AS BIGINT) FROM %s".formatted(leftTable())))
                .matches("VALUES (-9223372036854775808), (9223372036854775807), (null)")
                .isFullyPushedDown(); // Cast is not applied here because source and cast type are same

        assertThatThrownBy(() -> getQueryRunner().execute("SELECT CAST(c_big_number AS TINYINT) FROM %s".formatted(leftTable())))
                .hasMessageContaining("Value out of range for 2 bytes");
        assertThatThrownBy(() -> getQueryRunner().execute("SELECT CAST(c_big_number AS SMALLINT) FROM %s".formatted(leftTable())))
                .hasMessageContaining("Value out of range for 2 bytes");
        assertThatThrownBy(() -> getQueryRunner().execute("SELECT CAST(c_big_number AS INTEGER) FROM %s".formatted(leftTable())))
                .hasMessageContaining("Value out of range for 4 bytes");
        assertThat(query("SELECT CAST(c_big_number AS BIGINT) FROM %s".formatted(leftTable())))
                .matches("VALUES (-9223372036854775808), (9223372036854775807), (null)")
                .isFullyPushedDown(); // Cast is not applied here because source and cast type are same
    }

    @Override
    protected List<CastTestCase> supportedCastTypePushdown()
    {
        return ImmutableList.of(
                new CastTestCase("c_boolean", "tinyint", Optional.of("c_smallint")),
                new CastTestCase("c_smallint", "tinyint", Optional.of("c_smallint")),
                new CastTestCase("c_integer", "tinyint", Optional.of("c_smallint")),
                new CastTestCase("c_bigint", "tinyint", Optional.of("c_smallint")),
                new CastTestCase("c_decimal_10_2", "tinyint", Optional.of("c_smallint")),
                new CastTestCase("c_decimal_19_2", "tinyint", Optional.of("c_smallint")),
                new CastTestCase("c_decimal_30_2", "tinyint", Optional.of("c_smallint")),
                new CastTestCase("c_decimal_negative", "tinyint", Optional.of("c_smallint")),

                new CastTestCase("c_boolean", "smallint", Optional.of("c_smallint")),
                new CastTestCase("c_smallint", "smallint", Optional.of("c_smallint")),
                new CastTestCase("c_integer", "smallint", Optional.of("c_smallint")),
                new CastTestCase("c_bigint", "smallint", Optional.of("c_smallint")),
                new CastTestCase("c_decimal_10_2", "smallint", Optional.of("c_smallint")),
                new CastTestCase("c_decimal_19_2", "smallint", Optional.of("c_smallint")),
                new CastTestCase("c_decimal_30_2", "smallint", Optional.of("c_smallint")),
                new CastTestCase("c_decimal_negative", "smallint", Optional.of("c_smallint")),

                // To reduce the test run time, verifying alias data types column with only integer cast type
                new CastTestCase("c_boolean", "integer", Optional.of("c_integer")),
                new CastTestCase("c_smallint", "integer", Optional.of("c_integer")),
                new CastTestCase("c_int2", "integer", Optional.of("c_integer")),
                new CastTestCase("c_integer", "integer", Optional.of("c_integer")),
                new CastTestCase("c_int", "integer", Optional.of("c_integer")),
                new CastTestCase("c_int4", "integer", Optional.of("c_integer")),
                new CastTestCase("c_bigint", "integer", Optional.of("c_integer")),
                new CastTestCase("c_int8", "integer", Optional.of("c_integer")),
                new CastTestCase("c_decimal_10_2", "integer", Optional.of("c_integer")),
                new CastTestCase("c_decimal_19_2", "integer", Optional.of("c_integer")),
                new CastTestCase("c_decimal_30_2", "integer", Optional.of("c_integer")),
                new CastTestCase("c_numeric_10_2", "integer", Optional.of("c_integer")),
                new CastTestCase("c_numeric_19_2", "integer", Optional.of("c_integer")),
                new CastTestCase("c_numeric_30_2", "integer", Optional.of("c_integer")),
                new CastTestCase("c_decimal_negative", "integer", Optional.of("c_integer")),

                new CastTestCase("c_boolean", "bigint", Optional.of("c_bigint")),
                new CastTestCase("c_smallint", "bigint", Optional.of("c_bigint")),
                new CastTestCase("c_integer", "bigint", Optional.of("c_bigint")),
                new CastTestCase("c_bigint", "bigint", Optional.of("c_bigint")),
                new CastTestCase("c_decimal_10_2", "bigint", Optional.of("c_bigint")),
                new CastTestCase("c_decimal_19_2", "bigint", Optional.of("c_bigint")),
                new CastTestCase("c_decimal_30_2", "bigint", Optional.of("c_bigint")),
                new CastTestCase("c_decimal_negative", "bigint", Optional.of("c_bigint")));
    }

    @Override
    protected List<CastTestCase> unsupportedCastTypePushdown()
    {
        return ImmutableList.of(
                new CastTestCase("c_real", "smallint", Optional.of("c_smallint")),
                new CastTestCase("c_float4", "smallint", Optional.of("c_smallint")),
                new CastTestCase("c_double_precision", "smallint", Optional.of("c_smallint")),
                new CastTestCase("c_float", "smallint", Optional.of("c_smallint")),
                new CastTestCase("c_float8", "smallint", Optional.of("c_smallint")),
                new CastTestCase("c_varchar_numeric", "smallint", Optional.of("c_smallint")),
                new CastTestCase("c_text_numeric", "smallint", Optional.of("c_smallint")),
                new CastTestCase("c_nvarchar_numeric", "smallint", Optional.of("c_smallint")),
                new CastTestCase("c_varchar_numeric_sign", "smallint", Optional.of("c_smallint")),

                new CastTestCase("c_smallint", "boolean", Optional.of("c_boolean")),
                new CastTestCase("c_real", "double", Optional.of("c_double_precision")),
                new CastTestCase("c_double_precision", "real", Optional.of("c_real")),
                new CastTestCase("c_double_precision", "decimal(10,2)", Optional.of("c_decimal_10_2")),
                new CastTestCase("c_char_10", "char(50)", Optional.of("c_char_50")),
                new CastTestCase("c_char_10", "char(256)", Optional.of("c_bpchar")),
                new CastTestCase("c_varchar_10", "varchar(50)", Optional.of("c_varchar_50")),
                new CastTestCase("c_nvarchar_10", "varchar(50)", Optional.of("c_nvarchar_50")),
                new CastTestCase("c_varchar_10", "varchar(50)", Optional.of("c_text")),
                new CastTestCase("c_timestamp", "date", Optional.of("c_date")),
                new CastTestCase("c_timestamp", "time", Optional.of("c_time")),
                new CastTestCase("c_date", "timestamp", Optional.of("c_timestamp")));
    }

    @Override
    protected List<CastTestCase> failCast()
    {
        return ImmutableList.of(
                new CastTestCase("c_varchar_decimal", "smallint"),
                new CastTestCase("c_varchar_decimal_sign", "smallint"),
                new CastTestCase("c_varchar_alpha_numeric", "smallint"),
                new CastTestCase("c_char_50", "smallint"),
                new CastTestCase("c_char_numeric", "smallint"),
                new CastTestCase("c_bpchar_numeric", "smallint"),

                // These column types are not supported by default by trino. These types are forced mapped to varchar.
                new CastTestCase("c_timetz", "smallint"));
    }
}

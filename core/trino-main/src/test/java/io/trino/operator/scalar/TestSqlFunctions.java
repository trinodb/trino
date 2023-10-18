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
package io.trino.operator.scalar;

import io.trino.sql.query.QueryAssertions;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestSqlFunctions
{
    private static final double[] DOUBLE_VALUES = {123, -123, 123.45, -123.45, 0};
    private static final int[] intLefts = {9, 10, 11, -9, -10, -11, 0};
    private static final int[] intRights = {3, -3};
    private static final double[] doubleLefts = {9, 10, 11, -9, -10, -11, 9.1, 10.1, 11.1, -9.1, -10.1, -11.1};
    private static final double[] doubleRights = {3, -3, 3.1, -3.1};
    private static final double GREATEST_DOUBLE_LESS_THAN_HALF = 0x1.fffffffffffffp-2;

    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testFormatTrinoQuery()
    {
        // NULL input
        assertThat(assertions.function("format_trino_query", "NULL"))
                .isNull(VARCHAR);

        // empty input
        assertTrinoExceptionThrownBy(assertions.function("format_trino_query", "''")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessageStartingWith("Input cannot be parsed as a statement: []: line 1:2: mismatched input '<EOF>'. Expecting: ");

        // input that is not SQL
        assertTrinoExceptionThrownBy(assertions.function("format_trino_query", "'this is not sql'")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessageStartingWith("Input cannot be parsed as a statement: [this is not sql]: line 1:2: mismatched input 'this'. Expecting: ");

        // trailing semicolon
        assertTrinoExceptionThrownBy(assertions.function("format_trino_query", "'SELECT 1;'")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessageStartingWith("Input cannot be parsed as a statement: [SELECT 1;]: line 1:10: mismatched input ';'. Expecting: ");

        // remove whitespace
        assertFormatTrinoQuery("\n    \t  SELECT    1        \n\n\n", "SELECT 1");

        // strip `-- ...` comments
        assertFormatTrinoQuery("-- comment\nSELECT 1", "SELECT 1");

        // strip `/* ... */` comments
        assertFormatTrinoQuery("\n/* dbt\nlikes such\n comments*/ SELECT 1 ", "SELECT 1");

        // SELECT FROM
        assertFormatTrinoQuery(
                "SELECT one_column FROM a_table",
                """
                SELECT one_column
                FROM
                  a_table""");

        assertFormatTrinoQuery(
                "SELECT col1, col2 FROM a_table",
                """
                SELECT
                  col1
                , col2
                FROM
                  a_table""");

        // VALUES
        assertFormatTrinoQuery(
                " VALUES 1, 2, 3",
                """
                        VALUES\s
                          1
                        , 2
                        , 3""");

        // one aggregation
        assertFormatTrinoQuery(
                """
                        -- queries like this one are surprisingly popular
                             SELECT count(0) FROM
                             cat.schem.tab
                        """,
                """
                        SELECT count(0)
                        FROM
                          cat.schem.tab""");

        // two aggregations
        assertFormatTrinoQuery(
                """
                        -- queries like this one are surprisingly popular
                             SELECT count(0), max("some.col.with.dots") FROM
                             cat.schem.tab
                        """,
                """
                        SELECT
                          count(0)
                        , max("some.col.with.dots")
                        FROM
                          cat.schem.tab""");

        // CTAS
        assertFormatTrinoQuery(
                """
                        -- some important query
                             CREATE TABLE "cat"."sch"."tab ela" WITH (   \s
                                  -- every table should have partitioning
                                  partitioning=ARRAY[  'regionkey'])
                        AS SELECT regionkey, /* comment,*/ nationkey,
                          max(nationkey) OVER () AS max_nationkey -- because why not
                        FROM tpch."tiny".nation
                        /* yeah */
                        """,
                """
                        CREATE TABLE "cat"."sch"."tab ela"
                        WITH (
                           partitioning = ARRAY['regionkey']
                        ) AS SELECT
                          regionkey
                        , nationkey
                        , max(nationkey) OVER () max_nationkey
                        FROM
                          tpch."tiny".nation""");
    }

    private void assertFormatTrinoQuery(@Language("SQL") String input, @Language("SQL") String expected)
    {
        String argumentLiteral = "'%s'".formatted(input.replaceAll("'", "''"));
        assertThat(assertions.function("format_trino_query", argumentLiteral))
                .isEqualTo(expected);
    }
}

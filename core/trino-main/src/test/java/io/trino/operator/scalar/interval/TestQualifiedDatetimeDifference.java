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
package io.trino.operator.scalar.interval;

import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.spi.StandardErrorCode.EXPRESSION_NOT_AGGREGATE;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
class TestQualifiedDatetimeDifference
{
    private QueryAssertions assertions;

    @BeforeAll
    void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterAll
    void close()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    void testOperandTypes()
    {
        for (String[] example : new String[][] {
                {"DATE '2024-01-01'", "DATE '2024-01-03'", "DAY", "-2"},
                {"TIME '12:00:01'", "TIME '12:00:00'", "SECOND(2, 0)", "1"},
                {"TIME '12:00:00 +01:00'", "TIME '12:00:00 +00:00'", "HOUR", "-1"},
                {"TIMESTAMP '2024-01-01 12:00:00'", "TIMESTAMP '2024-01-03 13:00:00'", "DAY", "-2"},
                {"TIMESTAMP '2024-01-01 12:00:00 +01:00'", "TIMESTAMP '2024-01-01 12:00:00 +00:00'", "HOUR", "-1"},
                {"DATE '2024-01-01'", "TIMESTAMP '2024-01-01 01:00:00'", "HOUR", "-1"},
        }) {
            String difference = "(%s - %s) %s".formatted(example[0], example[1], example[2]);
            assertThat(assertions.expression("CAST(" + difference + " AS varchar)")).isEqualTo(example[3]);
            assertThat(assertions.query("SELECT CAST(" + difference + " AS varchar)"))
                    .matches("VALUES varchar '%s'".formatted(example[3]));
        }
        for (String type : new String[] {"date", "time(3)", "time(9) with time zone", "timestamp(3)", "timestamp(9) with time zone"}) {
            assertThat(assertions.expression("((CAST(NULL AS " + type + ") - CAST(NULL AS " + type + ")) DAY) IS NULL"))
                    .isEqualTo(true);
        }
        assertThat(assertions.expression("((NULL - DATE '2024-01-01') DAY) IS NULL")).isEqualTo(true);
        assertThat(assertions.query("SELECT (a - b) day FROM (VALUES (3, 1)) t(a, b)"))
                .matches("VALUES 2");
    }

    @Test
    void testAliasExpressionGrouping()
    {
        for (String field : new String[] {"day", "hour", "minute", "second", "year", "month"}) {
            assertThat(assertions.query("SELECT (10 - 3 + 2) " + field)).matches("VALUES 9");
            assertThat(assertions.query("SELECT (10 - (3 + 2)) " + field)).matches("VALUES 5");
            assertThat(assertions.query("SELECT ROW((10 - 3 + 2) " + field + ")"))
                    .matches("SELECT ROW(9 AS " + field + ")");
            assertThat(assertions.query("SELECT * FROM (VALUES (1, 10), (1, 3)) t(k, x) PIVOT ((sum(x) - min(x) + max(x)) " + field + " FOR k IN (1))"))
                    .matches("VALUES BIGINT '20'");
            assertThat(assertions.query("SELECT * FROM (VALUES (9, 1)) t(k, x) PIVOT (sum(x) FOR k IN ((10 - 3 + 2) " + field + "))"))
                    .matches("VALUES BIGINT '1'");
        }
        assertThat(assertions.expression("CAST((TIMESTAMP '2024-01-03 00:00:00' - (TIMESTAMP '2024-01-01 00:00:00' + INTERVAL '1' DAY)) DAY(3) AS varchar)"))
                .isEqualTo("1");
    }

    @Test
    void testAggregation()
    {
        String source = "FROM (VALUES (1, TIMESTAMP '2024-01-01 00:00:00'), (1, TIMESTAMP '2024-01-03 00:00:00'), (2, TIMESTAMP '2024-01-04 00:00:00')) t(k, ts)";
        assertThat(assertions.query("SELECT (max(ts) - min(ts)) DAY(3) " + source))
                .matches("VALUES INTERVAL '3' DAY(3)");
        assertThat(assertions.query("SELECT k, (max(ts) - min(ts)) DAY(3) " + source + " GROUP BY k"))
                .matches("VALUES (1, INTERVAL '2' DAY(3)), (2, INTERVAL '0' DAY(3))");
        assertThat(assertions.query("SELECT k " + source + " GROUP BY k HAVING (max(ts) - min(ts)) DAY(3) > INTERVAL '1' DAY"))
                .matches("VALUES 1");
        assertThat(assertions.query("SELECT (ts - TIMESTAMP '2024-01-01 00:00:00') DAY(3) " + source + " GROUP BY ts"))
                .matches("VALUES INTERVAL '0' DAY(3), INTERVAL '2' DAY(3), INTERVAL '3' DAY(3)");
        for (String difference : new String[] {"max(ts) - ts", "ts - min(ts)"}) {
            assertThat(assertions.query("SELECT (" + difference + ") DAY(3) " + source))
                    .failure()
                    .hasErrorCode(EXPRESSION_NOT_AGGREGATE);
        }
    }

    @Test
    void testIntervalLiteralsInSelectItems()
    {
        for (String field : new String[] {"YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND"}) {
            assertThat(assertions.query("SELECT INTERVAL '1' " + field))
                    .matches("VALUES INTERVAL '1' " + field);
            assertThat(assertions.query("SELECT INTERVAL '1' " + field + " result"))
                    .matches("VALUES INTERVAL '1' " + field);
            assertThat(assertions.query("SELECT INTERVAL '3' " + field + " / 2 = INTERVAL '" + (field.equals("SECOND") ? "2" : "1") + "' " + field))
                    .matches("VALUES true");
        }
    }

    @Test
    void testRoundingAndOverflow()
    {
        for (int precision : new int[] {0, 7, 12}) {
            for (String[] years : new String[][] {{"200000", "-200000"}, {"-200000", "200000"}}) {
                assertTrinoExceptionThrownBy(assertions.expression("(CAST(TIMESTAMP '%s-01-01 00:00:00' AS TIMESTAMP(%s)) - CAST(TIMESTAMP '%s-01-01 00:00:00' AS TIMESTAMP(%s))) DAY(9)".formatted(years[0], precision, years[1], precision))::evaluate)
                        .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
            }
        }
        String origin = "TIMESTAMP '2024-01-01 00:00:00.000000000'";
        for (String[] example : new String[][] {
                {"00:00:01.999999600", "SECOND(2, 6)", "2.000000"},
                {"00:00:01.999999600", "SECOND(2, 9)", "1.999999600"},
                {"00:01:59.900000000", "MINUTE", "1"},
        }) {
            String difference = "(TIMESTAMP '2024-01-01 %s' - %s) %s".formatted(example[0], origin, example[1]);
            assertThat(assertions.expression("CAST(" + difference + " AS varchar)")).isEqualTo(example[2]);
        }
        assertThat(assertions.expression("CAST((" + origin + " - TIMESTAMP '2024-01-01 00:00:01.999999500') SECOND(2, 6) AS varchar)"))
                .isEqualTo("-1.999999");
        assertTrinoExceptionThrownBy(() -> assertions.expression("(TIMESTAMP '2024-01-01 00:01:39.9999996' - " + origin + ") SECOND(2, 6)").evaluate())
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
        assertTrinoExceptionThrownBy(() -> assertions.expression("((DATE '2024-05-01' - DATE '2024-01-01') DAY)").evaluate())
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
    }

    @Test
    void testErrorsAreSpecificToQualifiedSyntax()
    {
        for (String difference : new String[] {"1 - 2", "INTERVAL '2' DAY - INTERVAL '1' DAY", "DATE '2024-01-01' - 1"}) {
            assertTrinoExceptionThrownBy(() -> assertions.expression("((" + difference + ") DAY)").evaluate())
                    .hasErrorCode(TYPE_MISMATCH)
                    .hasMessageContaining("requires datetime operands");
        }
        String difference = "TIMESTAMP '2024-02-01 00:00:00' - TIMESTAMP '2024-01-01 00:00:00'";
        for (String qualifier : new String[] {"DAY(0)", "SECOND(2, 13)"}) {
            assertTrinoExceptionThrownBy(() -> assertions.expression("(" + difference + ") " + qualifier).evaluate())
                    .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                    .hasMessageContaining("precision must be in range");
            assertTrinoExceptionThrownBy(() -> assertions.expression("CAST(" + difference + " AS INTERVAL " + qualifier + ")").evaluate())
                    .hasErrorCode(INVALID_FUNCTION_ARGUMENT);
        }
        for (String qualifier : new String[] {"MONTH", "YEAR", "YEAR TO MONTH"}) {
            assertTrinoExceptionThrownBy(() -> assertions.expression("((" + difference + ") " + qualifier + ")").evaluate())
                    .hasErrorCode(NOT_SUPPORTED);
            assertTrinoExceptionThrownBy(() -> assertions.expression("CAST(" + difference + " AS INTERVAL " + qualifier + ")").evaluate())
                    .hasErrorCode(TYPE_MISMATCH);
        }
    }

    @Test
    void testAliasesAfterCompoundExpressions()
    {
        for (String field : new String[] {"day", "hour", "minute", "second", "year", "month"}) {
            assertThat(assertions.query("SELECT 10 + (3 - 2) " + field)).matches("VALUES 11");
            assertThat(assertions.query("SELECT 3 * (2) " + field)).matches("VALUES 6");
            assertThat(assertions.query("SELECT ROW(10 + (3 - 2) " + field + ")"))
                    .matches("SELECT ROW(11 AS " + field + ")");
            assertThat(assertions.query("SELECT * FROM (VALUES (1, 10), (1, 3)) t(k, x) PIVOT (sum(x) + (max(x) - min(x)) " + field + " FOR k IN (1))"))
                    .matches("VALUES BIGINT '20'");
            assertThat(assertions.query("SELECT * FROM (VALUES (11, 1)) t(k, x) PIVOT (sum(x) FOR k IN (10 + (3 - 2) " + field + "))"))
                    .matches("VALUES BIGINT '1'");
        }
    }
}

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

import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.INVALID_LITERAL;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
class TestIntervalPrecision
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
    void testOversizedGenericTypeParameters()
    {
        for (String type : new String[] {
                "\"$interval_day_time\"(5,5,4294967298,4294967308)",
                "\"$interval_day_time\"(4294967301,5,2,6)",
                "\"$interval_day_time\"(5,4294967301,2,6)",
                "\"$interval_day_time\"(5,5,4294967298,6)",
                "\"$interval_day_time\"(5,5,2,4294967308)",
                "\"$interval_year_month\"(4294967296,1,2)",
                "\"$interval_year_month\"(0,4294967297,2)",
                "\"$interval_year_month\"(0,1,4294967298)",
        }) {
            assertTrinoExceptionThrownBy(() -> assertions.expression("CAST('1' AS " + type + ")").evaluate())
                    .hasErrorCode(TYPE_MISMATCH)
                    .hasMessageContaining("Unknown type");
        }
    }

    @Test
    void testRoundingParsedIntervals()
    {
        for (String[] example : new String[][] {
                {"1.9999994", "6", "1.999999"},
                {"1.9999995", "6", "2.000000"},
                {"1.9999996", "6", "2.000000"},
                {"-1.9999994", "6", "-1.999999"},
                {"-1.9999995", "6", "-1.999999"},
                {"-1.9999996", "6", "-2.000000"},
                {"0.499999999999", "0", "0"},
                {"-0.499999999999", "0", "0"},
                {"1.234499999999", "3", "1.234"},
                {"-1.234500000001", "3", "-1.235"},
                {"-1.2345", "3", "-1.234"},
        }) {
            String literal = "INTERVAL '%s' SECOND(2, %s)".formatted(example[0], example[1]);
            String cast = "CAST('%s' AS INTERVAL SECOND(2, %s))".formatted(example[0], example[1]);
            assertThat(assertions.expression("CAST(" + literal + " AS varchar)")).isEqualTo(example[2]);
            assertThat(assertions.expression("CAST(" + cast + " AS varchar)")).isEqualTo(example[2]);
            assertThat(assertions.query("SELECT CAST(" + literal + " AS varchar)"))
                    .matches("VALUES varchar '%s'".formatted(example[2]));
        }
        assertThat(assertions.expression("CAST(INTERVAL - '1.9999994' SECOND(2, 6) AS varchar)"))
                .isEqualTo("-1.999999");
        assertTrinoExceptionThrownBy(() -> assertions.expression("INTERVAL '99.9999996' SECOND(2, 6)").evaluate())
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
        assertTrinoExceptionThrownBy(() -> assertions.expression("CAST('99.9999996' AS INTERVAL SECOND(2, 6))").evaluate())
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
    }

    @Test
    void testDatetimeResultPrecision()
    {
        for (String type : new String[] {"timestamp(3)", "timestamp(3) with time zone", "time(3)", "time(3) with time zone"}) {
            String value = type.startsWith("timestamp") ? "2024-01-01 12:00:00" : "12:00:00";
            if (type.endsWith("with time zone")) {
                value += " +00:00";
            }
            String operand = "CAST('%s' AS %s)".formatted(value, type);
            assertThat(assertions.expression("typeof(" + operand + " + INTERVAL '1' DAY)"))
                    .isEqualTo(type);
            assertThat(assertions.expression("typeof(" + operand + " + INTERVAL '0.000001' SECOND)"))
                    .isEqualTo(type.replace("(3)", "(6)"));
            assertThat(assertions.expression("typeof(" + operand + " + INTERVAL '0.000000001' SECOND)"))
                    .isEqualTo(type.replace("(3)", "(9)"));
        }
    }

    @Test
    void testComparisonsAndVarcharRoundTrips()
    {
        for (String qualifier : new String[] {"DAY(2)", "SECOND(2, 9)", "MONTH(2)"}) {
            for (int left = -1; left <= 1; left++) {
                String value = "INTERVAL '%s' %s".formatted(left, qualifier);
                assertThat(assertions.expression("CAST(CAST(" + value + " AS varchar) AS INTERVAL " + qualifier + ") = " + value))
                        .isEqualTo(true);
                for (int right = -1; right <= 1; right++) {
                    String other = "INTERVAL '%s' %s".formatted(right, qualifier);
                    assertThat(assertions.expression(value + " = " + other)).isEqualTo(left == right);
                    assertThat(assertions.expression(value + " <> " + other)).isEqualTo(left != right);
                    assertThat(assertions.expression(value + " < " + other)).isEqualTo(left < right);
                    assertThat(assertions.expression(value + " <= " + other)).isEqualTo(left <= right);
                    assertThat(assertions.expression(value + " > " + other)).isEqualTo(left > right);
                    assertThat(assertions.expression(value + " >= " + other)).isEqualTo(left >= right);
                }
            }
        }
        assertTrinoExceptionThrownBy(() -> assertions.expression("CAST(INTERVAL '-123-11' YEAR TO MONTH AS varchar(6))").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }

    @Test
    void testLegacyIntervalOperations()
    {
        assertThat(assertions.expression("day_of_month(INTERVAL '123' DAY)"))
                .isEqualTo(123L);
        for (String literal : new String[] {"INTERVAL '0' DAY", "INTERVAL '-12' DAY", "INTERVAL '-2-3' YEAR TO MONTH"}) {
            assertThat(assertions.expression(literal + " = " + literal)).isEqualTo(true);
            assertThat(assertions.expression(literal + " <= " + literal)).isEqualTo(true);
            assertThat(assertions.expression(literal + " >= " + literal)).isEqualTo(true);
        }
        assertThat(assertions.expression("INTERVAL '-2' DAY < INTERVAL '-1' DAY")).isEqualTo(true);
        assertThat(assertions.expression("INTERVAL '-2' YEAR < INTERVAL '-1' YEAR")).isEqualTo(true);
        assertTrinoExceptionThrownBy(() -> assertions.expression("CAST(INTERVAL '123 12:34:56' DAY TO SECOND AS varchar(13))").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }

    @Test
    void testInvalidPrecisionVariableIsUserError()
    {
        for (String expression : new String[] {"INTERVAL '1' DAY(x)", "CAST('1' AS INTERVAL DAY(x))", "CAST('1' AS INTERVAL DAY TO SECOND(x))"}) {
            assertTrinoExceptionThrownBy(() -> assertions.expression(expression).evaluate())
                    .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                    .hasMessageContaining("must be a number or a numeric variable: x");
        }
    }

    @Test
    void testLiteralFractionRoundsOnceWithItsSign()
    {
        assertThat(assertions.query("SELECT CAST(INTERVAL '0.0000000000009' SECOND(2, 12) AS varchar)"))
                .matches("VALUES varchar '0.000000000001'");
        assertThat(assertions.query("SELECT CAST(INTERVAL - '0.0000000000005' SECOND(2, 12) AS varchar)"))
                .matches("VALUES varchar '0.000000000000'");
        assertThat(assertions.query("SELECT CAST(INTERVAL - '0.0005000000001' SECOND(2, 3) AS varchar)"))
                .matches("VALUES varchar '-0.001'");
    }

    @Test
    void testCastRejectsRepeatedSigns()
    {
        for (String type : new String[] {"DAY", "HOUR", "MINUTE", "SECOND", "SECOND(2, 12)", "DAY TO SECOND"}) {
            for (String sign : new String[] {"--", "-+", "+-", "++"}) {
                assertTrinoExceptionThrownBy(() -> assertions.expression("CAST('%s5' AS INTERVAL %s)".formatted(sign, type)).evaluate())
                        .hasErrorCode(INVALID_LITERAL);
            }
        }
        assertThat(assertions.expression("CAST(CAST('+5' AS INTERVAL SECOND) AS BIGINT)")).isEqualTo(5L);
        assertThat(assertions.expression("CAST(CAST('-5' AS INTERVAL SECOND) AS BIGINT)")).isEqualTo(-5L);
    }

    @Test
    void testLeadingFieldOverflow()
    {
        for (String type : new String[] {"DAY", "HOUR", "MINUTE", "SECOND", "SECOND(2, 12)", "DAY TO SECOND"}) {
            for (String sign : new String[] {"", "+", "-"}) {
                for (String magnitude : new String[] {"99999999999999", "99999999999999999999"}) {
                    String value = sign + magnitude;
                    assertTrinoExceptionThrownBy(() -> assertions.expression("CAST('%s' AS INTERVAL %s)".formatted(value, type)).evaluate())
                            .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
                    assertTrinoExceptionThrownBy(() -> assertions.expression("INTERVAL '%s' %s".formatted(value, type)).evaluate())
                            .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
                    assertTrinoExceptionThrownBy(() -> assertions.expression("CAST('%sx' AS INTERVAL %s)".formatted(value, type)).evaluate())
                            .hasErrorCode(INVALID_LITERAL);
                }
            }
        }
    }
}

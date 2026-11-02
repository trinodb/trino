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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
class TestIntervalConsumers
{
    private final QueryAssertions assertions = new QueryAssertions();

    @AfterAll
    void close()
    {
        assertions.close();
    }

    @Test
    void testDateArithmetic()
    {
        for (int precision = 0; precision <= 12; precision++) {
            String day = "INTERVAL '86400' SECOND(5,%s)".formatted(precision);
            assertThat(assertions.expression("DATE '2024-01-01' + " + day)).matches("DATE '2024-01-02'");
            assertThat(assertions.expression(day + " + DATE '2024-01-01'")).matches("DATE '2024-01-02'");
            assertThat(assertions.expression("DATE '2024-01-02' - " + day)).matches("DATE '2024-01-01'");
            assertThat(assertions.expression("DATE '2024-01-01' - (-" + day + ")")).matches("DATE '2024-01-02'");
            String unit = precision == 0 ? "1" : "0." + "0".repeat(precision - 1) + "1";
            for (String sign : new String[] {"", "-"}) {
                String fraction = "INTERVAL '%s%s' SECOND(1,%s)".formatted(sign, unit, precision);
                for (String expression : new String[] {"DATE '2024-01-01' + " + fraction, fraction + " + DATE '2024-01-01'", "DATE '2024-01-01' - " + fraction}) {
                    assertTrinoExceptionThrownBy(assertions.expression(expression)::evaluate)
                            .hasErrorCode(INVALID_FUNCTION_ARGUMENT);
                }
            }
        }
    }

    @Test
    void testToMilliseconds()
    {
        assertThat(assertions.expression("to_milliseconds(TIMESTAMP '2024-01-01 00:00:01.000000001' - TIMESTAMP '2024-01-01 00:00:00.000000001')"))
                .isEqualTo(1000L);
        for (int precision : new int[] {6, 9, 12}) {
            assertThat(assertions.expression("to_milliseconds(INTERVAL '1' SECOND(1,%s))".formatted(precision))).isEqualTo(1000L);
        }
        for (String[] example : new String[][] {
                {"1.999999999999", "1999"}, {"-1.999999999999", "-1999"},
                {"-0.000999999999", "0"}, {"-0.001000000000", "-1"}, {"-0.001000000001", "-1"},
                {"-9223372036854.775808", "-9223372036854775"},
        }) {
            assertThat(assertions.expression("to_milliseconds(INTERVAL '%s' SECOND(13,12))".formatted(example[0])))
                    .isEqualTo(Long.parseLong(example[1]));
        }
    }

    @Test
    void testTimestampSequence()
    {
        String start = "TIMESTAMP '2024-01-01 00:00:00.000000000'";
        String middle = "TIMESTAMP '2024-01-01 00:00:00.000000001'";
        String stop = "TIMESTAMP '2024-01-01 00:00:00.000000002'";
        assertThat(assertions.expression("sequence(%s, %s, INTERVAL '0.000000001' SECOND(1,9))".formatted(start, stop)))
                .matches("ARRAY[%s, %s, %s]".formatted(start, middle, stop));
        assertThat(assertions.expression("sequence(%s, %s, INTERVAL '-0.000000001' SECOND(1,9))".formatted(stop, start)))
                .matches("ARRAY[%s, %s, %s]".formatted(stop, middle, start));
        assertThat(assertions.expression("sequence(TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:00:02', INTERVAL '1' SECOND(1,9))"))
                .matches("ARRAY[TIMESTAMP '2024-01-01 00:00:00.000000000', TIMESTAMP '2024-01-01 00:00:01.000000000', TIMESTAMP '2024-01-01 00:00:02.000000000']");
        for (int precision : new int[] {7, 9, 12}) {
            assertThat(assertions.expression("sequence(CAST(TIMESTAMP '2024-01-01 00:00:00.0000009' AS TIMESTAMP(%s)), CAST(TIMESTAMP '2024-01-01 00:00:00.0000011' AS TIMESTAMP(%s)), INTERVAL '0.0000001' SECOND(1,%s))".formatted(precision, precision, precision)))
                    .matches("CAST(ARRAY[TIMESTAMP '2024-01-01 00:00:00.0000009', TIMESTAMP '2024-01-01 00:00:00.0000010', TIMESTAMP '2024-01-01 00:00:00.0000011'] AS ARRAY(TIMESTAMP(%s)))".formatted(precision));
        }
        assertThat(assertions.expression("sequence(TIMESTAMP '2024-01-01 00:00:00.0000009', TIMESTAMP '2024-01-01 00:00:00.0000018', INTERVAL '0.000001' SECOND(1,6))"))
                .matches("ARRAY[TIMESTAMP '2024-01-01 00:00:00.0000009']");
        assertThat(assertions.expression("sequence(TIMESTAMP '2024-01-01 00:00:00.0000018', TIMESTAMP '2024-01-01 00:00:00.0000009', INTERVAL '-0.000001' SECOND(1,6))"))
                .matches("ARRAY[TIMESTAMP '2024-01-01 00:00:00.0000018']");
        assertThat(assertions.expression("cardinality(sequence(TIMESTAMP '-200000-01-01 00:00:00', TIMESTAMP '200000-01-01 00:00:00', INTERVAL '100000000' DAY(9)))"))
                .isEqualTo(2L);
        for (String step : new String[] {"0", "-0.000000001", "0.0000000000001"}) {
            assertTrinoExceptionThrownBy(assertions.expression("sequence(%s, %s, INTERVAL '%s' SECOND(1,12))".formatted(start, stop, step))::evaluate)
                    .hasErrorCode(INVALID_FUNCTION_ARGUMENT);
        }
        assertTrinoExceptionThrownBy(assertions.expression("sequence(%s, TIMESTAMP '2024-01-01 00:00:01', INTERVAL '0.000000000001' SECOND(1,12))".formatted(start))::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessage("result of sequence function must not have more than 10000 entries");
    }

    @Test
    void testDateSequence()
    {
        for (int precision = 0; precision <= 12; precision++) {
            assertThat(assertions.expression("sequence(DATE '2024-01-01', DATE '2024-01-03', INTERVAL '86400' SECOND(5,%s))".formatted(precision)))
                    .matches("ARRAY[DATE '2024-01-01', DATE '2024-01-02', DATE '2024-01-03']");
            assertThat(assertions.expression("sequence(DATE '2024-01-03', DATE '2024-01-01', INTERVAL '-86400' SECOND(5,%s))".formatted(precision)))
                    .matches("ARRAY[DATE '2024-01-03', DATE '2024-01-02', DATE '2024-01-01']");
        }
        for (String step : new String[] {"0", "1", "86400.000000000001", "-0.000000000001"}) {
            assertTrinoExceptionThrownBy(assertions.expression("sequence(DATE '2024-01-01', DATE '2024-01-03', INTERVAL '%s' SECOND(5,12))".formatted(step))::evaluate)
                    .hasErrorCode(INVALID_FUNCTION_ARGUMENT);
        }
    }

    @Test
    void testTimestampBoundaryOperations()
    {
        for (int precision : new int[] {6, 7}) {
            assertTrinoExceptionThrownBy(assertions.expression("CAST(TIMESTAMP '1970-01-01 00:00:00' AS TIMESTAMP(%s)) - INTERVAL '-9223372036854.775808' SECOND(13,6)".formatted(precision))::evaluate)
                    .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
            assertThat(assertions.expression("CAST(TIMESTAMP '1969-12-31 23:59:59.999999' AS TIMESTAMP(%s)) - INTERVAL '-9223372036854.775808' SECOND(13,6)".formatted(precision)))
                    .matches("CAST(TIMESTAMP '1970-01-01 00:00:00' + INTERVAL '9223372036854.775807' SECOND(13,6) AS TIMESTAMP(%s))".formatted(precision));
        }
        assertThat(assertions.expression("(TIMESTAMP '1970-01-01 00:00:00.0000000' + INTERVAL '-9223372036854.7758075' SECOND(13,7)) + INTERVAL '-0.0000005' SECOND(1,7)"))
                .matches("TIMESTAMP '1970-01-01 00:00:00.0000000' + INTERVAL '-9223372036854.7758080' SECOND(13,7)");
        assertThat(assertions.expression("(TIMESTAMP '1970-01-01 00:00:00.0000000' + INTERVAL '9223372036854.7758070' SECOND(13,7)) - INTERVAL '-0.0000005' SECOND(1,7)"))
                .matches("TIMESTAMP '1970-01-01 00:00:00.0000000' + INTERVAL '9223372036854.7758075' SECOND(13,7)");
    }

    @Test
    void testTimestampDifferenceOverflow()
    {
        for (int precision : new int[] {0, 6, 7, 12}) {
            for (String[] years : new String[][] {{"200000", "-200000"}, {"-200000", "200000"}}) {
                assertTrinoExceptionThrownBy(assertions.expression("CAST(TIMESTAMP '%s-01-01 00:00:00' AS TIMESTAMP(%s)) - CAST(TIMESTAMP '%s-01-01 00:00:00' AS TIMESTAMP(%s))".formatted(years[0], precision, years[1], precision))::evaluate)
                        .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
            }
        }
    }
}

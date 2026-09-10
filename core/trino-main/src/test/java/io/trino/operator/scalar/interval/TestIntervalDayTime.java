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

import io.trino.sql.ExpressionFormatter;
import io.trino.sql.query.QueryAssertions;
import io.trino.sql.query.QueryAssertions.ExpressionAssertProvider.Result;
import io.trino.sql.tree.IntervalLiteral;
import io.trino.type.SqlIntervalDayTime;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.time.Duration;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.function.OperatorType.DIVIDE;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.INDETERMINATE;
import static io.trino.spi.function.OperatorType.LESS_THAN;
import static io.trino.spi.function.OperatorType.MULTIPLY;
import static io.trino.spi.function.OperatorType.NEGATION;
import static io.trino.spi.function.OperatorType.SUBTRACT;
import static io.trino.spi.type.IntervalField.DAY;
import static io.trino.spi.type.IntervalField.HOUR;
import static io.trino.spi.type.IntervalField.SECOND;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.IntervalDayTimeType.createIntervalDayTimeType;
import static io.trino.util.DateTimeUtils.formatDayTimeInterval;
import static java.time.temporal.ChronoUnit.MICROS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestIntervalDayTime
{
    protected QueryAssertions assertions;

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
    public void testLiterals()
    {
        assertThat(assertions.expression("INTERVAL '12 10:45:32.123' DAY TO SECOND"))
                .isEqualTo(interval(12, 10, 45, 32, 123));

        assertThat(assertions.expression("INTERVAL '12 10:45:32.12' DAY TO SECOND"))
                .isEqualTo(interval(12, 10, 45, 32, 120));

        assertThat(assertions.expression("INTERVAL '12 10:45:32' DAY TO SECOND"))
                .isEqualTo(interval(12, 10, 45, 32, 0));

        assertThat(assertions.expression("INTERVAL '12 10:45' DAY TO SECOND"))
                .isEqualTo(interval(12, 10, 45, 0, 0));

        assertThat(assertions.expression("INTERVAL '12 10' DAY TO SECOND"))
                .isEqualTo(interval(12, 10, 0, 0, 0));

        assertThat(assertions.expression("INTERVAL '12' DAY TO SECOND"))
                .isEqualTo(interval(12, 0, 0, 0, 0));

        assertThat(assertions.expression("INTERVAL '12 10:45' DAY TO MINUTE"))
                .isEqualTo(interval(12, 10, 45, 0, 0));

        assertThat(assertions.expression("INTERVAL '12 10' DAY TO MINUTE"))
                .isEqualTo(interval(12, 10, 0, 0, 0));

        assertThat(assertions.expression("INTERVAL '12' DAY TO MINUTE"))
                .isEqualTo(interval(12, 0, 0, 0, 0));

        assertThat(assertions.expression("INTERVAL '12 10' DAY TO HOUR"))
                .isEqualTo(interval(12, 10, 0, 0, 0));

        assertThat(assertions.expression("INTERVAL '12' DAY TO HOUR"))
                .isEqualTo(interval(12, 0, 0, 0, 0));

        assertThat(assertions.expression("INTERVAL '12' DAY"))
                .isEqualTo(interval(12, 0, 0, 0, 0));

        assertThat(assertions.expression("INTERVAL '30' DAY"))
                .isEqualTo(interval(30, 0, 0, 0, 0));

        assertThat(assertions.expression("INTERVAL '90' DAY"))
                .isEqualTo(interval(90, 0, 0, 0, 0));

        assertThat(assertions.expression("INTERVAL '10:45:32.123' HOUR TO SECOND"))
                .isEqualTo(interval(0, 10, 45, 32, 123));

        assertThat(assertions.expression("INTERVAL '10:45:32.12' HOUR TO SECOND"))
                .isEqualTo(interval(0, 10, 45, 32, 120));

        assertThat(assertions.expression("INTERVAL '10:45:32' HOUR TO SECOND"))
                .isEqualTo(interval(0, 10, 45, 32, 0));

        assertThat(assertions.expression("INTERVAL '10:45' HOUR TO SECOND"))
                .isEqualTo(interval(0, 10, 45, 0, 0));

        assertThat(assertions.expression("INTERVAL '10' HOUR TO SECOND"))
                .isEqualTo(interval(0, 10, 0, 0, 0));

        assertThat(assertions.expression("INTERVAL '10:45' HOUR TO MINUTE"))
                .isEqualTo(interval(0, 10, 45, 0, 0));

        assertThat(assertions.expression("INTERVAL '10' HOUR TO MINUTE"))
                .isEqualTo(interval(0, 10, 0, 0, 0));

        assertThat(assertions.expression("INTERVAL '10' HOUR"))
                .isEqualTo(interval(0, 10, 0, 0, 0));

        assertThat(assertions.expression("INTERVAL '45:32.123' MINUTE TO SECOND"))
                .isEqualTo(interval(0, 0, 45, 32, 123));

        assertThat(assertions.expression("INTERVAL '45:32.12' MINUTE TO SECOND"))
                .isEqualTo(interval(0, 0, 45, 32, 120));

        assertThat(assertions.expression("INTERVAL '45:32' MINUTE TO SECOND"))
                .isEqualTo(interval(0, 0, 45, 32, 0));

        assertThat(assertions.expression("INTERVAL '45' MINUTE TO SECOND"))
                .isEqualTo(interval(0, 0, 45, 0, 0));

        assertThat(assertions.expression("INTERVAL '45' MINUTE"))
                .isEqualTo(interval(0, 0, 45, 0, 0));

        assertThat(assertions.expression("INTERVAL '32.123' SECOND"))
                .isEqualTo(interval(0, 0, 0, 32, 123));

        assertThat(assertions.expression("INTERVAL '32.12' SECOND"))
                .isEqualTo(interval(0, 0, 0, 32, 120));

        assertThat(assertions.expression("INTERVAL '32' SECOND"))
                .isEqualTo(interval(0, 0, 0, 32, 0));

        // Invalid literals
        assertThatThrownBy(assertions.expression("INTERVAL '12X' DAY")::evaluate)
                .hasMessage("line 1:12: Invalid INTERVAL DAY value: 12X");

        assertThatThrownBy(assertions.expression("INTERVAL '12 10' DAY")::evaluate)
                .hasMessage("line 1:12: Invalid INTERVAL DAY value: 12 10");

        assertThatThrownBy(assertions.expression("INTERVAL '12 X' DAY TO HOUR")::evaluate)
                .hasMessage("line 1:12: Invalid INTERVAL DAY TO HOUR value: 12 X");

        assertThatThrownBy(assertions.expression("INTERVAL '12 -10' DAY TO HOUR")::evaluate)
                .hasMessage("line 1:12: Invalid INTERVAL DAY TO HOUR value: 12 -10");

        assertThatThrownBy(assertions.expression("INTERVAL '--12 -10' DAY TO HOUR")::evaluate)
                .hasMessage("line 1:12: Invalid INTERVAL DAY TO HOUR value: --12 -10");

        // Invalid qualifiers (DAY TO xxx)
        assertThatThrownBy(assertions.expression("INTERVAL '12' DAY TO DAY")::evaluate)
                .hasMessage("line 1:33: mismatched input 'DAY'. Expecting: ')', ',', 'HOUR', 'MINUTE', 'SECOND'");

        assertThatThrownBy(assertions.expression("INTERVAL '12-10' DAY TO YEAR")::evaluate)
                .hasMessage("line 1:36: mismatched input 'YEAR'. Expecting: ')', ',', 'HOUR', 'MINUTE', 'SECOND'");

        assertThatThrownBy(assertions.expression("INTERVAL '12-10' DAY TO MONTH")::evaluate)
                .hasMessage("line 1:36: mismatched input 'MONTH'. Expecting: ')', ',', 'HOUR', 'MINUTE', 'SECOND'");

        // Invalid qualifiers (HOUR TO xxx)
        assertThatThrownBy(assertions.expression("INTERVAL '10' HOUR TO HOUR")::evaluate)
                .hasMessage("line 1:26: Invalid interval qualifier: HOUR TO HOUR");

        assertThatThrownBy(assertions.expression("INTERVAL '12-10' HOUR TO YEAR")::evaluate)
                .hasMessage("line 1:37: mismatched input 'YEAR'. Expecting: ')', ',', 'HOUR', 'MINUTE', 'SECOND'");

        assertThatThrownBy(assertions.expression("INTERVAL '12-10' HOUR TO MONTH")::evaluate)
                .hasMessage("line 1:37: mismatched input 'MONTH'. Expecting: ')', ',', 'HOUR', 'MINUTE', 'SECOND'");

        assertThatThrownBy(assertions.expression("INTERVAL '12-10' HOUR TO DAY")::evaluate)
                .hasMessage("line 1:37: mismatched input 'DAY'. Expecting: ')', ',', 'HOUR', 'MINUTE', 'SECOND'");

        // Invalid qualifiers (MINUTE TO xxx)
        assertThatThrownBy(assertions.expression("INTERVAL '45' MINUTE TO MINUTE")::evaluate)
                .hasMessage("line 1:26: Invalid interval qualifier: MINUTE TO MINUTE");

        assertThatThrownBy(assertions.expression("INTERVAL '12-10' MINUTE TO YEAR")::evaluate)
                .hasMessage("line 1:39: mismatched input 'YEAR'. Expecting: ')', ',', 'HOUR', 'MINUTE', 'SECOND'");

        assertThatThrownBy(assertions.expression("INTERVAL '12-10' MINUTE TO MONTH")::evaluate)
                .hasMessage("line 1:39: mismatched input 'MONTH'. Expecting: ')', ',', 'HOUR', 'MINUTE', 'SECOND'");

        assertThatThrownBy(assertions.expression("INTERVAL '12-10' MINUTE TO DAY")::evaluate)
                .hasMessage("line 1:39: mismatched input 'DAY'. Expecting: ')', ',', 'HOUR', 'MINUTE', 'SECOND'");

        assertThatThrownBy(assertions.expression("INTERVAL '12-10' MINUTE TO HOUR")::evaluate)
                .hasMessage("line 1:29: Invalid interval qualifier: MINUTE TO HOUR");

        // Invalid qualifiers (SECOND TO xxx)
        assertThatThrownBy(assertions.expression("INTERVAL '32' SECOND TO SECOND")::evaluate)
                .hasMessage("line 1:36: mismatched input 'SECOND'. Expecting: ')', ','");

        assertThatThrownBy(assertions.expression("INTERVAL '12-10' SECOND TO YEAR")::evaluate)
                .hasMessage("line 1:39: mismatched input 'YEAR'. Expecting: ')', ','");

        assertThatThrownBy(assertions.expression("INTERVAL '12-10' SECOND TO MONTH")::evaluate)
                .hasMessage("line 1:39: mismatched input 'MONTH'. Expecting: ')', ','");

        assertThatThrownBy(assertions.expression("INTERVAL '12-10' SECOND TO DAY")::evaluate)
                .hasMessage("line 1:39: mismatched input 'DAY'. Expecting: ')', ','");

        assertThatThrownBy(assertions.expression("INTERVAL '12-10' SECOND TO HOUR")::evaluate)
                .hasMessage("line 1:39: mismatched input 'HOUR'. Expecting: ')', ','");

        assertThatThrownBy(assertions.expression("INTERVAL '12-10' SECOND TO MINUTE")::evaluate)
                .hasMessage("line 1:39: mismatched input 'MINUTE'. Expecting: ')', ','");
    }

    @Test
    public void testCast()
    {
        // casting to another second-ending qualifier preserves the value and its fractional digits
        assertThat(assertions.expression("CAST(CAST(INTERVAL '2 10:45:32.123' DAY TO SECOND AS INTERVAL HOUR TO SECOND) AS VARCHAR)"))
                .hasType(VARCHAR)
                .isEqualTo("2 10:45:32.123000");

        // casting to a qualifier without a SECOND field rounds away the sub-second component
        assertThat(assertions.expression("CAST(CAST(INTERVAL '2 10:45:32.123' DAY TO SECOND AS INTERVAL MINUTE(4)) AS VARCHAR)"))
                .hasType(VARCHAR)
                .isEqualTo("2 10:45:32");
    }

    @Test
    public void testCastFromVarchar()
    {
        assertThat(assertions.expression("CAST('2 10:45:32.123' AS INTERVAL DAY TO SECOND)"))
                .isEqualTo(interval(2, 10, 45, 32, 123));

        // the string is interpreted according to the target qualifier
        assertThat(assertions.expression("CAST('45' AS INTERVAL MINUTE)"))
                .isEqualTo(interval(0, 0, 45, 0, 0));

        assertThat(assertions.expression("CAST('10:45' AS INTERVAL HOUR TO MINUTE)"))
                .isEqualTo(interval(0, 10, 45, 0, 0));

        // round-trips through a character string
        assertThat(assertions.expression("CAST(CAST(INTERVAL '5 04:03:02.001' DAY TO SECOND AS VARCHAR) AS INTERVAL DAY TO SECOND)"))
                .isEqualTo(interval(5, 4, 3, 2, 1));

        // a value that does not fit the target's leading precision is rejected
        assertTrinoExceptionThrownBy(assertions.expression("CAST('340 00:00:00' AS INTERVAL DAY(2) TO SECOND)")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessageContaining("interval field overflow: 340 day does not fit a leading precision of 2");
    }

    @Test
    public void testCastToBigint()
    {
        assertThat(assertions.expression("CAST(INTERVAL '5' DAY AS BIGINT)")).isEqualTo(5L);
        assertThat(assertions.expression("CAST(INTERVAL '5' HOUR AS BIGINT)")).isEqualTo(5L);
        assertThat(assertions.expression("CAST(INTERVAL '90' MINUTE AS BIGINT)")).isEqualTo(90L);
        assertThat(assertions.expression("CAST(INTERVAL '5' SECOND AS BIGINT)")).isEqualTo(5L);

        // a floored negative long-form value at an exact field boundary truncates toward zero, matching the
        // short form: -0.999999999999 seconds is 0 whole seconds, not -1 (regression: the dropped
        // sub-microsecond fraction divided the floored micros one unit too far from zero)
        assertThat(assertions.expression("CAST(INTERVAL '-0.999999999999' SECOND(2, 12) AS BIGINT)")).isEqualTo(0L);
        assertThat(assertions.expression("CAST(INTERVAL '-1.000000000001' SECOND(2, 12) AS BIGINT)")).isEqualTo(-1L);
        assertThat(assertions.expression("CAST(INTERVAL '1.999999999999' SECOND(2, 12) AS BIGINT)")).isEqualTo(1L);

        // a cast to a number is defined only for a single-field interval
        assertThatThrownBy(assertions.expression("CAST(INTERVAL '1 02' DAY TO HOUR AS BIGINT)")::evaluate)
                .hasMessageContaining("to hour to bigint");
    }

    @Test
    public void testCastFromInteger()
    {
        assertThat(assertions.expression("CAST(CAST(5 AS INTERVAL HOUR(10)) AS VARCHAR)"))
                .hasType(VARCHAR)
                .isEqualTo("0 05:00:00");
        assertThat(assertions.expression("CAST(CAST(BIGINT '90' AS INTERVAL MINUTE(12)) AS VARCHAR)"))
                .hasType(VARCHAR)
                .isEqualTo("0 01:30:00");
    }

    @Test
    public void testObject()
    {
        assertThat(new SqlIntervalDayTime(12, 10, 45, 32, 123)).isEqualTo(new SqlIntervalDayTime(1_075_532_123_000L));
        assertThat(new SqlIntervalDayTime(-12, -10, -45, -32, -123)).isEqualTo(new SqlIntervalDayTime(-1_075_532_123_000L));
    }

    @Test
    public void testComparison()
    {
        assertThat(assertions.operator(EQUAL, "INTERVAL '3' SECOND", "INTERVAL '3' SECOND"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "INTERVAL '6' DAY", "INTERVAL '6' DAY"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "INTERVAL '3' SECOND", "INTERVAL '4' SECOND"))
                .isEqualTo(false);

        assertThat(assertions.expression("a <> b")
                .binding("a", "INTERVAL '3' SECOND")
                .binding("b", "INTERVAL '4' SECOND"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "INTERVAL '3' SECOND", "INTERVAL '4' SECOND"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "INTERVAL '6' DAY", "INTERVAL '5' DAY"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "INTERVAL '3' SECOND")
                .binding("low", "INTERVAL '2' SECOND")
                .binding("high", "INTERVAL '4' SECOND"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "INTERVAL '3' SECOND")
                .binding("low", "INTERVAL '4' SECOND")
                .binding("high", "INTERVAL '5' SECOND"))
                .isEqualTo(false);
    }

    @Test
    public void testIndeterminate()
    {
        assertThat(assertions.operator(INDETERMINATE, "CAST(NULL AS INTERVAL DAY TO SECOND)"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "INTERVAL '45' MINUTE TO SECOND"))
                .isEqualTo(false);
    }

    @Test
    public void testCastToVarchar()
    {
        assertThat(assertions.expression("CAST(a AS varchar)")
                .binding("a", "INTERVAL '12 10:45:32.123' DAY TO SECOND"))
                .hasType(VARCHAR)
                .isEqualTo("12 10:45:32.123000");

        assertThat(assertions.expression("CAST(a AS varchar)")
                .binding("a", "INTERVAL '12 10:45' DAY TO MINUTE"))
                .hasType(VARCHAR)
                .isEqualTo("12 10:45:00");

        assertThat(assertions.expression("CAST(a AS varchar)")
                .binding("a", "INTERVAL '12 10' DAY TO HOUR"))
                .hasType(VARCHAR)
                .isEqualTo("12 10:00:00");

        assertThat(assertions.expression("CAST(a AS varchar)")
                .binding("a", "INTERVAL '12' DAY"))
                .hasType(VARCHAR)
                .isEqualTo("12 00:00:00");

        assertThat(assertions.expression("CAST(a AS varchar)")
                .binding("a", "INTERVAL '10:45:32.123' HOUR TO SECOND"))
                .hasType(VARCHAR)
                .isEqualTo("0 10:45:32.123000");

        assertThat(assertions.expression("CAST(a AS varchar)")
                .binding("a", "INTERVAL '10' HOUR"))
                .hasType(VARCHAR)
                .isEqualTo("0 10:00:00");

        assertThat(assertions.expression("CAST(a AS varchar)")
                .binding("a", "INTERVAL '45:32.123' MINUTE TO SECOND"))
                .hasType(VARCHAR)
                .isEqualTo("0 00:45:32.123000");

        assertThat(assertions.expression("CAST(a AS varchar)")
                .binding("a", "INTERVAL '32.123' SECOND"))
                .hasType(VARCHAR)
                .isEqualTo("0 00:00:32.123");
    }

    @Test
    void testRoundTrip()
    {
        assertRoundTrip("INTERVAL '0' SECOND", "INTERVAL '0 0:00:00' DAY TO SECOND");
        assertRoundTrip("INTERVAL '0.4' SECOND", "INTERVAL '0 0:00:00.400' DAY TO SECOND");
        assertRoundTrip("INTERVAL '59.999' SECOND", "INTERVAL '0 0:00:59.999' DAY TO SECOND");
        assertRoundTrip("INTERVAL '90061' SECOND", "INTERVAL '1 1:01:01' DAY TO SECOND");
        assertRoundTrip("INTERVAL '15:30' MINUTE TO SECOND", "INTERVAL '0 0:15:30' DAY TO SECOND");
        assertRoundTrip("INTERVAL '1500' MINUTE", "INTERVAL '1 1:00:00' DAY TO SECOND");
        assertRoundTrip("INTERVAL '2:00:45' HOUR TO SECOND", "INTERVAL '0 2:00:45' DAY TO SECOND");
        assertRoundTrip("INTERVAL '17520' HOUR", "INTERVAL '730 0:00:00' DAY TO SECOND");
        assertRoundTrip("INTERVAL '340' DAY", "INTERVAL '340 0:00:00' DAY TO SECOND");
        assertRoundTrip("INTERVAL '2 6' DAY TO HOUR", "INTERVAL '2 6:00:00' DAY TO SECOND");
        assertRoundTrip("INTERVAL '1 23:59:59.999' DAY TO SECOND", "INTERVAL '1 23:59:59.999' DAY TO SECOND");
    }

    private void assertRoundTrip(@Language("SQL") String input, @Language("SQL") String expectedFormatted)
    {
        Result evaluatedResult = assertions.expression(input).evaluate();
        SqlIntervalDayTime originalInterval = (SqlIntervalDayTime) evaluatedResult.value();

        Duration duration = Duration.of(originalInterval.getMicros(), MICROS);
        IntervalLiteral formattedLiteral = formatDayTimeInterval(duration);
        String formatted = ExpressionFormatter.formatExpression(formattedLiteral);
        assertThat(formatted).isEqualTo(expectedFormatted);

        Result reparsedResult = assertions.expression(formatted).evaluate();
        SqlIntervalDayTime reparsedInterval = (SqlIntervalDayTime) reparsedResult.value();
        assertThat(reparsedInterval).isEqualTo(originalInterval);
    }

    @Test
    public void testAdd()
    {
        // adding two intervals yields the union of their fields
        assertThat(assertions.operator(ADD, "INTERVAL '3' SECOND", "INTERVAL '3' SECOND"))
                .hasType(createIntervalDayTimeType(SECOND, SECOND, 13, 0))
                .isEqualTo(interval(0, 0, 0, 6, 0));

        assertThat(assertions.operator(ADD, "INTERVAL '6' DAY", "INTERVAL '6' DAY"))
                .hasType(createIntervalDayTimeType(DAY, DAY))
                .isEqualTo(interval(12, 0, 0, 0, 0));

        assertThat(assertions.operator(ADD, "INTERVAL '3' SECOND", "INTERVAL '6' DAY"))
                .hasType(createIntervalDayTimeType(DAY, SECOND, 9, 0))
                .isEqualTo(interval(6, 0, 0, 3, 0));

        assertTrinoExceptionThrownBy(assertions.operator(
                ADD,
                "INTERVAL '1' SECOND * 5000000000000",
                "INTERVAL '1' SECOND * 5000000000000")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("interval day to second addition overflow: 5000000000000000000 + 5000000000000000000");

        assertTrinoExceptionThrownBy(assertions.operator(
                ADD,
                "INTERVAL '1' SECOND * (-5000000000000)",
                "INTERVAL '1' SECOND * (-5000000000000)")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("interval day to second addition overflow: -5000000000000000000 + -5000000000000000000");

        // two long intervals add their picoseconds, keeping the wider fractional precision
        assertThat(assertions.expression("INTERVAL '1.123456789' SECOND(13, 9) + INTERVAL '0.000000111' SECOND(13, 9)"))
                .hasType(createIntervalDayTimeType(SECOND, SECOND, 13, 9))
                .isEqualTo(new SqlIntervalDayTime(1_123_456, 900_000, 9));

        // the picoseconds carry into the microseconds
        assertThat(assertions.expression("INTERVAL '0.999999999' SECOND(13, 9) + INTERVAL '0.000000001' SECOND(13, 9)"))
                .hasType(createIntervalDayTimeType(SECOND, SECOND, 13, 9))
                .isEqualTo(new SqlIntervalDayTime(1_000_000, 0, 9));

        // a short operand widens to the long result, contributing zero picoseconds
        assertThat(assertions.expression("INTERVAL '1.5' SECOND(13, 2) + INTERVAL '0.000000001' SECOND(13, 9)"))
                .hasType(createIntervalDayTimeType(SECOND, SECOND, 13, 9))
                .isEqualTo(new SqlIntervalDayTime(1_500_000, 1_000, 9));
    }

    @Test
    public void testSubtract()
    {
        assertThat(assertions.operator(SUBTRACT, "INTERVAL '6' SECOND", "INTERVAL '3' SECOND"))
                .hasType(createIntervalDayTimeType(SECOND, SECOND, 13, 0))
                .isEqualTo(interval(0, 0, 0, 3, 0));

        assertThat(assertions.operator(SUBTRACT, "INTERVAL '9' DAY", "INTERVAL '6' DAY"))
                .hasType(createIntervalDayTimeType(DAY, DAY))
                .isEqualTo(interval(3, 0, 0, 0, 0));

        assertThat(assertions.operator(SUBTRACT, "INTERVAL '3' SECOND", "INTERVAL '6' DAY"))
                .hasType(createIntervalDayTimeType(DAY, SECOND, 9, 0))
                .isEqualTo(interval(-5, -23, -59, -57, 0));

        assertTrinoExceptionThrownBy(assertions.operator(
                SUBTRACT,
                "INTERVAL '1' SECOND * (-5000000000000)",
                "INTERVAL '1' SECOND * 5000000000000")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("interval day to second subtraction overflow: -5000000000000000000 - 5000000000000000000");

        assertTrinoExceptionThrownBy(assertions.operator(
                SUBTRACT,
                "INTERVAL '1' SECOND * 5000000000000",
                "INTERVAL '1' SECOND * (-5000000000000)")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("interval day to second subtraction overflow: 5000000000000000000 - -5000000000000000000");

        // subtracting long intervals borrows from the microseconds when the picoseconds underflow
        assertThat(assertions.expression("INTERVAL '1.000000000' SECOND(13, 9) - INTERVAL '0.000000001' SECOND(13, 9)"))
                .hasType(createIntervalDayTimeType(SECOND, SECOND, 13, 9))
                .isEqualTo(new SqlIntervalDayTime(999_999, 999_000, 9));

        // a difference that crosses into negative borrows into a negative microseconds with a positive picosecond increment
        assertThat(assertions.expression("INTERVAL '0.000000001' SECOND(13, 9) - INTERVAL '0.000000002' SECOND(13, 9)"))
                .hasType(createIntervalDayTimeType(SECOND, SECOND, 13, 9))
                .isEqualTo(new SqlIntervalDayTime(-1, 999_000, 9));
    }

    @Test
    public void testMultiply()
    {
        // multiplying by a number preserves the interval's qualifier
        assertThat(assertions.operator(MULTIPLY, "INTERVAL '6' SECOND", "2"))
                .hasType(createIntervalDayTimeType(SECOND, SECOND, 13, 0))
                .isEqualTo(interval(0, 0, 0, 12, 0));

        assertThat(assertions.operator(MULTIPLY, "2", "INTERVAL '6' SECOND"))
                .hasType(createIntervalDayTimeType(SECOND, SECOND, 13, 0))
                .isEqualTo(interval(0, 0, 0, 12, 0));

        assertThat(assertions.operator(MULTIPLY, "INTERVAL '1' SECOND", "2.5"))
                .hasType(createIntervalDayTimeType(SECOND, SECOND, 13, 0))
                .isEqualTo(interval(0, 0, 0, 2, 500));

        assertThat(assertions.operator(MULTIPLY, "2.5", "INTERVAL '1' SECOND"))
                .hasType(createIntervalDayTimeType(SECOND, SECOND, 13, 0))
                .isEqualTo(interval(0, 0, 0, 2, 500));

        assertThat(assertions.operator(MULTIPLY, "INTERVAL '6' DAY", "2"))
                .hasType(createIntervalDayTimeType(DAY, DAY))
                .isEqualTo(interval(12, 0, 0, 0, 0));

        assertThat(assertions.operator(MULTIPLY, "2", "INTERVAL '6' DAY"))
                .hasType(createIntervalDayTimeType(DAY, DAY))
                .isEqualTo(interval(12, 0, 0, 0, 0));

        // a day scaled by a fraction keeps the DAY qualifier, with sub-day content retained
        assertThat(assertions.operator(MULTIPLY, "INTERVAL '1' DAY", "2.5"))
                .hasType(createIntervalDayTimeType(DAY, DAY))
                .isEqualTo(interval(2, 12, 0, 0, 0));

        assertThat(assertions.operator(MULTIPLY, "2.5", "INTERVAL '1' DAY"))
                .hasType(createIntervalDayTimeType(DAY, DAY))
                .isEqualTo(interval(2, 12, 0, 0, 0));

        assertTrinoExceptionThrownBy(assertions.operator(MULTIPLY, "INTERVAL '6' SECOND", "nan()")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertTrinoExceptionThrownBy(assertions.operator(MULTIPLY, "nan()", "INTERVAL '6' DAY")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertTrinoExceptionThrownBy(assertions.operator(MULTIPLY, "INTERVAL '1' SECOND", "9223372036854775807")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("interval day to second multiplication overflow: 1000000 * 9223372036854775807");

        assertTrinoExceptionThrownBy(assertions.operator(MULTIPLY, "9223372036854775807", "INTERVAL '1' SECOND")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("interval day to second multiplication overflow: 9223372036854775807 * 1000000");

        // multiplying a long interval by an integer scales its picoseconds, carrying into the microseconds
        assertThat(assertions.expression("INTERVAL '1.111111111' SECOND(13, 9) * 2"))
                .hasType(createIntervalDayTimeType(SECOND, SECOND, 13, 9))
                .isEqualTo(new SqlIntervalDayTime(2_222_222, 222_000, 9));

        assertThat(assertions.expression("INTERVAL '0.999999999' SECOND(13, 9) * 2"))
                .hasType(createIntervalDayTimeType(SECOND, SECOND, 13, 9))
                .isEqualTo(new SqlIntervalDayTime(1_999_999, 998_000, 9));

        // multiplying a long interval by a real factor rounds to its fractional precision
        assertThat(assertions.expression("INTERVAL '1' SECOND(13, 9) * 2.5"))
                .hasType(createIntervalDayTimeType(SECOND, SECOND, 13, 9))
                .isEqualTo(new SqlIntervalDayTime(2_500_000, 0, 9));

        assertTrinoExceptionThrownBy(assertions.expression("INTERVAL '1' SECOND(13, 9) * nan()")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testDivide()
    {
        assertThat(assertions.operator(DIVIDE, "INTERVAL '3' SECOND", "2"))
                .hasType(createIntervalDayTimeType(SECOND, SECOND, 13, 0))
                .isEqualTo(interval(0, 0, 0, 1, 500));

        assertThat(assertions.operator(DIVIDE, "INTERVAL '6' SECOND", "2.5"))
                .hasType(createIntervalDayTimeType(SECOND, SECOND, 13, 0))
                .isEqualTo(interval(0, 0, 0, 2, 400));

        // a day divided down keeps the DAY qualifier, with sub-day content retained
        assertThat(assertions.operator(DIVIDE, "INTERVAL '3' DAY", "2"))
                .hasType(createIntervalDayTimeType(DAY, DAY))
                .isEqualTo(interval(1, 12, 0, 0, 0));

        assertThat(assertions.operator(DIVIDE, "INTERVAL '4' DAY", "2.5"))
                .hasType(createIntervalDayTimeType(DAY, DAY))
                .isEqualTo(interval(1, 14, 24, 0, 0));

        assertTrinoExceptionThrownBy(assertions.operator(DIVIDE, "INTERVAL '6' SECOND", "nan()")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertTrinoExceptionThrownBy(assertions.operator(DIVIDE, "INTERVAL '6' SECOND", "0E0")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        // dividing a long interval rounds to its fractional precision
        assertThat(assertions.expression("INTERVAL '1' SECOND(13, 9) / 4"))
                .hasType(createIntervalDayTimeType(SECOND, SECOND, 13, 9))
                .isEqualTo(new SqlIntervalDayTime(250_000, 0, 9));

        assertTrinoExceptionThrownBy(assertions.expression("INTERVAL '1' SECOND(13, 9) / 0E0")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testNegation()
    {
        // negation preserves the interval's qualifier
        assertThat(assertions.operator(NEGATION, "INTERVAL '3' SECOND"))
                .hasType(createIntervalDayTimeType(SECOND, SECOND, 13, 0))
                .isEqualTo(interval(0, 0, 0, -3, 0));

        assertThat(assertions.operator(NEGATION, "INTERVAL '6' DAY"))
                .hasType(createIntervalDayTimeType(DAY, DAY))
                .isEqualTo(interval(-6, 0, 0, 0, 0));

        assertTrinoExceptionThrownBy(assertions.operator(
                NEGATION,
                "INTERVAL '1' SECOND * (-9223372036854) - INTERVAL '0.775808' SECOND")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("interval day to second negation overflow: -9223372036854775808");

        // negating a long interval negates the (micros, picos) pair, rendering the same magnitude
        assertThat(assertions.expression("CAST(- INTERVAL '1.123456789' SECOND(13, 9) AS VARCHAR)"))
                .hasType(VARCHAR)
                .isEqualTo("-0 00:00:01.123456789");
    }

    @Test
    public void testLeadingPrecision()
    {
        // a literal with no explicit precision takes the width of its leading field; a multi-field
        // qualifier ending in SECOND keeps the implicit fractional-seconds default (6), inferring only
        // the leading precision from the value
        assertThat(assertions.expression("INTERVAL '5' DAY")).hasType(createIntervalDayTimeType(DAY, DAY, 1));
        assertThat(assertions.expression("INTERVAL '340' HOUR")).hasType(createIntervalDayTimeType(HOUR, HOUR, 3));
        assertThat(assertions.expression("INTERVAL '12 10:45:32' DAY TO SECOND")).hasType(createIntervalDayTimeType(DAY, SECOND, 2, 6));

        // an explicit precision is honored
        assertThat(assertions.expression("INTERVAL '5' DAY(4)")).hasType(createIntervalDayTimeType(DAY, DAY, 4));
        assertThat(assertions.expression("INTERVAL '5 10' DAY(4) TO HOUR")).hasType(createIntervalDayTimeType(DAY, HOUR, 4));

        // a derived interval takes the field-maximum leading precision
        assertThat(assertions.operator(ADD, "INTERVAL '3' SECOND", "INTERVAL '3' SECOND")).hasType(createIntervalDayTimeType(SECOND, SECOND, 13, 0));

        // an out-of-range explicit precision is rejected
        assertThatThrownBy(assertions.expression("INTERVAL '5' DAY(10)")::evaluate)
                .hasMessageContaining("INTERVAL day leading precision must be in range [1, 9]: 10");

        // a value that does not fit an explicit leading precision is rejected
        assertThat(assertions.expression("INTERVAL '99' DAY(2)")).hasType(createIntervalDayTimeType(DAY, DAY, 2));
        assertTrinoExceptionThrownBy(assertions.expression("INTERVAL '340' DAY(2)")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessageContaining("interval field overflow: 340 day does not fit a leading precision of 2");
    }

    @Test
    public void testFractionalSecondsPrecision()
    {
        // an explicit fractional-seconds precision (the second argument of SECOND) rounds the value
        assertThat(assertions.expression("INTERVAL '1.5678' SECOND(13, 2)"))
                .hasType(createIntervalDayTimeType(SECOND, SECOND, 13, 2))
                .isEqualTo(interval(0, 0, 0, 1, 570));

        // rounding is half-up
        assertThat(assertions.expression("INTERVAL '1.5' SECOND(1, 0)"))
                .hasType(createIntervalDayTimeType(SECOND, SECOND, 1, 0))
                .isEqualTo(interval(0, 0, 0, 2, 0));

        // a composite qualifier carries the fractional precision on the trailing SECOND
        assertThat(assertions.expression("INTERVAL '1 02:03:04.5678' DAY(1) TO SECOND(3)"))
                .hasType(createIntervalDayTimeType(DAY, SECOND, 1, 3))
                .isEqualTo(interval(1, 2, 3, 4, 568));

        // a bare trailing SECOND keeps the implicit fractional precision of 6
        assertThat(assertions.expression("INTERVAL '1.123456' SECOND"))
                .hasType(createIntervalDayTimeType(SECOND, SECOND, 1, 6));

        // the value renders with exactly the declared number of fractional digits
        assertThat(assertions.expression("CAST(INTERVAL '1.5678' SECOND(13, 2) AS VARCHAR)")).isEqualTo("0 00:00:01.57");
        assertThat(assertions.expression("CAST(INTERVAL '1.5' SECOND(13, 0) AS VARCHAR)")).isEqualTo("0 00:00:02");
        // a half tie rounds toward positive infinity, so a negative tie rounds toward zero
        assertThat(assertions.expression("CAST(INTERVAL '-1.5' SECOND(13, 0) AS VARCHAR)")).isEqualTo("-0 00:00:01");

        // an explicit cast narrows the value to the target fractional precision
        assertThat(assertions.expression("CAST(INTERVAL '1.5678' SECOND AS INTERVAL SECOND(13, 1))"))
                .hasType(createIntervalDayTimeType(SECOND, SECOND, 13, 1))
                .isEqualTo(interval(0, 0, 0, 1, 600));

        // a fractional-seconds precision above 6 uses the picosecond long form
        assertThat(assertions.expression("INTERVAL '1.123456789' SECOND(13, 9)"))
                .hasType(createIntervalDayTimeType(SECOND, SECOND, 13, 9))
                .isEqualTo(new SqlIntervalDayTime(1_123_456, 789_000, 9));
        assertThat(assertions.expression("INTERVAL '1 02:03:04.123456789012' DAY(1) TO SECOND(12)"))
                .hasType(createIntervalDayTimeType(DAY, SECOND, 1, 12))
                .isEqualTo(new SqlIntervalDayTime(93784_123456L, 789_012, 12));
        // a value beyond the declared picosecond precision is rounded
        assertThat(assertions.expression("INTERVAL '1.1234567894' SECOND(13, 9)"))
                .hasType(createIntervalDayTimeType(SECOND, SECOND, 13, 9))
                .isEqualTo(new SqlIntervalDayTime(1_123_456, 789_000, 9));
        // a tie at the picosecond boundary rounds toward positive infinity: the positive tie rounds up, the negative one toward zero
        assertThat(assertions.expression("INTERVAL '0.0000000005' SECOND(13, 9)"))
                .hasType(createIntervalDayTimeType(SECOND, SECOND, 13, 9))
                .isEqualTo(new SqlIntervalDayTime(0, 1_000, 9));
        assertThat(assertions.expression("INTERVAL '-0.0000000005' SECOND(13, 9)"))
                .hasType(createIntervalDayTimeType(SECOND, SECOND, 13, 9))
                .isEqualTo(new SqlIntervalDayTime(0, 0, 9));

        // a floored negative long-form value whose leading field lands exactly on a field boundary still
        // fits its declared leading precision — the sub-microsecond fraction keeps the leading field
        // measured toward zero (regression: -99.999999999999 SECOND(2) spuriously overflowed the field)
        assertThat(assertions.expression("INTERVAL '-99.999999999999' SECOND(2, 12)"))
                .hasType(createIntervalDayTimeType(SECOND, SECOND, 2, 12))
                .isEqualTo(new SqlIntervalDayTime(-100_000_000, 1, 12));
    }

    @Test
    public void testFractionalSecondsCast()
    {
        // widening a short interval to a long fractional precision adds zero picoseconds
        assertThat(assertions.expression("CAST(INTERVAL '1.5' SECOND(13, 2) AS INTERVAL SECOND(13, 9))"))
                .hasType(createIntervalDayTimeType(SECOND, SECOND, 13, 9))
                .isEqualTo(new SqlIntervalDayTime(1_500_000, 0, 9));

        // narrowing a long interval below microsecond resolution drops the sub-microsecond fraction
        assertThat(assertions.expression("CAST(INTERVAL '1.123456789' SECOND(13, 9) AS INTERVAL SECOND(13, 2))"))
                .hasType(createIntervalDayTimeType(SECOND, SECOND, 13, 2))
                .isEqualTo(interval(0, 0, 0, 1, 120));

        // narrowing to microsecond resolution rounds the microseconds half-up by the sub-microsecond fraction
        assertThat(assertions.expression("CAST(INTERVAL '1.9999999' SECOND(13, 7) AS INTERVAL SECOND(13, 6))"))
                .hasType(createIntervalDayTimeType(SECOND, SECOND, 13, 6))
                .isEqualTo(interval(0, 0, 0, 2, 0));

        // narrowing one long precision to another rounds the picoseconds
        assertThat(assertions.expression("CAST(INTERVAL '1.123456789012' SECOND(13, 12) AS INTERVAL SECOND(13, 9))"))
                .hasType(createIntervalDayTimeType(SECOND, SECOND, 13, 9))
                .isEqualTo(new SqlIntervalDayTime(1_123_456, 789_000, 9));

        // a long interval round-trips through a character string keeping its picoseconds
        assertThat(assertions.expression("CAST(INTERVAL '1.123456789' SECOND(13, 9) AS VARCHAR)"))
                .hasType(VARCHAR)
                .isEqualTo("0 00:00:01.123456789");
        assertThat(assertions.expression("CAST('1.123456789' AS INTERVAL SECOND(13, 9))"))
                .hasType(createIntervalDayTimeType(SECOND, SECOND, 13, 9))
                .isEqualTo(new SqlIntervalDayTime(1_123_456, 789_000, 9));

        // a cast to a number reads the whole-field count, ignoring the sub-second fraction
        assertThat(assertions.expression("CAST(INTERVAL '90.123456789' SECOND(13, 9) AS BIGINT)")).isEqualTo(90L);
    }

    @Test
    public void testFractionalSecondsAggregation()
    {
        // sum keeps the input's fractional-seconds precision and carries the picoseconds
        assertThat(assertions.query("SELECT sum(x) FROM (VALUES INTERVAL '0 00:00:01.000000001' DAY(9) TO SECOND(9), INTERVAL '0 00:00:02.000000002' DAY(9) TO SECOND(9)) t(x)"))
                .matches("VALUES INTERVAL '0 00:00:03.000000003' DAY(9) TO SECOND(9)");
        assertThat(assertions.query("SELECT sum(x) FROM (VALUES INTERVAL '0 00:00:00.999999999' DAY(9) TO SECOND(9), INTERVAL '0 00:00:00.000000001' DAY(9) TO SECOND(9)) t(x)"))
                .matches("VALUES INTERVAL '0 00:00:01' DAY(9) TO SECOND(9)");

        // a picosecond precision below microsecond resolution still sums losslessly
        assertThat(assertions.query("SELECT sum(x) FROM (VALUES INTERVAL '0 00:00:01.123456789012' DAY(9) TO SECOND(12), INTERVAL '0 00:00:00.000000000001' DAY(9) TO SECOND(12)) t(x)"))
                .matches("VALUES INTERVAL '0 00:00:01.123456789013' DAY(9) TO SECOND(12)");

        // avg accumulates the picoseconds in 128-bit precision and rounds the quotient to the precision
        assertThat(assertions.query("SELECT avg(x) FROM (VALUES INTERVAL '0 00:00:01.000000000' DAY(9) TO SECOND(9), INTERVAL '0 00:00:02.000000002' DAY(9) TO SECOND(9)) t(x)"))
                .matches("VALUES INTERVAL '0 00:00:01.500000001' DAY(9) TO SECOND(9)");
        assertThat(assertions.query("SELECT avg(x) FROM (VALUES INTERVAL '0 00:00:00.000000001' DAY(9) TO SECOND(9), INTERVAL '0 00:00:00.000000002' DAY(9) TO SECOND(9)) t(x)"))
                .matches("VALUES INTERVAL '0 00:00:00.000000002' DAY(9) TO SECOND(9)");

        // sum and avg of negative intervals carry the sign through the picosecond borrow
        assertThat(assertions.query("SELECT sum(x) FROM (VALUES INTERVAL '-0 00:00:01.000000001' DAY(9) TO SECOND(9), INTERVAL '-0 00:00:02.000000002' DAY(9) TO SECOND(9)) t(x)"))
                .matches("VALUES INTERVAL '-0 00:00:03.000000003' DAY(9) TO SECOND(9)");
        assertThat(assertions.query("SELECT avg(x) FROM (VALUES INTERVAL '-0 00:00:00.000000002' DAY(9) TO SECOND(9), INTERVAL '-0 00:00:00.000000004' DAY(9) TO SECOND(9)) t(x)"))
                .matches("VALUES INTERVAL '-0 00:00:00.000000003' DAY(9) TO SECOND(9)");

        // a single interval already exceeds the 64-bit picosecond range (~107 days), so avg's 128-bit accumulator is load-bearing
        assertThat(assertions.query("SELECT avg(x) FROM (VALUES INTERVAL '200 00:00:00.000000000' DAY(9) TO SECOND(9), INTERVAL '100 00:00:00.000000000' DAY(9) TO SECOND(9)) t(x)"))
                .matches("VALUES INTERVAL '150 00:00:00.000000000' DAY(9) TO SECOND(9)");

        // empty input yields null
        assertThat(assertions.query("SELECT sum(x) FROM (SELECT INTERVAL '0 00:00:01.000000001' DAY(9) TO SECOND(9) WHERE false) t(x)"))
                .matches("VALUES CAST(NULL AS INTERVAL DAY(9) TO SECOND(9))");
    }

    private static SqlIntervalDayTime interval(int day, int hour, int minute, int second, int milliseconds)
    {
        return new SqlIntervalDayTime(day, hour, minute, second, milliseconds);
    }
}

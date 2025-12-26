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
package io.trino.type;

import io.trino.sql.ExpressionFormatter;
import io.trino.sql.query.QueryAssertions;
import io.trino.sql.query.QueryAssertions.ExpressionAssertProvider.Result;
import io.trino.sql.tree.IntervalLiteral;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.time.Duration;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.function.OperatorType.DIVIDE;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.INDETERMINATE;
import static io.trino.spi.function.OperatorType.LESS_THAN;
import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.trino.spi.function.OperatorType.MULTIPLY;
import static io.trino.spi.function.OperatorType.NEGATION;
import static io.trino.spi.function.OperatorType.SUBTRACT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.util.DateTimeUtils.formatDayTimeInterval;
import static java.util.concurrent.TimeUnit.DAYS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestIntervalDayTime
{
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
    public void testObject()
    {
        assertThat(new SqlIntervalDayTime(12, 10, 45, 32, 123)).isEqualTo(new SqlIntervalDayTime(1_075_532_123));
        assertThat(new SqlIntervalDayTime(-12, -10, -45, -32, -123)).isEqualTo(new SqlIntervalDayTime(-1_075_532_123));

        assertThat(new SqlIntervalDayTime(30, 0, 0, 0, 0)).isEqualTo(new SqlIntervalDayTime(DAYS.toMillis(30)));
        assertThat(new SqlIntervalDayTime(-30, 0, 0, 0, 0)).isEqualTo(new SqlIntervalDayTime(-DAYS.toMillis(30)));

        assertThat(new SqlIntervalDayTime(90, 0, 0, 0, 0)).isEqualTo(new SqlIntervalDayTime(DAYS.toMillis(90)));
        assertThat(new SqlIntervalDayTime(-90, 0, 0, 0, 0)).isEqualTo(new SqlIntervalDayTime(-DAYS.toMillis(90)));
    }

    @Test
    public void testAdd()
    {
        assertThat(assertions.operator(ADD, "INTERVAL '3' SECOND", "INTERVAL '3' SECOND"))
                .matches("INTERVAL '6' SECOND");

        assertThat(assertions.operator(ADD, "INTERVAL '6' DAY", "INTERVAL '6' DAY"))
                .matches("INTERVAL '12' DAY");

        assertThat(assertions.operator(ADD, "INTERVAL '3' SECOND", "INTERVAL '6' DAY"))
                .matches("INTERVAL '6 00:00:03.000' DAY TO SECOND");
    }

    @Test
    public void testSubtract()
    {
        assertThat(assertions.operator(SUBTRACT, "INTERVAL '6' SECOND", "INTERVAL '3' SECOND"))
                .matches("INTERVAL '3' SECOND");

        assertThat(assertions.operator(SUBTRACT, "INTERVAL '9' DAY", "INTERVAL '6' DAY"))
                .matches("INTERVAL '3' DAY");

        assertThat(assertions.operator(SUBTRACT, "INTERVAL '3' SECOND", "INTERVAL '6' DAY"))
                .matches("INTERVAL '-5 23:59:57.000' DAY TO SECOND");
    }

    @Test
    public void testMultiply()
    {
        assertThat(assertions.operator(MULTIPLY, "INTERVAL '6' SECOND", "2"))
                .matches("INTERVAL '12' SECOND");

        assertThat(assertions.operator(MULTIPLY, "2", "INTERVAL '6' SECOND"))
                .matches("INTERVAL '12' SECOND");

        assertThat(assertions.operator(MULTIPLY, "INTERVAL '1' SECOND", "2.5"))
                .matches("INTERVAL '2.5' SECOND");

        assertThat(assertions.operator(MULTIPLY, "2.5", "INTERVAL '1' SECOND"))
                .matches("INTERVAL '2.5' SECOND");

        assertThat(assertions.operator(MULTIPLY, "INTERVAL '6' DAY", "2"))
                .matches("INTERVAL '12' DAY");

        assertThat(assertions.operator(MULTIPLY, "2", "INTERVAL '6' DAY"))
                .matches("INTERVAL '12' DAY");

        assertThat(assertions.operator(MULTIPLY, "INTERVAL '1' DAY", "2.5"))
                .matches("INTERVAL '2 12:00:00.000' DAY TO SECOND");

        assertThat(assertions.operator(MULTIPLY, "2.5", "INTERVAL '1' DAY"))
                .matches("INTERVAL '2 12:00:00.000' DAY TO SECOND");

        assertTrinoExceptionThrownBy(assertions.operator(MULTIPLY, "INTERVAL '6' SECOND", "nan()")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertTrinoExceptionThrownBy(assertions.operator(MULTIPLY, "nan()", "INTERVAL '6' DAY")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testDivide()
    {
        assertThat(assertions.operator(DIVIDE, "INTERVAL '3' SECOND", "2"))
                .matches("INTERVAL '1.5' SECOND");

        assertThat(assertions.operator(DIVIDE, "INTERVAL '6' SECOND", "2.5"))
                .matches("INTERVAL '2.4' SECOND");

        assertThat(assertions.operator(DIVIDE, "INTERVAL '3' DAY", "2"))
                .matches("INTERVAL '1 12:00:00.000' DAY TO SECOND");

        assertThat(assertions.operator(DIVIDE, "INTERVAL '4' DAY", "2.5"))
                .matches("INTERVAL '1 14:24:00.000' DAY TO SECOND");

        assertTrinoExceptionThrownBy(assertions.operator(DIVIDE, "INTERVAL '6' SECOND", "nan()")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertTrinoExceptionThrownBy(assertions.operator(DIVIDE, "INTERVAL '6' DAY", "nan()")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertTrinoExceptionThrownBy(assertions.operator(DIVIDE, "INTERVAL '6' SECOND", "0E0")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertTrinoExceptionThrownBy(assertions.operator(DIVIDE, "INTERVAL '6' DAY", "0")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testNegation()
    {
        assertThat(assertions.operator(NEGATION, "INTERVAL '3' SECOND"))
                .matches("INTERVAL '-3' SECOND");

        assertThat(assertions.operator(NEGATION, "INTERVAL '6' DAY"))
                .matches("INTERVAL '-6' DAY");
    }

    @Test
    public void testEqual()
    {
        assertThat(assertions.operator(EQUAL, "INTERVAL '3' SECOND", "INTERVAL '3' SECOND"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "INTERVAL '6' DAY", "INTERVAL '6' DAY"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "INTERVAL '3' SECOND", "INTERVAL '4' SECOND"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "INTERVAL '7' DAY", "INTERVAL '6' DAY"))
                .isEqualTo(false);
    }

    @Test
    public void testNotEqual()
    {
        assertThat(assertions.expression("a <> b")
                .binding("a", "INTERVAL '3' SECOND")
                .binding("b", "INTERVAL '4' SECOND"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <> b")
                .binding("a", "INTERVAL '6' DAY")
                .binding("b", "INTERVAL '7' DAY"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <> b")
                .binding("a", "INTERVAL '3' SECOND")
                .binding("b", "INTERVAL '3' SECOND"))
                .isEqualTo(false);

        assertThat(assertions.expression("a <> b")
                .binding("a", "INTERVAL '6' DAY")
                .binding("b", "INTERVAL '6' DAY"))
                .isEqualTo(false);
    }

    @Test
    public void testLessThan()
    {
        assertThat(assertions.operator(LESS_THAN, "INTERVAL '3' SECOND", "INTERVAL '4' SECOND"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "INTERVAL '6' DAY", "INTERVAL '7' DAY"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "INTERVAL '3' SECOND", "INTERVAL '3' SECOND"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "INTERVAL '3' SECOND", "INTERVAL '2' SECOND"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "INTERVAL '6' DAY", "INTERVAL '6' DAY"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "INTERVAL '6' DAY", "INTERVAL '5' DAY"))
                .isEqualTo(false);
    }

    @Test
    public void testLessThanOrEqual()
    {
        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "INTERVAL '3' SECOND", "INTERVAL '4' SECOND"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "INTERVAL '3' SECOND", "INTERVAL '3' SECOND"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "INTERVAL '6' DAY", "INTERVAL '6' DAY"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "INTERVAL '6' DAY", "INTERVAL '7' DAY"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "INTERVAL '3' SECOND", "INTERVAL '2' SECOND"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "INTERVAL '6' DAY", "INTERVAL '5' DAY"))
                .isEqualTo(false);
    }

    @Test
    public void testGreaterThan()
    {
        assertThat(assertions.expression("a > b")
                .binding("a", "INTERVAL '3' SECOND")
                .binding("b", "INTERVAL '2' SECOND"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "INTERVAL '6' DAY")
                .binding("b", "INTERVAL '5' DAY"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "INTERVAL '3' SECOND")
                .binding("b", "INTERVAL '3' SECOND"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "INTERVAL '3' SECOND")
                .binding("b", "INTERVAL '4' SECOND"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "INTERVAL '6' DAY")
                .binding("b", "INTERVAL '6' DAY"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "INTERVAL '6' DAY")
                .binding("b", "INTERVAL '7' DAY"))
                .isEqualTo(false);
    }

    @Test
    public void testGreaterThanOrEqual()
    {
        assertThat(assertions.expression("a >= b")
                .binding("a", "INTERVAL '3' SECOND")
                .binding("b", "INTERVAL '2' SECOND"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "INTERVAL '3' SECOND")
                .binding("b", "INTERVAL '3' SECOND"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "INTERVAL '6' DAY")
                .binding("b", "INTERVAL '5' DAY"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "INTERVAL '6' DAY")
                .binding("b", "INTERVAL '6' DAY"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "INTERVAL '3' SECOND")
                .binding("b", "INTERVAL '4' SECOND"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "INTERVAL '6' DAY")
                .binding("b", "INTERVAL '7' DAY"))
                .isEqualTo(false);
    }

    @Test
    public void testBetween()
    {
        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "INTERVAL '3' SECOND")
                .binding("low", "INTERVAL '2' SECOND")
                .binding("high", "INTERVAL '4' SECOND"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "INTERVAL '3' SECOND")
                .binding("low", "INTERVAL '3' SECOND")
                .binding("high", "INTERVAL '4' SECOND"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "INTERVAL '3' SECOND")
                .binding("low", "INTERVAL '2' SECOND")
                .binding("high", "INTERVAL '3' SECOND"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "INTERVAL '3' SECOND")
                .binding("low", "INTERVAL '3' SECOND")
                .binding("high", "INTERVAL '3' SECOND"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "INTERVAL '3' SECOND")
                .binding("low", "INTERVAL '4' SECOND")
                .binding("high", "INTERVAL '5' SECOND"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "INTERVAL '3' SECOND")
                .binding("low", "INTERVAL '1' SECOND")
                .binding("high", "INTERVAL '2' SECOND"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "INTERVAL '3' SECOND")
                .binding("low", "INTERVAL '4' SECOND")
                .binding("high", "INTERVAL '2' SECOND"))
                .isEqualTo(false);
    }

    @Test
    public void testCastToVarchar()
    {
        assertThat(assertions.expression("CAST(a AS varchar)")
                .binding("a", "INTERVAL '12 10:45:32.123' DAY TO SECOND"))
                .hasType(VARCHAR)
                .isEqualTo("12 10:45:32.123");

        assertThat(assertions.expression("CAST(a AS varchar)")
                .binding("a", "INTERVAL '12 10:45:32.123' DAY TO SECOND"))
                .hasType(VARCHAR)
                .isEqualTo("12 10:45:32.123");

        assertThat(assertions.expression("CAST(a AS varchar)")
                .binding("a", "INTERVAL '12 10:45:32.12' DAY TO SECOND"))
                .hasType(VARCHAR)
                .isEqualTo("12 10:45:32.120");

        assertThat(assertions.expression("CAST(a AS varchar)")
                .binding("a", "INTERVAL '12 10:45:32' DAY TO SECOND"))
                .hasType(VARCHAR)
                .isEqualTo("12 10:45:32.000");

        assertThat(assertions.expression("CAST(a AS varchar)")
                .binding("a", "INTERVAL '12 10:45' DAY TO SECOND"))
                .hasType(VARCHAR)
                .isEqualTo("12 10:45:00.000");

        assertThat(assertions.expression("CAST(a AS varchar)")
                .binding("a", "INTERVAL '12 10' DAY TO SECOND"))
                .hasType(VARCHAR)
                .isEqualTo("12 10:00:00.000");

        assertThat(assertions.expression("CAST(a AS varchar)")
                .binding("a", "INTERVAL '12' DAY TO SECOND"))
                .hasType(VARCHAR)
                .isEqualTo("12 00:00:00.000");

        assertThat(assertions.expression("CAST(a AS varchar)")
                .binding("a", "INTERVAL '12 10:45' DAY TO MINUTE"))
                .hasType(VARCHAR)
                .isEqualTo("12 10:45:00.000");

        assertThat(assertions.expression("CAST(a AS varchar)")
                .binding("a", "INTERVAL '12 10' DAY TO MINUTE"))
                .hasType(VARCHAR)
                .isEqualTo("12 10:00:00.000");

        assertThat(assertions.expression("CAST(a AS varchar)")
                .binding("a", "INTERVAL '12' DAY TO MINUTE"))
                .hasType(VARCHAR)
                .isEqualTo("12 00:00:00.000");

        assertThat(assertions.expression("CAST(a AS varchar)")
                .binding("a", "INTERVAL '12 10' DAY TO HOUR"))
                .hasType(VARCHAR)
                .isEqualTo("12 10:00:00.000");

        assertThat(assertions.expression("CAST(a AS varchar)")
                .binding("a", "INTERVAL '12' DAY TO HOUR"))
                .hasType(VARCHAR)
                .isEqualTo("12 00:00:00.000");

        assertThat(assertions.expression("CAST(a AS varchar)")
                .binding("a", "INTERVAL '12' DAY"))
                .hasType(VARCHAR)
                .isEqualTo("12 00:00:00.000");

        assertThat(assertions.expression("CAST(a AS varchar)")
                .binding("a", "INTERVAL '10:45:32.123' HOUR TO SECOND"))
                .hasType(VARCHAR)
                .isEqualTo("0 10:45:32.123");

        assertThat(assertions.expression("CAST(a AS varchar)")
                .binding("a", "INTERVAL '10:45:32.12' HOUR TO SECOND"))
                .hasType(VARCHAR)
                .isEqualTo("0 10:45:32.120");

        assertThat(assertions.expression("CAST(a AS varchar)")
                .binding("a", "INTERVAL '10:45:32' HOUR TO SECOND"))
                .hasType(VARCHAR)
                .isEqualTo("0 10:45:32.000");

        assertThat(assertions.expression("CAST(a AS varchar)")
                .binding("a", "INTERVAL '10:45' HOUR TO SECOND"))
                .hasType(VARCHAR)
                .isEqualTo("0 10:45:00.000");

        assertThat(assertions.expression("CAST(a AS varchar)")
                .binding("a", "INTERVAL '10' HOUR TO SECOND"))
                .hasType(VARCHAR)
                .isEqualTo("0 10:00:00.000");

        assertThat(assertions.expression("CAST(a AS varchar)")
                .binding("a", "INTERVAL '10:45' HOUR TO MINUTE"))
                .hasType(VARCHAR)
                .isEqualTo("0 10:45:00.000");

        assertThat(assertions.expression("CAST(a AS varchar)")
                .binding("a", "INTERVAL '10' HOUR TO MINUTE"))
                .hasType(VARCHAR)
                .isEqualTo("0 10:00:00.000");

        assertThat(assertions.expression("CAST(a AS varchar)")
                .binding("a", "INTERVAL '10' HOUR"))
                .hasType(VARCHAR)
                .isEqualTo("0 10:00:00.000");

        assertThat(assertions.expression("CAST(a AS varchar)")
                .binding("a", "INTERVAL '45:32.123' MINUTE TO SECOND"))
                .hasType(VARCHAR)
                .isEqualTo("0 00:45:32.123");

        assertThat(assertions.expression("CAST(a AS varchar)")
                .binding("a", "INTERVAL '45:32.12' MINUTE TO SECOND"))
                .hasType(VARCHAR)
                .isEqualTo("0 00:45:32.120");

        assertThat(assertions.expression("CAST(a AS varchar)")
                .binding("a", "INTERVAL '45:32' MINUTE TO SECOND"))
                .hasType(VARCHAR)
                .isEqualTo("0 00:45:32.000");

        assertThat(assertions.expression("CAST(a AS varchar)")
                .binding("a", "INTERVAL '45' MINUTE TO SECOND"))
                .hasType(VARCHAR)
                .isEqualTo("0 00:45:00.000");

        assertThat(assertions.expression("CAST(a AS varchar)")
                .binding("a", "INTERVAL '45' MINUTE"))
                .hasType(VARCHAR)
                .isEqualTo("0 00:45:00.000");

        assertThat(assertions.expression("CAST(a AS varchar)")
                .binding("a", "INTERVAL '32.123' SECOND"))
                .hasType(VARCHAR)
                .isEqualTo("0 00:00:32.123");

        assertThat(assertions.expression("CAST(a AS varchar)")
                .binding("a", "INTERVAL '32.12' SECOND"))
                .hasType(VARCHAR)
                .isEqualTo("0 00:00:32.120");

        assertThat(assertions.expression("CAST(a AS varchar)")
                .binding("a", "INTERVAL '32' SECOND"))
                .hasType(VARCHAR)
                .isEqualTo("0 00:00:32.000");
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
    void testIntervalDayTimeRoundTrip()
    {
        testIntervalDayTimeRoundTrip("INTERVAL '0' SECOND", "INTERVAL '0 0:00:00' DAY TO SECOND");
        testIntervalDayTimeRoundTrip("INTERVAL -'0' SECOND", "INTERVAL '0 0:00:00' DAY TO SECOND");
        testIntervalDayTimeRoundTrip("INTERVAL '0.000' SECOND", "INTERVAL '0 0:00:00' DAY TO SECOND");
        testIntervalDayTimeRoundTrip("INTERVAL '0.4' SECOND", "INTERVAL '0 0:00:00.400' DAY TO SECOND");
        testIntervalDayTimeRoundTrip("INTERVAL '0.04' SECOND", "INTERVAL '0 0:00:00.040' DAY TO SECOND");
        testIntervalDayTimeRoundTrip("INTERVAL '0.040' SECOND", "INTERVAL '0 0:00:00.040' DAY TO SECOND");
        testIntervalDayTimeRoundTrip("INTERVAL '45' SECOND", "INTERVAL '0 0:00:45' DAY TO SECOND");
        testIntervalDayTimeRoundTrip("INTERVAL -'45' SECOND", "INTERVAL -'0 0:00:45' DAY TO SECOND");
        testIntervalDayTimeRoundTrip("INTERVAL '0.555' SECOND", "INTERVAL '0 0:00:00.555' DAY TO SECOND");
        testIntervalDayTimeRoundTrip("INTERVAL '59.999' SECOND", "INTERVAL '0 0:00:59.999' DAY TO SECOND");
        testIntervalDayTimeRoundTrip("INTERVAL '60' SECOND", "INTERVAL '0 0:01:00' DAY TO SECOND");
        testIntervalDayTimeRoundTrip("INTERVAL '61' SECOND", "INTERVAL '0 0:01:01' DAY TO SECOND");
        testIntervalDayTimeRoundTrip("INTERVAL '3661' SECOND", "INTERVAL '0 1:01:01' DAY TO SECOND");
        testIntervalDayTimeRoundTrip("INTERVAL '90061' SECOND", "INTERVAL '1 1:01:01' DAY TO SECOND");

        testIntervalDayTimeRoundTrip("INTERVAL '0' MINUTE", "INTERVAL '0 0:00:00' DAY TO SECOND");
        testIntervalDayTimeRoundTrip("INTERVAL -'0' MINUTE", "INTERVAL '0 0:00:00' DAY TO SECOND");
        testIntervalDayTimeRoundTrip("INTERVAL '25' MINUTE", "INTERVAL '0 0:25:00' DAY TO SECOND");
        testIntervalDayTimeRoundTrip("INTERVAL -'25' MINUTE", "INTERVAL -'0 0:25:00' DAY TO SECOND");
        testIntervalDayTimeRoundTrip("INTERVAL '15:30' MINUTE TO SECOND", "INTERVAL '0 0:15:30' DAY TO SECOND");
        testIntervalDayTimeRoundTrip("INTERVAL '59:00.999' MINUTE TO SECOND", "INTERVAL '0 0:59:00.999' DAY TO SECOND");
        testIntervalDayTimeRoundTrip("INTERVAL '60' MINUTE", "INTERVAL '0 1:00:00' DAY TO SECOND");
        testIntervalDayTimeRoundTrip("INTERVAL '61' MINUTE", "INTERVAL '0 1:01:00' DAY TO SECOND");
        testIntervalDayTimeRoundTrip("INTERVAL '1500' MINUTE", "INTERVAL '1 1:00:00' DAY TO SECOND");
        testIntervalDayTimeRoundTrip("INTERVAL '1501' MINUTE", "INTERVAL '1 1:01:00' DAY TO SECOND");

        testIntervalDayTimeRoundTrip("INTERVAL '0' HOUR", "INTERVAL '0 0:00:00' DAY TO SECOND");
        testIntervalDayTimeRoundTrip("INTERVAL -'0' HOUR", "INTERVAL '0 0:00:00' DAY TO SECOND");
        testIntervalDayTimeRoundTrip("INTERVAL '8' HOUR", "INTERVAL '0 8:00:00' DAY TO SECOND");
        testIntervalDayTimeRoundTrip("INTERVAL -'8' HOUR", "INTERVAL -'0 8:00:00' DAY TO SECOND");
        testIntervalDayTimeRoundTrip("INTERVAL '2:45' HOUR TO MINUTE", "INTERVAL '0 2:45:00' DAY TO SECOND");
        testIntervalDayTimeRoundTrip("INTERVAL '2:00:45' HOUR TO SECOND", "INTERVAL '0 2:00:45' DAY TO SECOND");
        testIntervalDayTimeRoundTrip("INTERVAL '1:30:45' HOUR TO SECOND", "INTERVAL '0 1:30:45' DAY TO SECOND");
        testIntervalDayTimeRoundTrip("INTERVAL '1:00:00.999' HOUR TO SECOND", "INTERVAL '0 1:00:00.999' DAY TO SECOND");
        testIntervalDayTimeRoundTrip("INTERVAL '24' HOUR", "INTERVAL '1 0:00:00' DAY TO SECOND");
        testIntervalDayTimeRoundTrip("INTERVAL '25' HOUR", "INTERVAL '1 1:00:00' DAY TO SECOND");
        testIntervalDayTimeRoundTrip("INTERVAL '17520' HOUR", "INTERVAL '730 0:00:00' DAY TO SECOND");

        testIntervalDayTimeRoundTrip("INTERVAL '0' DAY", "INTERVAL '0 0:00:00' DAY TO SECOND");
        testIntervalDayTimeRoundTrip("INTERVAL -'0' DAY", "INTERVAL '0 0:00:00' DAY TO SECOND");
        testIntervalDayTimeRoundTrip("INTERVAL '340' DAY", "INTERVAL '340 0:00:00' DAY TO SECOND");
        testIntervalDayTimeRoundTrip("INTERVAL -'340' DAY", "INTERVAL -'340 0:00:00' DAY TO SECOND");
        testIntervalDayTimeRoundTrip("INTERVAL '2 6' DAY TO HOUR", "INTERVAL '2 6:00:00' DAY TO SECOND");
        testIntervalDayTimeRoundTrip("INTERVAL '3 0:30' DAY TO MINUTE", "INTERVAL '3 0:30:00' DAY TO SECOND");
        testIntervalDayTimeRoundTrip("INTERVAL '3 12:30' DAY TO MINUTE", "INTERVAL '3 12:30:00' DAY TO SECOND");
        testIntervalDayTimeRoundTrip("INTERVAL '1 0:00:15' DAY TO SECOND", "INTERVAL '1 0:00:15' DAY TO SECOND");
        testIntervalDayTimeRoundTrip("INTERVAL '1 4:20:15' DAY TO SECOND", "INTERVAL '1 4:20:15' DAY TO SECOND");
        testIntervalDayTimeRoundTrip("INTERVAL '1 0:00:00.999' DAY TO SECOND", "INTERVAL '1 0:00:00.999' DAY TO SECOND");
        testIntervalDayTimeRoundTrip("INTERVAL '1 23:59:59.999' DAY TO SECOND", "INTERVAL '1 23:59:59.999' DAY TO SECOND");
    }

    private void testIntervalDayTimeRoundTrip(@Language("SQL") String input, @Language("SQL") String expectedFormatted)
    {
        Result evaluatedResult = assertions.expression(input).evaluate();
        assertThat(evaluatedResult.type()).isEqualTo(IntervalDayTimeType.INTERVAL_DAY_TIME);
        SqlIntervalDayTime originalInterval = (SqlIntervalDayTime) evaluatedResult.value();

        Duration duration = Duration.ofMillis(originalInterval.getMillis());
        IntervalLiteral formattedLiteral = formatDayTimeInterval(duration);
        String formatted = ExpressionFormatter.formatExpression(formattedLiteral);
        assertThat(formatted).isEqualTo(expectedFormatted);

        Result reparsedResult = assertions.expression(formatted).evaluate();
        SqlIntervalDayTime reparsedInterval = (SqlIntervalDayTime) reparsedResult.value();
        assertThat(reparsedInterval).isEqualTo(originalInterval);
    }
}

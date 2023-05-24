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

import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

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
import static java.util.concurrent.TimeUnit.DAYS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.testng.Assert.assertEquals;

@TestInstance(PER_CLASS)
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
        assertEquals(new SqlIntervalDayTime(12, 10, 45, 32, 123), new SqlIntervalDayTime(1_075_532_123));
        assertEquals(new SqlIntervalDayTime(-12, -10, -45, -32, -123), new SqlIntervalDayTime(-1_075_532_123));

        assertEquals(new SqlIntervalDayTime(30, 0, 0, 0, 0), new SqlIntervalDayTime(DAYS.toMillis(30)));
        assertEquals(new SqlIntervalDayTime(-30, 0, 0, 0, 0), new SqlIntervalDayTime(-DAYS.toMillis(30)));

        assertEquals(new SqlIntervalDayTime(90, 0, 0, 0, 0), new SqlIntervalDayTime(DAYS.toMillis(90)));
        assertEquals(new SqlIntervalDayTime(-90, 0, 0, 0, 0), new SqlIntervalDayTime(-DAYS.toMillis(90)));
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

        assertTrinoExceptionThrownBy(() -> assertions.operator(MULTIPLY, "INTERVAL '6' SECOND", "nan()").evaluate())
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.operator(MULTIPLY, "nan()", "INTERVAL '6' DAY").evaluate())
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

        assertTrinoExceptionThrownBy(() -> assertions.operator(DIVIDE, "INTERVAL '6' SECOND", "nan()").evaluate())
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.operator(DIVIDE, "INTERVAL '6' DAY", "nan()").evaluate())
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.operator(DIVIDE, "INTERVAL '6' SECOND", "0E0").evaluate())
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.operator(DIVIDE, "INTERVAL '6' DAY", "0").evaluate())
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
}

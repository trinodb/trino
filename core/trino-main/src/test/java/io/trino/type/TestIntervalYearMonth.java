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
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.testng.Assert.assertEquals;

@TestInstance(PER_CLASS)
public class TestIntervalYearMonth
{
    private static final int MAX_SHORT = Short.MAX_VALUE;

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
        assertEquals(new SqlIntervalYearMonth(3, 11), new SqlIntervalYearMonth(47));
        assertEquals(new SqlIntervalYearMonth(-3, -11), new SqlIntervalYearMonth(-47));

        assertEquals(new SqlIntervalYearMonth(MAX_SHORT, 0), new SqlIntervalYearMonth(393_204));
        assertEquals(new SqlIntervalYearMonth(MAX_SHORT, MAX_SHORT), new SqlIntervalYearMonth(425_971));

        assertEquals(new SqlIntervalYearMonth(-MAX_SHORT, 0), new SqlIntervalYearMonth(-393_204));
        assertEquals(new SqlIntervalYearMonth(-MAX_SHORT, -MAX_SHORT), new SqlIntervalYearMonth(-425_971));
    }

    @Test
    public void testAdd()
    {
        assertThat(assertions.operator(ADD, "INTERVAL '3' MONTH", "INTERVAL '3' MONTH"))
                .matches("INTERVAL '6' MONTH");

        assertThat(assertions.operator(ADD, "INTERVAL '6' YEAR", "INTERVAL '6' YEAR"))
                .matches("INTERVAL '12' YEAR");

        assertThat(assertions.operator(ADD, "INTERVAL '3' MONTH", "INTERVAL '6' YEAR"))
                .matches("INTERVAL '6-3' YEAR TO MONTH");
    }

    @Test
    public void testSubtract()
    {
        assertThat(assertions.operator(SUBTRACT, "INTERVAL '6' MONTH", "INTERVAL '3' MONTH"))
                .matches("INTERVAL '3' MONTH");

        assertThat(assertions.operator(SUBTRACT, "INTERVAL '9' YEAR", "INTERVAL '6' YEAR"))
                .matches("INTERVAL '3' YEAR");

        assertThat(assertions.operator(SUBTRACT, "INTERVAL '3' MONTH", "INTERVAL '6' YEAR"))
                .matches("INTERVAL '-5-9' YEAR TO MONTH");
    }

    @Test
    public void testMultiply()
    {
        assertThat(assertions.operator(MULTIPLY, "INTERVAL '6' MONTH", "2"))
                .matches("INTERVAL '12' MONTH");

        assertThat(assertions.operator(MULTIPLY, "2", "INTERVAL '6' MONTH"))
                .matches("INTERVAL '12' MONTH");

        assertThat(assertions.operator(MULTIPLY, "INTERVAL '10' MONTH", "2.5"))
                .matches("INTERVAL '25' MONTH");

        assertThat(assertions.operator(MULTIPLY, "2.5", "INTERVAL '10' MONTH"))
                .matches("INTERVAL '25' MONTH");

        assertThat(assertions.operator(MULTIPLY, "INTERVAL '6' YEAR", "2"))
                .matches("INTERVAL '12' YEAR");

        assertThat(assertions.operator(MULTIPLY, "2", "INTERVAL '6' YEAR"))
                .matches("INTERVAL '12' YEAR");

        assertThat(assertions.operator(MULTIPLY, "INTERVAL '1' YEAR", "2.5"))
                .matches("INTERVAL '2-6' YEAR TO MONTH");

        assertThat(assertions.operator(MULTIPLY, "2.5", "INTERVAL '1' YEAR"))
                .matches("INTERVAL '2-6' YEAR TO MONTH");

        assertTrinoExceptionThrownBy(() -> assertions.operator(MULTIPLY, "INTERVAL '6' MONTH", "nan()").evaluate())
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.operator(MULTIPLY, "nan()", "INTERVAL '6' YEAR").evaluate())
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testDivide()
    {
        assertThat(assertions.operator(DIVIDE, "INTERVAL '30' MONTH", "2"))
                .matches("INTERVAL '15' MONTH");

        assertThat(assertions.operator(DIVIDE, "INTERVAL '60' MONTH", "2.5"))
                .matches("INTERVAL '24' MONTH");

        assertThat(assertions.operator(DIVIDE, "INTERVAL '3' YEAR", "2"))
                .matches("INTERVAL '18' MONTH");

        assertThat(assertions.operator(DIVIDE, "INTERVAL '4' YEAR", "4.8"))
                .matches("INTERVAL '10' MONTH");

        assertTrinoExceptionThrownBy(() -> assertions.operator(DIVIDE, "INTERVAL '6' MONTH", "nan()").evaluate())
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.operator(DIVIDE, "INTERVAL '6' YEAR", "nan()").evaluate())
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.operator(DIVIDE, "INTERVAL '6' MONTH", "0E0").evaluate())
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.operator(DIVIDE, "INTERVAL '6' YEAR", "0").evaluate())
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testNegation()
    {
        assertThat(assertions.operator(NEGATION, "INTERVAL '3' MONTH"))
                .matches("INTERVAL '-3' MONTH");

        assertThat(assertions.operator(NEGATION, "INTERVAL '6' YEAR"))
                .matches("INTERVAL '-72' MONTH");
    }

    @Test
    public void testEqual()
    {
        assertThat(assertions.operator(EQUAL, "INTERVAL '3' MONTH", "INTERVAL '3' MONTH"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "INTERVAL '6' YEAR", "INTERVAL '6' YEAR"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "INTERVAL '3' MONTH", "INTERVAL '4' MONTH"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "INTERVAL '7' YEAR", "INTERVAL '6' YEAR"))
                .isEqualTo(false);
    }

    @Test
    public void testNotEqual()
    {
        assertThat(assertions.expression("a <> b")
                .binding("a", "INTERVAL '3' MONTH")
                .binding("b", "INTERVAL '4' MONTH"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <> b")
                .binding("a", "INTERVAL '6' YEAR")
                .binding("b", "INTERVAL '7' YEAR"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <> b")
                .binding("a", "INTERVAL '3' MONTH")
                .binding("b", "INTERVAL '3' MONTH"))
                .isEqualTo(false);

        assertThat(assertions.expression("a <> b")
                .binding("a", "INTERVAL '6' YEAR")
                .binding("b", "INTERVAL '6' YEAR"))
                .isEqualTo(false);
    }

    @Test
    public void testLessThan()
    {
        assertThat(assertions.operator(LESS_THAN, "INTERVAL '3' MONTH", "INTERVAL '4' MONTH"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "INTERVAL '6' YEAR", "INTERVAL '7' YEAR"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "INTERVAL '3' MONTH", "INTERVAL '3' MONTH"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "INTERVAL '3' MONTH", "INTERVAL '2' MONTH"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "INTERVAL '6' YEAR", "INTERVAL '6' YEAR"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "INTERVAL '6' YEAR", "INTERVAL '5' YEAR"))
                .isEqualTo(false);
    }

    @Test
    public void testLessThanOrEqual()
    {
        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "INTERVAL '3' MONTH", "INTERVAL '4' MONTH"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "INTERVAL '3' MONTH", "INTERVAL '3' MONTH"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "INTERVAL '6' YEAR", "INTERVAL '6' YEAR"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "INTERVAL '6' YEAR", "INTERVAL '7' YEAR"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "INTERVAL '3' MONTH", "INTERVAL '2' MONTH"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "INTERVAL '6' YEAR", "INTERVAL '5' YEAR"))
                .isEqualTo(false);
    }

    @Test
    public void testGreaterThan()
    {
        assertThat(assertions.expression("a > b")
                .binding("a", "INTERVAL '3' MONTH")
                .binding("b", "INTERVAL '2' MONTH"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "INTERVAL '6' YEAR")
                .binding("b", "INTERVAL '5' YEAR"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "INTERVAL '3' MONTH")
                .binding("b", "INTERVAL '3' MONTH"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "INTERVAL '3' MONTH")
                .binding("b", "INTERVAL '4' MONTH"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "INTERVAL '6' YEAR")
                .binding("b", "INTERVAL '6' YEAR"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "INTERVAL '6' YEAR")
                .binding("b", "INTERVAL '7' YEAR"))
                .isEqualTo(false);
    }

    @Test
    public void testGreaterThanOrEqual()
    {
        assertThat(assertions.expression("a >= b")
                .binding("a", "INTERVAL '3' MONTH")
                .binding("b", "INTERVAL '2' MONTH"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "INTERVAL '3' MONTH")
                .binding("b", "INTERVAL '3' MONTH"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "INTERVAL '6' YEAR")
                .binding("b", "INTERVAL '5' YEAR"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "INTERVAL '6' YEAR")
                .binding("b", "INTERVAL '6' YEAR"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "INTERVAL '3' MONTH")
                .binding("b", "INTERVAL '4' MONTH"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "INTERVAL '6' YEAR")
                .binding("b", "INTERVAL '7' YEAR"))
                .isEqualTo(false);
    }

    @Test
    public void testBetween()
    {
        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "INTERVAL '3' MONTH")
                .binding("low", "INTERVAL '2' MONTH")
                .binding("high", "INTERVAL '4' MONTH"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "INTERVAL '3' MONTH")
                .binding("low", "INTERVAL '3' MONTH")
                .binding("high", "INTERVAL '4' MONTH"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "INTERVAL '3' MONTH")
                .binding("low", "INTERVAL '2' MONTH")
                .binding("high", "INTERVAL '3' MONTH"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "INTERVAL '3' MONTH")
                .binding("low", "INTERVAL '3' MONTH")
                .binding("high", "INTERVAL '3' MONTH"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "INTERVAL '3' MONTH")
                .binding("low", "INTERVAL '4' MONTH")
                .binding("high", "INTERVAL '5' MONTH"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "INTERVAL '3' MONTH")
                .binding("low", "INTERVAL '1' MONTH")
                .binding("high", "INTERVAL '2' MONTH"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "INTERVAL '3' MONTH")
                .binding("low", "INTERVAL '4' MONTH")
                .binding("high", "INTERVAL '2' MONTH"))
                .isEqualTo(false);
    }

    @Test
    public void testCastToVarchar()
    {
        assertThat(assertions.expression("CAST(a AS varchar)")
                .binding("a", "INTERVAL '124-30' YEAR TO MONTH"))
                .hasType(VARCHAR)
                .isEqualTo("126-6");

        assertThat(assertions.expression("CAST(a AS varchar)")
                .binding("a", "INTERVAL '124-30' YEAR TO MONTH"))
                .hasType(VARCHAR)
                .isEqualTo("126-6");

        assertThat(assertions.expression("CAST(a AS varchar)")
                .binding("a", "INTERVAL '124' YEAR TO MONTH"))
                .hasType(VARCHAR)
                .isEqualTo("124-0");

        assertThat(assertions.expression("CAST(a AS varchar)")
                .binding("a", "INTERVAL '124' YEAR"))
                .hasType(VARCHAR)
                .isEqualTo("124-0");

        assertThat(assertions.expression("CAST(a AS varchar)")
                .binding("a", "INTERVAL '30' MONTH"))
                .hasType(VARCHAR)
                .isEqualTo("2-6");
    }

    @Test
    public void testIndeterminate()
    {
        assertThat(assertions.operator(INDETERMINATE, "cast(null as INTERVAL YEAR TO MONTH)"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "INTERVAL '124' YEAR TO MONTH"))
                .isEqualTo(false);
    }
}

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
import io.trino.type.SqlIntervalYearMonth;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

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
import static io.trino.spi.type.IntervalField.MONTH;
import static io.trino.spi.type.IntervalField.YEAR;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.IntervalYearMonthType.createIntervalYearMonthType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestIntervalYearMonth
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
        assertThat(assertions.expression("INTERVAL '124-30' YEAR TO MONTH"))
                .isEqualTo(interval(124, 30));

        assertThat(assertions.expression("INTERVAL '124' YEAR TO MONTH"))
                .isEqualTo(interval(124, 0));

        assertThat(assertions.expression("INTERVAL '30' MONTH"))
                .isEqualTo(interval(0, 30));

        assertThat(assertions.expression("INTERVAL '32767' YEAR"))
                .isEqualTo(interval(32767, 0));

        assertThat(assertions.expression("INTERVAL '32767' MONTH"))
                .isEqualTo(interval(0, 32767));

        assertThat(assertions.expression("INTERVAL '32767-32767' YEAR TO MONTH"))
                .isEqualTo(interval(32767, 32767));

        // Invalid literals
        assertThatThrownBy(assertions.expression("INTERVAL '124X' YEAR")::evaluate)
                .hasMessage("line 1:12: Invalid INTERVAL YEAR value: 124X");

        assertThatThrownBy(assertions.expression("INTERVAL '124-30' YEAR")::evaluate)
                .hasMessage("line 1:12: Invalid INTERVAL YEAR value: 124-30");

        assertThatThrownBy(assertions.expression("INTERVAL '124-X' YEAR TO MONTH")::evaluate)
                .hasMessage("line 1:12: Invalid INTERVAL YEAR TO MONTH value: 124-X");

        assertThatThrownBy(assertions.expression("INTERVAL '124--30' YEAR TO MONTH")::evaluate)
                .hasMessage("line 1:12: Invalid INTERVAL YEAR TO MONTH value: 124--30");

        assertThatThrownBy(assertions.expression("INTERVAL '--124--30' YEAR TO MONTH")::evaluate)
                .hasMessage("line 1:12: Invalid INTERVAL YEAR TO MONTH value: --124--30");

        // Invalid qualifiers (YEAR TO xxx)
        assertThatThrownBy(assertions.expression("INTERVAL '32767' YEAR TO YEAR")::evaluate)
                .hasMessage("line 1:37: mismatched input 'YEAR'. Expecting: ')', ',', 'MONTH'");

        assertThatThrownBy(assertions.expression("INTERVAL '124-30' YEAR TO DAY")::evaluate)
                .hasMessage("line 1:38: mismatched input 'DAY'. Expecting: ')', ',', 'MONTH'");

        assertThatThrownBy(assertions.expression("INTERVAL '124-30' YEAR TO HOUR")::evaluate)
                .hasMessage("line 1:38: mismatched input 'HOUR'. Expecting: ')', ',', 'MONTH'");

        assertThatThrownBy(assertions.expression("INTERVAL '124-30' YEAR TO MINUTE")::evaluate)
                .hasMessage("line 1:38: mismatched input 'MINUTE'. Expecting: ')', ',', 'MONTH'");

        assertThatThrownBy(assertions.expression("INTERVAL '124-30' YEAR TO SECOND")::evaluate)
                .hasMessage("line 1:38: mismatched input 'SECOND'. Expecting: ')', ',', 'MONTH'");

        // Invalid qualifiers (MONTH TO xxx)
        assertThatThrownBy(assertions.expression("INTERVAL '30' MONTH TO MONTH")::evaluate)
                .hasMessage("line 1:35: mismatched input 'MONTH'. Expecting: ')', ','");

        assertThatThrownBy(assertions.expression("INTERVAL '124-30' MONTH TO YEAR")::evaluate)
                .hasMessage("line 1:39: mismatched input 'YEAR'. Expecting: ')', ','");

        assertThatThrownBy(assertions.expression("INTERVAL '124-30' MONTH TO DAY")::evaluate)
                .hasMessage("line 1:39: mismatched input 'DAY'. Expecting: ')', ','");

        assertThatThrownBy(assertions.expression("INTERVAL '124-30' MONTH TO HOUR")::evaluate)
                .hasMessage("line 1:39: mismatched input 'HOUR'. Expecting: ')', ','");

        assertThatThrownBy(assertions.expression("INTERVAL '124-30' MONTH TO MINUTE")::evaluate)
                .hasMessage("line 1:39: mismatched input 'MINUTE'. Expecting: ')', ','");

        assertThatThrownBy(assertions.expression("INTERVAL '124-30' MONTH TO SECOND")::evaluate)
                .hasMessage("line 1:39: mismatched input 'SECOND'. Expecting: ')', ','");
    }

    @Test
    public void testObject()
    {
        assertThat(new SqlIntervalYearMonth(3, 11)).isEqualTo(new SqlIntervalYearMonth(47));
        assertThat(new SqlIntervalYearMonth(-3, -11)).isEqualTo(new SqlIntervalYearMonth(-47));

        assertThat(new SqlIntervalYearMonth(Short.MAX_VALUE, 0)).isEqualTo(new SqlIntervalYearMonth(393_204));
        assertThat(new SqlIntervalYearMonth(Short.MAX_VALUE, Short.MAX_VALUE)).isEqualTo(new SqlIntervalYearMonth(425_971));
    }

    @Test
    public void testAdd()
    {
        // adding two intervals yields the union of their fields
        assertThat(assertions.operator(ADD, "INTERVAL '3' MONTH", "INTERVAL '3' MONTH"))
                .hasType(createIntervalYearMonthType(MONTH, MONTH))
                .isEqualTo(new SqlIntervalYearMonth(0, 6));

        assertThat(assertions.operator(ADD, "INTERVAL '6' YEAR", "INTERVAL '6' YEAR"))
                .hasType(createIntervalYearMonthType(YEAR, YEAR))
                .isEqualTo(new SqlIntervalYearMonth(12, 0));

        assertThat(assertions.operator(ADD, "INTERVAL '3' MONTH", "INTERVAL '6' YEAR"))
                .hasType(createIntervalYearMonthType(YEAR, MONTH))
                .isEqualTo(new SqlIntervalYearMonth(6, 3));

        assertTrinoExceptionThrownBy(assertions.operator(
                ADD,
                "INTERVAL '1' MONTH * 2000000000",
                "INTERVAL '1' MONTH * 2000000000")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("interval year to month addition overflow: 2000000000 + 2000000000");

        assertTrinoExceptionThrownBy(assertions.operator(
                ADD,
                "INTERVAL '1' MONTH * (-2000000000)",
                "INTERVAL '1' MONTH * (-2000000000)")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("interval year to month addition overflow: -2000000000 + -2000000000");
    }

    @Test
    public void testSubtract()
    {
        assertThat(assertions.operator(SUBTRACT, "INTERVAL '6' MONTH", "INTERVAL '3' MONTH"))
                .hasType(createIntervalYearMonthType(MONTH, MONTH))
                .isEqualTo(new SqlIntervalYearMonth(0, 3));

        assertThat(assertions.operator(SUBTRACT, "INTERVAL '9' YEAR", "INTERVAL '6' YEAR"))
                .hasType(createIntervalYearMonthType(YEAR, YEAR))
                .isEqualTo(new SqlIntervalYearMonth(3, 0));

        assertThat(assertions.operator(SUBTRACT, "INTERVAL '3' MONTH", "INTERVAL '6' YEAR"))
                .hasType(createIntervalYearMonthType(YEAR, MONTH))
                .isEqualTo(new SqlIntervalYearMonth(-5, -9));

        assertTrinoExceptionThrownBy(assertions.operator(
                SUBTRACT,
                "INTERVAL '1' MONTH * (-2000000000)",
                "INTERVAL '1' MONTH * 2000000000")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("interval year to month subtraction overflow: -2000000000 - 2000000000");

        assertTrinoExceptionThrownBy(assertions.operator(
                SUBTRACT,
                "INTERVAL '1' MONTH * 2000000000",
                "INTERVAL '1' MONTH * (-2000000000)")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("interval year to month subtraction overflow: 2000000000 - -2000000000");
    }

    @Test
    public void testMultiply()
    {
        // multiplying by a number preserves the interval's qualifier
        assertThat(assertions.operator(MULTIPLY, "INTERVAL '6' MONTH", "2"))
                .hasType(createIntervalYearMonthType(MONTH, MONTH))
                .isEqualTo(new SqlIntervalYearMonth(0, 12));

        assertThat(assertions.operator(MULTIPLY, "2", "INTERVAL '6' MONTH"))
                .hasType(createIntervalYearMonthType(MONTH, MONTH))
                .isEqualTo(new SqlIntervalYearMonth(0, 12));

        assertThat(assertions.operator(MULTIPLY, "INTERVAL '10' MONTH", "2.5"))
                .hasType(createIntervalYearMonthType(MONTH, MONTH))
                .isEqualTo(new SqlIntervalYearMonth(0, 25));

        assertThat(assertions.operator(MULTIPLY, "2.5", "INTERVAL '10' MONTH"))
                .hasType(createIntervalYearMonthType(MONTH, MONTH))
                .isEqualTo(new SqlIntervalYearMonth(0, 25));

        assertThat(assertions.operator(MULTIPLY, "INTERVAL '6' YEAR", "2"))
                .hasType(createIntervalYearMonthType(YEAR, YEAR))
                .isEqualTo(new SqlIntervalYearMonth(12, 0));

        assertThat(assertions.operator(MULTIPLY, "2", "INTERVAL '6' YEAR"))
                .hasType(createIntervalYearMonthType(YEAR, YEAR))
                .isEqualTo(new SqlIntervalYearMonth(12, 0));

        // a year scaled by a fraction keeps the YEAR qualifier, with sub-year content retained
        assertThat(assertions.operator(MULTIPLY, "INTERVAL '1' YEAR", "2.5"))
                .hasType(createIntervalYearMonthType(YEAR, YEAR))
                .isEqualTo(interval(2, 6));

        assertThat(assertions.operator(MULTIPLY, "2.5", "INTERVAL '1' YEAR"))
                .hasType(createIntervalYearMonthType(YEAR, YEAR))
                .isEqualTo(interval(2, 6));

        assertTrinoExceptionThrownBy(assertions.operator(MULTIPLY, "INTERVAL '6' MONTH", "nan()")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertTrinoExceptionThrownBy(assertions.operator(MULTIPLY, "nan()", "INTERVAL '6' YEAR")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertTrinoExceptionThrownBy(assertions.operator(MULTIPLY, "INTERVAL '2' MONTH", "2000000000")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("interval year to month multiplication overflow: 2 * 2000000000");

        assertTrinoExceptionThrownBy(assertions.operator(MULTIPLY, "2000000000", "INTERVAL '2' MONTH")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("interval year to month multiplication overflow: 2000000000 * 2");
    }

    @Test
    public void testDivide()
    {
        assertThat(assertions.operator(DIVIDE, "INTERVAL '30' MONTH", "2"))
                .hasType(createIntervalYearMonthType(MONTH, MONTH))
                .isEqualTo(new SqlIntervalYearMonth(0, 15));

        assertThat(assertions.operator(DIVIDE, "INTERVAL '60' MONTH", "2.5"))
                .hasType(createIntervalYearMonthType(MONTH, MONTH))
                .isEqualTo(new SqlIntervalYearMonth(0, 24));

        // a year divided down keeps the YEAR qualifier, with sub-year content retained
        assertThat(assertions.operator(DIVIDE, "INTERVAL '3' YEAR", "2"))
                .hasType(createIntervalYearMonthType(YEAR, YEAR))
                .isEqualTo(interval(1, 6));

        assertThat(assertions.operator(DIVIDE, "INTERVAL '4' YEAR", "4.8"))
                .hasType(createIntervalYearMonthType(YEAR, YEAR))
                .isEqualTo(interval(0, 10));

        assertTrinoExceptionThrownBy(assertions.operator(DIVIDE, "INTERVAL '6' MONTH", "nan()")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertTrinoExceptionThrownBy(assertions.operator(DIVIDE, "INTERVAL '6' YEAR", "nan()")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertTrinoExceptionThrownBy(assertions.operator(DIVIDE, "INTERVAL '6' MONTH", "0E0")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        assertTrinoExceptionThrownBy(assertions.operator(DIVIDE, "INTERVAL '6' YEAR", "0")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testNegation()
    {
        // negation preserves the interval's qualifier
        assertThat(assertions.operator(NEGATION, "INTERVAL '3' MONTH"))
                .hasType(createIntervalYearMonthType(MONTH, MONTH))
                .isEqualTo(new SqlIntervalYearMonth(0, -3));

        assertThat(assertions.operator(NEGATION, "INTERVAL '6' YEAR"))
                .hasType(createIntervalYearMonthType(YEAR, YEAR))
                .isEqualTo(new SqlIntervalYearMonth(-6, 0));

        assertTrinoExceptionThrownBy(assertions.operator(
                NEGATION,
                "INTERVAL '1' MONTH * (-2147483647) - INTERVAL '1' MONTH")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("interval year to month negation overflow: -2147483648");
    }

    @Test
    public void testComparison()
    {
        assertThat(assertions.operator(EQUAL, "INTERVAL '3' MONTH", "INTERVAL '3' MONTH"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "INTERVAL '6' YEAR", "INTERVAL '6' YEAR"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "INTERVAL '3' MONTH", "INTERVAL '4' MONTH"))
                .isEqualTo(false);

        assertThat(assertions.expression("a <> b")
                .binding("a", "INTERVAL '3' MONTH")
                .binding("b", "INTERVAL '4' MONTH"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "INTERVAL '3' MONTH", "INTERVAL '4' MONTH"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "INTERVAL '6' YEAR", "INTERVAL '5' YEAR"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "INTERVAL '3' MONTH")
                .binding("low", "INTERVAL '2' MONTH")
                .binding("high", "INTERVAL '4' MONTH"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "INTERVAL '3' MONTH")
                .binding("low", "INTERVAL '4' MONTH")
                .binding("high", "INTERVAL '5' MONTH"))
                .isEqualTo(false);
    }

    @Test
    public void testIndeterminate()
    {
        assertThat(assertions.operator(INDETERMINATE, "cast(null as INTERVAL YEAR TO MONTH)"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "INTERVAL '124' YEAR TO MONTH"))
                .isEqualTo(false);
    }

    @Test
    public void testCastFromVarchar()
    {
        // the leading year exceeds the implicit precision of 2, so the target needs an explicit precision
        assertThat(assertions.expression("CAST('124-30' AS INTERVAL YEAR(3) TO MONTH)"))
                .isEqualTo(interval(124, 30));

        // the string is interpreted according to the target qualifier
        assertThat(assertions.expression("CAST('30' AS INTERVAL MONTH)"))
                .isEqualTo(interval(0, 30));

        // round-trips through a character string
        assertThat(assertions.expression("CAST(CAST(INTERVAL '7-4' YEAR TO MONTH AS VARCHAR) AS INTERVAL YEAR TO MONTH)"))
                .isEqualTo(interval(7, 4));

        // a value that does not fit the target's leading precision is rejected
        assertTrinoExceptionThrownBy(assertions.expression("CAST('124-30' AS INTERVAL YEAR(2) TO MONTH)")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessageContaining("interval field overflow: 126 year does not fit a leading precision of 2");
    }

    @Test
    public void testCastToVarchar()
    {
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
    public void testCastToBigint()
    {
        assertThat(assertions.expression("CAST(INTERVAL '5' YEAR AS BIGINT)")).isEqualTo(5L);
        assertThat(assertions.expression("CAST(INTERVAL '30' MONTH AS BIGINT)")).isEqualTo(30L);

        // a cast to a number is defined only for a single-field interval
        assertThatThrownBy(assertions.expression("CAST(INTERVAL '5-3' YEAR TO MONTH AS BIGINT)")::evaluate)
                .hasMessageContaining("to month to bigint");
    }

    @Test
    public void testCastFromInteger()
    {
        assertThat(assertions.expression("CAST(CAST(5 AS INTERVAL YEAR(9)) AS VARCHAR)"))
                .hasType(VARCHAR)
                .isEqualTo("5-0");
        assertThat(assertions.expression("CAST(CAST(30 AS INTERVAL MONTH(10)) AS VARCHAR)"))
                .hasType(VARCHAR)
                .isEqualTo("2-6");
    }

    @Test
    public void testLeadingPrecision()
    {
        // a literal with no explicit precision takes the width of its leading field
        assertThat(assertions.expression("INTERVAL '5' YEAR")).hasType(createIntervalYearMonthType(YEAR, YEAR, 1));
        assertThat(assertions.expression("INTERVAL '340' MONTH")).hasType(createIntervalYearMonthType(MONTH, MONTH, 3));
        assertThat(assertions.expression("INTERVAL '124-30' YEAR TO MONTH")).hasType(createIntervalYearMonthType(YEAR, MONTH, 3));

        // an explicit precision is honored
        assertThat(assertions.expression("INTERVAL '5' YEAR(4)")).hasType(createIntervalYearMonthType(YEAR, YEAR, 4));
        assertThat(assertions.expression("INTERVAL '5-3' YEAR(4) TO MONTH")).hasType(createIntervalYearMonthType(YEAR, MONTH, 4));

        // a derived interval takes the field-maximum leading precision
        assertThat(assertions.operator(ADD, "INTERVAL '3' MONTH", "INTERVAL '3' MONTH")).hasType(createIntervalYearMonthType(MONTH, MONTH));

        // an out-of-range explicit precision is rejected
        assertThatThrownBy(assertions.expression("INTERVAL '5' YEAR(10)")::evaluate)
                .hasMessageContaining("INTERVAL year leading precision must be in range [1, 9]: 10");

        // a value that does not fit an explicit leading precision is rejected
        assertThat(assertions.expression("INTERVAL '99' YEAR(2)")).hasType(createIntervalYearMonthType(YEAR, YEAR, 2));
        assertTrinoExceptionThrownBy(assertions.expression("INTERVAL '340' YEAR(2)")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessageContaining("interval field overflow: 340 year does not fit a leading precision of 2");
    }

    private static SqlIntervalYearMonth interval(int year, int month)
    {
        return new SqlIntervalYearMonth(year, month);
    }
}

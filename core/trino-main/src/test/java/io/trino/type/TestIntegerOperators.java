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

import static io.trino.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.StandardErrorCode.INVALID_LITERAL;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.function.OperatorType.DIVIDE;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.INDETERMINATE;
import static io.trino.spi.function.OperatorType.IS_DISTINCT_FROM;
import static io.trino.spi.function.OperatorType.LESS_THAN;
import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.trino.spi.function.OperatorType.MODULUS;
import static io.trino.spi.function.OperatorType.MULTIPLY;
import static io.trino.spi.function.OperatorType.NEGATION;
import static io.trino.spi.function.OperatorType.SUBTRACT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestIntegerOperators
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
    public void testLiteral()
    {
        assertThat(assertions.expression("INTEGER '37'"))
                .isEqualTo(37);

        assertThat(assertions.expression("INTEGER '17'"))
                .isEqualTo(17);

        assertTrinoExceptionThrownBy(assertions.expression("INTEGER '" + ((long) Integer.MAX_VALUE + 1L) + "'")::evaluate)
                .hasErrorCode(INVALID_LITERAL);
    }

    @Test
    public void testUnaryPlus()
    {
        assertThat(assertions.expression("+INTEGER '37'"))
                .isEqualTo(37);

        assertThat(assertions.expression("+INTEGER '17'"))
                .isEqualTo(17);
    }

    @Test
    public void testUnaryMinus()
    {
        assertThat(assertions.expression("INTEGER '-37'"))
                .isEqualTo(-37);

        assertThat(assertions.expression("INTEGER '-17'"))
                .isEqualTo(-17);

        assertTrinoExceptionThrownBy(assertions.expression("INTEGER '-" + Integer.MIN_VALUE + "'")::evaluate)
                .hasErrorCode(INVALID_LITERAL);
    }

    @Test
    public void testAdd()
    {
        assertThat(assertions.operator(ADD, "INTEGER '37'", "INTEGER '37'"))
                .isEqualTo(37 + 37);

        assertThat(assertions.operator(ADD, "INTEGER '37'", "INTEGER '17'"))
                .isEqualTo(37 + 17);

        assertThat(assertions.operator(ADD, "INTEGER '17'", "INTEGER '37'"))
                .isEqualTo(17 + 37);

        assertThat(assertions.operator(ADD, "INTEGER '17'", "INTEGER '17'"))
                .isEqualTo(17 + 17);

        assertTrinoExceptionThrownBy(assertions.expression(format("INTEGER '%s' + INTEGER '1'", Integer.MAX_VALUE))::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("integer addition overflow: 2147483647 + 1");
    }

    @Test
    public void testSubtract()
    {
        assertThat(assertions.operator(SUBTRACT, "INTEGER '37'", "INTEGER '37'"))
                .isEqualTo(0);

        assertThat(assertions.operator(SUBTRACT, "INTEGER '37'", "INTEGER '17'"))
                .isEqualTo(37 - 17);

        assertThat(assertions.operator(SUBTRACT, "INTEGER '17'", "INTEGER '37'"))
                .isEqualTo(17 - 37);

        assertThat(assertions.operator(SUBTRACT, "INTEGER '17'", "INTEGER '17'"))
                .isEqualTo(0);

        assertTrinoExceptionThrownBy(assertions.expression(format("INTEGER '%s' - INTEGER '1'", Integer.MIN_VALUE))::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("integer subtraction overflow: -2147483648 - 1");
    }

    @Test
    public void testMultiply()
    {
        assertThat(assertions.operator(MULTIPLY, "INTEGER '37'", "INTEGER '37'"))
                .isEqualTo(37 * 37);

        assertThat(assertions.operator(MULTIPLY, "INTEGER '37'", "INTEGER '17'"))
                .isEqualTo(37 * 17);

        assertThat(assertions.operator(MULTIPLY, "INTEGER '17'", "INTEGER '37'"))
                .isEqualTo(17 * 37);

        assertThat(assertions.operator(MULTIPLY, "INTEGER '17'", "INTEGER '17'"))
                .isEqualTo(17 * 17);

        assertTrinoExceptionThrownBy(assertions.expression(format("INTEGER '%s' * INTEGER '2'", Integer.MAX_VALUE))::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("integer multiplication overflow: 2147483647 * 2");
    }

    @Test
    public void testDivide()
    {
        assertThat(assertions.operator(DIVIDE, "INTEGER '37'", "INTEGER '37'"))
                .isEqualTo(1);

        assertThat(assertions.operator(DIVIDE, "INTEGER '37'", "INTEGER '17'"))
                .isEqualTo(37 / 17);

        assertThat(assertions.operator(DIVIDE, "INTEGER '17'", "INTEGER '37'"))
                .isEqualTo(17 / 37);

        assertThat(assertions.operator(DIVIDE, "INTEGER '17'", "INTEGER '17'"))
                .isEqualTo(1);

        assertTrinoExceptionThrownBy(assertions.operator(DIVIDE, "INTEGER '17'", "INTEGER '0'")::evaluate)
                .hasErrorCode(DIVISION_BY_ZERO);
    }

    @Test
    public void testModulus()
    {
        assertThat(assertions.operator(MODULUS, "INTEGER '37'", "INTEGER '37'"))
                .isEqualTo(0);

        assertThat(assertions.operator(MODULUS, "INTEGER '37'", "INTEGER '17'"))
                .isEqualTo(37 % 17);

        assertThat(assertions.operator(MODULUS, "INTEGER '17'", "INTEGER '37'"))
                .isEqualTo(17 % 37);

        assertThat(assertions.operator(MODULUS, "INTEGER '17'", "INTEGER '17'"))
                .isEqualTo(0);

        assertTrinoExceptionThrownBy(assertions.operator(MODULUS, "INTEGER '17'", "INTEGER '0'")::evaluate)
                .hasErrorCode(DIVISION_BY_ZERO);
    }

    @Test
    public void testNegation()
    {
        assertThat(assertions.operator(NEGATION, "(INTEGER '37')"))
                .isEqualTo(-37);

        assertThat(assertions.operator(NEGATION, "(INTEGER '17')"))
                .isEqualTo(-17);

        assertThat(assertions.expression("-(INTEGER '" + Integer.MAX_VALUE + "')"))
                .isEqualTo(Integer.MIN_VALUE + 1);

        assertTrinoExceptionThrownBy(assertions.expression(format("-(INTEGER '%s')", Integer.MIN_VALUE))::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("integer negation overflow: -2147483648");
    }

    @Test
    public void testEqual()
    {
        assertThat(assertions.operator(EQUAL, "INTEGER '37'", "INTEGER '37'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "INTEGER '37'", "INTEGER '17'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "INTEGER '17'", "INTEGER '37'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "INTEGER '17'", "INTEGER '17'"))
                .isEqualTo(true);
    }

    @Test
    public void testNotEqual()
    {
        assertThat(assertions.expression("a <> b")
                .binding("a", "INTEGER '37'")
                .binding("b", "INTEGER '37'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a <> b")
                .binding("a", "INTEGER '37'")
                .binding("b", "INTEGER '17'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <> b")
                .binding("a", "INTEGER '17'")
                .binding("b", "INTEGER '37'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <> b")
                .binding("a", "INTEGER '17'")
                .binding("b", "INTEGER '17'"))
                .isEqualTo(false);
    }

    @Test
    public void testLessThan()
    {
        assertThat(assertions.operator(LESS_THAN, "INTEGER '37'", "INTEGER '37'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "INTEGER '37'", "INTEGER '17'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "INTEGER '17'", "INTEGER '37'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "INTEGER '17'", "INTEGER '17'"))
                .isEqualTo(false);
    }

    @Test
    public void testLessThanOrEqual()
    {
        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "INTEGER '37'", "INTEGER '37'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "INTEGER '37'", "INTEGER '17'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "INTEGER '17'", "INTEGER '37'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "INTEGER '17'", "INTEGER '17'"))
                .isEqualTo(true);
    }

    @Test
    public void testGreaterThan()
    {
        assertThat(assertions.expression("a > b")
                .binding("a", "INTEGER '37'")
                .binding("b", "INTEGER '37'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "INTEGER '37'")
                .binding("b", "INTEGER '17'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "INTEGER '17'")
                .binding("b", "INTEGER '37'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "INTEGER '17'")
                .binding("b", "INTEGER '17'"))
                .isEqualTo(false);
    }

    @Test
    public void testGreaterThanOrEqual()
    {
        assertThat(assertions.expression("a >= b")
                .binding("a", "INTEGER '37'")
                .binding("b", "INTEGER '37'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "INTEGER '37'")
                .binding("b", "INTEGER '17'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "INTEGER '17'")
                .binding("b", "INTEGER '37'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "INTEGER '17'")
                .binding("b", "INTEGER '17'"))
                .isEqualTo(true);
    }

    @Test
    public void testBetween()
    {
        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "INTEGER '37'")
                .binding("low", "INTEGER '37'")
                .binding("high", "INTEGER '37'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "INTEGER '37'")
                .binding("low", "INTEGER '37'")
                .binding("high", "INTEGER '17'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "INTEGER '37'")
                .binding("low", "INTEGER '17'")
                .binding("high", "INTEGER '37'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "INTEGER '37'")
                .binding("low", "INTEGER '17'")
                .binding("high", "INTEGER '17'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "INTEGER '17'")
                .binding("low", "INTEGER '37'")
                .binding("high", "INTEGER '37'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "INTEGER '17'")
                .binding("low", "INTEGER '37'")
                .binding("high", "INTEGER '17'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "INTEGER '17'")
                .binding("low", "INTEGER '17'")
                .binding("high", "INTEGER '37'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "INTEGER '17'")
                .binding("low", "INTEGER '17'")
                .binding("high", "INTEGER '17'"))
                .isEqualTo(true);
    }

    @Test
    public void testCastToBigint()
    {
        assertThat(assertions.expression("cast(a as bigint)")
                .binding("a", "INTEGER '37'"))
                .isEqualTo(37L);

        assertThat(assertions.expression("cast(a as bigint)")
                .binding("a", "INTEGER '17'"))
                .isEqualTo(17L);
    }

    @Test
    public void testCastToSmallint()
    {
        assertThat(assertions.expression("cast(a as smallint)")
                .binding("a", "INTEGER '37'"))
                .isEqualTo((short) 37);

        assertThat(assertions.expression("cast(a as smallint)")
                .binding("a", "INTEGER '17'"))
                .isEqualTo((short) 17);
    }

    @Test
    public void testCastToTinyint()
    {
        assertThat(assertions.expression("cast(a as tinyint)")
                .binding("a", "INTEGER '37'"))
                .isEqualTo((byte) 37);

        assertThat(assertions.expression("cast(a as tinyint)")
                .binding("a", "INTEGER '17'"))
                .isEqualTo((byte) 17);
    }

    @Test
    public void testCastToVarchar()
    {
        assertThat(assertions.expression("cast(a as varchar)")
                .binding("a", "INTEGER '37'"))
                .hasType(VARCHAR)
                .isEqualTo("37");

        assertThat(assertions.expression("cast(a as varchar)")
                .binding("a", "INTEGER '17'"))
                .hasType(VARCHAR)
                .isEqualTo("17");

        assertThat(assertions.expression("cast(a as varchar(3))")
                .binding("a", "INTEGER '123'"))
                .hasType(createVarcharType(3))
                .isEqualTo("123");

        assertThat(assertions.expression("cast(a as varchar(50))")
                .binding("a", "INTEGER '123'"))
                .hasType(createVarcharType(50))
                .isEqualTo("123");

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as varchar(2))")
                .binding("a", "INTEGER '123'")
                .evaluate())
                .hasMessage("Value 123 cannot be represented as varchar(2)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testCastToDouble()
    {
        assertThat(assertions.expression("cast(a as double)")
                .binding("a", "INTEGER '37'"))
                .isEqualTo(37.0);

        assertThat(assertions.expression("cast(a as double)")
                .binding("a", "INTEGER '17'"))
                .isEqualTo(17.0);
    }

    @Test
    public void testCastToFloat()
    {
        assertThat(assertions.expression("cast(a as real)")
                .binding("a", "INTEGER '37'"))
                .isEqualTo(37.0f);

        assertThat(assertions.expression("cast(a as real)")
                .binding("a", "INTEGER '-2147483648'"))
                .isEqualTo(-2147483648.0f);

        assertThat(assertions.expression("cast(a as real)")
                .binding("a", "INTEGER '0'"))
                .isEqualTo(0.0f);
    }

    @Test
    public void testCastToBoolean()
    {
        assertThat(assertions.expression("cast(a as boolean)")
                .binding("a", "INTEGER '37'"))
                .isEqualTo(true);

        assertThat(assertions.expression("cast(a as boolean)")
                .binding("a", "INTEGER '17'"))
                .isEqualTo(true);

        assertThat(assertions.expression("cast(a as boolean)")
                .binding("a", "INTEGER '0'"))
                .isEqualTo(false);
    }

    @Test
    public void testCastFromVarchar()
    {
        assertThat(assertions.expression("cast(a as integer)")
                .binding("a", "'37'"))
                .isEqualTo(37);

        assertThat(assertions.expression("cast(a as integer)")
                .binding("a", "'17'"))
                .isEqualTo(17);
    }

    @Test
    public void testIsDistinctFrom()
    {
        assertThat(assertions.operator(IS_DISTINCT_FROM, "cast(NULL as INTEGER)", "CAST(NULL AS INTEGER)"))
                .isEqualTo(false);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "37", "37"))
                .isEqualTo(false);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "37", "38"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "NULL", "37"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "37", "NULL"))
                .isEqualTo(true);
    }

    @Test
    public void testIndeterminate()
    {
        assertThat(assertions.operator(INDETERMINATE, "cast(null as integer)"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "12"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "0"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "-23"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "cast(1.4 as integer)"))
                .isEqualTo(false);
    }
}

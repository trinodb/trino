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

import io.trino.spi.type.Type;
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
public class TestSmallintOperators
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
        assertThat(assertions.expression("SMALLINT '37'"))
                .isEqualTo((short) 37);

        assertThat(assertions.expression("SMALLINT '17'"))
                .isEqualTo((short) 17);

        assertTrinoExceptionThrownBy(assertions.expression("SMALLINT '" + ((long) Short.MAX_VALUE + 1L) + "'")::evaluate)
                .hasErrorCode(INVALID_LITERAL);
    }

    @Test
    public void testUnaryPlus()
    {
        assertThat(assertions.expression("+SMALLINT '37'"))
                .isEqualTo((short) 37);

        assertThat(assertions.expression("+SMALLINT '17'"))
                .isEqualTo((short) 17);
    }

    @Test
    public void testUnaryMinus()
    {
        assertThat(assertions.expression("SMALLINT '-37'"))
                .isEqualTo((short) -37);

        assertThat(assertions.expression("SMALLINT '-17'"))
                .isEqualTo((short) -17);

        assertTrinoExceptionThrownBy(assertions.expression("SMALLINT '-" + Short.MIN_VALUE + "'")::evaluate)
                .hasErrorCode(INVALID_LITERAL);
    }

    @Test
    public void testAdd()
    {
        assertThat(assertions.operator(ADD, "SMALLINT '37'", "SMALLINT '37'"))
                .isEqualTo((short) (37 + 37));

        assertThat(assertions.operator(ADD, "SMALLINT '37'", "SMALLINT '17'"))
                .isEqualTo((short) (37 + 17));

        assertThat(assertions.operator(ADD, "SMALLINT '17'", "SMALLINT '37'"))
                .isEqualTo((short) (17 + 37));

        assertThat(assertions.operator(ADD, "SMALLINT '17'", "SMALLINT '17'"))
                .isEqualTo((short) (17 + 17));

        assertTrinoExceptionThrownBy(assertions.expression(format("SMALLINT '%s' + SMALLINT '1'", Short.MAX_VALUE))::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("smallint addition overflow: 32767 + 1");
    }

    @Test
    public void testSubtract()
    {
        assertThat(assertions.operator(SUBTRACT, "SMALLINT '37'", "SMALLINT '37'"))
                .isEqualTo((short) 0);

        assertThat(assertions.operator(SUBTRACT, "SMALLINT '37'", "SMALLINT '17'"))
                .isEqualTo((short) (37 - 17));

        assertThat(assertions.operator(SUBTRACT, "SMALLINT '17'", "SMALLINT '37'"))
                .isEqualTo((short) (17 - 37));

        assertThat(assertions.operator(SUBTRACT, "SMALLINT '17'", "SMALLINT '17'"))
                .isEqualTo((short) 0);

        assertTrinoExceptionThrownBy(assertions.expression(format("SMALLINT '%s' - SMALLINT '1'", Short.MIN_VALUE))::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("smallint subtraction overflow: -32768 - 1");
    }

    @Test
    public void testMultiply()
    {
        assertThat(assertions.operator(MULTIPLY, "SMALLINT '37'", "SMALLINT '37'"))
                .isEqualTo((short) (37 * 37));

        assertThat(assertions.operator(MULTIPLY, "SMALLINT '37'", "SMALLINT '17'"))
                .isEqualTo((short) (37 * 17));

        assertThat(assertions.operator(MULTIPLY, "SMALLINT '17'", "SMALLINT '37'"))
                .isEqualTo((short) (17 * 37));

        assertThat(assertions.operator(MULTIPLY, "SMALLINT '17'", "SMALLINT '17'"))
                .isEqualTo((short) (17 * 17));

        assertTrinoExceptionThrownBy(assertions.expression(format("SMALLINT '%s' * SMALLINT '2'", Short.MAX_VALUE))::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("smallint multiplication overflow: 32767 * 2");
    }

    @Test
    public void testDivide()
    {
        assertThat(assertions.operator(DIVIDE, "SMALLINT '37'", "SMALLINT '37'"))
                .isEqualTo((short) 1);

        assertThat(assertions.operator(DIVIDE, "SMALLINT '37'", "SMALLINT '17'"))
                .isEqualTo((short) (37 / 17));

        assertThat(assertions.operator(DIVIDE, "SMALLINT '17'", "SMALLINT '37'"))
                .isEqualTo((short) (17 / 37));

        assertThat(assertions.operator(DIVIDE, "SMALLINT '17'", "SMALLINT '17'"))
                .isEqualTo((short) 1);

        assertTrinoExceptionThrownBy(assertions.operator(DIVIDE, "SMALLINT '17'", "SMALLINT '0'")::evaluate)
                .hasErrorCode(DIVISION_BY_ZERO);
    }

    @Test
    public void testModulus()
    {
        assertThat(assertions.operator(MODULUS, "SMALLINT '37'", "SMALLINT '37'"))
                .isEqualTo((short) 0);

        assertThat(assertions.operator(MODULUS, "SMALLINT '37'", "SMALLINT '17'"))
                .isEqualTo((short) (37 % 17));

        assertThat(assertions.operator(MODULUS, "SMALLINT '17'", "SMALLINT '37'"))
                .isEqualTo((short) (17 % 37));

        assertThat(assertions.operator(MODULUS, "SMALLINT '17'", "SMALLINT '17'"))
                .isEqualTo((short) 0);

        assertTrinoExceptionThrownBy(assertions.operator(MODULUS, "SMALLINT '17'", "SMALLINT '0'")::evaluate)
                .hasErrorCode(DIVISION_BY_ZERO);
    }

    @Test
    public void testNegation()
    {
        assertThat(assertions.operator(NEGATION, "(SMALLINT '37')"))
                .isEqualTo((short) -37);

        assertThat(assertions.operator(NEGATION, "(SMALLINT '17')"))
                .isEqualTo((short) -17);

        assertThat(assertions.expression("-(SMALLINT '" + Short.MAX_VALUE + "')"))
                .isEqualTo((short) (Short.MIN_VALUE + 1));

        assertTrinoExceptionThrownBy(assertions.expression(format("-(SMALLINT '%s')", Short.MIN_VALUE))::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("smallint negation overflow: -32768");
    }

    @Test
    public void testEqual()
    {
        assertThat(assertions.operator(EQUAL, "SMALLINT '37'", "SMALLINT '37'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "SMALLINT '37'", "SMALLINT '17'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "SMALLINT '17'", "SMALLINT '37'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "SMALLINT '17'", "SMALLINT '17'"))
                .isEqualTo(true);
    }

    @Test
    public void testNotEqual()
    {
        assertThat(assertions.expression("a <> b")
                .binding("a", "SMALLINT '37'")
                .binding("b", "SMALLINT '37'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a <> b")
                .binding("a", "SMALLINT '37'")
                .binding("b", "SMALLINT '17'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <> b")
                .binding("a", "SMALLINT '17'")
                .binding("b", "SMALLINT '37'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <> b")
                .binding("a", "SMALLINT '17'")
                .binding("b", "SMALLINT '17'"))
                .isEqualTo(false);
    }

    @Test
    public void testLessThan()
    {
        assertThat(assertions.operator(LESS_THAN, "SMALLINT '37'", "SMALLINT '37'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "SMALLINT '37'", "SMALLINT '17'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "SMALLINT '17'", "SMALLINT '37'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "SMALLINT '17'", "SMALLINT '17'"))
                .isEqualTo(false);
    }

    @Test
    public void testLessThanOrEqual()
    {
        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "SMALLINT '37'", "SMALLINT '37'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "SMALLINT '37'", "SMALLINT '17'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "SMALLINT '17'", "SMALLINT '37'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "SMALLINT '17'", "SMALLINT '17'"))
                .isEqualTo(true);
    }

    @Test
    public void testGreaterThan()
    {
        assertThat(assertions.expression("a > b")
                .binding("a", "SMALLINT '37'")
                .binding("b", "SMALLINT '37'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "SMALLINT '37'")
                .binding("b", "SMALLINT '17'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "SMALLINT '17'")
                .binding("b", "SMALLINT '37'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "SMALLINT '17'")
                .binding("b", "SMALLINT '17'"))
                .isEqualTo(false);
    }

    @Test
    public void testGreaterThanOrEqual()
    {
        assertThat(assertions.expression("a >= b")
                .binding("a", "SMALLINT '37'")
                .binding("b", "SMALLINT '37'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "SMALLINT '37'")
                .binding("b", "SMALLINT '17'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "SMALLINT '17'")
                .binding("b", "SMALLINT '37'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "SMALLINT '17'")
                .binding("b", "SMALLINT '17'"))
                .isEqualTo(true);
    }

    @Test
    public void testBetween()
    {
        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "SMALLINT '37'")
                .binding("low", "SMALLINT '37'")
                .binding("high", "SMALLINT '37'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "SMALLINT '37'")
                .binding("low", "SMALLINT '37'")
                .binding("high", "SMALLINT '17'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "SMALLINT '37'")
                .binding("low", "SMALLINT '17'")
                .binding("high", "SMALLINT '37'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "SMALLINT '37'")
                .binding("low", "SMALLINT '17'")
                .binding("high", "SMALLINT '17'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "SMALLINT '17'")
                .binding("low", "SMALLINT '37'")
                .binding("high", "SMALLINT '37'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "SMALLINT '17'")
                .binding("low", "SMALLINT '37'")
                .binding("high", "SMALLINT '17'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "SMALLINT '17'")
                .binding("low", "SMALLINT '17'")
                .binding("high", "SMALLINT '37'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "SMALLINT '17'")
                .binding("low", "SMALLINT '17'")
                .binding("high", "SMALLINT '17'"))
                .isEqualTo(true);
    }

    @Test
    public void testCastToBigint()
    {
        assertThat(assertions.expression("cast(a as bigint)")
                .binding("a", "SMALLINT '37'"))
                .isEqualTo(37L);

        assertThat(assertions.expression("cast(a as bigint)")
                .binding("a", "SMALLINT '17'"))
                .isEqualTo(17L);
    }

    @Test
    public void testCastToInteger()
    {
        assertThat(assertions.expression("cast(a as integer)")
                .binding("a", "SMALLINT '37'"))
                .isEqualTo(37);

        assertThat(assertions.expression("cast(a as integer)")
                .binding("a", "SMALLINT '17'"))
                .isEqualTo(17);
    }

    @Test
    public void testCastToTinyint()
    {
        assertThat(assertions.expression("cast(a as tinyint)")
                .binding("a", "SMALLINT '37'"))
                .isEqualTo((byte) 37);

        assertThat(assertions.expression("cast(a as tinyint)")
                .binding("a", "SMALLINT '17'"))
                .isEqualTo((byte) 17);
    }

    @Test
    public void testCastToVarchar()
    {
        assertThat(assertions.expression("cast(a as varchar)")
                .binding("a", "SMALLINT '37'"))
                .hasType(VARCHAR)
                .isEqualTo("37");

        assertThat(assertions.expression("cast(a as varchar)")
                .binding("a", "SMALLINT '17'"))
                .hasType(VARCHAR)
                .isEqualTo("17");

        Type type1 = createVarcharType(3);
        assertThat(assertions.expression("cast(a as varchar(3))")
                .binding("a", "SMALLINT '123'"))
                .hasType(type1)
                .isEqualTo("123");

        Type type = createVarcharType(50);
        assertThat(assertions.expression("cast(a as varchar(50))")
                .binding("a", "SMALLINT '123'"))
                .hasType(type)
                .isEqualTo("123");

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as varchar(2))")
                .binding("a", "SMALLINT '123'").evaluate())
                .hasMessage("Value 123 cannot be represented as varchar(2)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testCastToDouble()
    {
        assertThat(assertions.expression("cast(a as double)")
                .binding("a", "SMALLINT '37'"))
                .isEqualTo(37.0);

        assertThat(assertions.expression("cast(a as double)")
                .binding("a", "SMALLINT '17'"))
                .isEqualTo(17.0);
    }

    @Test
    public void testCastToFloat()
    {
        assertThat(assertions.expression("cast(a as real)")
                .binding("a", "SMALLINT '37'"))
                .isEqualTo(37.0f);

        assertThat(assertions.expression("cast(a as real)")
                .binding("a", "SMALLINT '-32768'"))
                .isEqualTo(-32768.0f);

        assertThat(assertions.expression("cast(a as real)")
                .binding("a", "SMALLINT '0'"))
                .isEqualTo(0.0f);
    }

    @Test
    public void testCastToBoolean()
    {
        assertThat(assertions.expression("cast(a as boolean)")
                .binding("a", "SMALLINT '37'"))
                .isEqualTo(true);

        assertThat(assertions.expression("cast(a as boolean)")
                .binding("a", "SMALLINT '17'"))
                .isEqualTo(true);

        assertThat(assertions.expression("cast(a as boolean)")
                .binding("a", "SMALLINT '0'"))
                .isEqualTo(false);
    }

    @Test
    public void testCastFromVarchar()
    {
        assertThat(assertions.expression("cast(a as smallint)")
                .binding("a", "'37'"))
                .isEqualTo((short) 37);

        assertThat(assertions.expression("cast(a as smallint)")
                .binding("a", "'17'"))
                .isEqualTo((short) 17);
    }

    @Test
    public void testIsDistinctFrom()
    {
        assertThat(assertions.operator(IS_DISTINCT_FROM, "cast(NULL as SMALLINT)", "CAST(NULL AS SMALLINT)"))
                .isEqualTo(false);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "SMALLINT '37'", "SMALLINT '37'"))
                .isEqualTo(false);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "SMALLINT '37'", "SMALLINT '38'"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "NULL", "SMALLINT '37'"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "SMALLINT '37'", "NULL"))
                .isEqualTo(true);
    }

    @Test
    public void testIndeterminate()
    {
        assertThat(assertions.operator(INDETERMINATE, "cast(null as smallint)"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "cast(12 as smallint)"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "cast(0 as smallint)"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "cast(-23 as smallint)"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "cast(1.4 as smallint)"))
                .isEqualTo(false);
    }
}

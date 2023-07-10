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
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestTinyintOperators
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
        assertThat(assertions.expression("TINYINT '37'"))
                .isEqualTo((byte) 37);

        assertThat(assertions.expression("TINYINT '17'"))
                .isEqualTo((byte) 17);

        assertTrinoExceptionThrownBy(() -> assertions.expression("TINYINT '" + ((long) Byte.MAX_VALUE + 1L) + "'").evaluate())
                .hasErrorCode(INVALID_LITERAL);
    }

    @Test
    public void testUnaryPlus()
    {
        assertThat(assertions.expression("+TINYINT '37'"))
                .isEqualTo((byte) 37);

        assertThat(assertions.expression("+TINYINT '17'"))
                .isEqualTo((byte) 17);
    }

    @Test
    public void testUnaryMinus()
    {
        assertThat(assertions.expression("TINYINT '-37'"))
                .isEqualTo((byte) -37);

        assertThat(assertions.expression("TINYINT '-17'"))
                .isEqualTo((byte) -17);

        assertTrinoExceptionThrownBy(() -> assertions.expression("TINYINT '-" + Byte.MIN_VALUE + "'").evaluate())
                .hasErrorCode(INVALID_LITERAL);
    }

    @Test
    public void testAdd()
    {
        assertThat(assertions.operator(ADD, "TINYINT '37'", "TINYINT '37'"))
                .isEqualTo((byte) (37 + 37));

        assertThat(assertions.operator(ADD, "TINYINT '37'", "TINYINT '17'"))
                .isEqualTo((byte) (37 + 17));

        assertThat(assertions.operator(ADD, "TINYINT '17'", "TINYINT '37'"))
                .isEqualTo((byte) (17 + 37));

        assertThat(assertions.operator(ADD, "TINYINT '17'", "TINYINT '17'"))
                .isEqualTo((byte) (17 + 17));

        assertTrinoExceptionThrownBy(() -> assertions.operator(ADD, "TINYINT '%s'".formatted(Byte.MAX_VALUE), "TINYINT '1'").evaluate())
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("tinyint addition overflow: 127 + 1");
    }

    @Test
    public void testSubtract()
    {
        assertThat(assertions.operator(SUBTRACT, "TINYINT '37'", "TINYINT '37'"))
                .isEqualTo((byte) 0);

        assertThat(assertions.operator(SUBTRACT, "TINYINT '37'", "TINYINT '17'"))
                .isEqualTo((byte) (37 - 17));

        assertThat(assertions.operator(SUBTRACT, "TINYINT '17'", "TINYINT '37'"))
                .isEqualTo((byte) (17 - 37));

        assertThat(assertions.operator(SUBTRACT, "TINYINT '17'", "TINYINT '17'"))
                .isEqualTo((byte) 0);

        assertTrinoExceptionThrownBy(() -> assertions.operator(SUBTRACT, "TINYINT '%s'".formatted(Byte.MIN_VALUE), "TINYINT '1'").evaluate())
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("tinyint subtraction overflow: -128 - 1");
    }

    @Test
    public void testMultiply()
    {
        assertThat(assertions.operator(MULTIPLY, "TINYINT '11'", "TINYINT '11'"))
                .isEqualTo((byte) (11 * 11));

        assertThat(assertions.operator(MULTIPLY, "TINYINT '11'", "TINYINT '9'"))
                .isEqualTo((byte) (11 * 9));

        assertThat(assertions.operator(MULTIPLY, "TINYINT '9'", "TINYINT '11'"))
                .isEqualTo((byte) (9 * 11));

        assertThat(assertions.operator(MULTIPLY, "TINYINT '9'", "TINYINT '9'"))
                .isEqualTo((byte) (9 * 9));

        assertTrinoExceptionThrownBy(() -> assertions.operator(MULTIPLY, "TINYINT '%s'".formatted(Byte.MAX_VALUE), "TINYINT '2'").evaluate())
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("tinyint multiplication overflow: 127 * 2");
    }

    @Test
    public void testDivide()
    {
        assertThat(assertions.operator(DIVIDE, "TINYINT '37'", "TINYINT '37'"))
                .isEqualTo((byte) 1);

        assertThat(assertions.operator(DIVIDE, "TINYINT '37'", "TINYINT '17'"))
                .isEqualTo((byte) (37 / 17));

        assertThat(assertions.operator(DIVIDE, "TINYINT '17'", "TINYINT '37'"))
                .isEqualTo((byte) (17 / 37));

        assertThat(assertions.operator(DIVIDE, "TINYINT '17'", "TINYINT '17'"))
                .isEqualTo((byte) 1);

        assertTrinoExceptionThrownBy(() -> assertions.operator(DIVIDE, "TINYINT '17'", "TINYINT '0'").evaluate())
                .hasErrorCode(DIVISION_BY_ZERO);
    }

    @Test
    public void testModulus()
    {
        assertThat(assertions.operator(MODULUS, "TINYINT '37'", "TINYINT '37'"))
                .isEqualTo((byte) 0);

        assertThat(assertions.operator(MODULUS, "TINYINT '37'", "TINYINT '17'"))
                .isEqualTo((byte) (37 % 17));

        assertThat(assertions.operator(MODULUS, "TINYINT '17'", "TINYINT '37'"))
                .isEqualTo((byte) (17 % 37));

        assertThat(assertions.operator(MODULUS, "TINYINT '17'", "TINYINT '17'"))
                .isEqualTo((byte) 0);

        assertTrinoExceptionThrownBy(() -> assertions.operator(MODULUS, "TINYINT '17'", "TINYINT '0'").evaluate())
                .hasErrorCode(DIVISION_BY_ZERO);
    }

    @Test
    public void testNegation()
    {
        assertThat(assertions.operator(NEGATION, "(TINYINT '37')"))
                .isEqualTo((byte) -37);

        assertThat(assertions.operator(NEGATION, "(TINYINT '17')"))
                .isEqualTo((byte) -17);

        assertThat(assertions.operator(NEGATION, "TINYINT '" + Byte.MAX_VALUE + "'"))
                .isEqualTo((byte) (Byte.MIN_VALUE + 1));

        assertTrinoExceptionThrownBy(() -> assertions.operator(NEGATION, "TINYINT '" + Byte.MIN_VALUE + "'").evaluate())
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("tinyint negation overflow: -128");
    }

    @Test
    public void testEqual()
    {
        assertThat(assertions.operator(EQUAL, "TINYINT '37'", "TINYINT '37'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "TINYINT '37'", "TINYINT '17'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "TINYINT '17'", "TINYINT '37'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "TINYINT '17'", "TINYINT '17'"))
                .isEqualTo(true);
    }

    @Test
    public void testNotEqual()
    {
        assertThat(assertions.expression("a <> b")
                .binding("a", "TINYINT '37'")
                .binding("b", "TINYINT '37'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a <> b")
                .binding("a", "TINYINT '37'")
                .binding("b", "TINYINT '17'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <> b")
                .binding("a", "TINYINT '17'")
                .binding("b", "TINYINT '37'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <> b")
                .binding("a", "TINYINT '17'")
                .binding("b", "TINYINT '17'"))
                .isEqualTo(false);
    }

    @Test
    public void testLessThan()
    {
        assertThat(assertions.operator(LESS_THAN, "TINYINT '37'", "TINYINT '37'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "TINYINT '37'", "TINYINT '17'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "TINYINT '17'", "TINYINT '37'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "TINYINT '17'", "TINYINT '17'"))
                .isEqualTo(false);
    }

    @Test
    public void testLessThanOrEqual()
    {
        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "TINYINT '37'", "TINYINT '37'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "TINYINT '37'", "TINYINT '17'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "TINYINT '17'", "TINYINT '37'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "TINYINT '17'", "TINYINT '17'"))
                .isEqualTo(true);
    }

    @Test
    public void testGreaterThan()
    {
        assertThat(assertions.expression("a > b")
                .binding("a", "TINYINT '37'")
                .binding("b", "TINYINT '37'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "TINYINT '37'")
                .binding("b", "TINYINT '17'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "TINYINT '17'")
                .binding("b", "TINYINT '37'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "TINYINT '17'")
                .binding("b", "TINYINT '17'"))
                .isEqualTo(false);
    }

    @Test
    public void testGreaterThanOrEqual()
    {
        assertThat(assertions.expression("a >= b")
                .binding("a", "TINYINT '37'")
                .binding("b", "TINYINT '37'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "TINYINT '37'")
                .binding("b", "TINYINT '17'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "TINYINT '17'")
                .binding("b", "TINYINT '37'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "TINYINT '17'")
                .binding("b", "TINYINT '17'"))
                .isEqualTo(true);
    }

    @Test
    public void testBetween()
    {
        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "TINYINT '37'")
                .binding("low", "TINYINT '37'")
                .binding("high", "TINYINT '37'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "TINYINT '37'")
                .binding("low", "TINYINT '37'")
                .binding("high", "TINYINT '17'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "TINYINT '37'")
                .binding("low", "TINYINT '17'")
                .binding("high", "TINYINT '37'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "TINYINT '37'")
                .binding("low", "TINYINT '17'")
                .binding("high", "TINYINT '17'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "TINYINT '17'")
                .binding("low", "TINYINT '37'")
                .binding("high", "TINYINT '37'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "TINYINT '17'")
                .binding("low", "TINYINT '37'")
                .binding("high", "TINYINT '17'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "TINYINT '17'")
                .binding("low", "TINYINT '17'")
                .binding("high", "TINYINT '37'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "TINYINT '17'")
                .binding("low", "TINYINT '17'")
                .binding("high", "TINYINT '17'"))
                .isEqualTo(true);
    }

    @Test
    public void testCastToBigint()
    {
        assertThat(assertions.expression("cast(a as bigint)")
                .binding("a", "TINYINT '37'"))
                .isEqualTo(37L);

        assertThat(assertions.expression("cast(a as bigint)")
                .binding("a", "TINYINT '17'"))
                .isEqualTo(17L);
    }

    @Test
    public void testCastToInteger()
    {
        assertThat(assertions.expression("cast(a as integer)")
                .binding("a", "TINYINT '37'"))
                .isEqualTo(37);

        assertThat(assertions.expression("cast(a as integer)")
                .binding("a", "TINYINT '17'"))
                .isEqualTo(17);
    }

    @Test
    public void testCastToSmallint()
    {
        assertThat(assertions.expression("cast(a as smallint)")
                .binding("a", "TINYINT '37'"))
                .isEqualTo((short) 37);

        assertThat(assertions.expression("cast(a as smallint)")
                .binding("a", "TINYINT '17'"))
                .isEqualTo((short) 17);
    }

    @Test
    public void testCastToVarchar()
    {
        assertThat(assertions.expression("cast(a as varchar)")
                .binding("a", "TINYINT '37'"))
                .hasType(VARCHAR)
                .isEqualTo("37");

        assertThat(assertions.expression("cast(a as varchar)")
                .binding("a", "TINYINT '17'"))
                .hasType(VARCHAR)
                .isEqualTo("17");

        assertThat(assertions.expression("cast(a as varchar(3))")
                .binding("a", "TINYINT '123'"))
                .hasType(createVarcharType(3))
                .isEqualTo("123");

        assertThat(assertions.expression("cast(a as varchar(50))")
                .binding("a", "TINYINT '123'"))
                .hasType(createVarcharType(50))
                .isEqualTo("123");

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as varchar(2))")
                .binding("a", "TINYINT '123'")
                .evaluate())
                .hasMessage("Value 123 cannot be represented as varchar(2)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testCastToDouble()
    {
        assertThat(assertions.expression("cast(a as double)")
                .binding("a", "TINYINT '37'"))
                .isEqualTo(37.0);

        assertThat(assertions.expression("cast(a as double)")
                .binding("a", "TINYINT '17'"))
                .isEqualTo(17.0);
    }

    @Test
    public void testCastToFloat()
    {
        assertThat(assertions.expression("cast(a as real)")
                .binding("a", "TINYINT '37'"))
                .isEqualTo(37.0f);

        assertThat(assertions.expression("cast(a as real)")
                .binding("a", "TINYINT '-128'"))
                .isEqualTo(-128.0f);

        assertThat(assertions.expression("cast(a as real)")
                .binding("a", "TINYINT '0'"))
                .isEqualTo(0.0f);
    }

    @Test
    public void testCastToBoolean()
    {
        assertThat(assertions.expression("cast(a as boolean)")
                .binding("a", "TINYINT '37'"))
                .isEqualTo(true);

        assertThat(assertions.expression("cast(a as boolean)")
                .binding("a", "TINYINT '17'"))
                .isEqualTo(true);

        assertThat(assertions.expression("cast(a as boolean)")
                .binding("a", "TINYINT '0'"))
                .isEqualTo(false);
    }

    @Test
    public void testCastFromVarchar()
    {
        assertThat(assertions.expression("cast(a as tinyint)")
                .binding("a", "'37'"))
                .isEqualTo((byte) 37);

        assertThat(assertions.expression("cast(a as tinyint)")
                .binding("a", "'17'"))
                .isEqualTo((byte) 17);
    }

    @Test
    public void testIsDistinctFrom()
    {
        assertThat(assertions.operator(IS_DISTINCT_FROM, "CAST(NULL AS TINYINT)", "CAST(NULL AS TINYINT)"))
                .isEqualTo(false);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "TINYINT '37'", "TINYINT '37'"))
                .isEqualTo(false);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "TINYINT '37'", "TINYINT '38'"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "NULL", "TINYINT '37'"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "TINYINT '37'", "NULL"))
                .isEqualTo(true);
    }

    @Test
    public void testIndeterminate()
    {
        assertThat(assertions.operator(INDETERMINATE, "cast(null as tinyint)"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "cast(12 as tinyint)"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "cast(0 as tinyint)"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "cast(-23 as tinyint)"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "cast(1.4 as tinyint)"))
                .isEqualTo(false);
    }
}

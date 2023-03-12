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

import io.trino.spi.function.OperatorType;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
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
public class TestBigintOperators
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
        assertThat(assertions.expression("100000000037"))
                .isEqualTo(100000000037L);

        assertThat(assertions.expression("100000000017"))
                .isEqualTo(100000000017L);
    }

    @Test
    public void testTypeConstructor()
    {
        assertThat(assertions.expression("BIGINT '9223372036854775807'"))
                .isEqualTo(9223372036854775807L);

        assertThat(assertions.expression("BIGINT '-9223372036854775807'"))
                .isEqualTo(-9223372036854775807L);

        assertThat(assertions.expression("BIGINT '+754'"))
                .isEqualTo(754L);
    }

    @Test
    public void testUnaryPlus()
    {
        assertThat(assertions.expression("+100000000037"))
                .isEqualTo(100000000037L);

        assertThat(assertions.expression("+100000000017"))
                .isEqualTo(100000000017L);
    }

    @Test
    public void testUnaryMinus()
    {
        assertThat(assertions.expression("-100000000037"))
                .isEqualTo(-100000000037L);

        assertThat(assertions.expression("-100000000017"))
                .isEqualTo(-100000000017L);
    }

    @Test
    public void testAdd()
    {
        assertThat(assertions.operator(ADD, "37", "100000000037"))
                .isEqualTo(37 + 100000000037L);

        assertThat(assertions.operator(ADD, "37", "100000000017"))
                .isEqualTo(37 + 100000000017L);

        assertThat(assertions.operator(ADD, "100000000017", "37"))
                .isEqualTo(100000000017L + 37L);

        assertThat(assertions.operator(ADD, "100000000017", "100000000017"))
                .isEqualTo(100000000017L + 100000000017L);
    }

    @Test
    public void testSubtract()
    {
        assertThat(assertions.operator(SUBTRACT, "100000000037", "37"))
                .isEqualTo(100000000037L - 37L);

        assertThat(assertions.operator(SUBTRACT, "37", "100000000017"))
                .isEqualTo(37 - 100000000017L);

        assertThat(assertions.operator(SUBTRACT, "100000000017", "37"))
                .isEqualTo(100000000017L - 37L);

        assertThat(assertions.operator(SUBTRACT, "100000000017", "100000000017"))
                .isEqualTo(0L);
    }

    @Test
    public void testMultiply()
    {
        assertThat(assertions.operator(MULTIPLY, "100000000037", "37"))
                .isEqualTo(100000000037L * 37L);

        assertThat(assertions.operator(MULTIPLY, "37", "100000000017"))
                .isEqualTo(37 * 100000000017L);

        assertThat(assertions.operator(MULTIPLY, "100000000017", "37"))
                .isEqualTo(100000000017L * 37L);

        assertThat(assertions.operator(MULTIPLY, "100000000017", "10000017"))
                .isEqualTo(100000000017L * 10000017L);
    }

    @Test
    public void testDivide()
    {
        assertThat(assertions.operator(DIVIDE, "100000000037", "37"))
                .isEqualTo(100000000037L / 37L);

        assertThat(assertions.operator(DIVIDE, "37", "100000000017"))
                .isEqualTo(37 / 100000000017L);

        assertThat(assertions.operator(DIVIDE, "100000000017", "37"))
                .isEqualTo(100000000017L / 37L);

        assertThat(assertions.operator(DIVIDE, "100000000017", "100000000017"))
                .isEqualTo(1L);
    }

    @Test
    public void testModulus()
    {
        assertThat(assertions.operator(MODULUS, "100000000037", "37"))
                .isEqualTo(100000000037L % 37L);

        assertThat(assertions.operator(MODULUS, "37", "100000000017"))
                .isEqualTo(37 % 100000000017L);

        assertThat(assertions.operator(MODULUS, "100000000017", "37"))
                .isEqualTo(100000000017L % 37L);

        assertThat(assertions.operator(MODULUS, "100000000017", "100000000017"))
                .isEqualTo(0L);
    }

    @Test
    public void testNegation()
    {
        assertThat(assertions.operator(OperatorType.NEGATION, "(100000000037)"))
                .isEqualTo(-100000000037L);

        assertThat(assertions.operator(OperatorType.NEGATION, "(100000000017)"))
                .isEqualTo(-100000000017L);
    }

    @Test
    public void testEqual()
    {
        assertThat(assertions.operator(EQUAL, "100000000037", "100000000037"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "37", "100000000017"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "100000000017", "37"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "100000000017", "100000000017"))
                .isEqualTo(true);
    }

    @Test
    public void testNotEqual()
    {
        assertThat(assertions.expression("a <> b")
                .binding("a", "100000000037")
                .binding("b", "100000000037"))
                .isEqualTo(false);

        assertThat(assertions.expression("a <> b")
                .binding("a", "37")
                .binding("b", "100000000017"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <> b")
                .binding("a", "100000000017")
                .binding("b", "37"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <> b")
                .binding("a", "100000000017")
                .binding("b", "100000000017"))
                .isEqualTo(false);
    }

    @Test
    public void testLessThan()
    {
        assertThat(assertions.operator(LESS_THAN, "100000000037", "100000000037"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "100000000037", "100000000017"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "100000000017", "100000000037"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "100000000017", "100000000017"))
                .isEqualTo(false);
    }

    @Test
    public void testLessThanOrEqual()
    {
        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "100000000037", "100000000037"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "100000000037", "100000000017"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "100000000017", "100000000037"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "100000000017", "100000000017"))
                .isEqualTo(true);
    }

    @Test
    public void testGreaterThan()
    {
        assertThat(assertions.expression("a > b")
                .binding("a", "100000000037")
                .binding("b", "100000000037"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "100000000037")
                .binding("b", "100000000017"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "100000000017")
                .binding("b", "100000000037"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "100000000017")
                .binding("b", "100000000017"))
                .isEqualTo(false);
    }

    @Test
    public void testGreaterThanOrEqual()
    {
        assertThat(assertions.expression("a >= b")
                .binding("a", "100000000037")
                .binding("b", "100000000037"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "100000000037")
                .binding("b", "100000000017"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "100000000017")
                .binding("b", "100000000037"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "100000000017")
                .binding("b", "100000000017"))
                .isEqualTo(true);
    }

    @Test
    public void testBetween()
    {
        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "100000000037")
                .binding("low", "100000000037")
                .binding("high", "100000000037"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "100000000037")
                .binding("low", "100000000037")
                .binding("high", "100000000017"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "100000000037")
                .binding("low", "100000000017")
                .binding("high", "100000000037"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "100000000037")
                .binding("low", "100000000017")
                .binding("high", "100000000017"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "100000000017")
                .binding("low", "100000000037")
                .binding("high", "100000000037"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "100000000017")
                .binding("low", "100000000037")
                .binding("high", "100000000017"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "100000000017")
                .binding("low", "100000000017")
                .binding("high", "100000000037"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "100000000017")
                .binding("low", "100000000017")
                .binding("high", "100000000017"))
                .isEqualTo(true);
    }

    @Test
    public void testCastToBigint()
    {
        assertThat(assertions.expression("cast(a as bigint)")
                .binding("a", "100000000037"))
                .isEqualTo(100000000037L);

        assertThat(assertions.expression("cast(a as bigint)")
                .binding("a", "100000000017"))
                .isEqualTo(100000000017L);
    }

    @Test
    public void testCastToVarchar()
    {
        assertThat(assertions.expression("cast(a as varchar)")
                .binding("a", "BIGINT '37'"))
                .hasType(VARCHAR)
                .isEqualTo("37");

        assertThat(assertions.expression("cast(a as varchar)")
                .binding("a", "100000000017"))
                .hasType(VARCHAR)
                .isEqualTo("100000000017");

        assertThat(assertions.expression("cast(a as varchar(13))")
                .binding("a", "100000000017"))
                .hasType(createVarcharType(13))
                .isEqualTo("100000000017");

        assertThat(assertions.expression("cast(a as varchar(50))")
                .binding("a", "100000000017"))
                .hasType(createVarcharType(50))
                .isEqualTo("100000000017");

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as varchar(2))")
                .binding("a", "100000000017")
                .evaluate())
                .hasMessage("Value 100000000017 cannot be represented as varchar(2)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testCastToDouble()
    {
        assertThat(assertions.expression("cast(a as double)")
                .binding("a", "BIGINT '37'"))
                .isEqualTo(37.0);

        assertThat(assertions.expression("cast(a as double)")
                .binding("a", "100000000017"))
                .isEqualTo(100000000017.0);
    }

    @Test
    public void testCastToFloat()
    {
        assertThat(assertions.expression("cast(a as real)")
                .binding("a", "BIGINT '37'"))
                .isEqualTo(37.0f);

        assertThat(assertions.expression("cast(a as real)")
                .binding("a", "-100000000017"))
                .isEqualTo(-100000000017.0f);

        assertThat(assertions.expression("cast(a as real)")
                .binding("a", "BIGINT '0'"))
                .isEqualTo(0.0f);
    }

    @Test
    public void testCastToBoolean()
    {
        assertThat(assertions.expression("cast(a as boolean)")
                .binding("a", "BIGINT '37'"))
                .isEqualTo(true);

        assertThat(assertions.expression("cast(a as boolean)")
                .binding("a", "100000000017"))
                .isEqualTo(true);

        assertThat(assertions.expression("cast(a as boolean)")
                .binding("a", "BIGINT '0'"))
                .isEqualTo(false);
    }

    @Test
    public void testCastFromVarchar()
    {
        assertThat(assertions.expression("cast(a as bigint)")
                .binding("a", "'100000000037'"))
                .isEqualTo(100000000037L);

        assertThat(assertions.expression("cast(a as bigint)")
                .binding("a", "'100000000017'"))
                .isEqualTo(100000000017L);
    }

    @Test
    public void testIsDistinctFrom()
    {
        assertThat(assertions.operator(IS_DISTINCT_FROM, "CAST(NULL AS BIGINT)", "CAST(NULL AS BIGINT)"))
                .isEqualTo(false);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "100000000037", "100000000037"))
                .isEqualTo(false);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "100000000037", "100000000038"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "NULL", "100000000037"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "100000000037", "NULL"))
                .isEqualTo(true);
    }

    @Test
    public void testOverflowAdd()
    {
        assertTrinoExceptionThrownBy(() -> assertions.operator(ADD, Long.toString(Long.MAX_VALUE), "BIGINT '1'").evaluate())
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("bigint addition overflow: 9223372036854775807 + 1");
    }

    @Test
    public void testUnderflowSubtract()
    {
        assertTrinoExceptionThrownBy(() -> assertions.operator(SUBTRACT, Long.toString(Long.MIN_VALUE), "BIGINT '1'").evaluate())
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("bigint subtraction overflow: -9223372036854775808 - 1");
    }

    @Test
    public void testOverflowMultiply()
    {
        assertTrinoExceptionThrownBy(() -> assertions.operator(MULTIPLY, Long.toString(Long.MAX_VALUE), "2").evaluate())
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("bigint multiplication overflow: 9223372036854775807 * 2");

        assertTrinoExceptionThrownBy(() -> assertions.operator(MULTIPLY, Long.toString(Long.MIN_VALUE), "-1").evaluate())
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("bigint multiplication overflow: -9223372036854775808 * -1");
    }

    @Test
    public void testOverflowDivide()
    {
        assertTrinoExceptionThrownBy(() -> assertions.operator(DIVIDE, Long.toString(Long.MIN_VALUE), "-1").evaluate())
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("bigint division overflow: -9223372036854775808 / -1");
    }

    @Test
    public void testIndeterminate()
    {
        assertThat(assertions.operator(INDETERMINATE, "cast(null as bigint)"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "cast(1 as bigint)"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "cast(4499999999 as bigint)"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "4499999999"))
                .isEqualTo(false);
    }

    @Test
    public void testNegateOverflow()
    {
        assertTrinoExceptionThrownBy(() -> assertions.operator(NEGATION, Long.toString(Long.MIN_VALUE)).evaluate())
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("bigint negation overflow: -9223372036854775808");
    }
}

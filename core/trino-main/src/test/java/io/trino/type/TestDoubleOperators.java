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

import java.lang.invoke.MethodHandle;

import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
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
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.lang.Double.doubleToLongBits;
import static java.lang.Double.doubleToRawLongBits;
import static java.lang.Double.isNaN;
import static java.lang.Double.longBitsToDouble;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@TestInstance(PER_CLASS)
public class TestDoubleOperators
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
        assertThat(assertions.expression("37.7E0"))
                .isEqualTo(37.7);

        assertThat(assertions.expression("17.1E0"))
                .isEqualTo(17.1);
    }

    @Test
    public void testTypeConstructor()
    {
        assertThat(assertions.expression("DOUBLE '12.34'"))
                .isEqualTo(12.34);

        assertThat(assertions.expression("DOUBLE '-17.6'"))
                .isEqualTo(-17.6);

        assertThat(assertions.expression("DOUBLE '+754'"))
                .isEqualTo(754.0);

        assertThat(assertions.expression("DOUBLE 'NaN'"))
                .isEqualTo(Double.NaN);

        assertThat(assertions.expression("DOUBLE '-NaN'"))
                .isEqualTo(Double.NaN);

        assertThat(assertions.expression("DOUBLE PRECISION '12.34'"))
                .isEqualTo(12.34);

        assertThat(assertions.expression("DOUBLE PRECISION '-17.6'"))
                .isEqualTo(-17.6);

        assertThat(assertions.expression("DOUBLE PRECISION '+754'"))
                .isEqualTo(754.0);

        assertThat(assertions.expression("DOUBLE PRECISION 'NaN'"))
                .isEqualTo(Double.NaN);

        assertThat(assertions.expression("DOUBLE PRECISION '-NaN'"))
                .isEqualTo(Double.NaN);
    }

    @Test
    public void testAdd()
    {
        assertThat(assertions.operator(ADD, "37.7E0", "37.7E0"))
                .isEqualTo(37.7 + 37.7);

        assertThat(assertions.operator(ADD, "37.7E0", "17.1E0"))
                .isEqualTo(37.7 + 17.1);

        assertThat(assertions.operator(ADD, "17.1E0", "37.7E0"))
                .isEqualTo(17.1 + 37.7);

        assertThat(assertions.operator(ADD, "17.1E0", "17.1E0"))
                .isEqualTo(17.1 + 17.1);

        assertThat(assertions.operator(ADD, "DOUBLE 'NaN'", "37.7E0"))
                .isEqualTo(Double.NaN);

        assertThat(assertions.operator(ADD, "37.7E0", "DOUBLE 'NaN'"))
                .isEqualTo(Double.NaN);

        assertThat(assertions.operator(ADD, "DOUBLE 'NaN'", "DOUBLE '-NaN'"))
                .isEqualTo(Double.NaN);
    }

    @Test
    public void testSubtract()
    {
        assertThat(assertions.operator(SUBTRACT, "37.7E0", "37.7E0"))
                .isEqualTo(0.0);

        assertThat(assertions.operator(SUBTRACT, "37.7E0", "17.1E0"))
                .isEqualTo(37.7 - 17.1);

        assertThat(assertions.operator(SUBTRACT, "17.1E0", "37.7E0"))
                .isEqualTo(17.1 - 37.7);

        assertThat(assertions.operator(SUBTRACT, "17.1E0", "17.1E0"))
                .isEqualTo(0.0);

        assertThat(assertions.operator(SUBTRACT, "DOUBLE 'NaN'", "37.7E0"))
                .isEqualTo(Double.NaN);

        assertThat(assertions.operator(SUBTRACT, "37.7E0", "DOUBLE 'NaN'"))
                .isEqualTo(Double.NaN);

        assertThat(assertions.operator(SUBTRACT, "DOUBLE 'NaN'", "DOUBLE 'NaN'"))
                .isEqualTo(Double.NaN);
    }

    @Test
    public void testMultiply()
    {
        assertThat(assertions.operator(MULTIPLY, "37.7E0", "37.7E0"))
                .isEqualTo(37.7 * 37.7);

        assertThat(assertions.operator(MULTIPLY, "37.7E0", "17.1E0"))
                .isEqualTo(37.7 * 17.1);

        assertThat(assertions.operator(MULTIPLY, "17.1E0", "37.7E0"))
                .isEqualTo(17.1 * 37.7);

        assertThat(assertions.operator(MULTIPLY, "17.1E0", "17.1E0"))
                .isEqualTo(17.1 * 17.1);

        assertThat(assertions.operator(MULTIPLY, "DOUBLE 'NaN'", "37.7E0"))
                .isEqualTo(Double.NaN);

        assertThat(assertions.operator(MULTIPLY, "37.7E0", "DOUBLE 'NaN'"))
                .isEqualTo(Double.NaN);

        assertThat(assertions.operator(MULTIPLY, "DOUBLE 'NaN'", "DOUBLE '-NaN'"))
                .isEqualTo(Double.NaN);
    }

    @Test
    public void testDivide()
    {
        assertThat(assertions.operator(DIVIDE, "37.7E0", "37.7E0"))
                .isEqualTo(1.0);

        assertThat(assertions.operator(DIVIDE, "37.7E0", "17.1E0"))
                .isEqualTo(37.7 / 17.1);

        assertThat(assertions.operator(DIVIDE, "17.1E0", "37.7E0"))
                .isEqualTo(17.1 / 37.7);

        assertThat(assertions.operator(DIVIDE, "17.1E0", "17.1E0"))
                .isEqualTo(1.0);

        assertThat(assertions.operator(DIVIDE, "DOUBLE 'NaN'", "37.7E0"))
                .isEqualTo(Double.NaN);

        assertThat(assertions.operator(DIVIDE, "37.7E0", "DOUBLE 'NaN'"))
                .isEqualTo(Double.NaN);

        assertThat(assertions.operator(DIVIDE, "DOUBLE 'NaN'", "DOUBLE '-NaN'"))
                .isEqualTo(Double.NaN);
    }

    @Test
    public void testModulus()
    {
        assertThat(assertions.operator(MODULUS, "37.7E0", "37.7E0"))
                .isEqualTo(0.0);

        assertThat(assertions.operator(MODULUS, "37.7E0", "17.1E0"))
                .isEqualTo(37.7 % 17.1);

        assertThat(assertions.operator(MODULUS, "17.1E0", "37.7E0"))
                .isEqualTo(17.1 % 37.7);

        assertThat(assertions.operator(MODULUS, "17.1E0", "17.1E0"))
                .isEqualTo(0.0);

        assertThat(assertions.operator(MODULUS, "DOUBLE 'NaN'", "37.7E0"))
                .isEqualTo(Double.NaN);

        assertThat(assertions.operator(MODULUS, "37.7E0", "DOUBLE 'NaN'"))
                .isEqualTo(Double.NaN);

        assertThat(assertions.operator(MODULUS, "DOUBLE 'NaN'", "DOUBLE 'NaN'"))
                .isEqualTo(Double.NaN);
    }

    @Test
    public void testNegation()
    {
        assertThat(assertions.operator(NEGATION, "(37.7E0)"))
                .isEqualTo(-37.7);

        assertThat(assertions.operator(NEGATION, "(17.1E0)"))
                .isEqualTo(-17.1);

        assertThat(assertions.operator(NEGATION, "DOUBLE 'NaN'"))
                .isEqualTo(Double.NaN);
    }

    @Test
    public void testEqual()
    {
        assertThat(assertions.operator(EQUAL, "37.7E0", "37.7E0"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "37.7E0", "17.1E0"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "17.1E0", "37.7E0"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "17.1E0", "17.1E0"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "0E0", "-0E0"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "DOUBLE 'NaN'", "37.7E0"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "37.7E0", "DOUBLE 'NaN'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "DOUBLE 'NaN'", "DOUBLE 'NaN'"))
                .isEqualTo(false);
    }

    @Test
    public void testNotEqual()
    {
        assertThat(assertions.expression("a <> b")
                .binding("a", "37.7E0")
                .binding("b", "37.7E0"))
                .isEqualTo(false);

        assertThat(assertions.expression("a <> b")
                .binding("a", "37.7E0")
                .binding("b", "17.1E0"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <> b")
                .binding("a", "17.1E0")
                .binding("b", "37.7E0"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <> b")
                .binding("a", "17.1E0")
                .binding("b", "17.1E0"))
                .isEqualTo(false);

        assertThat(assertions.expression("a <> b")
                .binding("a", "DOUBLE 'NaN'")
                .binding("b", "37.7E0"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <> b")
                .binding("a", "37.7E0")
                .binding("b", "DOUBLE 'NaN'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <> b")
                .binding("a", "DOUBLE 'NaN'")
                .binding("b", "DOUBLE 'NaN'"))
                .isEqualTo(true);
    }

    @Test
    public void testLessThan()
    {
        assertThat(assertions.operator(LESS_THAN, "37.7E0", "37.7E0"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "37.7E0", "17.1E0"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "17.1E0", "37.7E0"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "17.1E0", "17.1E0"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "DOUBLE 'NaN'", "37.7E0"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "37.7E0", "DOUBLE 'NaN'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "DOUBLE 'NaN'", "DOUBLE 'NaN'"))
                .isEqualTo(false);
    }

    @Test
    public void testLessThanOrEqual()
    {
        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "37.7E0", "37.7E0"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "37.7E0", "17.1E0"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "17.1E0", "37.7E0"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "17.1E0", "17.1E0"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "DOUBLE 'NaN'", "37.7E0"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "37.7E0", "DOUBLE 'NaN'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "DOUBLE 'NaN'", "DOUBLE 'NaN'"))
                .isEqualTo(false);
    }

    @Test
    public void testGreaterThan()
    {
        assertThat(assertions.expression("a > b")
                .binding("a", "37.7E0")
                .binding("b", "37.7E0"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "37.7E0")
                .binding("b", "17.1E0"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "17.1E0")
                .binding("b", "37.7E0"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "17.1E0")
                .binding("b", "17.1E0"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "DOUBLE 'NaN'")
                .binding("b", "37.7E0"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "37.7E0")
                .binding("b", "DOUBLE 'NaN'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "DOUBLE 'NaN'")
                .binding("b", "DOUBLE 'NaN'"))
                .isEqualTo(false);
    }

    @Test
    public void testGreaterThanOrEqual()
    {
        assertThat(assertions.expression("a >= b")
                .binding("a", "37.7E0")
                .binding("b", "37.7E0"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "37.7E0")
                .binding("b", "17.1E0"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "17.1E0")
                .binding("b", "37.7E0"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "17.1E0")
                .binding("b", "17.1E0"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "DOUBLE 'NaN'")
                .binding("b", "37.7E0"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "37.7E0")
                .binding("b", "DOUBLE 'NaN'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "DOUBLE 'NaN'")
                .binding("b", "DOUBLE 'NaN'"))
                .isEqualTo(false);
    }

    @Test
    public void testBetween()
    {
        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "37.7E0")
                .binding("low", "37.7E0")
                .binding("high", "37.7E0"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "37.7E0")
                .binding("low", "37.7E0")
                .binding("high", "17.1E0"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "37.7E0")
                .binding("low", "17.1E0")
                .binding("high", "37.7E0"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "37.7E0")
                .binding("low", "17.1E0")
                .binding("high", "17.1E0"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "17.1E0")
                .binding("low", "37.7E0")
                .binding("high", "37.7E0"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "17.1E0")
                .binding("low", "37.7E0")
                .binding("high", "17.1E0"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "17.1E0")
                .binding("low", "17.1E0")
                .binding("high", "37.7E0"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "17.1E0")
                .binding("low", "17.1E0")
                .binding("high", "17.1E0"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "DOUBLE 'NaN'")
                .binding("low", "17.1E0")
                .binding("high", "37.7E0"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "17.1E0")
                .binding("low", "DOUBLE '-NaN'")
                .binding("high", "DOUBLE 'NaN'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "17.1E0")
                .binding("low", "DOUBLE 'NaN'")
                .binding("high", "DOUBLE '-NaN'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "17.1E0")
                .binding("low", "17.1E0")
                .binding("high", "DOUBLE 'NaN'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "DOUBLE 'NaN'")
                .binding("low", "DOUBLE 'NaN'")
                .binding("high", "DOUBLE 'NaN'"))
                .isEqualTo(false);
    }

    @Test
    public void testCastToVarchar()
    {
        assertThat(assertions.expression("cast(a as varchar)")
                .binding("a", "37.7E0"))
                .hasType(VARCHAR)
                .isEqualTo("3.77E1");

        assertThat(assertions.expression("cast(a as varchar)")
                .binding("a", "17.1E0"))
                .hasType(VARCHAR)
                .isEqualTo("1.71E1");

        assertThat(assertions.expression("cast(0E0 / 0E0 AS varchar)"))
                .hasType(VARCHAR)
                .isEqualTo("NaN");

        assertThat(assertions.expression("cast(a as varchar(6))")
                .binding("a", "12e2"))
                .hasType(createVarcharType(6))
                .isEqualTo("1.2E3");

        assertThat(assertions.expression("cast(a as varchar(50))")
                .binding("a", "12e2"))
                .hasType(createVarcharType(50))
                .isEqualTo("1.2E3");

        assertThat(assertions.expression("cast(a as varchar(50))")
                .binding("a", "12345678.9e0"))
                .hasType(createVarcharType(50))
                .isEqualTo("1.23456789E7");

        assertThat(assertions.expression("cast(a as varchar(3))")
                .binding("a", "DOUBLE 'NaN'"))
                .hasType(createVarcharType(3))
                .isEqualTo("NaN");

        assertThat(assertions.expression("cast(a as varchar(50))")
                .binding("a", "DOUBLE 'Infinity'"))
                .hasType(createVarcharType(50))
                .isEqualTo("Infinity");

        assertThat(assertions.expression("cast(a as varchar(5))")
                .binding("a", "12e2"))
                .hasType(createVarcharType(5))
                .isEqualTo("1.2E3");

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as varchar(4))")
                .binding("a", "12e2")
                .evaluate())
                .hasMessage("Value 1200.0 (1.2E3) cannot be represented as varchar(4)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as varchar(2))")
                .binding("a", "0e0")
                .evaluate())
                .hasMessage("Value 0.0 (0E0) cannot be represented as varchar(2)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as varchar(3))")
                .binding("a", "-0e0")
                .evaluate())
                .hasMessage("Value -0.0 (-0E0) cannot be represented as varchar(3)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(0e0 / 0e0 AS varchar(2))").evaluate())
                .hasMessage("Value NaN (NaN) cannot be represented as varchar(2)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as varchar(7))")
                .binding("a", "DOUBLE 'Infinity'")
                .evaluate())
                .hasMessage("Value Infinity (Infinity) cannot be represented as varchar(7)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testCastToBigint()
    {
        assertThat(assertions.expression("cast(a as bigint)")
                .binding("a", "37.7E0"))
                .isEqualTo(38L);

        assertThat(assertions.expression("cast(a as bigint)")
                .binding("a", "-37.7E0"))
                .isEqualTo(-38L);

        assertThat(assertions.expression("cast(a as bigint)")
                .binding("a", "17.1E0"))
                .isEqualTo(17L);

        assertThat(assertions.expression("cast(a as bigint)")
                .binding("a", "-17.1E0"))
                .isEqualTo(-17L);

        assertThat(assertions.expression("cast(a as bigint)")
                .binding("a", "9.2E18"))
                .isEqualTo(9200000000000000000L);

        assertThat(assertions.expression("cast(a as bigint)")
                .binding("a", "-9.2E18"))
                .isEqualTo(-9200000000000000000L);

        assertThat(assertions.expression("cast(a as bigint)")
                .binding("a", "2.21E9"))
                .isEqualTo(2210000000L);

        assertThat(assertions.expression("cast(a as bigint)")
                .binding("a", "-2.21E9"))
                .isEqualTo(-2210000000L);

        assertThat(assertions.expression("cast(a as bigint)")
                .binding("a", "17.5E0"))
                .isEqualTo(18L);

        assertThat(assertions.expression("cast(a as bigint)")
                .binding("a", "-17.5E0"))
                .isEqualTo(-18L);

        assertThat(assertions.expression("cast(a as bigint)")
                .binding("a", Double.toString(Math.nextDown(0x1.0p63))))
                .isEqualTo((long) Math.nextDown(0x1.0p63));

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as bigint)")
                .binding("a", Double.toString(0x1.0p63))
                .evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as bigint)")
                .binding("a", Double.toString(Math.nextUp(0x1.0p63))).evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as bigint)")
                .binding("a", Double.toString(Math.nextDown(-0x1.0p63))).evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertThat(assertions.expression("cast(a as bigint)")
                .binding("a", Double.toString(-0x1.0p63)))
                .isEqualTo((long) -0x1.0p63);

        assertThat(assertions.expression("cast(a as bigint)")
                .binding("a", Double.toString(Math.nextUp(-0x1.0p63))))
                .isEqualTo((long) Math.nextUp(-0x1.0p63));

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as bigint)")
                .binding("a", "9.3E18")
                .evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as bigint)")
                .binding("a", "-9.3E18")
                .evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as bigint)")
                .binding("a", "infinity()")
                .evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as bigint)")
                .binding("a", "-infinity()")
                .evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as bigint)")
                .binding("a", "nan()")
                .evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testCastNaN()
    {
        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as integer)")
                .binding("a", "nan()")
                .evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as smallint)")
                .binding("a", "nan()")
                .evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as tinyint)")
                .binding("a", "nan()")
                .evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertThat(assertions.expression("cast(a as real)")
                .binding("a", "nan()"))
                .isEqualTo(Float.NaN);
    }

    @Test
    public void testCastToBoolean()
    {
        assertThat(assertions.expression("cast(a as boolean)")
                .binding("a", "37.7E0"))
                .isEqualTo(true);

        assertThat(assertions.expression("cast(a as boolean)")
                .binding("a", "17.1E0"))
                .isEqualTo(true);

        assertThat(assertions.expression("cast(a as boolean)")
                .binding("a", "0.0E0"))
                .isEqualTo(false);

        assertThat(assertions.expression("cast(a as boolean)")
                .binding("a", "nan()"))
                .isEqualTo(true);
    }

    @Test
    public void testCastToFloat()
    {
        assertThat(assertions.expression("cast(a as real)")
                .binding("a", "'754.1985'"))
                .isEqualTo(754.1985f);

        assertThat(assertions.expression("cast(a as real)")
                .binding("a", "'-754.2008'"))
                .isEqualTo(-754.2008f);

        assertThat(assertions.expression("cast(a as real)")
                .binding("a", "'0.0'"))
                .isEqualTo(0.0f);

        assertThat(assertions.expression("cast(a as real)")
                .binding("a", "'-0.0'"))
                .isEqualTo(-0.0f);
    }

    @Test
    public void testCastFromVarchar()
    {
        assertThat(assertions.expression("cast(a as double)")
                .binding("a", "'37.7'"))
                .isEqualTo(37.7);

        assertThat(assertions.expression("cast(a as double)")
                .binding("a", "'17.1'"))
                .isEqualTo(17.1);

        assertThat(assertions.expression("cast(a as double)")
                .binding("a", "'NaN'"))
                .isEqualTo(Double.NaN);

        assertThat(assertions.expression("cast(a as double precision)")
                .binding("a", "'37.7'"))
                .isEqualTo(37.7);

        assertThat(assertions.expression("cast(a as double precision)")
                .binding("a", "'17.1'"))
                .isEqualTo(17.1);

        assertThat(assertions.expression("cast(a as double precision)")
                .binding("a", "'NaN'"))
                .isEqualTo(Double.NaN);
    }

    @Test
    public void testIsDistinctFrom()
    {
        assertThat(assertions.operator(IS_DISTINCT_FROM, "cast(NULL as DOUBLE)", "CAST(NULL AS DOUBLE)"))
                .isEqualTo(false);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "37.7", "37.7"))
                .isEqualTo(false);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "37", "37.8"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "NULL", "37.7"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "37.7", "NULL"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "nan()", "nan()"))
                .isEqualTo(false);
    }

    @Test
    public void testIndeterminate()
    {
        assertThat(assertions.operator(INDETERMINATE, "cast(null as double)"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "1.2"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "cast(1.2 as double)"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "cast(1 as double)"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "nan()"))
                .isEqualTo(false);
    }

    @Test
    public void testNanHash()
            throws Throwable
    {
        long[] nanRepresentations = new long[] {doubleToLongBits(Double.NaN), 0xfff8000000000000L, 0x7ff8123412341234L, 0xfff8123412341234L};
        for (long nanRepresentation : nanRepresentations) {
            assertTrue(isNaN(longBitsToDouble(nanRepresentation)));
            // longBitsToDouble() keeps the bitwise difference in NaN
            assertTrue(nanRepresentation == nanRepresentations[0]
                    || doubleToRawLongBits(longBitsToDouble(nanRepresentation)) != doubleToRawLongBits(longBitsToDouble(nanRepresentations[0])));

            assertEquals(executeHashOperator(longBitsToDouble(nanRepresentation)), executeHashOperator(longBitsToDouble(nanRepresentations[0])));
            assertEquals(executeXxHash64Operator(longBitsToDouble(nanRepresentation)), executeXxHash64Operator(longBitsToDouble(nanRepresentations[0])));
        }
    }

    @Test
    public void testZeroHash()
            throws Throwable
    {
        double[] zeroes = {0.0, -0.0};
        for (double zero : zeroes) {
            //noinspection SimplifiedTestNGAssertion
            assertTrue(zero == 0);
            assertEquals(executeHashOperator(zero), executeHashOperator(zeroes[0]));
            assertEquals(executeXxHash64Operator(zero), executeXxHash64Operator(zeroes[0]));
        }
    }

    private long executeHashOperator(double value)
            throws Throwable
    {
        MethodHandle hashCodeOperator = assertions.getQueryRunner()
                .getTypeManager()
                .getTypeOperators()
                .getHashCodeOperator(DOUBLE, simpleConvention(FAIL_ON_NULL, NEVER_NULL));

        return (long) hashCodeOperator.invokeExact(value);
    }

    private long executeXxHash64Operator(double value)
            throws Throwable
    {
        MethodHandle xxHash64Operator = assertions.getQueryRunner()
                .getTypeManager()
                .getTypeOperators()
                .getXxHash64Operator(DOUBLE, simpleConvention(FAIL_ON_NULL, NEVER_NULL));

        return (long) xxHash64Operator.invokeExact(value);
    }
}

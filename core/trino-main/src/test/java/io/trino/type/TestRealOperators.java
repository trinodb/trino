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
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.lang.Float.floatToIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Float.isNaN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@TestInstance(PER_CLASS)
public class TestRealOperators
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
    public void testTypeConstructor()
    {
        assertThat(assertions.expression("REAL '12.2'"))
                .isEqualTo(12.2f);

        assertThat(assertions.expression("REAL '-17.76'"))
                .isEqualTo(-17.76f);

        assertThat(assertions.expression("REAL 'NaN'"))
                .isEqualTo(Float.NaN);

        assertThat(assertions.expression("REAL '-NaN'"))
                .isEqualTo(Float.NaN);

        assertThat(assertions.expression("REAL 'Infinity'"))
                .isEqualTo(Float.POSITIVE_INFINITY);

        assertThat(assertions.expression("REAL '-Infinity'"))
                .isEqualTo(Float.NEGATIVE_INFINITY);
    }

    @Test
    public void testAdd()
    {
        assertThat(assertions.operator(ADD, "REAL '12.34'", "REAL '56.78'"))
                .isEqualTo(12.34f + 56.78f);

        assertThat(assertions.operator(ADD, "REAL '-17.34'", "REAL '-22.891'"))
                .isEqualTo(-17.34f + -22.891f);

        assertThat(assertions.operator(ADD, "REAL '-89.123'", "REAL '754.0'"))
                .isEqualTo(-89.123f + 754.0f);

        assertThat(assertions.operator(ADD, "REAL '-0.0'", "REAL '0.0'"))
                .isEqualTo(-0.0f + 0.0f);

        assertThat(assertions.operator(ADD, "REAL 'NaN'", "REAL '1.23'"))
                .isEqualTo(Float.NaN);

        assertThat(assertions.operator(ADD, "REAL '1.23'", "REAL 'NaN'"))
                .isEqualTo(Float.NaN);

        assertThat(assertions.operator(ADD, "REAL 'NaN'", "REAL '-NaN'"))
                .isEqualTo(Float.NaN);
    }

    @Test
    public void testSubtract()
    {
        assertThat(assertions.operator(SUBTRACT, "REAL '12.34'", "REAL '56.78'"))
                .isEqualTo(12.34f - 56.78f);

        assertThat(assertions.operator(SUBTRACT, "REAL '-17.34'", "REAL '-22.891'"))
                .isEqualTo(-17.34f - -22.891f);

        assertThat(assertions.operator(SUBTRACT, "REAL '-89.123'", "REAL '754.0'"))
                .isEqualTo(-89.123f - 754.0f);

        assertThat(assertions.operator(SUBTRACT, "REAL '-0.0'", "REAL '0.0'"))
                .isEqualTo(-0.0f - 0.0f);

        assertThat(assertions.operator(SUBTRACT, "REAL 'NaN'", "REAL '1.23'"))
                .isEqualTo(Float.NaN);

        assertThat(assertions.operator(SUBTRACT, "REAL '1.23'", "REAL 'NaN'"))
                .isEqualTo(Float.NaN);

        assertThat(assertions.operator(SUBTRACT, "REAL 'NaN'", "REAL 'NaN'"))
                .isEqualTo(Float.NaN);
    }

    @Test
    public void testMultiply()
    {
        assertThat(assertions.operator(MULTIPLY, "REAL '12.34'", "REAL '56.78'"))
                .isEqualTo(12.34f * 56.78f);

        assertThat(assertions.operator(MULTIPLY, "REAL '-17.34'", "REAL '-22.891'"))
                .isEqualTo(-17.34f * -22.891f);

        assertThat(assertions.operator(MULTIPLY, "REAL '-89.123'", "REAL '754.0'"))
                .isEqualTo(-89.123f * 754.0f);

        assertThat(assertions.operator(MULTIPLY, "REAL '-0.0'", "REAL '0.0'"))
                .isEqualTo(-0.0f * 0.0f);

        assertThat(assertions.operator(MULTIPLY, "REAL '-17.71'", "REAL '-1.0'"))
                .isEqualTo(-17.71f * -1.0f);

        assertThat(assertions.operator(MULTIPLY, "REAL 'NaN'", "REAL '1.23'"))
                .isEqualTo(Float.NaN);

        assertThat(assertions.operator(MULTIPLY, "REAL '1.23'", "REAL 'NaN'"))
                .isEqualTo(Float.NaN);

        assertThat(assertions.operator(MULTIPLY, "REAL 'NaN'", "REAL '-NaN'"))
                .isEqualTo(Float.NaN);
    }

    @Test
    public void testDivide()
    {
        assertThat(assertions.operator(DIVIDE, "REAL '12.34'", "REAL '56.78'"))
                .isEqualTo(12.34f / 56.78f);

        assertThat(assertions.operator(DIVIDE, "REAL '-17.34'", "REAL '-22.891'"))
                .isEqualTo(-17.34f / -22.891f);

        assertThat(assertions.operator(DIVIDE, "REAL '-89.123'", "REAL '754.0'"))
                .isEqualTo(-89.123f / 754.0f);

        assertThat(assertions.operator(DIVIDE, "REAL '-0.0'", "REAL '0.0'"))
                .isEqualTo(-0.0f / 0.0f);

        assertThat(assertions.operator(DIVIDE, "REAL '-17.71'", "REAL '-1.0'"))
                .isEqualTo(-17.71f / -1.0f);

        assertThat(assertions.operator(DIVIDE, "REAL 'NaN'", "REAL '1.23'"))
                .isEqualTo(Float.NaN);

        assertThat(assertions.operator(DIVIDE, "REAL '1.23'", "REAL 'NaN'"))
                .isEqualTo(Float.NaN);

        assertThat(assertions.operator(DIVIDE, "REAL 'NaN'", "REAL '-NaN'"))
                .isEqualTo(Float.NaN);
    }

    @Test
    public void testModulus()
    {
        assertThat(assertions.operator(MODULUS, "REAL '12.34'", "REAL '56.78'"))
                .isEqualTo(12.34f % 56.78f);

        assertThat(assertions.operator(MODULUS, "REAL '-17.34'", "REAL '-22.891'"))
                .isEqualTo(-17.34f % -22.891f);

        assertThat(assertions.operator(MODULUS, "REAL '-89.123'", "REAL '754.0'"))
                .isEqualTo(-89.123f % 754.0f);

        assertThat(assertions.operator(MODULUS, "REAL '-0.0'", "REAL '0.0'"))
                .isEqualTo(-0.0f % 0.0f);

        assertThat(assertions.operator(MODULUS, "REAL '-17.71'", "REAL '-1.0'"))
                .isEqualTo(-17.71f % -1.0f);

        assertThat(assertions.operator(MODULUS, "REAL 'NaN'", "REAL '1.23'"))
                .isEqualTo(Float.NaN);

        assertThat(assertions.operator(MODULUS, "REAL '1.23'", "REAL 'NaN'"))
                .isEqualTo(Float.NaN);

        assertThat(assertions.operator(MODULUS, "REAL 'NaN'", "REAL 'NaN'"))
                .isEqualTo(Float.NaN);
    }

    @Test
    public void testNegation()
    {
        assertThat(assertions.operator(NEGATION, "REAL '12.34'"))
                .isEqualTo(-12.34f);

        assertThat(assertions.operator(NEGATION, "REAL '-17.34'"))
                .isEqualTo(17.34f);

        assertThat(assertions.operator(NEGATION, "REAL '-0.0'"))
                .isEqualTo(0.0f);

        assertThat(assertions.operator(NEGATION, "REAL 'NaN'"))
                .isEqualTo(Float.NaN);

        assertThat(assertions.operator(NEGATION, "REAL '-NaN'"))
                .isEqualTo(Float.NaN);
    }

    @Test
    public void testEqual()
    {
        assertThat(assertions.operator(EQUAL, "REAL '12.34'", "REAL '12.34'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "REAL '12.340'", "REAL '12.34'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "REAL '-17.34'", "REAL '-17.34'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "REAL '71.17'", "REAL '23.45'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "REAL '-0.0'", "REAL '0.0'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "REAL 'NaN'", "REAL '1.23'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "REAL '1.23'", "REAL 'NaN'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "REAL 'NaN'", "REAL 'NaN'"))
                .isEqualTo(false);
    }

    @Test
    public void testNotEqual()
    {
        assertThat(assertions.expression("a <> b")
                .binding("a", "REAL '12.34'")
                .binding("b", "REAL '12.34'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a <> b")
                .binding("a", "REAL '12.34'")
                .binding("b", "REAL '12.340'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a <> b")
                .binding("a", "REAL '-17.34'")
                .binding("b", "REAL '-17.34'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a <> b")
                .binding("a", "REAL '71.17'")
                .binding("b", "REAL '23.45'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <> b")
                .binding("a", "REAL '-0.0'")
                .binding("b", "REAL '0.0'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a <> b")
                .binding("a", "REAL 'NaN'")
                .binding("b", "REAL '1.23'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <> b")
                .binding("a", "REAL '1.23'")
                .binding("b", "REAL 'NaN'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <> b")
                .binding("a", "REAL 'NaN'")
                .binding("b", "REAL 'NaN'"))
                .isEqualTo(true);
    }

    @Test
    public void testLessThan()
    {
        assertThat(assertions.operator(LESS_THAN, "REAL '12.34'", "REAL '754.123'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "REAL '-17.34'", "REAL '-16.34'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "REAL '71.17'", "REAL '23.45'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "REAL '-0.0'", "REAL '0.0'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "REAL 'NaN'", "REAL '1.23'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "REAL '1.23'", "REAL 'NaN'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "REAL 'NaN'", "REAL 'NaN'"))
                .isEqualTo(false);
    }

    @Test
    public void testLessThanOrEqual()
    {
        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "REAL '12.34'", "REAL '754.123'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "REAL '-17.34'", "REAL '-17.34'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "REAL '71.17'", "REAL '23.45'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "REAL '-0.0'", "REAL '0.0'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "REAL 'NaN'", "REAL '1.23'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "REAL '1.23'", "REAL 'NaN'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "REAL 'NaN'", "REAL 'NaN'"))
                .isEqualTo(false);
    }

    @Test
    public void testGreaterThan()
    {
        assertThat(assertions.expression("a > b")
                .binding("a", "REAL '12.34'")
                .binding("b", "REAL '754.123'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "REAL '-17.34'")
                .binding("b", "REAL '-17.34'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "REAL '71.17'")
                .binding("b", "REAL '23.45'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "REAL '-0.0'")
                .binding("b", "REAL '0.0'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "REAL 'NaN'")
                .binding("b", "REAL '1.23'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "REAL '1.23'")
                .binding("b", "REAL 'NaN'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "REAL 'NaN'")
                .binding("b", "REAL 'NaN'"))
                .isEqualTo(false);
    }

    @Test
    public void testGreaterThanOrEqual()
    {
        assertThat(assertions.expression("a >= b")
                .binding("a", "REAL '12.34'")
                .binding("b", "REAL '754.123'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "REAL '-17.34'")
                .binding("b", "REAL '-17.34'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "REAL '71.17'")
                .binding("b", "REAL '23.45'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "REAL '-0.0'")
                .binding("b", "REAL '0.0'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "REAL 'NaN'")
                .binding("b", "REAL '1.23'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "REAL '1.23'")
                .binding("b", "REAL 'NaN'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "REAL 'NaN'")
                .binding("b", "REAL 'NaN'"))
                .isEqualTo(false);
    }

    @Test
    public void testBetween()
    {
        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "REAL '12.34'")
                .binding("low", "REAL '9.12'")
                .binding("high", "REAL '25.89'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "REAL '-17.34'")
                .binding("low", "REAL '-17.34'")
                .binding("high", "REAL '-16.57'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "REAL '-17.34'")
                .binding("low", "REAL '-18.98'")
                .binding("high", "REAL '-17.34'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "REAL '0.0'")
                .binding("low", "REAL '-1.2'")
                .binding("high", "REAL '2.3'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "REAL '56.78'")
                .binding("low", "REAL '12.34'")
                .binding("high", "REAL '34.56'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "REAL '56.78'")
                .binding("low", "REAL '78.89'")
                .binding("high", "REAL '98.765'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "REAL 'NaN'")
                .binding("low", "REAL '-1.2'")
                .binding("high", "REAL '2.3'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "REAL '56.78'")
                .binding("low", "REAL '-NaN'")
                .binding("high", "REAL 'NaN'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "REAL '56.78'")
                .binding("low", "REAL 'NaN'")
                .binding("high", "REAL '-NaN'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "REAL '56.78'")
                .binding("low", "REAL '56.78'")
                .binding("high", "REAL 'NaN'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "REAL 'NaN'")
                .binding("low", "REAL 'NaN'")
                .binding("high", "REAL 'NaN'"))
                .isEqualTo(false);
    }

    @Test
    public void testCastToVarchar()
    {
        assertThat(assertions.expression("cast(a as VARCHAR)")
                .binding("a", "REAL '754.1985'"))
                .hasType(VARCHAR)
                .isEqualTo("7.541985E2");

        assertThat(assertions.expression("cast(a as VARCHAR)")
                .binding("a", "REAL '-754.2008'"))
                .hasType(VARCHAR)
                .isEqualTo("-7.542008E2");

        assertThat(assertions.expression("cast(a as VARCHAR)")
                .binding("a", "REAL 'Infinity'"))
                .hasType(VARCHAR)
                .isEqualTo("Infinity");

        assertThat(assertions.expression("CAST(a AS VARCHAR)")
                .binding("a", "CAST(nan() AS REAL)"))
                .hasType(VARCHAR)
                .isEqualTo("NaN");

        assertThat(assertions.expression("cast(a as varchar(6))")
                .binding("a", "REAL '12e2'"))
                .hasType(createVarcharType(6))
                .isEqualTo("1.2E3");

        assertThat(assertions.expression("cast(a as varchar(50))")
                .binding("a", "REAL '12e2'"))
                .hasType(createVarcharType(50))
                .isEqualTo("1.2E3");

        assertThat(assertions.expression("cast(a as varchar(50))")
                .binding("a", "REAL '12345678.9e0'"))
                .hasType(createVarcharType(50))
                .isEqualTo("1.234568E7");

        assertThat(assertions.expression("cast(a as varchar(3))")
                .binding("a", "REAL 'NaN'"))
                .hasType(createVarcharType(3))
                .isEqualTo("NaN");

        assertThat(assertions.expression("cast(a as varchar(50))")
                .binding("a", "REAL 'Infinity'"))
                .hasType(createVarcharType(50))
                .isEqualTo("Infinity");

        assertThat(assertions.expression("cast(a as varchar(5))")
                .binding("a", "REAL '12e2'"))
                .hasType(createVarcharType(5))
                .isEqualTo("1.2E3");

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as varchar(4))")
                .binding("a", "REAL '12e2'")
                .evaluate())
                .hasMessage("Value 1200.0 (1.2E3) cannot be represented as varchar(4)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as varchar(2))")
                .binding("a", "REAL '0e0'")
                .evaluate())
                .hasMessage("Value 0.0 (0E0) cannot be represented as varchar(2)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as varchar(3))")
                .binding("a", "REAL '-0e0'")
                .evaluate())
                .hasMessage("Value -0.0 (-0E0) cannot be represented as varchar(3)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("CAST(a AS varchar(2))")
                .binding("a", "nan()").evaluate())
                .hasMessage("Value NaN (NaN) cannot be represented as varchar(2)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as varchar(7))")
                .binding("a", "REAL 'Infinity'")
                .evaluate())
                .hasMessage("Value Infinity (Infinity) cannot be represented as varchar(7)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testCastToBigInt()
    {
        assertThat(assertions.expression("cast(a as BIGINT)")
                .binding("a", "REAL '754.1985'"))
                .isEqualTo(754L);

        assertThat(assertions.expression("cast(a as BIGINT)")
                .binding("a", "REAL '-754.2008'"))
                .isEqualTo(-754L);

        assertThat(assertions.expression("cast(a as BIGINT)")
                .binding("a", "REAL '1.98'"))
                .isEqualTo(2L);

        assertThat(assertions.expression("cast(a as BIGINT)")
                .binding("a", "REAL '-0.0'"))
                .isEqualTo(0L);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as BIGINT)")
                .binding("a", "REAL 'NaN'")
                .evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testCastToInteger()
    {
        assertThat(assertions.expression("cast(a as INTEGER)")
                .binding("a", "REAL '754.2008'"))
                .isEqualTo(754);

        assertThat(assertions.expression("cast(a as INTEGER)")
                .binding("a", "REAL '-754.1985'"))
                .isEqualTo(-754);

        assertThat(assertions.expression("cast(a as INTEGER)")
                .binding("a", "REAL '9.99'"))
                .isEqualTo(10);

        assertThat(assertions.expression("cast(a as INTEGER)")
                .binding("a", "REAL '-0.0'"))
                .isEqualTo(0);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as INTEGER)")
                .binding("a", "REAL 'NaN'")
                .evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testCastToSmallint()
    {
        assertThat(assertions.expression("cast(a as SMALLINT)")
                .binding("a", "REAL '754.2008'"))
                .isEqualTo((short) 754);

        assertThat(assertions.expression("cast(a as SMALLINT)")
                .binding("a", "REAL '-754.1985'"))
                .isEqualTo((short) -754);

        assertThat(assertions.expression("cast(a as SMALLINT)")
                .binding("a", "REAL '9.99'"))
                .isEqualTo((short) 10);

        assertThat(assertions.expression("cast(a as SMALLINT)")
                .binding("a", "REAL '-0.0'"))
                .isEqualTo((short) 0);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as SMALLINT)")
                .binding("a", "REAL 'NaN'")
                .evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testCastToTinyint()
    {
        assertThat(assertions.expression("cast(a as TINYINT)")
                .binding("a", "REAL '127.45'"))
                .isEqualTo((byte) 127);

        assertThat(assertions.expression("cast(a as TINYINT)")
                .binding("a", "REAL '-128.234'"))
                .isEqualTo((byte) -128);

        assertThat(assertions.expression("cast(a as TINYINT)")
                .binding("a", "REAL '9.99'"))
                .isEqualTo((byte) 10);

        assertThat(assertions.expression("cast(a as TINYINT)")
                .binding("a", "REAL '-0.0'"))
                .isEqualTo((byte) 0);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as TINYINT)")
                .binding("a", "REAL 'NaN'")
                .evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testCastToDouble()
    {
        assertThat(assertions.expression("cast(a as DOUBLE)")
                .binding("a", "REAL '754.1985'"))
                .isEqualTo((double) 754.1985f);

        assertThat(assertions.expression("cast(a as DOUBLE)")
                .binding("a", "REAL '-754.2008'"))
                .isEqualTo((double) -754.2008f);

        assertThat(assertions.expression("cast(a as DOUBLE)")
                .binding("a", "REAL '0.0'"))
                .isEqualTo((double) 0.0f);

        assertThat(assertions.expression("cast(a as DOUBLE)")
                .binding("a", "REAL '-0.0'"))
                .isEqualTo((double) -0.0f);

        assertThat(assertions.expression("cast(a as DOUBLE)")
                .binding("a", "REAL 'NaN'"))
                .isEqualTo(Double.NaN);
    }

    @Test
    public void testCastToBoolean()
    {
        assertThat(assertions.expression("cast(a as BOOLEAN)")
                .binding("a", "REAL '754.1985'"))
                .isEqualTo(true);

        assertThat(assertions.expression("cast(a as BOOLEAN)")
                .binding("a", "REAL '0.0'"))
                .isEqualTo(false);

        assertThat(assertions.expression("cast(a as BOOLEAN)")
                .binding("a", "REAL '-0.0'"))
                .isEqualTo(false);

        assertThat(assertions.expression("cast(a as BOOLEAN)")
                .binding("a", "REAL 'NaN'"))
                .isEqualTo(true);
    }

    @Test
    public void testIsDistinctFrom()
    {
        assertThat(assertions.operator(IS_DISTINCT_FROM, "CAST(NULL AS REAL)", "CAST(NULL AS REAL)"))
                .isEqualTo(false);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "REAL '37.7'", "REAL '37.7'"))
                .isEqualTo(false);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "REAL '37.7'", "REAL '37.8'"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "NULL", "REAL '37.7'"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "REAL '37.7'", "NULL"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "CAST(nan() AS REAL)", "CAST(nan() AS REAL)"))
                .isEqualTo(false);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "REAL 'NaN'", "REAL '37.8'"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "REAL '37.8'", "REAL 'NaN'"))
                .isEqualTo(true);
    }

    @Test
    public void testIndeterminate()
    {
        assertThat(assertions.operator(INDETERMINATE, "cast(null as real)"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "REAL '-1.2'"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "REAL '1.2'"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "REAL '123'"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "REAL 'NaN'"))
                .isEqualTo(false);
    }

    @Test
    public void testNanHash()
            throws Throwable
    {
        int[] nanRepresentations = {floatToIntBits(Float.NaN), 0xffc00000, 0x7fc00000, 0x7fc01234, 0xffc01234};
        for (int nanRepresentation : nanRepresentations) {
            assertTrue(isNaN(intBitsToFloat(nanRepresentation)));
            assertEquals(executeHashOperator(nanRepresentation), executeHashOperator(nanRepresentations[0]));
            assertEquals(executeXxHas64hOperator(nanRepresentation), executeXxHas64hOperator(nanRepresentations[0]));
        }
    }

    @Test
    public void testZeroHash()
            throws Throwable
    {
        int[] zeroes = {floatToIntBits(0.0f), floatToIntBits(-0.0f)};
        for (int zero : zeroes) {
            //noinspection SimplifiedTestNGAssertion
            assertTrue(intBitsToFloat(zero) == 0f);
            assertEquals(executeHashOperator(zero), executeHashOperator(zeroes[0]));
            assertEquals(executeXxHas64hOperator(zero), executeXxHas64hOperator(zeroes[0]));
        }
    }

    private long executeHashOperator(long value)
            throws Throwable
    {
        MethodHandle hashCodeOperator = assertions.getQueryRunner()
                .getTypeManager()
                .getTypeOperators()
                .getHashCodeOperator(REAL, simpleConvention(FAIL_ON_NULL, NEVER_NULL));

        return (long) hashCodeOperator.invokeExact((long) intBitsToFloat((int) value));
    }

    private long executeXxHas64hOperator(long value)
            throws Throwable
    {
        MethodHandle xxHash64Operator = assertions.getQueryRunner()
                .getTypeManager()
                .getTypeOperators()
                .getXxHash64Operator(REAL, simpleConvention(FAIL_ON_NULL, NEVER_NULL));

        return (long) xxHash64Operator.invokeExact((long) intBitsToFloat((int) value));
    }
}

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
package io.trino.operator.scalar;

import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestMathMethods
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
    public void testAbs()
    {
        assertThat(assertions.expression("a.abs()")
                .binding("a", "TINYINT '-7'"))
                .matches("abs(TINYINT '-7')");
        assertThat(assertions.expression("a.abs()")
                .binding("a", "SMALLINT '-7'"))
                .matches("abs(SMALLINT '-7')");
        assertThat(assertions.expression("a.abs()")
                .binding("a", "INTEGER '-7'"))
                .matches("abs(INTEGER '-7')");
        assertThat(assertions.expression("a.abs()")
                .binding("a", "BIGINT '-7'"))
                .matches("abs(BIGINT '-7')");
        assertThat(assertions.expression("a.abs()")
                .binding("a", "REAL '-7.5'"))
                .matches("abs(REAL '-7.5')");
        assertThat(assertions.expression("a.abs()")
                .binding("a", "DOUBLE '-7.5'"))
                .matches("abs(DOUBLE '-7.5')");
        assertThat(assertions.expression("a.abs()")
                .binding("a", "DECIMAL '-7.50'"))
                .matches("abs(DECIMAL '-7.50')");
    }

    @Test
    public void testCeiling()
    {
        assertThat(assertions.expression("a.ceiling()")
                .binding("a", "DOUBLE '3.1'"))
                .matches("ceiling(DOUBLE '3.1')");
        assertThat(assertions.expression("a.ceil()")
                .binding("a", "DOUBLE '3.1'"))
                .matches("ceil(DOUBLE '3.1')");
        assertThat(assertions.expression("a.ceiling()")
                .binding("a", "DECIMAL '3.12'"))
                .matches("ceiling(DECIMAL '3.12')");
    }

    @Test
    public void testFloor()
    {
        assertThat(assertions.expression("a.floor()")
                .binding("a", "DOUBLE '3.9'"))
                .matches("floor(DOUBLE '3.9')");
        assertThat(assertions.expression("a.floor()")
                .binding("a", "DECIMAL '3.99'"))
                .matches("floor(DECIMAL '3.99')");
    }

    @Test
    public void testRound()
    {
        assertThat(assertions.expression("a.round()")
                .binding("a", "DOUBLE '3.5'"))
                .matches("round(DOUBLE '3.5')");
        assertThat(assertions.expression("a.round(b)")
                .binding("a", "DOUBLE '3.14159'")
                .binding("b", "2"))
                .matches("round(DOUBLE '3.14159', 2)");
        assertThat(assertions.expression("a.round()")
                .binding("a", "DECIMAL '3.56'"))
                .matches("round(DECIMAL '3.56')");
        assertThat(assertions.expression("a.round(b)")
                .binding("a", "DECIMAL '3.567'")
                .binding("b", "2"))
                .matches("round(DECIMAL '3.567', 2)");
    }

    @Test
    public void testTruncate()
    {
        assertThat(assertions.expression("a.truncate()")
                .binding("a", "DOUBLE '3.99'"))
                .matches("truncate(DOUBLE '3.99')");
        assertThat(assertions.expression("a.truncate()")
                .binding("a", "DECIMAL '3.99'"))
                .matches("truncate(DECIMAL '3.99')");
        assertThat(assertions.expression("a.truncate(b)")
                .binding("a", "DECIMAL '3.987'")
                .binding("b", "1"))
                .matches("truncate(DECIMAL '3.987', 1)");
    }

    @Test
    public void testSign()
    {
        assertThat(assertions.expression("a.sign()")
                .binding("a", "BIGINT '-7'"))
                .matches("sign(BIGINT '-7')");
        assertThat(assertions.expression("a.sign()")
                .binding("a", "DOUBLE '-7.5'"))
                .matches("sign(DOUBLE '-7.5')");
        assertThat(assertions.expression("a.sign()")
                .binding("a", "DECIMAL '-7.50'"))
                .matches("sign(DECIMAL '-7.50')");
    }

    @Test
    public void testDoubleUnaryMethods()
    {
        assertThat(assertions.expression("a.sqrt()")
                .binding("a", "DOUBLE '16'"))
                .matches("sqrt(DOUBLE '16')");
        assertThat(assertions.expression("a.cbrt()")
                .binding("a", "DOUBLE '27'"))
                .matches("cbrt(DOUBLE '27')");
        assertThat(assertions.expression("a.exp()")
                .binding("a", "DOUBLE '1'"))
                .matches("exp(DOUBLE '1')");
        assertThat(assertions.expression("a.ln()")
                .binding("a", "DOUBLE '2.718281828459045'"))
                .matches("ln(DOUBLE '2.718281828459045')");
        assertThat(assertions.expression("a.log2()")
                .binding("a", "DOUBLE '8'"))
                .matches("log2(DOUBLE '8')");
        assertThat(assertions.expression("a.log10()")
                .binding("a", "DOUBLE '1000'"))
                .matches("log10(DOUBLE '1000')");
        assertThat(assertions.expression("a.sin()")
                .binding("a", "DOUBLE '1'"))
                .matches("sin(DOUBLE '1')");
        assertThat(assertions.expression("a.cos()")
                .binding("a", "DOUBLE '1'"))
                .matches("cos(DOUBLE '1')");
        assertThat(assertions.expression("a.tan()")
                .binding("a", "DOUBLE '1'"))
                .matches("tan(DOUBLE '1')");
        assertThat(assertions.expression("a.degrees()")
                .binding("a", "DOUBLE '3.141592653589793'"))
                .matches("degrees(DOUBLE '3.141592653589793')");
        assertThat(assertions.expression("a.radians()")
                .binding("a", "DOUBLE '180'"))
                .matches("radians(DOUBLE '180')");
    }

    @Test
    public void testBinaryMethods()
    {
        assertThat(assertions.expression("a.power(b)")
                .binding("a", "DOUBLE '2'")
                .binding("b", "DOUBLE '10'"))
                .matches("power(DOUBLE '2', DOUBLE '10')");
        assertThat(assertions.expression("a.pow(b)")
                .binding("a", "DOUBLE '2'")
                .binding("b", "DOUBLE '10'"))
                .matches("pow(DOUBLE '2', DOUBLE '10')");
        assertThat(assertions.expression("a.log(b)")
                .binding("a", "DOUBLE '2'")
                .binding("b", "DOUBLE '8'"))
                .matches("log(DOUBLE '2', DOUBLE '8')");
        assertThat(assertions.expression("a.atan2(b)")
                .binding("a", "DOUBLE '1'")
                .binding("b", "DOUBLE '1'"))
                .matches("atan2(DOUBLE '1', DOUBLE '1')");
        assertThat(assertions.expression("a.mod(b)")
                .binding("a", "BIGINT '10'")
                .binding("b", "BIGINT '3'"))
                .matches("mod(BIGINT '10', BIGINT '3')");
        assertThat(assertions.expression("a.mod(b)")
                .binding("a", "DOUBLE '10'")
                .binding("b", "DOUBLE '3'"))
                .matches("mod(DOUBLE '10', DOUBLE '3')");
    }

    @Test
    public void testPredicateMethods()
    {
        assertThat(assertions.expression("a.is_nan()")
                .binding("a", "nan()"))
                .matches("is_nan(nan())");
        assertThat(assertions.expression("a.is_finite()")
                .binding("a", "DOUBLE '1'"))
                .matches("is_finite(DOUBLE '1')");
        assertThat(assertions.expression("a.is_infinite()")
                .binding("a", "infinity()"))
                .matches("is_infinite(infinity())");
    }

    @Test
    public void testWidthBucket()
    {
        assertThat(assertions.expression("a.width_bucket(b, c, d)")
                .binding("a", "DOUBLE '3.14'")
                .binding("b", "DOUBLE '0'")
                .binding("c", "DOUBLE '4'")
                .binding("d", "BIGINT '4'"))
                .matches("width_bucket(DOUBLE '3.14', DOUBLE '0', DOUBLE '4', BIGINT '4')");
    }

    @Test
    public void testToBase()
    {
        assertThat(assertions.expression("a.to_base(b)")
                .binding("a", "BIGINT '255'")
                .binding("b", "BIGINT '16'"))
                .matches("to_base(BIGINT '255', BIGINT '16')");
    }

    @Test
    public void testStaticMethods()
    {
        assertThat(assertions.expression("double::pi()"))
                .matches("pi()");
        assertThat(assertions.expression("double::e()"))
                .matches("e()");
        assertThat(assertions.expression("double::nan()"))
                .matches("nan()");
        assertThat(assertions.expression("double::infinity()"))
                .matches("infinity()");
        assertThat(assertions.expression("bigint::from_base(a, b)")
                .binding("a", "'ff'")
                .binding("b", "BIGINT '16'"))
                .matches("from_base('ff', BIGINT '16')");
    }

    @Test
    public void testMethodNotResolvableAsFunction()
    {
        // The plain function namespace must not expose the receiver-less method as a function call.
        // (Spot-check via an instance method that has no like-named plain function arity.)
        assertTrinoExceptionThrownBy(() -> assertions.expression("a.nope()")
                .binding("a", "DOUBLE '1'")
                .evaluate())
                .hasErrorCode(FUNCTION_NOT_FOUND);
    }

    @Test
    public void testReceiverNotCoercibleAcrossBaseType()
    {
        // sqrt is a method on DOUBLE receivers only; an integer receiver has no sqrt method
        // (the receiver is matched on its exact base type, with no cross-type coercion).
        assertTrinoExceptionThrownBy(() -> assertions.expression("a.sqrt()")
                .binding("a", "INTEGER '16'")
                .evaluate())
                .hasErrorCode(FUNCTION_NOT_FOUND);
    }
}

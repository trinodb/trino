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
import io.trino.spi.type.SqlBigdecimal;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.math.BigDecimal;
import java.util.function.BiFunction;

import static com.google.common.base.Strings.nullToEmpty;
import static io.trino.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static io.trino.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.function.OperatorType.DIVIDE;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.IDENTICAL;
import static io.trino.spi.function.OperatorType.INDETERMINATE;
import static io.trino.spi.function.OperatorType.LESS_THAN;
import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.trino.spi.function.OperatorType.MODULUS;
import static io.trino.spi.function.OperatorType.MULTIPLY;
import static io.trino.spi.function.OperatorType.NEGATION;
import static io.trino.spi.function.OperatorType.SUBTRACT;
import static io.trino.spi.type.BigdecimalType.BIGDECIMAL;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.lang.Math.max;
import static java.math.RoundingMode.HALF_UP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestBigdecimalOperators
{
    private final String[] numbers = new String[] {
            "0",
            "-0",
            "0e3",
            "000.000",
            "000.000e3",
            "1",
            "-1",
            "1e3",
            "100.000",
            "100.000e3",
            "100.001",
            "100.001e3",
            "127",
            "128",
            "255",
            "256",
            "1234567890123456",
            "-1234567890123456",
            "12345678901234567890123456789012345678",
            "-12345678901234567890123456789012345678",
            ".1234567890123456",
            "-.1234567890123456",
            ".12345678901234567890123456789012345678",
            "-.12345678901234567890123456789012345678",
            "20050910133100123",
            "20050910.133100123",
            "100000000000000000000000000000000000000",
            "31439044302034031134344223124861871213334921133581369",
            "3.141592653589793238462643383279502884197169399375105820974944592307",
    };

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
    void testParse()
    {
        for (String number : numbers) {
            String expression = "BIGDECIMAL '%s'".formatted(number);
            assertThat(assertions.expression(expression)).describedAs(expression)
                    .isEqualTo(bigdecimal(new BigDecimal(number).stripTrailingZeros().toString()));
        }
    }

    @Test
    void testSmoke()
    {
        for (String left : numbers) {
            BigDecimal leftBigDecimal = new BigDecimal(left);
            String leftExpression = "BIGDECIMAL '%s'".formatted(left);
            for (String right : numbers) {
                BigDecimal rightBigDecimal = new BigDecimal(right);
                String rightExpression = "BIGDECIMAL '%s'".formatted(right);
                for (TestedOperator testedOperator : TestedOperator.values()) {
                    QueryAssertions.ExpressionAssertProvider operatorCall = assertions.operator(testedOperator.operator, leftExpression, rightExpression);
                    BigDecimal expected;
                    try {
                        expected = testedOperator.verification.apply(leftBigDecimal, rightBigDecimal);
                    }
                    catch (ArithmeticException e) {
                        if (nullToEmpty(e.getMessage()).matches("/ by zero|Division by zero|Division undefined|BigInteger divide by zero")) {
                            assertTrinoExceptionThrownBy(operatorCall::evaluate)
                                    .hasErrorCode(DIVISION_BY_ZERO);
                            continue;
                        }
                        throw new RuntimeException("Failed to calculate expected value for %s(%s, %s)".formatted(testedOperator.name(), left, right), e);
                    }
                    assertThat(operatorCall).as("%s(%s, %s)".formatted(testedOperator.operator.name(), leftExpression, rightExpression))
                            .isEqualTo(bigdecimal(expected.stripTrailingZeros().toString()));
                }
            }
        }
    }

    @Test
    void testAdd()
    {
        assertThat(assertions.operator(ADD, "BIGDECIMAL '137.7'", "BIGDECIMAL '17.1'"))
                .isEqualTo(bigdecimal("154.8"));

        assertThat(assertions.operator(ADD, "BIGDECIMAL '-1'", "BIGDECIMAL '-2'"))
                .isEqualTo(bigdecimal("-3"));

        assertThat(assertions.operator(ADD, "BIGDECIMAL '1'", "BIGDECIMAL '2'"))
                .isEqualTo(bigdecimal("3"));

        assertThat(assertions.operator(ADD, "BIGDECIMAL '.1234567890123456'", "BIGDECIMAL '.1234567890123456'"))
                .isEqualTo(bigdecimal("0.2469135780246912"));

        assertThat(assertions.operator(ADD, "BIGDECIMAL '-.1234567890123456'", "BIGDECIMAL '-.1234567890123456'"))
                .isEqualTo(bigdecimal("-0.2469135780246912"));

        assertThat(assertions.operator(ADD, "BIGDECIMAL '1234567890123456'", "BIGDECIMAL '1234567890123456'"))
                .isEqualTo(bigdecimal("2469135780246912"));

        assertThat(assertions.operator(ADD, "BIGDECIMAL '123456789012345678'", "BIGDECIMAL '123456789012345678'"))
                .isEqualTo(bigdecimal("246913578024691356"));

        assertThat(assertions.operator(ADD, "BIGDECIMAL '.123456789012345678'", "BIGDECIMAL '.123456789012345678'"))
                .isEqualTo(bigdecimal("0.246913578024691356"));

        assertThat(assertions.operator(ADD, "BIGDECIMAL '1234567890123456789'", "BIGDECIMAL '1234567890123456789'"))
                .isEqualTo(bigdecimal("2469135780246913578"));

        assertThat(assertions.operator(ADD, "BIGDECIMAL '12345678901234567890123456789012345678'", "BIGDECIMAL '12345678901234567890123456789012345678'"))
                .isEqualTo(bigdecimal("24691357802469135780246913578024691356"));

        assertThat(assertions.operator(ADD, "BIGDECIMAL '-99999999999999999999999999999999999999'", "BIGDECIMAL '99999999999999999999999999999999999999'"))
                .isEqualTo(bigdecimal("0"));

        // negative numbers
        assertThat(assertions.operator(ADD, "BIGDECIMAL '12345678901234567890'", "BIGDECIMAL '-12345678901234567890'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(ADD, "BIGDECIMAL '-12345678901234567890'", "BIGDECIMAL '12345678901234567890'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(ADD, "BIGDECIMAL '-12345678901234567890'", "BIGDECIMAL '-12345678901234567890'"))
                .isEqualTo(bigdecimal("-2.469135780246913578E+19"));

        assertThat(assertions.operator(ADD, "BIGDECIMAL '12345678901234567890'", "BIGDECIMAL '-12345678901234567891'"))
                .isEqualTo(bigdecimal("-1"));

        assertThat(assertions.operator(ADD, "BIGDECIMAL '-12345678901234567891'", "BIGDECIMAL '12345678901234567890'"))
                .isEqualTo(bigdecimal("-1"));

        assertThat(assertions.operator(ADD, "BIGDECIMAL '-12345678901234567890'", "BIGDECIMAL '12345678901234567891'"))
                .isEqualTo(bigdecimal("1"));

        assertThat(assertions.operator(ADD, "BIGDECIMAL '12345678901234567891'", "BIGDECIMAL '-12345678901234567890'"))
                .isEqualTo(bigdecimal("1"));

        assertThat(assertions.operator(ADD, "BIGDECIMAL '999999999999999999'", "BIGDECIMAL '999999999999999999'"))
                .isEqualTo(bigdecimal("1999999999999999998"));

        assertThat(assertions.operator(ADD, "BIGDECIMAL '999999999999999999'", "BIGDECIMAL '.999999999999999999'"))
                .isEqualTo(bigdecimal("999999999999999999.999999999999999999"));

        assertThat(assertions.operator(ADD, "BIGDECIMAL '123456789012345678901234567890'", "BIGDECIMAL '.12345678'"))
                .isEqualTo(bigdecimal("123456789012345678901234567890.12345678"));

        assertThat(assertions.operator(ADD, "BIGDECIMAL '.123456789012345678901234567890'", "BIGDECIMAL '12345678'"))
                .isEqualTo(bigdecimal("12345678.12345678901234567890123456789"));

        assertThat(assertions.operator(ADD, "BIGDECIMAL '.12345678'", "BIGDECIMAL '123456789012345678901234567890'"))
                .isEqualTo(bigdecimal("123456789012345678901234567890.12345678"));

        assertThat(assertions.operator(ADD, "BIGDECIMAL '12345678'", "BIGDECIMAL '.123456789012345678901234567890'"))
                .isEqualTo(bigdecimal("12345678.12345678901234567890123456789"));

        assertThat(assertions.operator(ADD, "BIGDECIMAL '99999999999999999999999999999999999999'", "BIGDECIMAL '1'"))
                .isEqualTo(bigdecimal("1E+38"));

        assertThat(assertions.operator(ADD, "BIGDECIMAL '.1'", "BIGDECIMAL '99999999999999999999999999999999999999'"))
                .isEqualTo(bigdecimal("99999999999999999999999999999999999999.1"));

        assertThat(assertions.operator(ADD, "BIGDECIMAL '1'", "BIGDECIMAL '99999999999999999999999999999999999999'"))
                .isEqualTo(bigdecimal("1E+38"));

        assertThat(assertions.operator(ADD, "BIGDECIMAL '99999999999999999999999999999999999999'", "BIGDECIMAL '.1'"))
                .isEqualTo(bigdecimal("99999999999999999999999999999999999999.1"));

        assertThat(assertions.operator(ADD, "BIGDECIMAL '99999999999999999999999999999999999999'", "BIGDECIMAL '99999999999999999999999999999999999999'"))
                .isEqualTo(bigdecimal("199999999999999999999999999999999999998"));

        assertThat(assertions.operator(ADD, "BIGDECIMAL '-99999999999999999999999999999999999999'", "BIGDECIMAL '-99999999999999999999999999999999999999'"))
                .isEqualTo(bigdecimal("-199999999999999999999999999999999999998"));

        assertThat(assertions.operator(ADD, "BIGDECIMAL '17014000000000000000000000000000000000'", "BIGDECIMAL '-7014000000000000000000000000000000000.1'"))
                .isEqualTo(bigdecimal("9999999999999999999999999999999999999.9"));

        assertThat(assertions.operator(ADD, "BIGDECIMAL '17015000000000000000000000000000000000'", "BIGDECIMAL '-7015000000000000000000000000000000000.1'"))
                .isEqualTo(bigdecimal("9999999999999999999999999999999999999.9"));

        assertThat(assertions.operator(ADD, "BIGDECIMAL '10'", "BIGDECIMAL '0.0000000000000000000000000000000000001'"))
                .isEqualTo(bigdecimal("10.0000000000000000000000000000000000001"));
    }

    @Test
    void testSubtract()
    {
        assertThat(assertions.operator(SUBTRACT, "BIGDECIMAL '107.7'", "BIGDECIMAL '17.1'"))
                .isEqualTo(bigdecimal("90.6"));

        assertThat(assertions.operator(SUBTRACT, "BIGDECIMAL '-1'", "BIGDECIMAL '-2'"))
                .isEqualTo(bigdecimal("1"));

        assertThat(assertions.operator(SUBTRACT, "BIGDECIMAL '1'", "BIGDECIMAL '2'"))
                .isEqualTo(bigdecimal("-1"));

        assertThat(assertions.operator(SUBTRACT, "BIGDECIMAL '.1234567890123456'", "BIGDECIMAL '.1234567890123456'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(SUBTRACT, "BIGDECIMAL '-.1234567890123456'", "BIGDECIMAL '-.1234567890123456'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(SUBTRACT, "BIGDECIMAL '1234567890123456'", "BIGDECIMAL '1234567890123456'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(SUBTRACT, "BIGDECIMAL '1234567890123456789'", "BIGDECIMAL '1234567890123456789'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(SUBTRACT, "BIGDECIMAL '.1234567890123456789'", "BIGDECIMAL '.1234567890123456789'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(SUBTRACT, "BIGDECIMAL '12345678901234567890'", "BIGDECIMAL '12345678901234567890'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(SUBTRACT, "BIGDECIMAL '12345678901234567890123456789012345678'", "BIGDECIMAL '12345678901234567890123456789012345678'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(SUBTRACT, "BIGDECIMAL '-12345678901234567890'", "BIGDECIMAL '12345678901234567890'"))
                .isEqualTo(bigdecimal("-2.469135780246913578E+19"));

        // negative numbers
        assertThat(assertions.operator(SUBTRACT, "BIGDECIMAL '12345678901234567890'", "BIGDECIMAL '12345678901234567890'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(SUBTRACT, "BIGDECIMAL '-12345678901234567890'", "BIGDECIMAL '-12345678901234567890'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(SUBTRACT, "BIGDECIMAL '-12345678901234567890'", "BIGDECIMAL '12345678901234567890'"))
                .isEqualTo(bigdecimal("-2.469135780246913578E+19"));

        assertThat(assertions.operator(SUBTRACT, "BIGDECIMAL '12345678901234567890'", "BIGDECIMAL '12345678901234567891'"))
                .isEqualTo(bigdecimal("-1"));

        assertThat(assertions.operator(SUBTRACT, "BIGDECIMAL '-12345678901234567891'", "BIGDECIMAL '-12345678901234567890'"))
                .isEqualTo(bigdecimal("-1"));

        assertThat(assertions.operator(SUBTRACT, "BIGDECIMAL '-12345678901234567890'", "BIGDECIMAL '-12345678901234567891'"))
                .isEqualTo(bigdecimal("1"));

        assertThat(assertions.operator(SUBTRACT, "BIGDECIMAL '12345678901234567891'", "BIGDECIMAL '12345678901234567890'"))
                .isEqualTo(bigdecimal("1"));

        assertThat(assertions.operator(SUBTRACT, "BIGDECIMAL '999999999999999999'", "BIGDECIMAL '999999999999999999'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(SUBTRACT, "BIGDECIMAL '999999999999999999'", "BIGDECIMAL '.999999999999999999'"))
                .isEqualTo(bigdecimal("999999999999999998.000000000000000001"));

        assertThat(assertions.operator(SUBTRACT, "BIGDECIMAL '123456789012345678901234567890'", "BIGDECIMAL '.00000001'"))
                .isEqualTo(bigdecimal("123456789012345678901234567889.99999999"));

        assertThat(assertions.operator(SUBTRACT, "BIGDECIMAL '.000000000000000000000000000001'", "BIGDECIMAL '87654321'"))
                .isEqualTo(bigdecimal("-87654320.999999999999999999999999999999"));

        assertThat(assertions.operator(SUBTRACT, "BIGDECIMAL '.00000001'", "BIGDECIMAL '123456789012345678901234567890'"))
                .isEqualTo(bigdecimal("-123456789012345678901234567889.99999999"));

        assertThat(assertions.operator(SUBTRACT, "BIGDECIMAL '12345678'", "BIGDECIMAL '.000000000000000000000000000001'"))
                .isEqualTo(bigdecimal("12345677.999999999999999999999999999999"));

        assertThat(assertions.operator(SUBTRACT, "BIGDECIMAL '-99999999999999999999999999999999999999'", "BIGDECIMAL '1'"))
                .isEqualTo(bigdecimal("-1E+38"));

        assertThat(assertions.operator(SUBTRACT, "BIGDECIMAL '.1'", "BIGDECIMAL '99999999999999999999999999999999999999'"))
                .isEqualTo(bigdecimal("-99999999999999999999999999999999999998.9"));

        assertThat(assertions.operator(SUBTRACT, "BIGDECIMAL '-1'", "BIGDECIMAL '99999999999999999999999999999999999999'"))
                .isEqualTo(bigdecimal("-1E+38"));

        assertThat(assertions.operator(SUBTRACT, "BIGDECIMAL '99999999999999999999999999999999999999'", "BIGDECIMAL '.1'"))
                .isEqualTo(bigdecimal("99999999999999999999999999999999999998.9"));

        assertThat(assertions.operator(SUBTRACT, "BIGDECIMAL '-99999999999999999999999999999999999999'", "BIGDECIMAL '99999999999999999999999999999999999999'"))
                .isEqualTo(bigdecimal("-199999999999999999999999999999999999998"));

        assertThat(assertions.operator(SUBTRACT, "BIGDECIMAL '17014000000000000000000000000000000000'", "BIGDECIMAL '7014000000000000000000000000000000000.1'"))
                .isEqualTo(bigdecimal("9999999999999999999999999999999999999.9"));

        assertThat(assertions.operator(SUBTRACT, "BIGDECIMAL '17015000000000000000000000000000000000'", "BIGDECIMAL '7015000000000000000000000000000000000.1'"))
                .isEqualTo(bigdecimal("9999999999999999999999999999999999999.9"));

        assertThat(assertions.operator(SUBTRACT, "BIGDECIMAL '10'", "BIGDECIMAL '0.0000000000000000000000000000000000001'"))
                .isEqualTo(bigdecimal("9.9999999999999999999999999999999999999"));
    }

    @Test
    void testMultiply()
    {
        assertThat(assertions.operator(MULTIPLY, "BIGDECIMAL '0'", "BIGDECIMAL '1'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MULTIPLY, "BIGDECIMAL '0'", "BIGDECIMAL '-1'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MULTIPLY, "BIGDECIMAL '1'", "BIGDECIMAL '0'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MULTIPLY, "BIGDECIMAL '-1'", "BIGDECIMAL '0'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MULTIPLY, "BIGDECIMAL '12'", "BIGDECIMAL '3'"))
                .isEqualTo(bigdecimal("36"));

        assertThat(assertions.operator(MULTIPLY, "BIGDECIMAL '12'", "BIGDECIMAL '-3'"))
                .isEqualTo(bigdecimal("-36"));

        assertThat(assertions.operator(MULTIPLY, "BIGDECIMAL '-12'", "BIGDECIMAL '3'"))
                .isEqualTo(bigdecimal("-36"));

        assertThat(assertions.operator(MULTIPLY, "BIGDECIMAL '1234567890123456'", "BIGDECIMAL '3'"))
                .isEqualTo(bigdecimal("3703703670370368"));

        assertThat(assertions.operator(MULTIPLY, "BIGDECIMAL '.1234567890123456'", "BIGDECIMAL '3'"))
                .isEqualTo(bigdecimal("0.3703703670370368"));

        assertThat(assertions.operator(MULTIPLY, "BIGDECIMAL '.1234567890123456'", "BIGDECIMAL '.3'"))
                .isEqualTo(bigdecimal("0.03703703670370368"));

        assertThat(assertions.operator(MULTIPLY, "BIGDECIMAL '0'", "BIGDECIMAL '1'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MULTIPLY, "BIGDECIMAL '0'", "BIGDECIMAL '-1'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MULTIPLY, "BIGDECIMAL '1'", "BIGDECIMAL '0'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MULTIPLY, "BIGDECIMAL '-1'", "BIGDECIMAL '0'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MULTIPLY, "BIGDECIMAL '12345678901234567'", "BIGDECIMAL '123456789012345670'"))
                .isEqualTo(bigdecimal("1.52415787532388345526596755677489E+33"));

        assertThat(assertions.operator(MULTIPLY, "BIGDECIMAL '-12345678901234567'", "BIGDECIMAL '123456789012345670'"))
                .isEqualTo(bigdecimal("-1.52415787532388345526596755677489E+33"));

        assertThat(assertions.operator(MULTIPLY, "BIGDECIMAL '-12345678901234567'", "BIGDECIMAL '-123456789012345670'"))
                .isEqualTo(bigdecimal("1.52415787532388345526596755677489E+33"));

        assertThat(assertions.operator(MULTIPLY, "BIGDECIMAL '.12345678901234567'", "BIGDECIMAL '.123456789012345670'"))
                .isEqualTo(bigdecimal("0.0152415787532388345526596755677489"));

        assertThat(assertions.operator(MULTIPLY, "BIGDECIMAL '0'", "1"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MULTIPLY, "BIGDECIMAL '0'", "-1"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MULTIPLY, "BIGDECIMAL '1'", "0"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MULTIPLY, "BIGDECIMAL '-1'", "0"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MULTIPLY, "BIGDECIMAL '12345678901234567890123456789012345678'", "BIGDECIMAL '3'"))
                .isEqualTo(bigdecimal("37037036703703703670370370367037037034"));

        assertThat(assertions.operator(MULTIPLY, "BIGDECIMAL '1234567890123456789.0123456789012345678'", "BIGDECIMAL '3'"))
                .isEqualTo(bigdecimal("3703703670370370367.0370370367037037034"));

        assertThat(assertions.operator(MULTIPLY, "BIGDECIMAL '.12345678901234567890123456789012345678'", "BIGDECIMAL '3'"))
                .isEqualTo(bigdecimal("0.37037036703703703670370370367037037034"));

        assertThat(assertions.operator(MULTIPLY, "0", "BIGDECIMAL '1'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MULTIPLY, "0", "BIGDECIMAL '-1'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MULTIPLY, "1", "BIGDECIMAL '0'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MULTIPLY, "-1", "BIGDECIMAL '0'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MULTIPLY, "BIGDECIMAL '3'", "BIGDECIMAL '12345678901234567890123456789012345678'"))
                .isEqualTo(bigdecimal("37037036703703703670370370367037037034"));

        assertThat(assertions.operator(MULTIPLY, "BIGDECIMAL '3'", "BIGDECIMAL '1234567890123456789.0123456789012345678'"))
                .isEqualTo(bigdecimal("3703703670370370367.0370370367037037034"));

        assertThat(assertions.operator(MULTIPLY, "BIGDECIMAL '3'", "BIGDECIMAL '.12345678901234567890123456789012345678'"))
                .isEqualTo(bigdecimal("0.37037036703703703670370370367037037034"));

        assertThat(assertions.operator(MULTIPLY, "BIGDECIMAL '0'", "BIGDECIMAL '1'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MULTIPLY, "BIGDECIMAL '0'", "BIGDECIMAL '-1'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MULTIPLY, "BIGDECIMAL '1'", "BIGDECIMAL '0'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MULTIPLY, "BIGDECIMAL '-1'", "BIGDECIMAL '0'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MULTIPLY, "BIGDECIMAL '3'", "BIGDECIMAL '2'"))
                .isEqualTo(bigdecimal("6"));

        assertThat(assertions.operator(MULTIPLY, "BIGDECIMAL '3'", "BIGDECIMAL '0.2'"))
                .isEqualTo(bigdecimal("0.6"));

        assertThat(assertions.operator(MULTIPLY, "BIGDECIMAL '.1234567890123456789'", "BIGDECIMAL '.1234567890123456789'"))
                .isEqualTo(bigdecimal("0.01524157875323883675019051998750190521"));

        assertThat(assertions.operator(MULTIPLY, "BIGDECIMAL '.1234567890123456789'", "BIGDECIMAL '.12345678901234567890'"))
                .isEqualTo(bigdecimal("0.01524157875323883675019051998750190521"));

        assertThat(assertions.operator(MULTIPLY, "BIGDECIMAL '.1'", "BIGDECIMAL '.12345678901234567890123456789012345678'"))
                .isEqualTo(bigdecimal("0.012345678901234567890123456789012345678"));

        // large multiplications
        assertThat(assertions.operator(MULTIPLY, "BIGDECIMAL '12345678901234567890123456789012345678'", "BIGDECIMAL '9'"))
                .isEqualTo(bigdecimal("111111110111111111011111111101111111102"));

        assertThat(assertions.operator(MULTIPLY, "BIGDECIMAL '.12345678901234567890123456789012345678'", "BIGDECIMAL '9'"))
                .isEqualTo(bigdecimal("1.11111110111111111011111111101111111102"));

        assertThat(assertions.operator(MULTIPLY, "BIGDECIMAL '12345678901234567890123456789012345678'", "BIGDECIMAL '-9'"))
                .isEqualTo(bigdecimal("-111111110111111111011111111101111111102"));

        assertThat(assertions.operator(MULTIPLY, "BIGDECIMAL '.12345678901234567890123456789012345678'", "BIGDECIMAL '-9'"))
                .isEqualTo(bigdecimal("-1.11111110111111111011111111101111111102"));
    }

    @Test
    void testDivide()
    {
        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '1'", "BIGDECIMAL '3'"))
                .isEqualTo(bigdecimal("0.333333"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '1.0'", "BIGDECIMAL '3'"))
                .isEqualTo(bigdecimal("0.333333"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '1.0'", "BIGDECIMAL '0.1'"))
                .isEqualTo(bigdecimal("1E+1"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '1.0'", "BIGDECIMAL '9.0'"))
                .isEqualTo(bigdecimal("0.111111"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '500.00'", "BIGDECIMAL '0.1'"))
                .isEqualTo(bigdecimal("5E+3"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '100.00'", "BIGDECIMAL '0.3'"))
                .isEqualTo(bigdecimal("333.333333"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '100.00'", "BIGDECIMAL '0.30'"))
                .isEqualTo(bigdecimal("333.333333"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '100.00'", "BIGDECIMAL '-0.30'"))
                .isEqualTo(bigdecimal("-333.333333"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '-100.00'", "BIGDECIMAL '0.30'"))
                .isEqualTo(bigdecimal("-333.333333"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '200.00'", "BIGDECIMAL '0.3'"))
                .isEqualTo(bigdecimal("666.666667"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '200.00000'", "BIGDECIMAL '0.3'"))
                .isEqualTo(bigdecimal("666.666667"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '200.00000'", "BIGDECIMAL '-0.3'"))
                .isEqualTo(bigdecimal("-666.666667"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '-200.00000'", "BIGDECIMAL '0.3'"))
                .isEqualTo(bigdecimal("-666.666667"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '999999999999999999'", "BIGDECIMAL '1'"))
                .isEqualTo(bigdecimal("999999999999999999"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '9'", "BIGDECIMAL '000000000000000003'"))
                .isEqualTo(bigdecimal("3"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '9.0'", "BIGDECIMAL '3.0'"))
                .isEqualTo(bigdecimal("3"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '999999999999999999'", "BIGDECIMAL '500000000000000000'"))
                .isEqualTo(bigdecimal("2"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '1'", "BIGDECIMAL '999999999999999999'"))
                .isEqualTo(bigdecimal("1E-18"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '-1'", "BIGDECIMAL '999999999999999999'"))
                .isEqualTo(bigdecimal("-1E-18"));

        // round
        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '9'", "BIGDECIMAL '5'"))
                .isEqualTo(bigdecimal("1.8"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '7'", "BIGDECIMAL '5'"))
                .isEqualTo(bigdecimal("1.4"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '-9'", "BIGDECIMAL '5'"))
                .isEqualTo(bigdecimal("-1.8"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '-7'", "BIGDECIMAL '5'"))
                .isEqualTo(bigdecimal("-1.4"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '-9'", "BIGDECIMAL '-5'"))
                .isEqualTo(bigdecimal("1.8"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '-7'", "BIGDECIMAL '-5'"))
                .isEqualTo(bigdecimal("1.4"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '9'", "BIGDECIMAL '-5'"))
                .isEqualTo(bigdecimal("-1.8"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '7'", "BIGDECIMAL '-5'"))
                .isEqualTo(bigdecimal("-1.4"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '-1'", "BIGDECIMAL '2'"))
                .isEqualTo(bigdecimal("-0.5"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '1'", "BIGDECIMAL '-2'"))
                .isEqualTo(bigdecimal("-0.5"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '-1'", "BIGDECIMAL '3'"))
                .isEqualTo(bigdecimal("-0.333333"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '10'", "BIGDECIMAL '.000000001'"))
                .isEqualTo(bigdecimal("1E+10"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '-10'", "BIGDECIMAL '.000000001'"))
                .isEqualTo(bigdecimal("-1E+10"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '10'", "BIGDECIMAL '-.000000001'"))
                .isEqualTo(bigdecimal("-1E+10"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '-10'", "BIGDECIMAL '-.000000001'"))
                .isEqualTo(bigdecimal("1E+10"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '200000000000000000000000000000000000'", "BIGDECIMAL '0.30'"))
                .isEqualTo(bigdecimal("666666666666666666666666666666666666.666667"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '200000000000000000000000000000000000'", "BIGDECIMAL '-0.30'"))
                .isEqualTo(bigdecimal("-666666666666666666666666666666666666.666667"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '-.20000000000000000000000000000000000000'", "BIGDECIMAL '0.30'"))
                .isEqualTo(bigdecimal("-0.666667"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '-.20000000000000000000000000000000000000'", "BIGDECIMAL '-0.30'"))
                .isEqualTo(bigdecimal("0.666667"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '.20000000000000000000000000000000000000'", "BIGDECIMAL '0.30'"))
                .isEqualTo(bigdecimal("0.666667"));

        // round
        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '500000000000000000000000000000075'", "BIGDECIMAL '50'"))
                .isEqualTo(bigdecimal("10000000000000000000000000000001.5"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '500000000000000000000000000000070'", "BIGDECIMAL '50'"))
                .isEqualTo(bigdecimal("10000000000000000000000000000001.4"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '-500000000000000000000000000000075'", "BIGDECIMAL '50'"))
                .isEqualTo(bigdecimal("-10000000000000000000000000000001.5"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '-500000000000000000000000000000070'", "BIGDECIMAL '50'"))
                .isEqualTo(bigdecimal("-10000000000000000000000000000001.4"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '500000000000000000000000000000075'", "BIGDECIMAL '-50'"))
                .isEqualTo(bigdecimal("-10000000000000000000000000000001.5"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '500000000000000000000000000000070'", "BIGDECIMAL '-50'"))
                .isEqualTo(bigdecimal("-10000000000000000000000000000001.4"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '-500000000000000000000000000000075'", "BIGDECIMAL '-50'"))
                .isEqualTo(bigdecimal("10000000000000000000000000000001.5"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '-500000000000000000000000000000070'", "BIGDECIMAL '-50'"))
                .isEqualTo(bigdecimal("10000000000000000000000000000001.4"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '-1'", "BIGDECIMAL '2'"))
                .isEqualTo(bigdecimal("-0.5"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '1'", "BIGDECIMAL '-2'"))
                .isEqualTo(bigdecimal("-0.5"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '-1'", "BIGDECIMAL '3'"))
                .isEqualTo(bigdecimal("-0.333333"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '0.1'", "BIGDECIMAL '.0000000000000000001'"))
                .isEqualTo(bigdecimal("1E+18"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '-0.1'", "BIGDECIMAL '.0000000000000000001'"))
                .isEqualTo(bigdecimal("-1E+18"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '0.1'", "BIGDECIMAL '-.0000000000000000001'"))
                .isEqualTo(bigdecimal("-1E+18"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '-0.1'", "BIGDECIMAL '-.0000000000000000001'"))
                .isEqualTo(bigdecimal("1E+18"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '9'", "BIGDECIMAL '000000000000000003.0'"))
                .isEqualTo(bigdecimal("3"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '1'", "BIGDECIMAL '99999999999999999999999999999999999999'"))
                .isEqualTo(bigdecimal("1E-38"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '-1'", "BIGDECIMAL '99999999999999999999999999999999999999'"))
                .isEqualTo(bigdecimal("-1E-38"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '1'", "BIGDECIMAL '-99999999999999999999999999999999999999'"))
                .isEqualTo(bigdecimal("-1E-38"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '-1'", "BIGDECIMAL '-99999999999999999999999999999999999999'"))
                .isEqualTo(bigdecimal("1E-38"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '99999999999999999999999999999999999999'", "BIGDECIMAL '11111111111111111111111111111111111111'"))
                .isEqualTo(bigdecimal("9"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '-99999999999999999999999999999999999999'", "BIGDECIMAL '11111111111111111111111111111111111111'"))
                .isEqualTo(bigdecimal("-9"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '99999999999999999999999999999999999999'", "BIGDECIMAL '-11111111111111111111111111111111111111'"))
                .isEqualTo(bigdecimal("-9"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '-99999999999999999999999999999999999999'", "BIGDECIMAL '-11111111111111111111111111111111111111'"))
                .isEqualTo(bigdecimal("9"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '11111111111111111111111111111111111111'", "BIGDECIMAL '99999999999999999999999999999999999999'"))
                .isEqualTo(bigdecimal("0.111111111111111111111111111111111111111"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '-11111111111111111111111111111111111111'", "BIGDECIMAL '99999999999999999999999999999999999999'"))
                .isEqualTo(bigdecimal("-0.111111111111111111111111111111111111111"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '11111111111111111111111111111111111111'", "BIGDECIMAL '-99999999999999999999999999999999999999'"))
                .isEqualTo(bigdecimal("-0.111111111111111111111111111111111111111"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '-11111111111111111111111111111111111111'", "BIGDECIMAL '-99999999999999999999999999999999999999'"))
                .isEqualTo(bigdecimal("0.111111111111111111111111111111111111111"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '99999999999999999999999999999999999998'", "BIGDECIMAL '99999999999999999999999999999999999999'"))
                .isEqualTo(bigdecimal("0.99999999999999999999999999999999999999"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '9999999999999999999999999999999999999.8'", "BIGDECIMAL '9999999999999999999999999999999999999.9'"))
                .isEqualTo(bigdecimal("0.99999999999999999999999999999999999999"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '9999999999999999999999.9'", "BIGDECIMAL '1111111111111111111111.100'"))
                .isEqualTo(bigdecimal("9"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '1635619.3155'", "BIGDECIMAL '47497517.7405'"))
                .isEqualTo(bigdecimal("0.0344358904066548"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '12345678901234567890123456789012345678'", "BIGDECIMAL '.1'"))
                .isEqualTo(bigdecimal("1.2345678901234567890123456789012345678E+38"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '12345678901234567890123456789012345678'", "BIGDECIMAL '.12345678901234567890123456789012345678'"))
                .isEqualTo(bigdecimal("1E+38"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '1'", "BIGDECIMAL '.12345678901234567890123456789012345678'"))
                .isEqualTo(bigdecimal("8.100000072900000663390006036849054935918"));

        // division by zero tests
        assertTrinoExceptionThrownBy(assertions.operator(DIVIDE, "BIGDECIMAL '1'", "BIGDECIMAL '0'")::evaluate)
                .hasErrorCode(DIVISION_BY_ZERO);

        assertTrinoExceptionThrownBy(assertions.operator(DIVIDE, "BIGDECIMAL '1.000000000000000000000000000000000000'", "BIGDECIMAL '0'")::evaluate)
                .hasErrorCode(DIVISION_BY_ZERO);

        assertTrinoExceptionThrownBy(assertions.operator(DIVIDE, "BIGDECIMAL '1.000000000000000000000000000000000000'", "BIGDECIMAL '0.0000000000000000000000000000000000000'")::evaluate)
                .hasErrorCode(DIVISION_BY_ZERO);

        assertTrinoExceptionThrownBy(assertions.operator(DIVIDE, "BIGDECIMAL '1'", "BIGDECIMAL '0.0000000000000000000000000000000000000'")::evaluate)
                .hasErrorCode(DIVISION_BY_ZERO);

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '1000'", "BIGDECIMAL '25'"))
                .isEqualTo(bigdecimal("4E+1"));
    }

    @Test
    void testModulus()
    {
        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '1'", "BIGDECIMAL '3'"))
                .isEqualTo(bigdecimal("1"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '10'", "BIGDECIMAL '3'"))
                .isEqualTo(bigdecimal("1"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '0'", "BIGDECIMAL '3'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '0'", "BIGDECIMAL '-3'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '10.0'", "BIGDECIMAL '3'"))
                .isEqualTo(bigdecimal("1"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '10.0'", "BIGDECIMAL '3.000'"))
                .isEqualTo(bigdecimal("1"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '7'", "BIGDECIMAL '3.0000000000000000'"))
                .isEqualTo(bigdecimal("1"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '7.00000000000000000'", "BIGDECIMAL '3.00000000000000000'"))
                .isEqualTo(bigdecimal("1"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '7.00000000000000000'", "BIGDECIMAL '3'"))
                .isEqualTo(bigdecimal("1"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '7'", "BIGDECIMAL '3'"))
                .isEqualTo(bigdecimal("1"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '.1'", "BIGDECIMAL '.03'"))
                .isEqualTo(bigdecimal("0.01"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '.0001'", "BIGDECIMAL '.03'"))
                .isEqualTo(bigdecimal("0.0001"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '-10'", "BIGDECIMAL '3'"))
                .isEqualTo(bigdecimal("-1"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '10'", "BIGDECIMAL '-3'"))
                .isEqualTo(bigdecimal("1"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '-10'", "BIGDECIMAL '-3'"))
                .isEqualTo(bigdecimal("-1"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '9'", "BIGDECIMAL '3'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '-9'", "BIGDECIMAL '3'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '9'", "BIGDECIMAL '-3'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '-9'", "BIGDECIMAL '-3'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '0'", "BIGDECIMAL '3'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '0'", "BIGDECIMAL '-3'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '7'", "BIGDECIMAL '3'"))
                .isEqualTo(bigdecimal("1"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '7'", "BIGDECIMAL '3'"))
                .isEqualTo(bigdecimal("1"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '7.00000000000000000'", "BIGDECIMAL '3'"))
                .isEqualTo(bigdecimal("1"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '-7.00000000000000000'", "BIGDECIMAL '3'"))
                .isEqualTo(bigdecimal("-1"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '7.0000000000000000'", "BIGDECIMAL '-3'"))
                .isEqualTo(bigdecimal("1"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '-7.0000000000000000'", "BIGDECIMAL '-3'"))
                .isEqualTo(bigdecimal("-1"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '9.00000000000000000'", "BIGDECIMAL '3'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '-9.00000000000000000'", "BIGDECIMAL '3'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '9.0000000000000000'", "BIGDECIMAL '-3'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '-9.0000000000000000'", "BIGDECIMAL '-3'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '0'", "BIGDECIMAL '3.0000000000000000000000000000000000000'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '0'", "BIGDECIMAL '-3.0000000000000000000000000000000000000'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '7'", "BIGDECIMAL '3.0000000000000000000000000000000000000'"))
                .isEqualTo(bigdecimal("1"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '7.00000000000000000'", "BIGDECIMAL '3.0000000000000000000000000000000000000'"))
                .isEqualTo(bigdecimal("1"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '.01'", "BIGDECIMAL '3.0000000000000000000000000000000000000'"))
                .isEqualTo(bigdecimal("0.01"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '-7'", "BIGDECIMAL '3.0000000000000000000000000000000000000'"))
                .isEqualTo(bigdecimal("-1"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '7'", "BIGDECIMAL '-3.0000000000000000000000000000000000000'"))
                .isEqualTo(bigdecimal("1"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '-7'", "BIGDECIMAL '-3.0000000000000000000000000000000000000'"))
                .isEqualTo(bigdecimal("-1"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '9'", "BIGDECIMAL '3.0000000000000000000000000000000000000'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '-9'", "BIGDECIMAL '3.0000000000000000000000000000000000000'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '9'", "BIGDECIMAL '-3.0000000000000000000000000000000000000'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '-9'", "BIGDECIMAL '-3.0000000000000000000000000000000000000'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '99999999999999999999999999999999999997'", "BIGDECIMAL '3'"))
                .isEqualTo(bigdecimal("1"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '99999999999999999999999999999999999997'", "BIGDECIMAL '3.0000000000000000'"))
                .isEqualTo(bigdecimal("1"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '-99999999999999999999999999999999999997'", "BIGDECIMAL '3'"))
                .isEqualTo(bigdecimal("-1"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '99999999999999999999999999999999999997'", "BIGDECIMAL '-3'"))
                .isEqualTo(bigdecimal("1"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '-99999999999999999999999999999999999997'", "BIGDECIMAL '-3'"))
                .isEqualTo(bigdecimal("-1"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '99999999999999999999999999999999999999'", "BIGDECIMAL '3'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '-99999999999999999999999999999999999999'", "BIGDECIMAL '3'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '99999999999999999999999999999999999999'", "BIGDECIMAL '-3'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '-99999999999999999999999999999999999999'", "BIGDECIMAL '-3'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '0.000000000000000000000000000000000000'", "BIGDECIMAL '3'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '0.000000000000000000000000000000000000'", "BIGDECIMAL '-3'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '7.000000000000000000000000000000000000'", "BIGDECIMAL '3'"))
                .isEqualTo(bigdecimal("1"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '-7.000000000000000000000000000000000000'", "BIGDECIMAL '3'"))
                .isEqualTo(bigdecimal("-1"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '7.000000000000000000000000000000000000'", "BIGDECIMAL '-3'"))
                .isEqualTo(bigdecimal("1"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '-7.000000000000000000000000000000000000'", "BIGDECIMAL '-3'"))
                .isEqualTo(bigdecimal("-1"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '9.000000000000000000000000000000000000'", "BIGDECIMAL '3'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '-9.000000000000000000000000000000000000'", "BIGDECIMAL '3'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '9.000000000000000000000000000000000000'", "BIGDECIMAL '-3'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '-9.000000000000000000000000000000000000'", "BIGDECIMAL '-3'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '0'", "BIGDECIMAL '3'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '0'", "BIGDECIMAL '-3'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '7'", "BIGDECIMAL '3'"))
                .isEqualTo(bigdecimal("1"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '7'", "BIGDECIMAL '3'"))
                .isEqualTo(bigdecimal("1"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '7'", "BIGDECIMAL '3'"))
                .isEqualTo(bigdecimal("1"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '-7'", "BIGDECIMAL '3'"))
                .isEqualTo(bigdecimal("-1"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '7'", "BIGDECIMAL '-3'"))
                .isEqualTo(bigdecimal("1"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '-7'", "BIGDECIMAL '-3'"))
                .isEqualTo(bigdecimal("-1"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '9'", "BIGDECIMAL '3'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '-9'", "BIGDECIMAL '3'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '9'", "BIGDECIMAL '-3'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '-9'", "BIGDECIMAL '-3'"))
                .isEqualTo(bigdecimal("0"));

        // division by zero tests
        assertTrinoExceptionThrownBy(assertions.operator(MODULUS, "BIGDECIMAL '1'", "BIGDECIMAL '0'")::evaluate)
                .hasErrorCode(DIVISION_BY_ZERO);

        assertTrinoExceptionThrownBy(assertions.operator(MODULUS, "BIGDECIMAL '1.000000000000000000000000000000000000'", "BIGDECIMAL '0'")::evaluate)
                .hasErrorCode(DIVISION_BY_ZERO);

        assertTrinoExceptionThrownBy(assertions.operator(MODULUS, "BIGDECIMAL '1.000000000000000000000000000000000000'", "BIGDECIMAL '0.0000000000000000000000000000000000000'")::evaluate)
                .hasErrorCode(DIVISION_BY_ZERO);

        assertTrinoExceptionThrownBy(assertions.operator(MODULUS, "BIGDECIMAL '1'", "BIGDECIMAL '0.0000000000000000000000000000000000000'")::evaluate)
                .hasErrorCode(DIVISION_BY_ZERO);

        assertTrinoExceptionThrownBy(assertions.operator(MODULUS, "BIGDECIMAL '1'", "BIGDECIMAL '0'")::evaluate)
                .hasErrorCode(DIVISION_BY_ZERO);
    }

    @Test
    void testNegation()
    {
        assertThat(assertions.operator(NEGATION, "BIGDECIMAL '1'"))
                .isEqualTo(bigdecimal("-1"));

        assertThat(assertions.operator(NEGATION, "BIGDECIMAL '-1'"))
                .isEqualTo(bigdecimal("1"));

        assertThat(assertions.operator(NEGATION, "BIGDECIMAL '123456.00000010' "))
                .isEqualTo(bigdecimal("-123456.0000001"));

        assertThat(assertions.operator(NEGATION, "BIGDECIMAL '1234567.00500010734' "))
                .isEqualTo(bigdecimal("-1234567.00500010734"));

        assertThat(assertions.operator(NEGATION, "BIGDECIMAL '-1234567.00500010734' "))
                .isEqualTo(bigdecimal("1234567.00500010734"));

        assertThat(assertions.operator(NEGATION, "BIGDECIMAL '0' "))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(NEGATION, "BIGDECIMAL '0.00000000000000000000' "))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(NEGATION, "BIGDECIMAL '12345678901234567890123456789012345678'"))
                .isEqualTo(bigdecimal("-12345678901234567890123456789012345678"));

        assertThat(assertions.operator(NEGATION, "BIGDECIMAL '-12345678901234567890123456789012345678'"))
                .isEqualTo(bigdecimal("12345678901234567890123456789012345678"));

        assertThat(assertions.operator(NEGATION, "BIGDECIMAL '123456789012345678.90123456789012345678'"))
                .isEqualTo(bigdecimal("-123456789012345678.90123456789012345678"));
    }

    @Test
    void testEqual()
    {
        assertThat(assertions.operator(EQUAL, "BIGDECIMAL '37'", "BIGDECIMAL '37'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "BIGDECIMAL '37'", "BIGDECIMAL '0037'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "BIGDECIMAL '37'", "BIGDECIMAL '37.0'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "BIGDECIMAL '37.0'", "BIGDECIMAL '37'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "BIGDECIMAL '-0.000'", "BIGDECIMAL '0.00000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "BIGDECIMAL '37'", "BIGDECIMAL '38'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "BIGDECIMAL '37'", "BIGDECIMAL '38.0'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "BIGDECIMAL '37.0'", "BIGDECIMAL '38'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "BIGDECIMAL '-37.000'", "BIGDECIMAL '37.000'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "BIGDECIMAL '-123456789123456789'", "BIGDECIMAL '-123456789123456789'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "BIGDECIMAL '-123456789123456789'", "BIGDECIMAL '123456789123456789'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "BIGDECIMAL '37'", "BIGDECIMAL '37.0000000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "BIGDECIMAL '37'", "BIGDECIMAL '00000000037.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "BIGDECIMAL '037.0'", "BIGDECIMAL '00000000037.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "BIGDECIMAL '-0.000'", "BIGDECIMAL '0.000000000000000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "BIGDECIMAL '37'", "BIGDECIMAL '00000000038.0000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "BIGDECIMAL '-37'", "BIGDECIMAL '00000000037.0000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "BIGDECIMAL '37.0000000000000000'", "BIGDECIMAL '00000000037.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "BIGDECIMAL '37.0000000000000000'", "BIGDECIMAL '00000000036.0000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "BIGDECIMAL '37.0000000000000000000000000'", "BIGDECIMAL '37'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "BIGDECIMAL '00000000037.0000000000000000000000'", "BIGDECIMAL '37'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "BIGDECIMAL '00000000037.0000000000000000000000'", "BIGDECIMAL '037.0'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "BIGDECIMAL '-0.000000000000000000000000000000000'", "BIGDECIMAL '0.000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "BIGDECIMAL '00000000038.0000000000000000000000'", "BIGDECIMAL '37'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "BIGDECIMAL '-00000000037.0000000000000000000000'", "BIGDECIMAL '37'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "BIGDECIMAL '00000000037.0000000000000000000000'", "BIGDECIMAL '37.0000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "BIGDECIMAL '00000000036.0000000000000000000000'", "BIGDECIMAL '37.0000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "BIGDECIMAL '37.0000000000000000000000000'", "BIGDECIMAL '37.0000000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "BIGDECIMAL '00000000037.0000000000000000000000'", "BIGDECIMAL '0037.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "BIGDECIMAL '0.0000000000000000000000000000000'", "BIGDECIMAL '-0.000000000000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "BIGDECIMAL '00000000038.0000000000000000000000'", "BIGDECIMAL '000000000037.00000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "BIGDECIMAL '-00000000038.0000000000000000000000'", "BIGDECIMAL '00000000038.0000000000000000000000'"))
                .isEqualTo(false);
    }

    @Test
    void testNotEqual()
    {
        assertThat(assertions.expression("a != b")
                .binding("a", "BIGDECIMAL '37'")
                .binding("b", "BIGDECIMAL '37'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "BIGDECIMAL '37'")
                .binding("b", "BIGDECIMAL '0037'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "BIGDECIMAL '37'")
                .binding("b", "BIGDECIMAL '37.0'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "BIGDECIMAL '37.0'")
                .binding("b", "BIGDECIMAL '37'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "BIGDECIMAL '0'")
                .binding("b", "BIGDECIMAL '-0.00'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "BIGDECIMAL '37'")
                .binding("b", "BIGDECIMAL '-37'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "BIGDECIMAL '37'")
                .binding("b", "BIGDECIMAL '38'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "BIGDECIMAL '37'")
                .binding("b", "BIGDECIMAL '38.0'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "BIGDECIMAL '37.0'")
                .binding("b", "BIGDECIMAL '38'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "BIGDECIMAL '37'")
                .binding("b", "BIGDECIMAL '-37'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "BIGDECIMAL '-999999999999999999'")
                .binding("b", "BIGDECIMAL '-999999999999999999'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "BIGDECIMAL '-999999999999999999'")
                .binding("b", "BIGDECIMAL '999999999999999998'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "BIGDECIMAL '37'")
                .binding("b", "BIGDECIMAL '37.0000000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "BIGDECIMAL '37'")
                .binding("b", "BIGDECIMAL '00000000037.0000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "BIGDECIMAL '037.0'")
                .binding("b", "BIGDECIMAL '00000000037.0000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "BIGDECIMAL '0'")
                .binding("b", "BIGDECIMAL '-0.0000000000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "BIGDECIMAL '37'")
                .binding("b", "BIGDECIMAL '00000000038.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "BIGDECIMAL '37'")
                .binding("b", "BIGDECIMAL '-000000000037.00000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "BIGDECIMAL '38.0000000000000000'")
                .binding("b", "BIGDECIMAL '00000000038.0000000000000000000001'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "BIGDECIMAL '-987654321987654321'")
                .binding("b", "BIGDECIMAL '-987654321987654321.00000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "BIGDECIMAL '37.0000000000000000000000000'")
                .binding("b", "BIGDECIMAL '37'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "BIGDECIMAL '00000000037.0000000000000000000000'")
                .binding("b", "BIGDECIMAL '37'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "BIGDECIMAL '00000000037.0000000000000000000000'")
                .binding("b", "BIGDECIMAL '037.0'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "BIGDECIMAL '0.0000000000000000000000000000'")
                .binding("b", "BIGDECIMAL '-0'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "BIGDECIMAL '00000000038.0000000000000000000000'")
                .binding("b", "BIGDECIMAL '37'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "BIGDECIMAL '00000000000037.00000000000000000000'")
                .binding("b", "BIGDECIMAL '-37'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "BIGDECIMAL '00000000037.0000000000000000000000'")
                .binding("b", "BIGDECIMAL '37.0000000000000001'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "BIGDECIMAL '-00000000000037.00000000000000000000'")
                .binding("b", "BIGDECIMAL '-37.0000000000000001'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "BIGDECIMAL '37.0000000000000000000000000'")
                .binding("b", "BIGDECIMAL '37.0000000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "BIGDECIMAL '00000000037.0000000000000000000000'")
                .binding("b", "BIGDECIMAL '0037.0000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "BIGDECIMAL '0.00000000000000000000000000000000000'")
                .binding("b", "BIGDECIMAL '-0.000000000000000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "BIGDECIMAL '00000000038.0000000000000000000000'")
                .binding("b", "BIGDECIMAL '000000000037.00000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "BIGDECIMAL '00000000000037.00000000000000000000'")
                .binding("b", "BIGDECIMAL '-00000000000037.00000000000000000000'"))
                .isEqualTo(true);
    }

    @Test
    void testLessThan()
    {
        assertThat(assertions.operator(LESS_THAN, "BIGDECIMAL '37'", "BIGDECIMAL '37'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "BIGDECIMAL '37'", "BIGDECIMAL '0037'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "BIGDECIMAL '37'", "BIGDECIMAL '37.0'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "BIGDECIMAL '37.0'", "BIGDECIMAL '37'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "BIGDECIMAL '0037.0'", "BIGDECIMAL '00036.0'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "BIGDECIMAL '37'", "BIGDECIMAL '38'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "BIGDECIMAL '37'", "BIGDECIMAL '0038.0'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "BIGDECIMAL '0037.0'", "BIGDECIMAL '38'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "BIGDECIMAL '-100'", "BIGDECIMAL '20'"))
                .isEqualTo(true);

        // test possible overflow on rescale
        assertThat(assertions.operator(LESS_THAN, "BIGDECIMAL '1'", "BIGDECIMAL '10000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "BIGDECIMAL '1.0000000000000000'", "BIGDECIMAL '10000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "BIGDECIMAL '-123456789123456789'", "BIGDECIMAL '-123456789123456789'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "BIGDECIMAL '-123456789123456789'", "BIGDECIMAL '123456789123456789'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "BIGDECIMAL '37'", "BIGDECIMAL '37.0000000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "BIGDECIMAL '37'", "BIGDECIMAL '00000000037.0000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "BIGDECIMAL '037.0'", "BIGDECIMAL '00000000037.0000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "BIGDECIMAL '037.0'", "BIGDECIMAL '00000000036.0000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "BIGDECIMAL '37'", "BIGDECIMAL '37.00000000000000000000000001'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "BIGDECIMAL '37'", "BIGDECIMAL '00000000038.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "BIGDECIMAL '037.0'", "BIGDECIMAL '00000000038.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "BIGDECIMAL '-100'", "BIGDECIMAL '20.00000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "BIGDECIMAL '38.0000000000000000'", "BIGDECIMAL '00000000038.0000000000000000000001'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "BIGDECIMAL '-987654321987654321'", "BIGDECIMAL '-987654321987654321.00000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "BIGDECIMAL '37.0000000000000000000000000'", "BIGDECIMAL '37'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "BIGDECIMAL '00000000037.0000000000000000000000'", "BIGDECIMAL '37'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "BIGDECIMAL '00000000037.0000000000000000000000'", "BIGDECIMAL '037.0'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "BIGDECIMAL '00000000037.0000000000000000000000'", "BIGDECIMAL '036.0'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "BIGDECIMAL '37.0000000000000000000000000'", "BIGDECIMAL '38'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "BIGDECIMAL '00000000037.0000000000000000000001'", "BIGDECIMAL '38'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "BIGDECIMAL '00000000037.0000000000000000000000'", "BIGDECIMAL '038.0'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "BIGDECIMAL '-00000000000100.000000000000'", "BIGDECIMAL '20'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "BIGDECIMAL '00000000037.0000000000000000000000'", "BIGDECIMAL '37.0000000000000001'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "BIGDECIMAL '-00000000000037.00000000000000000000'", "BIGDECIMAL '-37.0000000000000001'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "BIGDECIMAL '37.0000000000000000000000000'", "BIGDECIMAL '37.0000000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "BIGDECIMAL '37.0000000000000000000000000'", "BIGDECIMAL '00000037.0000000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "BIGDECIMAL '37.0000000000000000000000000'", "BIGDECIMAL '00000036.0000000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "BIGDECIMAL '00000000037.0000000000000000000000'", "BIGDECIMAL '38.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "BIGDECIMAL '00000000037.0000000000000000000000'", "BIGDECIMAL '000000000037.00000000000000000000001'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "BIGDECIMAL '-00000000000100.000000000000'", "BIGDECIMAL '0000000020.0000000000000'"))
                .isEqualTo(true);
    }

    @Test
    void testGreaterThan()
    {
        assertThat(assertions.expression("a > b")
                .binding("a", "BIGDECIMAL '37'")
                .binding("b", "BIGDECIMAL '37'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "BIGDECIMAL '37'")
                .binding("b", "BIGDECIMAL '0037'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "BIGDECIMAL '37'")
                .binding("b", "BIGDECIMAL '37.0'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "BIGDECIMAL '37.0'")
                .binding("b", "BIGDECIMAL '37'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "BIGDECIMAL '0037.0'")
                .binding("b", "BIGDECIMAL '00038.0'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "BIGDECIMAL '37'")
                .binding("b", "BIGDECIMAL '36'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "BIGDECIMAL '37'")
                .binding("b", "BIGDECIMAL '0036.0'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "BIGDECIMAL '0037.0'")
                .binding("b", "BIGDECIMAL '36'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "BIGDECIMAL '100'")
                .binding("b", "BIGDECIMAL '-20'"))
                .isEqualTo(true);

        // test possible overflow on rescale
        assertThat(assertions.expression("a > b")
                .binding("a", "BIGDECIMAL '10000000000000000'")
                .binding("b", "BIGDECIMAL '1'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "BIGDECIMAL '10000000000000000'")
                .binding("b", "BIGDECIMAL '1.0000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "BIGDECIMAL '-123456789123456788'")
                .binding("b", "BIGDECIMAL '-123456789123456789'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "BIGDECIMAL '-123456789123456789'")
                .binding("b", "BIGDECIMAL '123456789123456789'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "BIGDECIMAL '37'")
                .binding("b", "BIGDECIMAL '37.0000000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "BIGDECIMAL '37'")
                .binding("b", "BIGDECIMAL '00000000037.0000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "BIGDECIMAL '037.0'")
                .binding("b", "BIGDECIMAL '00000000037.0000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "BIGDECIMAL '037.0'")
                .binding("b", "BIGDECIMAL '00000000038.0000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "BIGDECIMAL '37'")
                .binding("b", "BIGDECIMAL '36.00000000000000000000000001'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "BIGDECIMAL '37'")
                .binding("b", "BIGDECIMAL '00000000036.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "BIGDECIMAL '037.0'")
                .binding("b", "BIGDECIMAL '00000000036.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "BIGDECIMAL '100'")
                .binding("b", "BIGDECIMAL '-0000000020.00000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "BIGDECIMAL '38.0000000000000000'")
                .binding("b", "BIGDECIMAL '00000000038.0000000000000000000001'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "BIGDECIMAL '-987654321987654320'")
                .binding("b", "BIGDECIMAL '-987654321987654321.00000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "BIGDECIMAL '37.0000000000000000000000000'")
                .binding("b", "BIGDECIMAL '37'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "BIGDECIMAL '00000000037.0000000000000000000000'")
                .binding("b", "BIGDECIMAL '37'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "BIGDECIMAL '00000000037.0000000000000000000000'")
                .binding("b", "BIGDECIMAL '037.0'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "BIGDECIMAL '00000000037.0000000000000000000000'")
                .binding("b", "BIGDECIMAL '038.0'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "BIGDECIMAL '37.0000000000000000000000000'")
                .binding("b", "BIGDECIMAL '36'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "BIGDECIMAL '00000000037.0000000000000000000001'")
                .binding("b", "BIGDECIMAL '36'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "BIGDECIMAL '00000000037.0000000000000000000000'")
                .binding("b", "BIGDECIMAL '036.0'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "BIGDECIMAL '0000000000100.000000000000'")
                .binding("b", "BIGDECIMAL '20'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "BIGDECIMAL '00000000037.0000000000000000000002'")
                .binding("b", "BIGDECIMAL '37.0000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "BIGDECIMAL '-00000000000037.00000000000000000000'")
                .binding("b", "BIGDECIMAL '-37.0000000000000001'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "BIGDECIMAL '37.0000000000000000000000000'")
                .binding("b", "BIGDECIMAL '37.0000000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "BIGDECIMAL '37.0000000000000000000000000'")
                .binding("b", "BIGDECIMAL '00000037.0000000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "BIGDECIMAL '37.0000000000000000000000000'")
                .binding("b", "BIGDECIMAL '00000038.0000000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "BIGDECIMAL '00000000037.0000000000000000000000'")
                .binding("b", "BIGDECIMAL '36.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "BIGDECIMAL '00000000037.0000000000000000000000'")
                .binding("b", "BIGDECIMAL '000000000036.9999999999999999999999'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "BIGDECIMAL '000000000000100.0000000000000000000000'")
                .binding("b", "BIGDECIMAL '-0000000020.00000000000000000000000'"))
                .isEqualTo(true);
    }

    @Test
    void testLessThanOrEqual()
    {
        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "BIGDECIMAL '37'", "BIGDECIMAL '36'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "BIGDECIMAL '37'", "BIGDECIMAL '000036.99999'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "BIGDECIMAL '37'", "BIGDECIMAL '37'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "BIGDECIMAL '37'", "BIGDECIMAL '0037'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "BIGDECIMAL '37'", "BIGDECIMAL '37.0'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "BIGDECIMAL '37.0'", "BIGDECIMAL '37'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "BIGDECIMAL '37'", "BIGDECIMAL '38'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "BIGDECIMAL '37'", "BIGDECIMAL '0038.0'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "BIGDECIMAL '0037.0'", "BIGDECIMAL '38'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "BIGDECIMAL '-100'", "BIGDECIMAL '20'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "BIGDECIMAL '-123456789123456789'", "BIGDECIMAL '-123456789123456789'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "BIGDECIMAL '-123456789123456789'", "BIGDECIMAL '123456789123456789'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "BIGDECIMAL '037.0'", "BIGDECIMAL '00000000036.0000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "BIGDECIMAL '037.0'", "BIGDECIMAL '00000000036.9999999999999999999'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "BIGDECIMAL '37'", "BIGDECIMAL '37.0000000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "BIGDECIMAL '37'", "BIGDECIMAL '00000000037.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "BIGDECIMAL '037.0'", "BIGDECIMAL '00000000037.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "BIGDECIMAL '37'", "BIGDECIMAL '37.00000000000000000000000001'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "BIGDECIMAL '37'", "BIGDECIMAL '00000000038.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "BIGDECIMAL '037.0'", "BIGDECIMAL '00000000038.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "BIGDECIMAL '-100'", "BIGDECIMAL '20.00000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "BIGDECIMAL '38.0000000000000000'", "BIGDECIMAL '00000000038.0000000000000000000001'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "BIGDECIMAL '-987654321987654321'", "BIGDECIMAL '-987654321987654321.00000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "BIGDECIMAL '00000000037.0000000000000000000000'", "BIGDECIMAL '036.0'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "BIGDECIMAL '00000000037.0000000000000000000000'", "BIGDECIMAL '000036.99999999'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "BIGDECIMAL '37.0000000000000000000000000'", "BIGDECIMAL '37'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "BIGDECIMAL '00000000037.0000000000000000000000'", "BIGDECIMAL '37'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "BIGDECIMAL '00000000037.0000000000000000000000'", "BIGDECIMAL '037.0'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "BIGDECIMAL '37.0000000000000000000000000'", "BIGDECIMAL '38'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "BIGDECIMAL '00000000037.0000000000000000000001'", "BIGDECIMAL '38'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "BIGDECIMAL '00000000037.0000000000000000000000'", "BIGDECIMAL '038.0'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "BIGDECIMAL '-00000000000100.000000000000'", "BIGDECIMAL '20'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "BIGDECIMAL '00000000037.0000000000000000000000'", "BIGDECIMAL '37.0000000000000001'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "BIGDECIMAL '-00000000000037.00000000000000000000'", "BIGDECIMAL '-37.0000000000000001'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "BIGDECIMAL '37.0000000000000000000000000'", "BIGDECIMAL '00000036.0000000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "BIGDECIMAL '37.0000000000000000000000000'", "BIGDECIMAL '37.0000000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "BIGDECIMAL '37.0000000000000000000000000'", "BIGDECIMAL '00000037.0000000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "BIGDECIMAL '00000000037.0000000000000000000000'", "BIGDECIMAL '38.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "BIGDECIMAL '00000000037.0000000000000000000000'", "BIGDECIMAL '000000000037.00000000000000000000001'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "BIGDECIMAL '-00000000000100.000000000000'", "BIGDECIMAL '0000000020.0000000000000'"))
                .isEqualTo(true);
    }

    @Test
    void testGreaterThanOrEqual()
    {
        assertThat(assertions.expression("a >= b")
                .binding("a", "BIGDECIMAL '37'")
                .binding("b", "BIGDECIMAL '38'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "BIGDECIMAL '37'")
                .binding("b", "BIGDECIMAL '000038.00001'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "BIGDECIMAL '37'")
                .binding("b", "BIGDECIMAL '37'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "BIGDECIMAL '37'")
                .binding("b", "BIGDECIMAL '0037'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "BIGDECIMAL '37'")
                .binding("b", "BIGDECIMAL '37.0'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "BIGDECIMAL '37.0'")
                .binding("b", "BIGDECIMAL '37'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "BIGDECIMAL '37'")
                .binding("b", "BIGDECIMAL '36'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "BIGDECIMAL '37'")
                .binding("b", "BIGDECIMAL '0036.0'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "BIGDECIMAL '0037.0'")
                .binding("b", "BIGDECIMAL '36'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "BIGDECIMAL '100'")
                .binding("b", "BIGDECIMAL '-20'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "BIGDECIMAL '-123456789123456789'")
                .binding("b", "BIGDECIMAL '-123456789123456789'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "BIGDECIMAL '-123456789123456789'")
                .binding("b", "BIGDECIMAL '123456789123456789'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "BIGDECIMAL '037.0'")
                .binding("b", "BIGDECIMAL '00000000038.0000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "BIGDECIMAL '037.0'")
                .binding("b", "BIGDECIMAL '00000000037.0000000000000000001'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "BIGDECIMAL '37'")
                .binding("b", "BIGDECIMAL '37.0000000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "BIGDECIMAL '37'")
                .binding("b", "BIGDECIMAL '00000000037.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "BIGDECIMAL '037.0'")
                .binding("b", "BIGDECIMAL '00000000037.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "BIGDECIMAL '37'")
                .binding("b", "BIGDECIMAL '36.9999999999999999999'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "BIGDECIMAL '37'")
                .binding("b", "BIGDECIMAL '00000000036.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "BIGDECIMAL '037.0'")
                .binding("b", "BIGDECIMAL '00000000036.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "BIGDECIMAL '100'")
                .binding("b", "BIGDECIMAL '-0000000020.00000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "BIGDECIMAL '38.0000000000000000'")
                .binding("b", "BIGDECIMAL '00000000038.0000000000000000000001'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "BIGDECIMAL '-987654321987654321'")
                .binding("b", "BIGDECIMAL '-987654321987654321.00000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "BIGDECIMAL '00000000037.0000000000000000000000'")
                .binding("b", "BIGDECIMAL '038.0'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "BIGDECIMAL '00000000037.0000000000000000000000'")
                .binding("b", "BIGDECIMAL '000037.00000001'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "BIGDECIMAL '37.0000000000000000000000000'")
                .binding("b", "BIGDECIMAL '37'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "BIGDECIMAL '00000000037.0000000000000000000000'")
                .binding("b", "BIGDECIMAL '37'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "BIGDECIMAL '00000000037.0000000000000000000000'")
                .binding("b", "BIGDECIMAL '037.0'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "BIGDECIMAL '37.0000000000000000000000000'")
                .binding("b", "BIGDECIMAL '36'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "BIGDECIMAL '00000000037.0000000000000000000001'")
                .binding("b", "BIGDECIMAL '36'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "BIGDECIMAL '00000000037.0000000000000000000000'")
                .binding("b", "BIGDECIMAL '036.0'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "BIGDECIMAL '0000000000100.000000000000'")
                .binding("b", "BIGDECIMAL '20'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "BIGDECIMAL '00000000037.0000000000000000000000'")
                .binding("b", "BIGDECIMAL '37.0000000000000001'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "BIGDECIMAL '-00000000000037.00000000000000000000'")
                .binding("b", "BIGDECIMAL '-37.0000000000000001'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "BIGDECIMAL '37.0000000000000000000000000'")
                .binding("b", "BIGDECIMAL '00000038.0000000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "BIGDECIMAL '37.0000000000000000000000000'")
                .binding("b", "BIGDECIMAL '37.0000000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "BIGDECIMAL '37.0000000000000000000000000'")
                .binding("b", "BIGDECIMAL '00000037.0000000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "BIGDECIMAL '00000000037.0000000000000000000000'")
                .binding("b", "BIGDECIMAL '36.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "BIGDECIMAL '00000000037.0000000000000000000000'")
                .binding("b", "BIGDECIMAL '000000000036.9999999999999999'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "BIGDECIMAL '000000000000100.0000000000000000000000'")
                .binding("b", "BIGDECIMAL '-0000000020.00000000000000000000000'"))
                .isEqualTo(true);
    }

    @Test
    void testBetween()
    {
        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "BIGDECIMAL '1'")
                .binding("low", "BIGDECIMAL '-5'")
                .binding("high", "BIGDECIMAL '5'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "BIGDECIMAL '-6'")
                .binding("low", "BIGDECIMAL '-5'")
                .binding("high", "BIGDECIMAL '5'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "BIGDECIMAL '6'")
                .binding("low", "BIGDECIMAL '-5'")
                .binding("high", "BIGDECIMAL '5'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "BIGDECIMAL '333333333333333333'")
                .binding("low", "BIGDECIMAL '-111111111111111111'")
                .binding("high", "BIGDECIMAL '999999999999999999'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "BIGDECIMAL '1'")
                .binding("low", "BIGDECIMAL '-5'")
                .binding("high", "BIGDECIMAL '5.00000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "BIGDECIMAL '-6'")
                .binding("low", "BIGDECIMAL '-5'")
                .binding("high", "BIGDECIMAL '5.00000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "BIGDECIMAL '6'")
                .binding("low", "BIGDECIMAL '-5'")
                .binding("high", "BIGDECIMAL '5.00000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "BIGDECIMAL '333333333333333333'")
                .binding("low", "BIGDECIMAL '-111111111111111111'")
                .binding("high", "BIGDECIMAL '9999999999999999999'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "BIGDECIMAL '1'")
                .binding("low", "BIGDECIMAL '-5.00000000000000000000'")
                .binding("high", "BIGDECIMAL '5.00000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "BIGDECIMAL '-6'")
                .binding("low", "BIGDECIMAL '-5.00000000000000000000'")
                .binding("high", "BIGDECIMAL '5.00000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "BIGDECIMAL '6'")
                .binding("low", "BIGDECIMAL '-5.00000000000000000000'")
                .binding("high", "BIGDECIMAL '5.00000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "BIGDECIMAL '333333333333333333'")
                .binding("low", "BIGDECIMAL '-1111111111111111111'")
                .binding("high", "BIGDECIMAL '9999999999999999999'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "BIGDECIMAL '1.00000000000000000000'")
                .binding("low", "BIGDECIMAL '-5'")
                .binding("high", "BIGDECIMAL '5'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "BIGDECIMAL '-6.00000000000000000000'")
                .binding("low", "BIGDECIMAL '-5'")
                .binding("high", "BIGDECIMAL '5'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "BIGDECIMAL '6.00000000000000000000'")
                .binding("low", "BIGDECIMAL '-5'")
                .binding("high", "BIGDECIMAL '5'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "BIGDECIMAL '333333333333333333.3'")
                .binding("low", "BIGDECIMAL '-111111111111111111'")
                .binding("high", "BIGDECIMAL '999999999999999999'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "BIGDECIMAL '1.00000000000000000000'")
                .binding("low", "BIGDECIMAL '-5'")
                .binding("high", "BIGDECIMAL '5.00000000000000000000' "))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "BIGDECIMAL '-6.00000000000000000000'")
                .binding("low", "BIGDECIMAL '-5'")
                .binding("high", "BIGDECIMAL '5.00000000000000000000' "))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "BIGDECIMAL '6.00000000000000000000'")
                .binding("low", "BIGDECIMAL '-5'")
                .binding("high", "BIGDECIMAL '5.00000000000000000000' "))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "BIGDECIMAL '333333333333333333.3'")
                .binding("low", "BIGDECIMAL '-111111111111111111'")
                .binding("high", "BIGDECIMAL '9999999999999999999'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "BIGDECIMAL '1.00000000000000000000'")
                .binding("low", "BIGDECIMAL '-5.00000000000000000000' ")
                .binding("high", "BIGDECIMAL '5'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "BIGDECIMAL '-6.00000000000000000000'")
                .binding("low", "BIGDECIMAL '-5.00000000000000000000' ")
                .binding("high", "BIGDECIMAL '5'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "BIGDECIMAL '6.00000000000000000000'")
                .binding("low", "BIGDECIMAL '-5.00000000000000000000' ")
                .binding("high", "BIGDECIMAL '5'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "BIGDECIMAL '333333333333333333.3'")
                .binding("low", "BIGDECIMAL '-111111111111111111.1'")
                .binding("high", "BIGDECIMAL '999999999999999999'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "BIGDECIMAL '1.00000000000000000000'")
                .binding("low", "BIGDECIMAL '-5.00000000000000000000' ")
                .binding("high", "BIGDECIMAL '5.00000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "BIGDECIMAL '-6.00000000000000000000'")
                .binding("low", "BIGDECIMAL '-5.00000000000000000000' ")
                .binding("high", "BIGDECIMAL '5.00000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "BIGDECIMAL '6.00000000000000000000'")
                .binding("low", "BIGDECIMAL '-5.00000000000000000000' ")
                .binding("high", "BIGDECIMAL '5.00000000000000000000'"))
                .isEqualTo(false);
    }

    @Test
    void testAddBigdecimalBigint()
    {
        // bigdecimal + bigint
        assertThat(assertions.operator(ADD, "BIGDECIMAL '123456789012345678'", "123456789012345678"))
                .isEqualTo(bigdecimal("246913578024691356"));

        assertThat(assertions.operator(ADD, "BIGDECIMAL '.123456789012345678'", "123456789012345678"))
                .isEqualTo(bigdecimal("123456789012345678.123456789012345678"));

        assertThat(assertions.operator(ADD, "BIGDECIMAL '-1234567890123456789'", "1234567890123456789"))
                .isEqualTo(bigdecimal("0"));

        // bigint + bigdecimal
        assertThat(assertions.operator(ADD, "123456789012345678", "BIGDECIMAL '123456789012345678'"))
                .isEqualTo(bigdecimal("246913578024691356"));

        assertThat(assertions.operator(ADD, "123456789012345678", "BIGDECIMAL '.123456789012345678'"))
                .isEqualTo(bigdecimal("123456789012345678.123456789012345678"));

        assertThat(assertions.operator(ADD, "1234567890123456789", "BIGDECIMAL '-1234567890123456789'"))
                .isEqualTo(bigdecimal("0"));
    }

    @Test
    void testSubtractBigdecimalBigint()
    {
        // bigdecimal - bigint
        assertThat(assertions.operator(SUBTRACT, "BIGDECIMAL '1234567890123456789'", "1234567890123456789"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(SUBTRACT, "BIGDECIMAL '.1234567890123456789'", "1234567890123456789"))
                .isEqualTo(bigdecimal("-1234567890123456788.8765432109876543211"));

        assertThat(assertions.operator(SUBTRACT, "BIGDECIMAL '-1234567890123456789'", "1234567890123456789"))
                .isEqualTo(bigdecimal("-2469135780246913578"));

        // bigint - bigdecimal
        assertThat(assertions.operator(SUBTRACT, "1234567890123456789", "BIGDECIMAL '1234567890123456789'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(SUBTRACT, "1234567890123456789", "BIGDECIMAL '.1234567890123456789'"))
                .isEqualTo(bigdecimal("1234567890123456788.8765432109876543211"));

        assertThat(assertions.operator(SUBTRACT, "-1234567890123456789", "BIGDECIMAL '1234567890123456789'"))
                .isEqualTo(bigdecimal("-2469135780246913578"));
    }

    @Test
    void testMultiplyBigdecimalBigint()
    {
        // bigdecimal bigint
        assertThat(assertions.operator(MULTIPLY, "BIGDECIMAL '12345678901234567'", "12345678901234567"))
                .isEqualTo(bigdecimal("152415787532388345526596755677489"));

        assertThat(assertions.operator(MULTIPLY, "BIGDECIMAL '-12345678901234567'", "12345678901234567"))
                .isEqualTo(bigdecimal("-152415787532388345526596755677489"));

        assertThat(assertions.operator(MULTIPLY, "BIGDECIMAL '-12345678901234567'", "-12345678901234567"))
                .isEqualTo(bigdecimal("152415787532388345526596755677489"));

        assertThat(assertions.operator(MULTIPLY, "BIGDECIMAL '.1234567890'", "BIGINT '3'"))
                .isEqualTo(bigdecimal("0.370370367"));

        assertThat(assertions.operator(MULTIPLY, "BIGDECIMAL '.1234567890'", "BIGINT '0'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MULTIPLY, "BIGDECIMAL '-.1234567890'", "BIGINT '0'"))
                .isEqualTo(bigdecimal("0"));

        // bigint bigdecimal
        assertThat(assertions.operator(MULTIPLY, "12345678901234567", "BIGDECIMAL '12345678901234567'"))
                .isEqualTo(bigdecimal("152415787532388345526596755677489"));

        assertThat(assertions.operator(MULTIPLY, "12345678901234567", "BIGDECIMAL '-12345678901234567'"))
                .isEqualTo(bigdecimal("-152415787532388345526596755677489"));

        assertThat(assertions.operator(MULTIPLY, "-12345678901234567", "BIGDECIMAL '-12345678901234567'"))
                .isEqualTo(bigdecimal("152415787532388345526596755677489"));

        assertThat(assertions.operator(MULTIPLY, "BIGINT '3'", "BIGDECIMAL '.1234567890'"))
                .isEqualTo(bigdecimal("0.370370367"));

        assertThat(assertions.operator(MULTIPLY, "BIGINT '3'", "BIGDECIMAL '.0000000000'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MULTIPLY, "BIGINT '-3'", "BIGDECIMAL '.0000000000'"))
                .isEqualTo(bigdecimal("0"));
    }

    @Test
    void testDivideBigdecimalBigint()
    {
        // bigint / bigdecimal
        assertThat(assertions.operator(DIVIDE, "BIGINT '9'", "BIGDECIMAL '3.0'"))
                .isEqualTo(bigdecimal("3"));

        assertThat(assertions.operator(DIVIDE, "BIGINT '-9'", "BIGDECIMAL '3.0'"))
                .isEqualTo(bigdecimal("-3"));

        assertThat(assertions.operator(DIVIDE, "BIGINT '9'", "BIGDECIMAL '-3.0'"))
                .isEqualTo(bigdecimal("-3"));

        assertThat(assertions.operator(DIVIDE, "BIGINT '-9'", "BIGDECIMAL '-3.0'"))
                .isEqualTo(bigdecimal("3"));

        assertThat(assertions.operator(DIVIDE, "BIGINT '9'", "BIGDECIMAL '000000000000000003.0'"))
                .isEqualTo(bigdecimal("3"));

        assertThat(assertions.operator(DIVIDE, "BIGINT '18'", "BIGDECIMAL '0.01'"))
                .isEqualTo(bigdecimal("1.8E+3"));

        assertThat(assertions.operator(DIVIDE, "BIGINT '9'", "BIGDECIMAL '00000000000000000.1'"))
                .isEqualTo(bigdecimal("9E+1"));

        assertThat(assertions.operator(DIVIDE, "BIGINT '9'", "BIGDECIMAL '300.0'"))
                .isEqualTo(bigdecimal("0.03"));

        assertThat(assertions.operator(DIVIDE, "BIGINT '-9'", "BIGDECIMAL '300.0'"))
                .isEqualTo(bigdecimal("-0.03"));

        assertThat(assertions.operator(DIVIDE, "BIGINT '9'", "BIGDECIMAL '-300.0'"))
                .isEqualTo(bigdecimal("-0.03"));

        assertThat(assertions.operator(DIVIDE, "BIGINT '-9'", "BIGDECIMAL '-300.0'"))
                .isEqualTo(bigdecimal("0.03"));

        // bigdecimal / bigint
        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '9.0'", "BIGINT '3'"))
                .isEqualTo(bigdecimal("3"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '-9.0'", "BIGINT '3'"))
                .isEqualTo(bigdecimal("-3"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '9.0'", "BIGINT '-3'"))
                .isEqualTo(bigdecimal("-3"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '-9.0'", "BIGINT '-3'"))
                .isEqualTo(bigdecimal("3"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '0.018'", "BIGINT '9'"))
                .isEqualTo(bigdecimal("0.002"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '-0.018'", "BIGINT '9'"))
                .isEqualTo(bigdecimal("-0.002"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '0.018'", "BIGINT '-9'"))
                .isEqualTo(bigdecimal("-0.002"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '-0.018'", "BIGINT '-9'"))
                .isEqualTo(bigdecimal("0.002"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '.999'", "BIGINT '9'"))
                .isEqualTo(bigdecimal("0.111"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '9.0'", "BIGINT '300'"))
                .isEqualTo(bigdecimal("0.03"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '-9.0'", "BIGINT '300'"))
                .isEqualTo(bigdecimal("-0.03"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '9.0'", "BIGINT '-300'"))
                .isEqualTo(bigdecimal("-0.03"));

        assertThat(assertions.operator(DIVIDE, "BIGDECIMAL '-9.0'", "BIGINT '-300'"))
                .isEqualTo(bigdecimal("0.03"));
    }

    @Test
    void testModulusBigdecimalBigint()
    {
        // bigint % bigdecimal
        assertThat(assertions.operator(MODULUS, "BIGINT '13'", "BIGDECIMAL '9.0'"))
                .isEqualTo(bigdecimal("4"));

        assertThat(assertions.operator(MODULUS, "BIGINT '18'", "BIGDECIMAL '0.01'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MODULUS, "BIGINT '9'", "BIGDECIMAL '.1'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MODULUS, "BIGINT '-9'", "BIGDECIMAL '.1'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MODULUS, "BIGINT '9'", "BIGDECIMAL '-.1'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MODULUS, "BIGINT '-9'", "BIGDECIMAL '-.1'"))
                .isEqualTo(bigdecimal("0"));

        // bigdecimal % bigint
        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '13.0'", "BIGINT '9'"))
                .isEqualTo(bigdecimal("4"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '-13.0'", "BIGINT '9'"))
                .isEqualTo(bigdecimal("-4"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '13.0'", "BIGINT '-9'"))
                .isEqualTo(bigdecimal("4"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '-13.0'", "BIGINT '-9'"))
                .isEqualTo(bigdecimal("-4"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '18.00'", "BIGINT '3'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '9.0'", "BIGINT '3'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '-9.0'", "BIGINT '3'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '9.0'", "BIGINT '-3'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '-9.0'", "BIGINT '-3'"))
                .isEqualTo(bigdecimal("0"));

        assertThat(assertions.operator(MODULUS, "BIGDECIMAL '5.128'", "BIGINT '2'"))
                .isEqualTo(bigdecimal("1.128"));
    }

    @Test
    void testMultiplyBigdecimalDecimal()
    {
        // TODO All These fail because currently there is no coercion between DECIMAL and BIGDECIMAL

        // bigdecimal decimal
        assertTrinoExceptionThrownBy(assertions.operator(MULTIPLY, "BIGDECIMAL '12345678901234567'", "DECIMAL '12345678901234567'")::evaluate)
                .hasErrorCode(FUNCTION_NOT_FOUND)
                .hasMessageContaining("Unexpected parameters (bigdecimal, decimal(17,0)) for function $operator$multiply");

        assertTrinoExceptionThrownBy(assertions.operator(MULTIPLY, "BIGDECIMAL '-12345678901234567'", "DECIMAL '12345678901234567'")::evaluate)
                .hasErrorCode(FUNCTION_NOT_FOUND)
                .hasMessageContaining("Unexpected parameters (bigdecimal, decimal(17,0)) for function $operator$multiply");

        assertTrinoExceptionThrownBy(assertions.operator(MULTIPLY, "BIGDECIMAL '-12345678901234567'", "DECIMAL '-12345678901234567'")::evaluate)
                .hasErrorCode(FUNCTION_NOT_FOUND)
                .hasMessageContaining("Unexpected parameters (bigdecimal, decimal(17,0)) for function $operator$multiply");

        assertTrinoExceptionThrownBy(assertions.operator(MULTIPLY, "BIGDECIMAL '.1234567890'", "DECIMAL '3'")::evaluate)
                .hasErrorCode(FUNCTION_NOT_FOUND)
                .hasMessageContaining("Unexpected parameters (bigdecimal, decimal(1,0)) for function $operator$multiply");

        assertTrinoExceptionThrownBy(assertions.operator(MULTIPLY, "BIGDECIMAL '.1234567890'", "DECIMAL '0'")::evaluate)
                .hasErrorCode(FUNCTION_NOT_FOUND)
                .hasMessageContaining("Unexpected parameters (bigdecimal, decimal(1,0)) for function $operator$multiply");

        assertTrinoExceptionThrownBy(assertions.operator(MULTIPLY, "BIGDECIMAL '-.1234567890'", "DECIMAL '0'")::evaluate)
                .hasErrorCode(FUNCTION_NOT_FOUND)
                .hasMessageContaining("Unexpected parameters (bigdecimal, decimal(1,0)) for function $operator$multiply");

        // decimal bigdecimal
        assertTrinoExceptionThrownBy(assertions.operator(MULTIPLY, "DECIMAL '12345678901234567'", "BIGDECIMAL '12345678901234567'")::evaluate)
                .hasErrorCode(FUNCTION_NOT_FOUND)
                .hasMessageContaining("Unexpected parameters (decimal(17,0), bigdecimal) for function $operator$multiply");

        assertTrinoExceptionThrownBy(assertions.operator(MULTIPLY, "DECIMAL '12345678901234567'", "BIGDECIMAL '-12345678901234567'")::evaluate)
                .hasErrorCode(FUNCTION_NOT_FOUND)
                .hasMessageContaining("Unexpected parameters (decimal(17,0), bigdecimal) for function $operator$multiply");

        assertTrinoExceptionThrownBy(assertions.operator(MULTIPLY, "DECIMAL '-12345678901234567'", "BIGDECIMAL '-12345678901234567'")::evaluate)
                .hasErrorCode(FUNCTION_NOT_FOUND)
                .hasMessageContaining("Unexpected parameters (decimal(17,0), bigdecimal) for function $operator$multiply");

        assertTrinoExceptionThrownBy(assertions.operator(MULTIPLY, "DECIMAL '3'", "BIGDECIMAL '.1234567890'")::evaluate)
                .hasErrorCode(FUNCTION_NOT_FOUND)
                .hasMessageContaining("Unexpected parameters (decimal(1,0), bigdecimal) for function $operator$multiply");

        assertTrinoExceptionThrownBy(assertions.operator(MULTIPLY, "DECIMAL '3'", "BIGDECIMAL '.0000000000'")::evaluate)
                .hasErrorCode(FUNCTION_NOT_FOUND)
                .hasMessageContaining("Unexpected parameters (decimal(1,0), bigdecimal) for function $operator$multiply");

        assertTrinoExceptionThrownBy(assertions.operator(MULTIPLY, "DECIMAL '-3'", "BIGDECIMAL '.0000000000'")::evaluate)
                .hasErrorCode(FUNCTION_NOT_FOUND)
                .hasMessageContaining("Unexpected parameters (decimal(1,0), bigdecimal) for function $operator$multiply");
    }

    @Test
    void testIdentical()
    {
        assertThat(assertions.operator(IDENTICAL, "CAST(NULL AS bigdecimal)", "CAST(NULL AS bigdecimal)"))
                .isEqualTo(true);

        assertThat(assertions.operator(IDENTICAL, "BIGDECIMAL '37'", "BIGDECIMAL '37'"))
                .isEqualTo(true);

        assertThat(assertions.operator(IDENTICAL, "37", "38"))
                .isEqualTo(false);

        assertThat(assertions.operator(IDENTICAL, "NULL", "37"))
                .isEqualTo(false);

        assertThat(assertions.operator(IDENTICAL, "37", "NULL"))
                .isEqualTo(false);

        assertThat(assertions.operator(IDENTICAL, "BIGDECIMAL '-2'", "BIGDECIMAL '-3'"))
                .isEqualTo(false);

        assertThat(assertions.operator(IDENTICAL, "BIGDECIMAL '-2'", "CAST(NULL AS bigdecimal)"))
                .isEqualTo(false);

        assertThat(assertions.operator(IDENTICAL, "BIGDECIMAL '-2'", "BIGDECIMAL '-2'"))
                .isEqualTo(true);

        assertThat(assertions.operator(IDENTICAL, "CAST(NULL AS bigdecimal)", "BIGDECIMAL '-2'"))
                .isEqualTo(false);

        assertThat(assertions.operator(IDENTICAL, "BIGDECIMAL '12345678901234567.89012345678901234567'", "BIGDECIMAL '12345678901234567.8902345678901234567'"))
                .isEqualTo(false);

        assertThat(assertions.operator(IDENTICAL, "BIGDECIMAL '12345678901234567.89012345678901234567'", "CAST(NULL AS bigdecimal)"))
                .isEqualTo(false);

        assertThat(assertions.operator(IDENTICAL, "CAST(NULL AS bigdecimal)", "CAST(NULL AS bigdecimal)"))
                .isEqualTo(true);

        assertThat(assertions.operator(IDENTICAL, "BIGDECIMAL '-12345678901234567.89012345678901234567'", "BIGDECIMAL '-12345678901234567.89012345678901234567'"))
                .isEqualTo(true);

        assertThat(assertions.operator(IDENTICAL, "CAST(NULL AS bigdecimal)", "BIGDECIMAL '12345678901234567.89012345678901234567'"))
                .isEqualTo(false);

        assertThat(assertions.operator(IDENTICAL, "BIGDECIMAL '12345678901234567.89012345678901234567'", "BIGDECIMAL '-3'"))
                .isEqualTo(false);

        assertThat(assertions.operator(IDENTICAL, "BIGDECIMAL '12345678901234567.89012345678901234567'", "CAST(NULL AS bigdecimal)"))
                .isEqualTo(false);

        assertThat(assertions.operator(IDENTICAL, "CAST(NULL AS bigdecimal)", "CAST(NULL AS bigdecimal)"))
                .isEqualTo(true);

        assertThat(assertions.operator(IDENTICAL, "BIGDECIMAL '00000000000000007.80000000000000000000'", "BIGDECIMAL '7.8'"))
                .isEqualTo(true);

        assertThat(assertions.operator(IDENTICAL, "CAST(NULL AS bigdecimal)", "BIGDECIMAL '7.8'"))
                .isEqualTo(false);

        // with unknown
        assertThat(assertions.operator(IDENTICAL, "NULL", "BIGDECIMAL '-2'"))
                .isEqualTo(false);

        assertThat(assertions.operator(IDENTICAL, "BIGDECIMAL '-2'", "NULL"))
                .isEqualTo(false);

        assertThat(assertions.operator(IDENTICAL, "NULL", "BIGDECIMAL '12345678901234567.89012345678901234567'"))
                .isEqualTo(false);

        assertThat(assertions.operator(IDENTICAL, "BIGDECIMAL '12345678901234567.89012345678901234567'", "NULL"))
                .isEqualTo(false);

        // delegation from other operator (exercises block-position convention implementation)
        assertThat(assertions.operator(IDENTICAL, "ARRAY [1.23, 4.56]", "ARRAY [1.23, 4.56]"))
                .isEqualTo(true);

        assertThat(assertions.operator(IDENTICAL, "ARRAY [1.23, NULL]", "ARRAY [1.23, 4.56]"))
                .isEqualTo(false);

        assertThat(assertions.operator(IDENTICAL, "ARRAY [1.23, NULL]", "ARRAY [NULL, 4.56]"))
                .isEqualTo(false);

        assertThat(assertions.operator(IDENTICAL, "ARRAY [1234567890.123456789, 9876543210.987654321]", "ARRAY [1234567890.123456789, 9876543210.987654321]"))
                .isEqualTo(true);

        assertThat(assertions.operator(IDENTICAL, "ARRAY [1234567890.123456789, NULL]", "ARRAY [1234567890.123456789, 9876543210.987654321]"))
                .isEqualTo(false);

        assertThat(assertions.operator(IDENTICAL, "ARRAY [1234567890.123456789, NULL]", "ARRAY [NULL, 9876543210.987654321]"))
                .isEqualTo(false);
    }

    @Test
    void testNullIf()
    {
        assertThat(assertions.function("nullif", "BIGDECIMAL '-2'", "BIGDECIMAL '-3'"))
                .isEqualTo(bigdecimal("-2"));

        assertThat(assertions.function("nullif", "BIGDECIMAL '-2'", "CAST(NULL AS bigdecimal)"))
                .isEqualTo(bigdecimal("-2"));

        assertThat(assertions.function("nullif", "BIGDECIMAL '-2'", "BIGDECIMAL '-2'"))
                .isNull(BIGDECIMAL);

        assertThat(assertions.function("nullif", "CAST(NULL AS bigdecimal)", "BIGDECIMAL '-2'"))
                .isNull(BIGDECIMAL);

        assertThat(assertions.function("nullif", "CAST(NULL AS bigdecimal)", "CAST(NULL AS bigdecimal)"))
                .isNull(BIGDECIMAL);

        assertThat(assertions.function("nullif", "BIGDECIMAL '12345678901234567.89012345678901234567'", "BIGDECIMAL '12345678901234567.8902345678901234567'"))
                .isEqualTo(bigdecimal("12345678901234567.89012345678901234567"));

        assertThat(assertions.function("nullif", "BIGDECIMAL '12345678901234567.89012345678901234567'", "CAST(NULL AS bigdecimal)"))
                .isEqualTo(bigdecimal("12345678901234567.89012345678901234567"));

        assertThat(assertions.function("nullif", "BIGDECIMAL '12345678901234567.89012345678901234567'", "BIGDECIMAL '12345678901234567.89012345678901234567'"))
                .isNull(BIGDECIMAL);

        assertThat(assertions.function("nullif", "CAST(NULL AS bigdecimal)", "BIGDECIMAL '12345678901234567.89012345678901234567'"))
                .isNull(BIGDECIMAL);

        assertThat(assertions.function("nullif", "CAST(NULL AS bigdecimal)", "CAST(NULL AS bigdecimal)"))
                .isNull(BIGDECIMAL);

        assertThat(assertions.function("nullif", "BIGDECIMAL '12345678901234567.89012345678901234567'", "BIGDECIMAL '-3'"))
                .isEqualTo(bigdecimal("12345678901234567.89012345678901234567"));

        assertThat(assertions.function("nullif", "BIGDECIMAL '12345678901234567.89012345678901234567'", "CAST(NULL AS bigdecimal)"))
                .isEqualTo(bigdecimal("12345678901234567.89012345678901234567"));

        assertThat(assertions.function("nullif", "BIGDECIMAL '12345678901234567.89012345678901234567'", "BIGDECIMAL '12345678901234567.89012345678901234567'"))
                .isNull(BIGDECIMAL);

        assertThat(assertions.function("nullif", "CAST(NULL AS bigdecimal)", "BIGDECIMAL '12345678901234567.89012345678901234567'"))
                .isNull(BIGDECIMAL);

        // with unknown
        assertThat(assertions.function("nullif", "NULL", "NULL"))
                .isNull(UnknownType.UNKNOWN);

        assertThat(assertions.function("nullif", "NULL", "BIGDECIMAL '-2'"))
                .isNull(UnknownType.UNKNOWN);

        assertThat(assertions.function("nullif", "BIGDECIMAL '-2'", "NULL"))
                .isEqualTo(bigdecimal("-2"));

        assertThat(assertions.function("nullif", "NULL", "BIGDECIMAL '12345678901234567.89012345678901234567'"))
                .isNull(UnknownType.UNKNOWN);

        assertThat(assertions.function("nullif", "BIGDECIMAL '12345678901234567.89012345678901234567'", "NULL"))
                .isEqualTo(bigdecimal("12345678901234567.89012345678901234567"));
    }

    @Test
    void testCoalesce()
    {
        assertThat(assertions.function("coalesce", "BIGDECIMAL '2.1'", "null", "CAST(NULL AS bigdecimal)"))
                .isEqualTo(bigdecimal("2.1"));

        assertThat(assertions.function("coalesce", "CAST(NULL AS bigdecimal)", "null", "CAST(NULL AS bigdecimal)"))
                .isNull(BIGDECIMAL);

        assertThat(assertions.function("coalesce", "BIGDECIMAL '3'", "BIGDECIMAL '2.1'", "null", "CAST(NULL AS bigdecimal)"))
                .isEqualTo(bigdecimal("3"));

        assertThat(assertions.function("coalesce", "CAST(NULL AS bigdecimal)", "null", "CAST(NULL AS bigdecimal)"))
                .isNull(BIGDECIMAL);
    }

    @Test
    void testIndeterminate()
    {
        assertThat(assertions.operator(INDETERMINATE, "CAST(NULL AS bigdecimal)"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "BIGDECIMAL '.999'"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "BIGDECIMAL '18'"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "BIGDECIMAL '9.0'"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "BIGDECIMAL '12345678901234567.89012345678901234567'"))
                .isEqualTo(false);
    }

    @Test
    void testCastToVarchar()
    {
        assertThat(assertions.expression("cast(a as varchar)")
                .binding("a", "BIGDECIMAL '0'"))
                .hasType(VARCHAR)
                .isEqualTo("0");

        assertThat(assertions.expression("cast(a as varchar)")
                .binding("a", "BIGDECIMAL '12345.6789'"))
                .hasType(VARCHAR)
                .isEqualTo("12345.6789");

        assertThat(assertions.expression("cast(a as varchar)")
                .binding("a", "BIGDECIMAL '-12345.6789'"))
                .hasType(VARCHAR)
                .isEqualTo("-12345.6789");

        assertThat(assertions.expression("cast(a as varchar)")
                .binding("a", "BIGDECIMAL '3.141592653589793238462643383279502884197169399375105820974944592307'"))
                .hasType(VARCHAR)
                .isEqualTo("3.141592653589793238462643383279502884197169399375105820974944592307");

        assertThat(assertions.expression("cast(a as varchar)")
                .binding("a", "BIGDECIMAL '123456789e-4'"))
                .hasType(VARCHAR)
                .isEqualTo("12345.6789");

        assertThat(assertions.expression("cast(a as varchar)")
                .binding("a", "BIGDECIMAL '123456789e+4'"))
                .hasType(VARCHAR)
                .isEqualTo("1234567890000");

        assertThat(assertions.expression("cast(a as varchar(4))")
                .binding("a", "BIGDECIMAL '1234'"))
                .hasType(createVarcharType(4))
                .isEqualTo("1234");

        assertTrinoExceptionThrownBy(assertions.expression("cast(a as varchar(3))")
                .binding("a", "BIGDECIMAL '1234'")::evaluate)
                .hasMessage("Value 1234 cannot be represented as varchar(3)");

        assertTrinoExceptionThrownBy(assertions.expression("cast(a as varchar(3))")
                .binding("a", "BIGDECIMAL '1e9'")::evaluate)
                .hasMessage("Value 1000000000 cannot be represented as varchar(3)");
    }

    private enum TestedOperator
    {
        ADD(OperatorType.ADD, BigDecimal::add),
        SUBTRACT(OperatorType.SUBTRACT, BigDecimal::subtract),
        MULTIPLY(OperatorType.MULTIPLY, BigDecimal::multiply),
        DIVIDE(OperatorType.DIVIDE, (a, b) -> a.divide(b, max(6, a.stripTrailingZeros().scale() + b.stripTrailingZeros().precision() + 1), HALF_UP)),
        MODULUS(OperatorType.MODULUS, BigDecimal::remainder),
        /**/;

        private final OperatorType operator;
        private final BiFunction<BigDecimal, BigDecimal, BigDecimal> verification;

        TestedOperator(OperatorType operator, BiFunction<BigDecimal, BigDecimal, BigDecimal> verification)
        {
            this.operator = operator;
            this.verification = verification;
        }
    }

    private static SqlBigdecimal bigdecimal(String value)
    {
        return new SqlBigdecimal(value);
    }
}

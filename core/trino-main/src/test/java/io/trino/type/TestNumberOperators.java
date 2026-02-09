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

import io.trino.spi.type.SqlDecimal;
import io.trino.spi.type.SqlNumber;
import io.trino.spi.type.TrinoNumber;
import io.trino.sql.query.QueryAssertions;
import io.trino.sql.query.QueryAssertions.ExpressionAssertProvider;
import io.trino.testing.QueryFailedException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.math.BigDecimal;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.nullToEmpty;
import static io.trino.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static io.trino.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
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
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.NumberType.NUMBER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.lang.Double.isFinite;
import static java.lang.Double.parseDouble;
import static java.lang.Math.max;
import static java.math.RoundingMode.HALF_UP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.offset;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestNumberOperators
{
    private final String[] bigDecimals = new String[] {
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

    private final String[] decimals = new String[] {
            "0",
            "3",
            "1234",
            "1001",
            "3.14159",
            "0.1003",
            "1.1003",
            "10007",
            "127",
            "128",
            "255",
            "256",
    };

    private final String[] doubles = new String[] {
            "NaN",
            "+Infinity",
            "-Infinity",
            "0",
            "1",
            "-1",
            "100",
            "1e-4",
            "1e+4",
            "-1e-4",
            "-1e+4",
            "3.1416",
            "1234",
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
        for (String number : bigDecimals) {
            String expression = "NUMBER '%s'".formatted(number);
            assertThat(assertions.expression(expression)).describedAs(expression)
                    .isEqualTo(number(new BigDecimal(number).stripTrailingZeros().toString()));
        }
    }

    /**
     * Compare arithmetic results between Trino NUMBER and {@link BigDecimal}.
     */
    @Test
    void testVerifyArithmeticWithBigDecimal()
    {
        for (String left : bigDecimals) {
            BigDecimal leftBigDecimal = new BigDecimal(left);
            String leftExpression = "NUMBER '%s'".formatted(left);
            for (String right : bigDecimals) {
                BigDecimal rightBigDecimal = new BigDecimal(right);
                String rightExpression = "NUMBER '%s'".formatted(right);
                for (var testedOperator : TestedArithmeticOperator.values()) {
                    ExpressionAssertProvider operatorCall = assertions.expression("l %s r".formatted(testedOperator.operator))
                            .binding("l", leftExpression)
                            .binding("r", rightExpression);
                    BigDecimal expected;
                    try {
                        expected = testedOperator.bigDecimalReferenceImplementation.apply(leftBigDecimal, rightBigDecimal);
                    }
                    catch (ArithmeticException e) {
                        if (nullToEmpty(e.getMessage()).matches("/ by zero|Division by zero|Division undefined|BigInteger divide by zero")) {
                            assertTrinoExceptionThrownBy(operatorCall::evaluate)
                                    .hasErrorCode(DIVISION_BY_ZERO);
                            continue;
                        }
                        throw new RuntimeException("Failed to calculate expected value for %s(%s, %s)".formatted(testedOperator.name(), left, right), e);
                    }
                    assertThat(operatorCall).as("%s(%s, %s)".formatted(testedOperator.operator, leftExpression, rightExpression))
                            .isEqualTo(number(expected.stripTrailingZeros().toString()));
                }
            }
        }
    }

    /**
     * Compare comparison results between Trino NUMBER and {@link BigDecimal}.
     */
    @Test
    void testVerifyComparisonWithBigDecimal()
    {
        for (String left : bigDecimals) {
            BigDecimal leftBigDecimal = new BigDecimal(left);
            String leftExpression = "NUMBER '%s'".formatted(left);
            for (String right : bigDecimals) {
                BigDecimal rightBigDecimal = new BigDecimal(right);
                String rightExpression = "NUMBER '%s'".formatted(right);
                for (var testedOperator : TestedComparisonOperator.values()) {
                    boolean expected = testedOperator.bigDecimalReferenceImplementation.test(leftBigDecimal, rightBigDecimal);
                    assertThat(assertions.expression("l %s r".formatted(testedOperator.operator))
                            .binding("l", leftExpression)
                            .binding("r", rightExpression))
                            .as("%s(%s, %s)".formatted(testedOperator.operator, leftExpression, rightExpression))
                            .isEqualTo(expected);
                }
            }
        }
    }

    /**
     * Compare arithmetic results between Trino NUMBER and DECIMAL.
     */
    @Test
    void testVerifyArithmeticWithTrinoDecimal()
    {
        for (String left : decimals) {
            String leftExpression = "NUMBER '%s'".formatted(left);
            for (String right : decimals) {
                String rightExpression = "NUMBER '%s'".formatted(right);
                for (var testedOperator : TestedArithmeticOperator.values()) {
                    ExpressionAssertProvider operatorCall = assertions.expression("l %s r".formatted(testedOperator.operator))
                            .binding("l", leftExpression)
                            .binding("r", rightExpression);
                    SqlDecimal expected;
                    try {
                        expected = (SqlDecimal) assertions.expression("l %s r".formatted(testedOperator.operator))
                                .binding("l", "DECIMAL '%s'".formatted(left))
                                .binding("r", "DECIMAL '%s'".formatted(right))
                                .evaluate().value();
                    }
                    catch (QueryFailedException e) {
                        if (nullToEmpty(e.getMessage()).matches("Division by zero")) {
                            assertTrinoExceptionThrownBy(operatorCall::evaluate)
                                    .hasErrorCode(DIVISION_BY_ZERO);
                            continue;
                        }
                        throw new RuntimeException("Failed to calculate expected value for %s(%s, %s)".formatted(testedOperator.name(), left, right), e);
                    }
                    assertThat(operatorCall).as("%s(%s, %s)".formatted(testedOperator.operator, leftExpression, rightExpression))
                            .isEqualTo(number(expected.toBigDecimal().stripTrailingZeros().toString()));
                }
            }
        }
    }

    /**
     * Compare comparison results between Trino NUMBER and DECIMAL.
     */
    @Test
    void testVerifyComparisonWithTrinoDecimal()
    {
        for (String left : decimals) {
            String leftExpression = "NUMBER '%s'".formatted(left);
            for (String right : decimals) {
                String rightExpression = "NUMBER '%s'".formatted(right);
                for (var testedOperator : TestedComparisonOperator.values()) {
                    boolean expected = (boolean) assertions.expression("l %s r".formatted(testedOperator.operator))
                            .binding("l", "DECIMAL '%s'".formatted(left))
                            .binding("r", "DECIMAL '%s'".formatted(right))
                            .evaluate().value();
                    assertThat(assertions.expression("l %s r".formatted(testedOperator.operator))
                            .binding("l", leftExpression)
                            .binding("r", rightExpression))
                            .as("%s(%s, %s)".formatted(testedOperator.operator, leftExpression, rightExpression))
                            .isEqualTo(expected);
                }
            }
        }
    }

    /**
     * Compare arithmetic results between Trino NUMBER and {@code double}
     */
    @Test
    void testVerifyArithmeticWithTrinoDouble()
    {
        for (String left : doubles) {
            String leftExpression = "NUMBER '%s'".formatted(left);
            for (String right : doubles) {
                String rightExpression = "NUMBER '%s'".formatted(right);
                for (var testedOperator : TestedArithmeticOperator.values()) {
                    ExpressionAssertProvider operatorCall = assertions.expression("l %s r".formatted(testedOperator.operator))
                            .binding("l", leftExpression)
                            .binding("r", rightExpression);
                    Double expected = (Double) assertions.expression("l %s r".formatted(testedOperator.operator))
                            .binding("l", "DOUBLE '%s'".formatted(left))
                            .binding("r", "DOUBLE '%s'".formatted(right))
                            .evaluate().value();
                    if (isFinite(parseDouble(left)) && parseDouble(right) == 0.0 && List.of("/", "%").contains(testedOperator.operator) && !isFinite(expected)) {
                        // For "Division by zero" we align with decimal semantics instead
                        assertTrinoExceptionThrownBy(operatorCall::evaluate)
                                .hasErrorCode(DIVISION_BY_ZERO);
                        continue;
                    }
                    assertThat(operatorCall)
                            .as("%s(%s, %s)".formatted(testedOperator.operator, leftExpression, rightExpression))
                            .satisfies(result -> {
                                SqlNumber sqlNumber = (SqlNumber) result;
                                assertThat(parseDouble(sqlNumber.toString()))
                                        .isEqualTo(expected, offset(1e-4));
                            });
                }
            }
        }
    }

    /**
     * Compare comparison results between Trino NUMBER and {@code double}.
     */
    @Test
    void testVerifyComparisonWithTrinoDouble()
    {
        for (String left : doubles) {
            String leftExpression = "NUMBER '%s'".formatted(left);
            for (String right : doubles) {
                String rightExpression = "NUMBER '%s'".formatted(right);
                for (var testedOperator : TestedComparisonOperator.values()) {
                    boolean expected = (Boolean) assertions.expression("l %s r".formatted(testedOperator.operator))
                            .binding("l", "DOUBLE '%s'".formatted(left))
                            .binding("r", "DOUBLE '%s'".formatted(right))
                            .evaluate().value();
                    assertThat(assertions.expression("l %s r".formatted(testedOperator.operator))
                            .binding("l", leftExpression)
                            .binding("r", rightExpression))
                            .as("%s(%s, %s)".formatted(testedOperator.operator, leftExpression, rightExpression))
                            .isEqualTo(expected);
                }
            }
        }
    }

    @Test
    void testAdd()
    {
        assertThat(assertions.operator(ADD, "NUMBER '137.7'", "NUMBER '17.1'"))
                .isEqualTo(number("154.8"));

        assertThat(assertions.operator(ADD, "NUMBER '-1'", "NUMBER '-2'"))
                .isEqualTo(number("-3"));

        assertThat(assertions.operator(ADD, "NUMBER '1'", "NUMBER '2'"))
                .isEqualTo(number("3"));

        assertThat(assertions.operator(ADD, "NUMBER '.1234567890123456'", "NUMBER '.1234567890123456'"))
                .isEqualTo(number("0.2469135780246912"));

        assertThat(assertions.operator(ADD, "NUMBER '-.1234567890123456'", "NUMBER '-.1234567890123456'"))
                .isEqualTo(number("-0.2469135780246912"));

        assertThat(assertions.operator(ADD, "NUMBER '1234567890123456'", "NUMBER '1234567890123456'"))
                .isEqualTo(number("2469135780246912"));

        assertThat(assertions.operator(ADD, "NUMBER '123456789012345678'", "NUMBER '123456789012345678'"))
                .isEqualTo(number("246913578024691356"));

        assertThat(assertions.operator(ADD, "NUMBER '.123456789012345678'", "NUMBER '.123456789012345678'"))
                .isEqualTo(number("0.246913578024691356"));

        assertThat(assertions.operator(ADD, "NUMBER '1234567890123456789'", "NUMBER '1234567890123456789'"))
                .isEqualTo(number("2469135780246913578"));

        assertThat(assertions.operator(ADD, "NUMBER '12345678901234567890123456789012345678'", "NUMBER '12345678901234567890123456789012345678'"))
                .isEqualTo(number("24691357802469135780246913578024691356"));

        assertThat(assertions.operator(ADD, "NUMBER '-99999999999999999999999999999999999999'", "NUMBER '99999999999999999999999999999999999999'"))
                .isEqualTo(number("0"));

        // negative numbers
        assertThat(assertions.operator(ADD, "NUMBER '12345678901234567890'", "NUMBER '-12345678901234567890'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(ADD, "NUMBER '-12345678901234567890'", "NUMBER '12345678901234567890'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(ADD, "NUMBER '-12345678901234567890'", "NUMBER '-12345678901234567890'"))
                .isEqualTo(number("-2.469135780246913578E+19"));

        assertThat(assertions.operator(ADD, "NUMBER '12345678901234567890'", "NUMBER '-12345678901234567891'"))
                .isEqualTo(number("-1"));

        assertThat(assertions.operator(ADD, "NUMBER '-12345678901234567891'", "NUMBER '12345678901234567890'"))
                .isEqualTo(number("-1"));

        assertThat(assertions.operator(ADD, "NUMBER '-12345678901234567890'", "NUMBER '12345678901234567891'"))
                .isEqualTo(number("1"));

        assertThat(assertions.operator(ADD, "NUMBER '12345678901234567891'", "NUMBER '-12345678901234567890'"))
                .isEqualTo(number("1"));

        assertThat(assertions.operator(ADD, "NUMBER '999999999999999999'", "NUMBER '999999999999999999'"))
                .isEqualTo(number("1999999999999999998"));

        assertThat(assertions.operator(ADD, "NUMBER '999999999999999999'", "NUMBER '.999999999999999999'"))
                .isEqualTo(number("999999999999999999.999999999999999999"));

        assertThat(assertions.operator(ADD, "NUMBER '123456789012345678901234567890'", "NUMBER '.12345678'"))
                .isEqualTo(number("123456789012345678901234567890.12345678"));

        assertThat(assertions.operator(ADD, "NUMBER '.123456789012345678901234567890'", "NUMBER '12345678'"))
                .isEqualTo(number("12345678.12345678901234567890123456789"));

        assertThat(assertions.operator(ADD, "NUMBER '.12345678'", "NUMBER '123456789012345678901234567890'"))
                .isEqualTo(number("123456789012345678901234567890.12345678"));

        assertThat(assertions.operator(ADD, "NUMBER '12345678'", "NUMBER '.123456789012345678901234567890'"))
                .isEqualTo(number("12345678.12345678901234567890123456789"));

        assertThat(assertions.operator(ADD, "NUMBER '99999999999999999999999999999999999999'", "NUMBER '1'"))
                .isEqualTo(number("1E+38"));

        assertThat(assertions.operator(ADD, "NUMBER '.1'", "NUMBER '99999999999999999999999999999999999999'"))
                .isEqualTo(number("99999999999999999999999999999999999999.1"));

        assertThat(assertions.operator(ADD, "NUMBER '1'", "NUMBER '99999999999999999999999999999999999999'"))
                .isEqualTo(number("1E+38"));

        assertThat(assertions.operator(ADD, "NUMBER '99999999999999999999999999999999999999'", "NUMBER '.1'"))
                .isEqualTo(number("99999999999999999999999999999999999999.1"));

        assertThat(assertions.operator(ADD, "NUMBER '99999999999999999999999999999999999999'", "NUMBER '99999999999999999999999999999999999999'"))
                .isEqualTo(number("199999999999999999999999999999999999998"));

        assertThat(assertions.operator(ADD, "NUMBER '-99999999999999999999999999999999999999'", "NUMBER '-99999999999999999999999999999999999999'"))
                .isEqualTo(number("-199999999999999999999999999999999999998"));

        assertThat(assertions.operator(ADD, "NUMBER '17014000000000000000000000000000000000'", "NUMBER '-7014000000000000000000000000000000000.1'"))
                .isEqualTo(number("9999999999999999999999999999999999999.9"));

        assertThat(assertions.operator(ADD, "NUMBER '17015000000000000000000000000000000000'", "NUMBER '-7015000000000000000000000000000000000.1'"))
                .isEqualTo(number("9999999999999999999999999999999999999.9"));

        assertThat(assertions.operator(ADD, "NUMBER '10'", "NUMBER '0.0000000000000000000000000000000000001'"))
                .isEqualTo(number("10.0000000000000000000000000000000000001"));

        assertThat(assertions.operator(ADD, "NUMBER '-Infinity'", "NUMBER '0'"))
                .isEqualTo(number(NEGATIVE_INFINITY));

        assertThat(assertions.operator(ADD, "NUMBER '-Infinity'", "NUMBER '1'"))
                .isEqualTo(number(NEGATIVE_INFINITY));

        assertThat(assertions.operator(ADD, "NUMBER '-Infinity'", "NUMBER '-1'"))
                .isEqualTo(number(NEGATIVE_INFINITY));

        assertThat(assertions.operator(ADD, "NUMBER '-Infinity'", "NUMBER 'Infinity'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(ADD, "NUMBER '-Infinity'", "NUMBER '-Infinity'"))
                .isEqualTo(number(NEGATIVE_INFINITY));

        assertThat(assertions.operator(ADD, "NUMBER '-Infinity'", "NUMBER 'NaN'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(ADD, "NUMBER '+Infinity'", "NUMBER '0'"))
                .isEqualTo(number(POSITIVE_INFINITY));

        assertThat(assertions.operator(ADD, "NUMBER '+Infinity'", "NUMBER '1'"))
                .isEqualTo(number(POSITIVE_INFINITY));

        assertThat(assertions.operator(ADD, "NUMBER '+Infinity'", "NUMBER '-1'"))
                .isEqualTo(number(POSITIVE_INFINITY));

        assertThat(assertions.operator(ADD, "NUMBER '+Infinity'", "NUMBER 'Infinity'"))
                .isEqualTo(number(POSITIVE_INFINITY));

        assertThat(assertions.operator(ADD, "NUMBER '+Infinity'", "NUMBER '-Infinity'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(ADD, "NUMBER '+Infinity'", "NUMBER 'NaN'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(ADD, "NUMBER 'NaN'", "NUMBER '0'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(ADD, "NUMBER 'NaN'", "NUMBER '1'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(ADD, "NUMBER 'NaN'", "NUMBER '-1'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(ADD, "NUMBER 'NaN'", "NUMBER 'Infinity'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(ADD, "NUMBER 'NaN'", "NUMBER '-Infinity'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(ADD, "NUMBER 'NaN'", "NUMBER 'NaN'"))
                .isEqualTo(number(NaN));
    }

    @Test
    void testSubtract()
    {
        assertThat(assertions.operator(SUBTRACT, "NUMBER '107.7'", "NUMBER '17.1'"))
                .isEqualTo(number("90.6"));

        assertThat(assertions.operator(SUBTRACT, "NUMBER '-1'", "NUMBER '-2'"))
                .isEqualTo(number("1"));

        assertThat(assertions.operator(SUBTRACT, "NUMBER '1'", "NUMBER '2'"))
                .isEqualTo(number("-1"));

        assertThat(assertions.operator(SUBTRACT, "NUMBER '.1234567890123456'", "NUMBER '.1234567890123456'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(SUBTRACT, "NUMBER '-.1234567890123456'", "NUMBER '-.1234567890123456'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(SUBTRACT, "NUMBER '1234567890123456'", "NUMBER '1234567890123456'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(SUBTRACT, "NUMBER '1234567890123456789'", "NUMBER '1234567890123456789'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(SUBTRACT, "NUMBER '.1234567890123456789'", "NUMBER '.1234567890123456789'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(SUBTRACT, "NUMBER '12345678901234567890'", "NUMBER '12345678901234567890'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(SUBTRACT, "NUMBER '12345678901234567890123456789012345678'", "NUMBER '12345678901234567890123456789012345678'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(SUBTRACT, "NUMBER '-12345678901234567890'", "NUMBER '12345678901234567890'"))
                .isEqualTo(number("-2.469135780246913578E+19"));

        // negative numbers
        assertThat(assertions.operator(SUBTRACT, "NUMBER '12345678901234567890'", "NUMBER '12345678901234567890'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(SUBTRACT, "NUMBER '-12345678901234567890'", "NUMBER '-12345678901234567890'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(SUBTRACT, "NUMBER '-12345678901234567890'", "NUMBER '12345678901234567890'"))
                .isEqualTo(number("-2.469135780246913578E+19"));

        assertThat(assertions.operator(SUBTRACT, "NUMBER '12345678901234567890'", "NUMBER '12345678901234567891'"))
                .isEqualTo(number("-1"));

        assertThat(assertions.operator(SUBTRACT, "NUMBER '-12345678901234567891'", "NUMBER '-12345678901234567890'"))
                .isEqualTo(number("-1"));

        assertThat(assertions.operator(SUBTRACT, "NUMBER '-12345678901234567890'", "NUMBER '-12345678901234567891'"))
                .isEqualTo(number("1"));

        assertThat(assertions.operator(SUBTRACT, "NUMBER '12345678901234567891'", "NUMBER '12345678901234567890'"))
                .isEqualTo(number("1"));

        assertThat(assertions.operator(SUBTRACT, "NUMBER '999999999999999999'", "NUMBER '999999999999999999'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(SUBTRACT, "NUMBER '999999999999999999'", "NUMBER '.999999999999999999'"))
                .isEqualTo(number("999999999999999998.000000000000000001"));

        assertThat(assertions.operator(SUBTRACT, "NUMBER '123456789012345678901234567890'", "NUMBER '.00000001'"))
                .isEqualTo(number("123456789012345678901234567889.99999999"));

        assertThat(assertions.operator(SUBTRACT, "NUMBER '.000000000000000000000000000001'", "NUMBER '87654321'"))
                .isEqualTo(number("-87654320.999999999999999999999999999999"));

        assertThat(assertions.operator(SUBTRACT, "NUMBER '.00000001'", "NUMBER '123456789012345678901234567890'"))
                .isEqualTo(number("-123456789012345678901234567889.99999999"));

        assertThat(assertions.operator(SUBTRACT, "NUMBER '12345678'", "NUMBER '.000000000000000000000000000001'"))
                .isEqualTo(number("12345677.999999999999999999999999999999"));

        assertThat(assertions.operator(SUBTRACT, "NUMBER '-99999999999999999999999999999999999999'", "NUMBER '1'"))
                .isEqualTo(number("-1E+38"));

        assertThat(assertions.operator(SUBTRACT, "NUMBER '.1'", "NUMBER '99999999999999999999999999999999999999'"))
                .isEqualTo(number("-99999999999999999999999999999999999998.9"));

        assertThat(assertions.operator(SUBTRACT, "NUMBER '-1'", "NUMBER '99999999999999999999999999999999999999'"))
                .isEqualTo(number("-1E+38"));

        assertThat(assertions.operator(SUBTRACT, "NUMBER '99999999999999999999999999999999999999'", "NUMBER '.1'"))
                .isEqualTo(number("99999999999999999999999999999999999998.9"));

        assertThat(assertions.operator(SUBTRACT, "NUMBER '-99999999999999999999999999999999999999'", "NUMBER '99999999999999999999999999999999999999'"))
                .isEqualTo(number("-199999999999999999999999999999999999998"));

        assertThat(assertions.operator(SUBTRACT, "NUMBER '17014000000000000000000000000000000000'", "NUMBER '7014000000000000000000000000000000000.1'"))
                .isEqualTo(number("9999999999999999999999999999999999999.9"));

        assertThat(assertions.operator(SUBTRACT, "NUMBER '17015000000000000000000000000000000000'", "NUMBER '7015000000000000000000000000000000000.1'"))
                .isEqualTo(number("9999999999999999999999999999999999999.9"));

        assertThat(assertions.operator(SUBTRACT, "NUMBER '10'", "NUMBER '0.0000000000000000000000000000000000001'"))
                .isEqualTo(number("9.9999999999999999999999999999999999999"));

        assertThat(assertions.operator(SUBTRACT, "NUMBER '-Infinity'", "NUMBER '0'"))
                .isEqualTo(number(NEGATIVE_INFINITY));

        assertThat(assertions.operator(SUBTRACT, "NUMBER '-Infinity'", "NUMBER '1'"))
                .isEqualTo(number(NEGATIVE_INFINITY));

        assertThat(assertions.operator(SUBTRACT, "NUMBER '-Infinity'", "NUMBER '-1'"))
                .isEqualTo(number(NEGATIVE_INFINITY));

        assertThat(assertions.operator(SUBTRACT, "NUMBER '-Infinity'", "NUMBER 'Infinity'"))
                .isEqualTo(number(NEGATIVE_INFINITY));

        assertThat(assertions.operator(SUBTRACT, "NUMBER '-Infinity'", "NUMBER '-Infinity'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(SUBTRACT, "NUMBER '-Infinity'", "NUMBER 'NaN'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(SUBTRACT, "NUMBER '+Infinity'", "NUMBER '0'"))
                .isEqualTo(number(POSITIVE_INFINITY));

        assertThat(assertions.operator(SUBTRACT, "NUMBER '+Infinity'", "NUMBER '1'"))
                .isEqualTo(number(POSITIVE_INFINITY));

        assertThat(assertions.operator(SUBTRACT, "NUMBER '+Infinity'", "NUMBER '-1'"))
                .isEqualTo(number(POSITIVE_INFINITY));

        assertThat(assertions.operator(SUBTRACT, "NUMBER '+Infinity'", "NUMBER 'Infinity'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(SUBTRACT, "NUMBER '+Infinity'", "NUMBER '-Infinity'"))
                .isEqualTo(number(POSITIVE_INFINITY));

        assertThat(assertions.operator(SUBTRACT, "NUMBER '+Infinity'", "NUMBER 'NaN'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(SUBTRACT, "NUMBER 'NaN'", "NUMBER '0'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(SUBTRACT, "NUMBER 'NaN'", "NUMBER '1'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(SUBTRACT, "NUMBER 'NaN'", "NUMBER '-1'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(SUBTRACT, "NUMBER 'NaN'", "NUMBER 'Infinity'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(SUBTRACT, "NUMBER 'NaN'", "NUMBER '-Infinity'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(SUBTRACT, "NUMBER 'NaN'", "NUMBER 'NaN'"))
                .isEqualTo(number(NaN));
    }

    @Test
    void testMultiply()
    {
        assertThat(assertions.operator(MULTIPLY, "NUMBER '0'", "NUMBER '1'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '0'", "NUMBER '-1'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '1'", "NUMBER '0'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '-1'", "NUMBER '0'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '12'", "NUMBER '3'"))
                .isEqualTo(number("36"));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '12'", "NUMBER '-3'"))
                .isEqualTo(number("-36"));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '-12'", "NUMBER '3'"))
                .isEqualTo(number("-36"));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '1234567890123456'", "NUMBER '3'"))
                .isEqualTo(number("3703703670370368"));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '.1234567890123456'", "NUMBER '3'"))
                .isEqualTo(number("0.3703703670370368"));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '.1234567890123456'", "NUMBER '.3'"))
                .isEqualTo(number("0.03703703670370368"));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '0'", "NUMBER '1'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '0'", "NUMBER '-1'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '1'", "NUMBER '0'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '-1'", "NUMBER '0'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '12345678901234567'", "NUMBER '123456789012345670'"))
                .isEqualTo(number("1.52415787532388345526596755677489E+33"));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '-12345678901234567'", "NUMBER '123456789012345670'"))
                .isEqualTo(number("-1.52415787532388345526596755677489E+33"));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '-12345678901234567'", "NUMBER '-123456789012345670'"))
                .isEqualTo(number("1.52415787532388345526596755677489E+33"));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '.12345678901234567'", "NUMBER '.123456789012345670'"))
                .isEqualTo(number("0.0152415787532388345526596755677489"));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '0'", "1"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '0'", "-1"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '1'", "0"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '-1'", "0"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '12345678901234567890123456789012345678'", "NUMBER '3'"))
                .isEqualTo(number("37037036703703703670370370367037037034"));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '1234567890123456789.0123456789012345678'", "NUMBER '3'"))
                .isEqualTo(number("3703703670370370367.0370370367037037034"));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '.12345678901234567890123456789012345678'", "NUMBER '3'"))
                .isEqualTo(number("0.37037036703703703670370370367037037034"));

        assertThat(assertions.operator(MULTIPLY, "0", "NUMBER '1'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MULTIPLY, "0", "NUMBER '-1'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MULTIPLY, "1", "NUMBER '0'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MULTIPLY, "-1", "NUMBER '0'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '3'", "NUMBER '12345678901234567890123456789012345678'"))
                .isEqualTo(number("37037036703703703670370370367037037034"));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '3'", "NUMBER '1234567890123456789.0123456789012345678'"))
                .isEqualTo(number("3703703670370370367.0370370367037037034"));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '3'", "NUMBER '.12345678901234567890123456789012345678'"))
                .isEqualTo(number("0.37037036703703703670370370367037037034"));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '0'", "NUMBER '1'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '0'", "NUMBER '-1'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '1'", "NUMBER '0'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '-1'", "NUMBER '0'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '3'", "NUMBER '2'"))
                .isEqualTo(number("6"));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '3'", "NUMBER '0.2'"))
                .isEqualTo(number("0.6"));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '.1234567890123456789'", "NUMBER '.1234567890123456789'"))
                .isEqualTo(number("0.01524157875323883675019051998750190521"));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '.1234567890123456789'", "NUMBER '.12345678901234567890'"))
                .isEqualTo(number("0.01524157875323883675019051998750190521"));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '.1'", "NUMBER '.12345678901234567890123456789012345678'"))
                .isEqualTo(number("0.012345678901234567890123456789012345678"));

        // large multiplications
        assertThat(assertions.operator(MULTIPLY, "NUMBER '12345678901234567890123456789012345678'", "NUMBER '9'"))
                .isEqualTo(number("111111110111111111011111111101111111102"));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '.12345678901234567890123456789012345678'", "NUMBER '9'"))
                .isEqualTo(number("1.11111110111111111011111111101111111102"));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '12345678901234567890123456789012345678'", "NUMBER '-9'"))
                .isEqualTo(number("-111111110111111111011111111101111111102"));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '.12345678901234567890123456789012345678'", "NUMBER '-9'"))
                .isEqualTo(number("-1.11111110111111111011111111101111111102"));

        // exceeding max precision
        assertThat(assertions.operator(
                MULTIPLY,
                "NUMBER '66253166201757048147019387751429455554737010320073440042660951972479154873726992102003231643719422615777393043'",
                "NUMBER '18262774589350675530582336394934775228395046345354563393581350068572539440130541361187755243849469688290680443'"))
                .isEqualTo(number("1.2099666401734756302615960804662809455013260652052016476208956457442034413389008286145859185565320271851315022978671553334828770682233313021073582891886195122714730571619107009764269825599972382875203E+219"));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '-Infinity'", "NUMBER '0'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '-Infinity'", "NUMBER '1'"))
                .isEqualTo(number(NEGATIVE_INFINITY));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '-Infinity'", "NUMBER '-1'"))
                .isEqualTo(number(POSITIVE_INFINITY));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '-Infinity'", "NUMBER 'Infinity'"))
                .isEqualTo(number(NEGATIVE_INFINITY));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '-Infinity'", "NUMBER '-Infinity'"))
                .isEqualTo(number(POSITIVE_INFINITY));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '-Infinity'", "NUMBER 'NaN'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '+Infinity'", "NUMBER '0'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '+Infinity'", "NUMBER '1'"))
                .isEqualTo(number(POSITIVE_INFINITY));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '+Infinity'", "NUMBER '-1'"))
                .isEqualTo(number(NEGATIVE_INFINITY));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '+Infinity'", "NUMBER 'Infinity'"))
                .isEqualTo(number(POSITIVE_INFINITY));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '+Infinity'", "NUMBER '-Infinity'"))
                .isEqualTo(number(NEGATIVE_INFINITY));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '+Infinity'", "NUMBER 'NaN'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(MULTIPLY, "NUMBER 'NaN'", "NUMBER '0'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(MULTIPLY, "NUMBER 'NaN'", "NUMBER '1'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(MULTIPLY, "NUMBER 'NaN'", "NUMBER '-1'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(MULTIPLY, "NUMBER 'NaN'", "NUMBER 'Infinity'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(MULTIPLY, "NUMBER 'NaN'", "NUMBER '-Infinity'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(MULTIPLY, "NUMBER 'NaN'", "NUMBER 'NaN'"))
                .isEqualTo(number(NaN));
    }

    @Test
    void testDivide()
    {
        assertThat(assertions.operator(DIVIDE, "NUMBER '1'", "NUMBER '3'"))
                .isEqualTo(number("0.333333"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '1.0'", "NUMBER '3'"))
                .isEqualTo(number("0.333333"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '1.0'", "NUMBER '0.1'"))
                .isEqualTo(number("1E+1"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '1.0'", "NUMBER '9.0'"))
                .isEqualTo(number("0.111111"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '1000'", "NUMBER '25'"))
                .isEqualTo(number("4E+1"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '500.00'", "NUMBER '0.1'"))
                .isEqualTo(number("5E+3"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '100.00'", "NUMBER '0.3'"))
                .isEqualTo(number("333.333333"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '100.00'", "NUMBER '0.30'"))
                .isEqualTo(number("333.333333"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '100.00'", "NUMBER '-0.30'"))
                .isEqualTo(number("-333.333333"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '-100.00'", "NUMBER '0.30'"))
                .isEqualTo(number("-333.333333"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '200.00'", "NUMBER '0.3'"))
                .isEqualTo(number("666.666667"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '200.00000'", "NUMBER '0.3'"))
                .isEqualTo(number("666.666667"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '200.00000'", "NUMBER '-0.3'"))
                .isEqualTo(number("-666.666667"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '-200.00000'", "NUMBER '0.3'"))
                .isEqualTo(number("-666.666667"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '999999999999999999'", "NUMBER '1'"))
                .isEqualTo(number("999999999999999999"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '9'", "NUMBER '000000000000000003'"))
                .isEqualTo(number("3"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '9.0'", "NUMBER '3.0'"))
                .isEqualTo(number("3"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '999999999999999999'", "NUMBER '500000000000000000'"))
                .isEqualTo(number("2"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '1'", "NUMBER '999999999999999999'"))
                .isEqualTo(number("1E-18"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '-1'", "NUMBER '999999999999999999'"))
                .isEqualTo(number("-1E-18"));

        // round
        assertThat(assertions.operator(DIVIDE, "NUMBER '9'", "NUMBER '5'"))
                .isEqualTo(number("1.8"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '7'", "NUMBER '5'"))
                .isEqualTo(number("1.4"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '-9'", "NUMBER '5'"))
                .isEqualTo(number("-1.8"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '-7'", "NUMBER '5'"))
                .isEqualTo(number("-1.4"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '-9'", "NUMBER '-5'"))
                .isEqualTo(number("1.8"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '-7'", "NUMBER '-5'"))
                .isEqualTo(number("1.4"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '9'", "NUMBER '-5'"))
                .isEqualTo(number("-1.8"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '7'", "NUMBER '-5'"))
                .isEqualTo(number("-1.4"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '-1'", "NUMBER '2'"))
                .isEqualTo(number("-0.5"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '1'", "NUMBER '-2'"))
                .isEqualTo(number("-0.5"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '-1'", "NUMBER '3'"))
                .isEqualTo(number("-0.333333"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '10'", "NUMBER '.000000001'"))
                .isEqualTo(number("1E+10"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '-10'", "NUMBER '.000000001'"))
                .isEqualTo(number("-1E+10"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '10'", "NUMBER '-.000000001'"))
                .isEqualTo(number("-1E+10"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '-10'", "NUMBER '-.000000001'"))
                .isEqualTo(number("1E+10"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '200000000000000000000000000000000000'", "NUMBER '0.30'"))
                .isEqualTo(number("666666666666666666666666666666666666.666667"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '200000000000000000000000000000000000'", "NUMBER '-0.30'"))
                .isEqualTo(number("-666666666666666666666666666666666666.666667"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '-.20000000000000000000000000000000000000'", "NUMBER '0.30'"))
                .isEqualTo(number("-0.666667"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '-.20000000000000000000000000000000000000'", "NUMBER '-0.30'"))
                .isEqualTo(number("0.666667"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '.20000000000000000000000000000000000000'", "NUMBER '0.30'"))
                .isEqualTo(number("0.666667"));

        // round
        assertThat(assertions.operator(DIVIDE, "NUMBER '500000000000000000000000000000075'", "NUMBER '50'"))
                .isEqualTo(number("10000000000000000000000000000001.5"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '500000000000000000000000000000070'", "NUMBER '50'"))
                .isEqualTo(number("10000000000000000000000000000001.4"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '-500000000000000000000000000000075'", "NUMBER '50'"))
                .isEqualTo(number("-10000000000000000000000000000001.5"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '-500000000000000000000000000000070'", "NUMBER '50'"))
                .isEqualTo(number("-10000000000000000000000000000001.4"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '500000000000000000000000000000075'", "NUMBER '-50'"))
                .isEqualTo(number("-10000000000000000000000000000001.5"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '500000000000000000000000000000070'", "NUMBER '-50'"))
                .isEqualTo(number("-10000000000000000000000000000001.4"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '-500000000000000000000000000000075'", "NUMBER '-50'"))
                .isEqualTo(number("10000000000000000000000000000001.5"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '-500000000000000000000000000000070'", "NUMBER '-50'"))
                .isEqualTo(number("10000000000000000000000000000001.4"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '-1'", "NUMBER '2'"))
                .isEqualTo(number("-0.5"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '1'", "NUMBER '-2'"))
                .isEqualTo(number("-0.5"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '-1'", "NUMBER '3'"))
                .isEqualTo(number("-0.333333"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '0.1'", "NUMBER '.0000000000000000001'"))
                .isEqualTo(number("1E+18"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '-0.1'", "NUMBER '.0000000000000000001'"))
                .isEqualTo(number("-1E+18"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '0.1'", "NUMBER '-.0000000000000000001'"))
                .isEqualTo(number("-1E+18"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '-0.1'", "NUMBER '-.0000000000000000001'"))
                .isEqualTo(number("1E+18"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '9'", "NUMBER '000000000000000003.0'"))
                .isEqualTo(number("3"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '1'", "NUMBER '99999999999999999999999999999999999999'"))
                .isEqualTo(number("1E-38"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '-1'", "NUMBER '99999999999999999999999999999999999999'"))
                .isEqualTo(number("-1E-38"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '1'", "NUMBER '-99999999999999999999999999999999999999'"))
                .isEqualTo(number("-1E-38"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '-1'", "NUMBER '-99999999999999999999999999999999999999'"))
                .isEqualTo(number("1E-38"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '99999999999999999999999999999999999999'", "NUMBER '11111111111111111111111111111111111111'"))
                .isEqualTo(number("9"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '-99999999999999999999999999999999999999'", "NUMBER '11111111111111111111111111111111111111'"))
                .isEqualTo(number("-9"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '99999999999999999999999999999999999999'", "NUMBER '-11111111111111111111111111111111111111'"))
                .isEqualTo(number("-9"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '-99999999999999999999999999999999999999'", "NUMBER '-11111111111111111111111111111111111111'"))
                .isEqualTo(number("9"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '11111111111111111111111111111111111111'", "NUMBER '99999999999999999999999999999999999999'"))
                .isEqualTo(number("0.111111111111111111111111111111111111111"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '-11111111111111111111111111111111111111'", "NUMBER '99999999999999999999999999999999999999'"))
                .isEqualTo(number("-0.111111111111111111111111111111111111111"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '11111111111111111111111111111111111111'", "NUMBER '-99999999999999999999999999999999999999'"))
                .isEqualTo(number("-0.111111111111111111111111111111111111111"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '-11111111111111111111111111111111111111'", "NUMBER '-99999999999999999999999999999999999999'"))
                .isEqualTo(number("0.111111111111111111111111111111111111111"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '99999999999999999999999999999999999998'", "NUMBER '99999999999999999999999999999999999999'"))
                .isEqualTo(number("0.99999999999999999999999999999999999999"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '9999999999999999999999999999999999999.8'", "NUMBER '9999999999999999999999999999999999999.9'"))
                .isEqualTo(number("0.99999999999999999999999999999999999999"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '9999999999999999999999.9'", "NUMBER '1111111111111111111111.100'"))
                .isEqualTo(number("9"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '1635619.3155'", "NUMBER '47497517.7405'"))
                .isEqualTo(number("0.0344358904066548"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '12345678901234567890123456789012345678'", "NUMBER '.1'"))
                .isEqualTo(number("1.2345678901234567890123456789012345678E+38"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '12345678901234567890123456789012345678'", "NUMBER '.12345678901234567890123456789012345678'"))
                .isEqualTo(number("1E+38"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '1'", "NUMBER '.12345678901234567890123456789012345678'"))
                .isEqualTo(number("8.100000072900000663390006036849054935918"));

        // division by zero tests
        assertTrinoExceptionThrownBy(assertions.operator(DIVIDE, "NUMBER '1'", "NUMBER '0'")::evaluate)
                .hasErrorCode(DIVISION_BY_ZERO);

        assertTrinoExceptionThrownBy(assertions.operator(DIVIDE, "NUMBER '1.000000000000000000000000000000000000'", "NUMBER '0'")::evaluate)
                .hasErrorCode(DIVISION_BY_ZERO);

        assertTrinoExceptionThrownBy(assertions.operator(DIVIDE, "NUMBER '1.000000000000000000000000000000000000'", "NUMBER '0.0000000000000000000000000000000000000'")::evaluate)
                .hasErrorCode(DIVISION_BY_ZERO);

        assertTrinoExceptionThrownBy(assertions.operator(DIVIDE, "NUMBER '1'", "NUMBER '0.0000000000000000000000000000000000000'")::evaluate)
                .hasErrorCode(DIVISION_BY_ZERO);

        assertThat(assertions.operator(DIVIDE, "NUMBER '1000'", "NUMBER '25'"))
                .isEqualTo(number("4E+1"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '-Infinity'", "NUMBER '0'"))
                .isEqualTo(number(NEGATIVE_INFINITY));

        assertThat(assertions.operator(DIVIDE, "NUMBER '-Infinity'", "NUMBER '1'"))
                .isEqualTo(number(NEGATIVE_INFINITY));

        assertThat(assertions.operator(DIVIDE, "NUMBER '-Infinity'", "NUMBER '-1'"))
                .isEqualTo(number(POSITIVE_INFINITY));

        assertThat(assertions.operator(DIVIDE, "NUMBER '-Infinity'", "NUMBER 'Infinity'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(DIVIDE, "NUMBER '-Infinity'", "NUMBER '-Infinity'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(DIVIDE, "NUMBER '-Infinity'", "NUMBER 'NaN'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(DIVIDE, "NUMBER '+Infinity'", "NUMBER '0'"))
                .isEqualTo(number(POSITIVE_INFINITY));

        assertThat(assertions.operator(DIVIDE, "NUMBER '+Infinity'", "NUMBER '1'"))
                .isEqualTo(number(POSITIVE_INFINITY));

        assertThat(assertions.operator(DIVIDE, "NUMBER '+Infinity'", "NUMBER '-1'"))
                .isEqualTo(number(NEGATIVE_INFINITY));

        assertThat(assertions.operator(DIVIDE, "NUMBER '+Infinity'", "NUMBER 'Infinity'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(DIVIDE, "NUMBER '+Infinity'", "NUMBER '-Infinity'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(DIVIDE, "NUMBER '+Infinity'", "NUMBER 'NaN'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(DIVIDE, "NUMBER 'NaN'", "NUMBER '0'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(DIVIDE, "NUMBER 'NaN'", "NUMBER '1'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(DIVIDE, "NUMBER 'NaN'", "NUMBER '-1'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(DIVIDE, "NUMBER 'NaN'", "NUMBER 'Infinity'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(DIVIDE, "NUMBER 'NaN'", "NUMBER '-Infinity'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(DIVIDE, "NUMBER 'NaN'", "NUMBER 'NaN'"))
                .isEqualTo(number(NaN));
    }

    @Test
    void testModulus()
    {
        assertThat(assertions.operator(MODULUS, "NUMBER '1'", "NUMBER '3'"))
                .isEqualTo(number("1"));

        assertThat(assertions.operator(MODULUS, "NUMBER '10'", "NUMBER '3'"))
                .isEqualTo(number("1"));

        assertThat(assertions.operator(MODULUS, "NUMBER '0'", "NUMBER '3'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MODULUS, "NUMBER '0'", "NUMBER '-3'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MODULUS, "NUMBER '10.0'", "NUMBER '3'"))
                .isEqualTo(number("1"));

        assertThat(assertions.operator(MODULUS, "NUMBER '10.0'", "NUMBER '3.000'"))
                .isEqualTo(number("1"));

        assertThat(assertions.operator(MODULUS, "NUMBER '7'", "NUMBER '3.0000000000000000'"))
                .isEqualTo(number("1"));

        assertThat(assertions.operator(MODULUS, "NUMBER '7.00000000000000000'", "NUMBER '3.00000000000000000'"))
                .isEqualTo(number("1"));

        assertThat(assertions.operator(MODULUS, "NUMBER '7.00000000000000000'", "NUMBER '3'"))
                .isEqualTo(number("1"));

        assertThat(assertions.operator(MODULUS, "NUMBER '7'", "NUMBER '3'"))
                .isEqualTo(number("1"));

        assertThat(assertions.operator(MODULUS, "NUMBER '.1'", "NUMBER '.03'"))
                .isEqualTo(number("0.01"));

        assertThat(assertions.operator(MODULUS, "NUMBER '.0001'", "NUMBER '.03'"))
                .isEqualTo(number("0.0001"));

        assertThat(assertions.operator(MODULUS, "NUMBER '-10'", "NUMBER '3'"))
                .isEqualTo(number("-1"));

        assertThat(assertions.operator(MODULUS, "NUMBER '10'", "NUMBER '-3'"))
                .isEqualTo(number("1"));

        assertThat(assertions.operator(MODULUS, "NUMBER '-10'", "NUMBER '-3'"))
                .isEqualTo(number("-1"));

        assertThat(assertions.operator(MODULUS, "NUMBER '9'", "NUMBER '3'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MODULUS, "NUMBER '-9'", "NUMBER '3'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MODULUS, "NUMBER '9'", "NUMBER '-3'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MODULUS, "NUMBER '-9'", "NUMBER '-3'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MODULUS, "NUMBER '0'", "NUMBER '3'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MODULUS, "NUMBER '0'", "NUMBER '-3'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MODULUS, "NUMBER '7'", "NUMBER '3'"))
                .isEqualTo(number("1"));

        assertThat(assertions.operator(MODULUS, "NUMBER '7'", "NUMBER '3'"))
                .isEqualTo(number("1"));

        assertThat(assertions.operator(MODULUS, "NUMBER '7.00000000000000000'", "NUMBER '3'"))
                .isEqualTo(number("1"));

        assertThat(assertions.operator(MODULUS, "NUMBER '-7.00000000000000000'", "NUMBER '3'"))
                .isEqualTo(number("-1"));

        assertThat(assertions.operator(MODULUS, "NUMBER '7.0000000000000000'", "NUMBER '-3'"))
                .isEqualTo(number("1"));

        assertThat(assertions.operator(MODULUS, "NUMBER '-7.0000000000000000'", "NUMBER '-3'"))
                .isEqualTo(number("-1"));

        assertThat(assertions.operator(MODULUS, "NUMBER '9.00000000000000000'", "NUMBER '3'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MODULUS, "NUMBER '-9.00000000000000000'", "NUMBER '3'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MODULUS, "NUMBER '9.0000000000000000'", "NUMBER '-3'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MODULUS, "NUMBER '-9.0000000000000000'", "NUMBER '-3'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MODULUS, "NUMBER '0'", "NUMBER '3.0000000000000000000000000000000000000'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MODULUS, "NUMBER '0'", "NUMBER '-3.0000000000000000000000000000000000000'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MODULUS, "NUMBER '7'", "NUMBER '3.0000000000000000000000000000000000000'"))
                .isEqualTo(number("1"));

        assertThat(assertions.operator(MODULUS, "NUMBER '7.00000000000000000'", "NUMBER '3.0000000000000000000000000000000000000'"))
                .isEqualTo(number("1"));

        assertThat(assertions.operator(MODULUS, "NUMBER '.01'", "NUMBER '3.0000000000000000000000000000000000000'"))
                .isEqualTo(number("0.01"));

        assertThat(assertions.operator(MODULUS, "NUMBER '-7'", "NUMBER '3.0000000000000000000000000000000000000'"))
                .isEqualTo(number("-1"));

        assertThat(assertions.operator(MODULUS, "NUMBER '7'", "NUMBER '-3.0000000000000000000000000000000000000'"))
                .isEqualTo(number("1"));

        assertThat(assertions.operator(MODULUS, "NUMBER '-7'", "NUMBER '-3.0000000000000000000000000000000000000'"))
                .isEqualTo(number("-1"));

        assertThat(assertions.operator(MODULUS, "NUMBER '9'", "NUMBER '3.0000000000000000000000000000000000000'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MODULUS, "NUMBER '-9'", "NUMBER '3.0000000000000000000000000000000000000'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MODULUS, "NUMBER '9'", "NUMBER '-3.0000000000000000000000000000000000000'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MODULUS, "NUMBER '-9'", "NUMBER '-3.0000000000000000000000000000000000000'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MODULUS, "NUMBER '99999999999999999999999999999999999997'", "NUMBER '3'"))
                .isEqualTo(number("1"));

        assertThat(assertions.operator(MODULUS, "NUMBER '99999999999999999999999999999999999997'", "NUMBER '3.0000000000000000'"))
                .isEqualTo(number("1"));

        assertThat(assertions.operator(MODULUS, "NUMBER '-99999999999999999999999999999999999997'", "NUMBER '3'"))
                .isEqualTo(number("-1"));

        assertThat(assertions.operator(MODULUS, "NUMBER '99999999999999999999999999999999999997'", "NUMBER '-3'"))
                .isEqualTo(number("1"));

        assertThat(assertions.operator(MODULUS, "NUMBER '-99999999999999999999999999999999999997'", "NUMBER '-3'"))
                .isEqualTo(number("-1"));

        assertThat(assertions.operator(MODULUS, "NUMBER '99999999999999999999999999999999999999'", "NUMBER '3'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MODULUS, "NUMBER '-99999999999999999999999999999999999999'", "NUMBER '3'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MODULUS, "NUMBER '99999999999999999999999999999999999999'", "NUMBER '-3'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MODULUS, "NUMBER '-99999999999999999999999999999999999999'", "NUMBER '-3'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MODULUS, "NUMBER '0.000000000000000000000000000000000000'", "NUMBER '3'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MODULUS, "NUMBER '0.000000000000000000000000000000000000'", "NUMBER '-3'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MODULUS, "NUMBER '7.000000000000000000000000000000000000'", "NUMBER '3'"))
                .isEqualTo(number("1"));

        assertThat(assertions.operator(MODULUS, "NUMBER '-7.000000000000000000000000000000000000'", "NUMBER '3'"))
                .isEqualTo(number("-1"));

        assertThat(assertions.operator(MODULUS, "NUMBER '7.000000000000000000000000000000000000'", "NUMBER '-3'"))
                .isEqualTo(number("1"));

        assertThat(assertions.operator(MODULUS, "NUMBER '-7.000000000000000000000000000000000000'", "NUMBER '-3'"))
                .isEqualTo(number("-1"));

        assertThat(assertions.operator(MODULUS, "NUMBER '9.000000000000000000000000000000000000'", "NUMBER '3'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MODULUS, "NUMBER '-9.000000000000000000000000000000000000'", "NUMBER '3'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MODULUS, "NUMBER '9.000000000000000000000000000000000000'", "NUMBER '-3'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MODULUS, "NUMBER '-9.000000000000000000000000000000000000'", "NUMBER '-3'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MODULUS, "NUMBER '0'", "NUMBER '3'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MODULUS, "NUMBER '0'", "NUMBER '-3'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MODULUS, "NUMBER '7'", "NUMBER '3'"))
                .isEqualTo(number("1"));

        assertThat(assertions.operator(MODULUS, "NUMBER '7'", "NUMBER '3'"))
                .isEqualTo(number("1"));

        assertThat(assertions.operator(MODULUS, "NUMBER '7'", "NUMBER '3'"))
                .isEqualTo(number("1"));

        assertThat(assertions.operator(MODULUS, "NUMBER '-7'", "NUMBER '3'"))
                .isEqualTo(number("-1"));

        assertThat(assertions.operator(MODULUS, "NUMBER '7'", "NUMBER '-3'"))
                .isEqualTo(number("1"));

        assertThat(assertions.operator(MODULUS, "NUMBER '-7'", "NUMBER '-3'"))
                .isEqualTo(number("-1"));

        assertThat(assertions.operator(MODULUS, "NUMBER '9'", "NUMBER '3'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MODULUS, "NUMBER '-9'", "NUMBER '3'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MODULUS, "NUMBER '9'", "NUMBER '-3'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MODULUS, "NUMBER '-9'", "NUMBER '-3'"))
                .isEqualTo(number("0"));

        // division by zero tests
        assertTrinoExceptionThrownBy(assertions.operator(MODULUS, "NUMBER '1'", "NUMBER '0'")::evaluate)
                .hasErrorCode(DIVISION_BY_ZERO);

        assertTrinoExceptionThrownBy(assertions.operator(MODULUS, "NUMBER '1.000000000000000000000000000000000000'", "NUMBER '0'")::evaluate)
                .hasErrorCode(DIVISION_BY_ZERO);

        assertTrinoExceptionThrownBy(assertions.operator(MODULUS, "NUMBER '1.000000000000000000000000000000000000'", "NUMBER '0.0000000000000000000000000000000000000'")::evaluate)
                .hasErrorCode(DIVISION_BY_ZERO);

        assertTrinoExceptionThrownBy(assertions.operator(MODULUS, "NUMBER '1'", "NUMBER '0.0000000000000000000000000000000000000'")::evaluate)
                .hasErrorCode(DIVISION_BY_ZERO);

        assertTrinoExceptionThrownBy(assertions.operator(MODULUS, "NUMBER '1'", "NUMBER '0'")::evaluate)
                .hasErrorCode(DIVISION_BY_ZERO);

        assertThat(assertions.operator(MODULUS, "NUMBER '-Infinity'", "NUMBER '0'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(MODULUS, "NUMBER '-Infinity'", "NUMBER '1'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(MODULUS, "NUMBER '-Infinity'", "NUMBER '-1'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(MODULUS, "NUMBER '-Infinity'", "NUMBER 'Infinity'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(MODULUS, "NUMBER '-Infinity'", "NUMBER '-Infinity'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(MODULUS, "NUMBER '-Infinity'", "NUMBER 'NaN'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(MODULUS, "NUMBER '+Infinity'", "NUMBER '0'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(MODULUS, "NUMBER '+Infinity'", "NUMBER '1'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(MODULUS, "NUMBER '+Infinity'", "NUMBER '-1'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(MODULUS, "NUMBER '+Infinity'", "NUMBER 'Infinity'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(MODULUS, "NUMBER '+Infinity'", "NUMBER '-Infinity'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(MODULUS, "NUMBER '+Infinity'", "NUMBER 'NaN'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(MODULUS, "NUMBER 'NaN'", "NUMBER '0'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(MODULUS, "NUMBER 'NaN'", "NUMBER '1'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(MODULUS, "NUMBER 'NaN'", "NUMBER '-1'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(MODULUS, "NUMBER 'NaN'", "NUMBER 'Infinity'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(MODULUS, "NUMBER 'NaN'", "NUMBER '-Infinity'"))
                .isEqualTo(number(NaN));

        assertThat(assertions.operator(MODULUS, "NUMBER 'NaN'", "NUMBER 'NaN'"))
                .isEqualTo(number(NaN));
    }

    @Test
    void testNegation()
    {
        assertThat(assertions.operator(NEGATION, "NUMBER '1'"))
                .isEqualTo(number("-1"));

        assertThat(assertions.operator(NEGATION, "NUMBER '-1'"))
                .isEqualTo(number("1"));

        assertThat(assertions.operator(NEGATION, "NUMBER '123456.00000010' "))
                .isEqualTo(number("-123456.0000001"));

        assertThat(assertions.operator(NEGATION, "NUMBER '1234567.00500010734' "))
                .isEqualTo(number("-1234567.00500010734"));

        assertThat(assertions.operator(NEGATION, "NUMBER '-1234567.00500010734' "))
                .isEqualTo(number("1234567.00500010734"));

        assertThat(assertions.operator(NEGATION, "NUMBER '0' "))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(NEGATION, "NUMBER '0.00000000000000000000' "))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(NEGATION, "NUMBER '12345678901234567890123456789012345678'"))
                .isEqualTo(number("-12345678901234567890123456789012345678"));

        assertThat(assertions.operator(NEGATION, "NUMBER '-12345678901234567890123456789012345678'"))
                .isEqualTo(number("12345678901234567890123456789012345678"));

        assertThat(assertions.operator(NEGATION, "NUMBER '123456789012345678.90123456789012345678'"))
                .isEqualTo(number("-123456789012345678.90123456789012345678"));

        assertThat(assertions.operator(NEGATION, "NUMBER '-Infinity'"))
                .isEqualTo(number(POSITIVE_INFINITY));

        assertThat(assertions.operator(NEGATION, "NUMBER '+Infinity'"))
                .isEqualTo(number(NEGATIVE_INFINITY));

        assertThat(assertions.operator(NEGATION, "NUMBER 'NaN'"))
                .isEqualTo(number(NaN));
    }

    @Test
    void testEqual()
    {
        assertThat(assertions.operator(EQUAL, "NUMBER '37'", "NUMBER '37'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "NUMBER '37'", "NUMBER '0037'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "NUMBER '37'", "NUMBER '37.0'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "NUMBER '37.0'", "NUMBER '37'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "NUMBER '-0.000'", "NUMBER '0.00000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "NUMBER '37'", "NUMBER '38'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "NUMBER '37'", "NUMBER '38.0'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "NUMBER '37.0'", "NUMBER '38'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "NUMBER '-37.000'", "NUMBER '37.000'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "NUMBER '-123456789123456789'", "NUMBER '-123456789123456789'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "NUMBER '-123456789123456789'", "NUMBER '123456789123456789'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "NUMBER '37'", "NUMBER '37.0000000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "NUMBER '37'", "NUMBER '00000000037.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "NUMBER '037.0'", "NUMBER '00000000037.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "NUMBER '-0.000'", "NUMBER '0.000000000000000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "NUMBER '37'", "NUMBER '00000000038.0000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "NUMBER '-37'", "NUMBER '00000000037.0000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "NUMBER '37.0000000000000000'", "NUMBER '00000000037.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "NUMBER '37.0000000000000000'", "NUMBER '00000000036.0000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "NUMBER '37.0000000000000000000000000'", "NUMBER '37'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "NUMBER '00000000037.0000000000000000000000'", "NUMBER '37'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "NUMBER '00000000037.0000000000000000000000'", "NUMBER '037.0'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "NUMBER '-0.000000000000000000000000000000000'", "NUMBER '0.000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "NUMBER '00000000038.0000000000000000000000'", "NUMBER '37'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "NUMBER '-00000000037.0000000000000000000000'", "NUMBER '37'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "NUMBER '00000000037.0000000000000000000000'", "NUMBER '37.0000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "NUMBER '00000000036.0000000000000000000000'", "NUMBER '37.0000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "NUMBER '37.0000000000000000000000000'", "NUMBER '37.0000000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "NUMBER '00000000037.0000000000000000000000'", "NUMBER '0037.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "NUMBER '0.0000000000000000000000000000000'", "NUMBER '-0.000000000000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "NUMBER '00000000038.0000000000000000000000'", "NUMBER '000000000037.00000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "NUMBER '-00000000038.0000000000000000000000'", "NUMBER '00000000038.0000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "NUMBER '-Infinity'", "NUMBER '0'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "NUMBER '-Infinity'", "NUMBER '1'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "NUMBER '-Infinity'", "NUMBER '-1'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "NUMBER '-Infinity'", "NUMBER 'Infinity'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "NUMBER '-Infinity'", "NUMBER '-Infinity'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "NUMBER '-Infinity'", "NUMBER 'NaN'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "NUMBER '+Infinity'", "NUMBER '0'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "NUMBER '+Infinity'", "NUMBER '1'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "NUMBER '+Infinity'", "NUMBER '-1'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "NUMBER '+Infinity'", "NUMBER 'Infinity'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "NUMBER '+Infinity'", "NUMBER '-Infinity'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "NUMBER '+Infinity'", "NUMBER 'NaN'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "NUMBER 'NaN'", "NUMBER '0'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "NUMBER 'NaN'", "NUMBER '1'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "NUMBER 'NaN'", "NUMBER '-1'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "NUMBER 'NaN'", "NUMBER 'Infinity'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "NUMBER 'NaN'", "NUMBER '-Infinity'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "NUMBER 'NaN'", "NUMBER 'NaN'"))
                .isEqualTo(false);
    }

    @Test
    void testNotEqual()
    {
        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER '37'")
                .binding("b", "NUMBER '37'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER '37'")
                .binding("b", "NUMBER '0037'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER '37'")
                .binding("b", "NUMBER '37.0'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER '37.0'")
                .binding("b", "NUMBER '37'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER '0'")
                .binding("b", "NUMBER '-0.00'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER '37'")
                .binding("b", "NUMBER '-37'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER '37'")
                .binding("b", "NUMBER '38'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER '37'")
                .binding("b", "NUMBER '38.0'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER '37.0'")
                .binding("b", "NUMBER '38'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER '37'")
                .binding("b", "NUMBER '-37'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER '-999999999999999999'")
                .binding("b", "NUMBER '-999999999999999999'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER '-999999999999999999'")
                .binding("b", "NUMBER '999999999999999998'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER '37'")
                .binding("b", "NUMBER '37.0000000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER '37'")
                .binding("b", "NUMBER '00000000037.0000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER '037.0'")
                .binding("b", "NUMBER '00000000037.0000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER '0'")
                .binding("b", "NUMBER '-0.0000000000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER '37'")
                .binding("b", "NUMBER '00000000038.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER '37'")
                .binding("b", "NUMBER '-000000000037.00000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER '38.0000000000000000'")
                .binding("b", "NUMBER '00000000038.0000000000000000000001'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER '-987654321987654321'")
                .binding("b", "NUMBER '-987654321987654321.00000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER '37.0000000000000000000000000'")
                .binding("b", "NUMBER '37'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER '00000000037.0000000000000000000000'")
                .binding("b", "NUMBER '37'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER '00000000037.0000000000000000000000'")
                .binding("b", "NUMBER '037.0'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER '0.0000000000000000000000000000'")
                .binding("b", "NUMBER '-0'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER '00000000038.0000000000000000000000'")
                .binding("b", "NUMBER '37'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER '00000000000037.00000000000000000000'")
                .binding("b", "NUMBER '-37'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER '00000000037.0000000000000000000000'")
                .binding("b", "NUMBER '37.0000000000000001'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER '-00000000000037.00000000000000000000'")
                .binding("b", "NUMBER '-37.0000000000000001'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER '37.0000000000000000000000000'")
                .binding("b", "NUMBER '37.0000000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER '00000000037.0000000000000000000000'")
                .binding("b", "NUMBER '0037.0000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER '0.00000000000000000000000000000000000'")
                .binding("b", "NUMBER '-0.000000000000000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER '00000000038.0000000000000000000000'")
                .binding("b", "NUMBER '000000000037.00000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER '00000000000037.00000000000000000000'")
                .binding("b", "NUMBER '-00000000000037.00000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER '-Infinity'")
                .binding("b", "NUMBER '0'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER '-Infinity'")
                .binding("b", "NUMBER '1'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER '-Infinity'")
                .binding("b", "NUMBER '-1'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER '-Infinity'")
                .binding("b", "NUMBER 'Infinity'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER '-Infinity'")
                .binding("b", "NUMBER '-Infinity'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER '-Infinity'")
                .binding("b", "NUMBER 'NaN'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER '+Infinity'")
                .binding("b", "NUMBER '0'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER '+Infinity'")
                .binding("b", "NUMBER '1'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER '+Infinity'")
                .binding("b", "NUMBER '-1'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER '+Infinity'")
                .binding("b", "NUMBER 'Infinity'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER '+Infinity'")
                .binding("b", "NUMBER '-Infinity'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER '+Infinity'")
                .binding("b", "NUMBER 'NaN'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER 'NaN'")
                .binding("b", "NUMBER '0'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER 'NaN'")
                .binding("b", "NUMBER '1'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER 'NaN'")
                .binding("b", "NUMBER '-1'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER 'NaN'")
                .binding("b", "NUMBER 'Infinity'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER 'NaN'")
                .binding("b", "NUMBER '-Infinity'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "NUMBER 'NaN'")
                .binding("b", "NUMBER 'NaN'"))
                .isEqualTo(true);
    }

    @Test
    void testLessThan()
    {
        assertThat(assertions.operator(LESS_THAN, "NUMBER '37'", "NUMBER '37'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "NUMBER '37'", "NUMBER '0037'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "NUMBER '37'", "NUMBER '37.0'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "NUMBER '37.0'", "NUMBER '37'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "NUMBER '0037.0'", "NUMBER '00036.0'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "NUMBER '37'", "NUMBER '38'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "NUMBER '37'", "NUMBER '0038.0'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "NUMBER '0037.0'", "NUMBER '38'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "NUMBER '-100'", "NUMBER '20'"))
                .isEqualTo(true);

        // test possible overflow on rescale
        assertThat(assertions.operator(LESS_THAN, "NUMBER '1'", "NUMBER '10000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "NUMBER '1.0000000000000000'", "NUMBER '10000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "NUMBER '-123456789123456789'", "NUMBER '-123456789123456789'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "NUMBER '-123456789123456789'", "NUMBER '123456789123456789'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "NUMBER '37'", "NUMBER '37.0000000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "NUMBER '37'", "NUMBER '00000000037.0000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "NUMBER '037.0'", "NUMBER '00000000037.0000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "NUMBER '037.0'", "NUMBER '00000000036.0000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "NUMBER '37'", "NUMBER '37.00000000000000000000000001'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "NUMBER '37'", "NUMBER '00000000038.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "NUMBER '037.0'", "NUMBER '00000000038.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "NUMBER '-100'", "NUMBER '20.00000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "NUMBER '38.0000000000000000'", "NUMBER '00000000038.0000000000000000000001'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "NUMBER '-987654321987654321'", "NUMBER '-987654321987654321.00000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "NUMBER '37.0000000000000000000000000'", "NUMBER '37'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "NUMBER '00000000037.0000000000000000000000'", "NUMBER '37'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "NUMBER '00000000037.0000000000000000000000'", "NUMBER '037.0'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "NUMBER '00000000037.0000000000000000000000'", "NUMBER '036.0'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "NUMBER '37.0000000000000000000000000'", "NUMBER '38'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "NUMBER '00000000037.0000000000000000000001'", "NUMBER '38'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "NUMBER '00000000037.0000000000000000000000'", "NUMBER '038.0'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "NUMBER '-00000000000100.000000000000'", "NUMBER '20'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "NUMBER '00000000037.0000000000000000000000'", "NUMBER '37.0000000000000001'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "NUMBER '-00000000000037.00000000000000000000'", "NUMBER '-37.0000000000000001'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "NUMBER '37.0000000000000000000000000'", "NUMBER '37.0000000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "NUMBER '37.0000000000000000000000000'", "NUMBER '00000037.0000000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "NUMBER '37.0000000000000000000000000'", "NUMBER '00000036.0000000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "NUMBER '00000000037.0000000000000000000000'", "NUMBER '38.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "NUMBER '00000000037.0000000000000000000000'", "NUMBER '000000000037.00000000000000000000001'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "NUMBER '-00000000000100.000000000000'", "NUMBER '0000000020.0000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "NUMBER '-Infinity'", "NUMBER '0'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "NUMBER '-Infinity'", "NUMBER '1'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "NUMBER '-Infinity'", "NUMBER '-1'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "NUMBER '-Infinity'", "NUMBER 'Infinity'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "NUMBER '-Infinity'", "NUMBER '-Infinity'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "NUMBER '-Infinity'", "NUMBER 'NaN'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "NUMBER '+Infinity'", "NUMBER '0'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "NUMBER '+Infinity'", "NUMBER '1'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "NUMBER '+Infinity'", "NUMBER '-1'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "NUMBER '+Infinity'", "NUMBER 'Infinity'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "NUMBER '+Infinity'", "NUMBER '-Infinity'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "NUMBER '+Infinity'", "NUMBER 'NaN'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "NUMBER 'NaN'", "NUMBER '0'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "NUMBER 'NaN'", "NUMBER '1'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "NUMBER 'NaN'", "NUMBER '-1'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "NUMBER 'NaN'", "NUMBER 'Infinity'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "NUMBER 'NaN'", "NUMBER '-Infinity'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "NUMBER 'NaN'", "NUMBER 'NaN'"))
                .isEqualTo(false);
    }

    @Test
    void testGreaterThan()
    {
        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '37'")
                .binding("b", "NUMBER '37'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '37'")
                .binding("b", "NUMBER '0037'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '37'")
                .binding("b", "NUMBER '37.0'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '37.0'")
                .binding("b", "NUMBER '37'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '0037.0'")
                .binding("b", "NUMBER '00038.0'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '37'")
                .binding("b", "NUMBER '36'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '37'")
                .binding("b", "NUMBER '0036.0'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '0037.0'")
                .binding("b", "NUMBER '36'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '100'")
                .binding("b", "NUMBER '-20'"))
                .isEqualTo(true);

        // test possible overflow on rescale
        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '10000000000000000'")
                .binding("b", "NUMBER '1'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '10000000000000000'")
                .binding("b", "NUMBER '1.0000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '-123456789123456788'")
                .binding("b", "NUMBER '-123456789123456789'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '-123456789123456789'")
                .binding("b", "NUMBER '123456789123456789'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '37'")
                .binding("b", "NUMBER '37.0000000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '37'")
                .binding("b", "NUMBER '00000000037.0000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '037.0'")
                .binding("b", "NUMBER '00000000037.0000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '037.0'")
                .binding("b", "NUMBER '00000000038.0000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '37'")
                .binding("b", "NUMBER '36.00000000000000000000000001'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '37'")
                .binding("b", "NUMBER '00000000036.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '037.0'")
                .binding("b", "NUMBER '00000000036.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '100'")
                .binding("b", "NUMBER '-0000000020.00000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '38.0000000000000000'")
                .binding("b", "NUMBER '00000000038.0000000000000000000001'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '-987654321987654320'")
                .binding("b", "NUMBER '-987654321987654321.00000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '37.0000000000000000000000000'")
                .binding("b", "NUMBER '37'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '00000000037.0000000000000000000000'")
                .binding("b", "NUMBER '37'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '00000000037.0000000000000000000000'")
                .binding("b", "NUMBER '037.0'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '00000000037.0000000000000000000000'")
                .binding("b", "NUMBER '038.0'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '37.0000000000000000000000000'")
                .binding("b", "NUMBER '36'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '00000000037.0000000000000000000001'")
                .binding("b", "NUMBER '36'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '00000000037.0000000000000000000000'")
                .binding("b", "NUMBER '036.0'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '0000000000100.000000000000'")
                .binding("b", "NUMBER '20'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '00000000037.0000000000000000000002'")
                .binding("b", "NUMBER '37.0000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '-00000000000037.00000000000000000000'")
                .binding("b", "NUMBER '-37.0000000000000001'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '37.0000000000000000000000000'")
                .binding("b", "NUMBER '37.0000000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '37.0000000000000000000000000'")
                .binding("b", "NUMBER '00000037.0000000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '37.0000000000000000000000000'")
                .binding("b", "NUMBER '00000038.0000000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '00000000037.0000000000000000000000'")
                .binding("b", "NUMBER '36.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '00000000037.0000000000000000000000'")
                .binding("b", "NUMBER '000000000036.9999999999999999999999'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '000000000000100.0000000000000000000000'")
                .binding("b", "NUMBER '-0000000020.00000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '-Infinity'")
                .binding("b", "NUMBER '0'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '-Infinity'")
                .binding("b", "NUMBER '1'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '-Infinity'")
                .binding("b", "NUMBER '-1'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '-Infinity'")
                .binding("b", "NUMBER 'Infinity'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '-Infinity'")
                .binding("b", "NUMBER '-Infinity'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '-Infinity'")
                .binding("b", "NUMBER 'NaN'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '+Infinity'")
                .binding("b", "NUMBER '0'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '+Infinity'")
                .binding("b", "NUMBER '1'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '+Infinity'")
                .binding("b", "NUMBER '-1'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '+Infinity'")
                .binding("b", "NUMBER 'Infinity'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '+Infinity'")
                .binding("b", "NUMBER '-Infinity'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER '+Infinity'")
                .binding("b", "NUMBER 'NaN'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER 'NaN'")
                .binding("b", "NUMBER '0'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER 'NaN'")
                .binding("b", "NUMBER '1'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER 'NaN'")
                .binding("b", "NUMBER '-1'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER 'NaN'")
                .binding("b", "NUMBER 'Infinity'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER 'NaN'")
                .binding("b", "NUMBER '-Infinity'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "NUMBER 'NaN'")
                .binding("b", "NUMBER 'NaN'"))
                .isEqualTo(false);
    }

    @Test
    void testLessThanOrEqual()
    {
        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '37'", "NUMBER '36'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '37'", "NUMBER '000036.99999'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '37'", "NUMBER '37'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '37'", "NUMBER '0037'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '37'", "NUMBER '37.0'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '37.0'", "NUMBER '37'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '37'", "NUMBER '38'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '37'", "NUMBER '0038.0'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '0037.0'", "NUMBER '38'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '-100'", "NUMBER '20'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '-123456789123456789'", "NUMBER '-123456789123456789'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '-123456789123456789'", "NUMBER '123456789123456789'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '037.0'", "NUMBER '00000000036.0000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '037.0'", "NUMBER '00000000036.9999999999999999999'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '37'", "NUMBER '37.0000000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '37'", "NUMBER '00000000037.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '037.0'", "NUMBER '00000000037.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '37'", "NUMBER '37.00000000000000000000000001'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '37'", "NUMBER '00000000038.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '037.0'", "NUMBER '00000000038.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '-100'", "NUMBER '20.00000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '38.0000000000000000'", "NUMBER '00000000038.0000000000000000000001'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '-987654321987654321'", "NUMBER '-987654321987654321.00000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '00000000037.0000000000000000000000'", "NUMBER '036.0'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '00000000037.0000000000000000000000'", "NUMBER '000036.99999999'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '37.0000000000000000000000000'", "NUMBER '37'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '00000000037.0000000000000000000000'", "NUMBER '37'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '00000000037.0000000000000000000000'", "NUMBER '037.0'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '37.0000000000000000000000000'", "NUMBER '38'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '00000000037.0000000000000000000001'", "NUMBER '38'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '00000000037.0000000000000000000000'", "NUMBER '038.0'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '-00000000000100.000000000000'", "NUMBER '20'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '00000000037.0000000000000000000000'", "NUMBER '37.0000000000000001'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '-00000000000037.00000000000000000000'", "NUMBER '-37.0000000000000001'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '37.0000000000000000000000000'", "NUMBER '00000036.0000000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '37.0000000000000000000000000'", "NUMBER '37.0000000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '37.0000000000000000000000000'", "NUMBER '00000037.0000000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '00000000037.0000000000000000000000'", "NUMBER '38.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '00000000037.0000000000000000000000'", "NUMBER '000000000037.00000000000000000000001'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '-00000000000100.000000000000'", "NUMBER '0000000020.0000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '-Infinity'", "NUMBER '0'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '-Infinity'", "NUMBER '1'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '-Infinity'", "NUMBER '-1'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '-Infinity'", "NUMBER 'Infinity'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '-Infinity'", "NUMBER '-Infinity'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '-Infinity'", "NUMBER 'NaN'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '+Infinity'", "NUMBER '0'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '+Infinity'", "NUMBER '1'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '+Infinity'", "NUMBER '-1'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '+Infinity'", "NUMBER 'Infinity'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '+Infinity'", "NUMBER '-Infinity'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER '+Infinity'", "NUMBER 'NaN'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER 'NaN'", "NUMBER '0'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER 'NaN'", "NUMBER '1'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER 'NaN'", "NUMBER '-1'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER 'NaN'", "NUMBER 'Infinity'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER 'NaN'", "NUMBER '-Infinity'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "NUMBER 'NaN'", "NUMBER 'NaN'"))
                .isEqualTo(false);
    }

    @Test
    void testGreaterThanOrEqual()
    {
        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '37'")
                .binding("b", "NUMBER '38'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '37'")
                .binding("b", "NUMBER '000038.00001'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '37'")
                .binding("b", "NUMBER '37'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '37'")
                .binding("b", "NUMBER '0037'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '37'")
                .binding("b", "NUMBER '37.0'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '37.0'")
                .binding("b", "NUMBER '37'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '37'")
                .binding("b", "NUMBER '36'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '37'")
                .binding("b", "NUMBER '0036.0'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '0037.0'")
                .binding("b", "NUMBER '36'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '100'")
                .binding("b", "NUMBER '-20'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '-123456789123456789'")
                .binding("b", "NUMBER '-123456789123456789'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '-123456789123456789'")
                .binding("b", "NUMBER '123456789123456789'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '037.0'")
                .binding("b", "NUMBER '00000000038.0000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '037.0'")
                .binding("b", "NUMBER '00000000037.0000000000000000001'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '37'")
                .binding("b", "NUMBER '37.0000000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '37'")
                .binding("b", "NUMBER '00000000037.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '037.0'")
                .binding("b", "NUMBER '00000000037.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '37'")
                .binding("b", "NUMBER '36.9999999999999999999'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '37'")
                .binding("b", "NUMBER '00000000036.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '037.0'")
                .binding("b", "NUMBER '00000000036.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '100'")
                .binding("b", "NUMBER '-0000000020.00000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '38.0000000000000000'")
                .binding("b", "NUMBER '00000000038.0000000000000000000001'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '-987654321987654321'")
                .binding("b", "NUMBER '-987654321987654321.00000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '00000000037.0000000000000000000000'")
                .binding("b", "NUMBER '038.0'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '00000000037.0000000000000000000000'")
                .binding("b", "NUMBER '000037.00000001'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '37.0000000000000000000000000'")
                .binding("b", "NUMBER '37'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '00000000037.0000000000000000000000'")
                .binding("b", "NUMBER '37'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '00000000037.0000000000000000000000'")
                .binding("b", "NUMBER '037.0'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '37.0000000000000000000000000'")
                .binding("b", "NUMBER '36'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '00000000037.0000000000000000000001'")
                .binding("b", "NUMBER '36'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '00000000037.0000000000000000000000'")
                .binding("b", "NUMBER '036.0'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '0000000000100.000000000000'")
                .binding("b", "NUMBER '20'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '00000000037.0000000000000000000000'")
                .binding("b", "NUMBER '37.0000000000000001'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '-00000000000037.00000000000000000000'")
                .binding("b", "NUMBER '-37.0000000000000001'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '37.0000000000000000000000000'")
                .binding("b", "NUMBER '00000038.0000000000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '37.0000000000000000000000000'")
                .binding("b", "NUMBER '37.0000000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '37.0000000000000000000000000'")
                .binding("b", "NUMBER '00000037.0000000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '00000000037.0000000000000000000000'")
                .binding("b", "NUMBER '36.0000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '00000000037.0000000000000000000000'")
                .binding("b", "NUMBER '000000000036.9999999999999999'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '000000000000100.0000000000000000000000'")
                .binding("b", "NUMBER '-0000000020.00000000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '-Infinity'")
                .binding("b", "NUMBER '0'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '-Infinity'")
                .binding("b", "NUMBER '1'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '-Infinity'")
                .binding("b", "NUMBER '-1'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '-Infinity'")
                .binding("b", "NUMBER 'Infinity'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '-Infinity'")
                .binding("b", "NUMBER '-Infinity'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '-Infinity'")
                .binding("b", "NUMBER 'NaN'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '+Infinity'")
                .binding("b", "NUMBER '0'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '+Infinity'")
                .binding("b", "NUMBER '1'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '+Infinity'")
                .binding("b", "NUMBER '-1'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '+Infinity'")
                .binding("b", "NUMBER 'Infinity'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '+Infinity'")
                .binding("b", "NUMBER '-Infinity'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER '+Infinity'")
                .binding("b", "NUMBER 'NaN'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER 'NaN'")
                .binding("b", "NUMBER '0'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER 'NaN'")
                .binding("b", "NUMBER '1'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER 'NaN'")
                .binding("b", "NUMBER '-1'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER 'NaN'")
                .binding("b", "NUMBER 'Infinity'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER 'NaN'")
                .binding("b", "NUMBER '-Infinity'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "NUMBER 'NaN'")
                .binding("b", "NUMBER 'NaN'"))
                .isEqualTo(false);
    }

    @Test
    void testBetween()
    {
        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "NUMBER '1'")
                .binding("low", "NUMBER '-5'")
                .binding("high", "NUMBER '5'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "NUMBER '-6'")
                .binding("low", "NUMBER '-5'")
                .binding("high", "NUMBER '5'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "NUMBER '6'")
                .binding("low", "NUMBER '-5'")
                .binding("high", "NUMBER '5'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "NUMBER '333333333333333333'")
                .binding("low", "NUMBER '-111111111111111111'")
                .binding("high", "NUMBER '999999999999999999'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "NUMBER '1'")
                .binding("low", "NUMBER '-5'")
                .binding("high", "NUMBER '5.00000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "NUMBER '-6'")
                .binding("low", "NUMBER '-5'")
                .binding("high", "NUMBER '5.00000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "NUMBER '6'")
                .binding("low", "NUMBER '-5'")
                .binding("high", "NUMBER '5.00000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "NUMBER '333333333333333333'")
                .binding("low", "NUMBER '-111111111111111111'")
                .binding("high", "NUMBER '9999999999999999999'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "NUMBER '1'")
                .binding("low", "NUMBER '-5.00000000000000000000'")
                .binding("high", "NUMBER '5.00000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "NUMBER '-6'")
                .binding("low", "NUMBER '-5.00000000000000000000'")
                .binding("high", "NUMBER '5.00000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "NUMBER '6'")
                .binding("low", "NUMBER '-5.00000000000000000000'")
                .binding("high", "NUMBER '5.00000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "NUMBER '333333333333333333'")
                .binding("low", "NUMBER '-1111111111111111111'")
                .binding("high", "NUMBER '9999999999999999999'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "NUMBER '1.00000000000000000000'")
                .binding("low", "NUMBER '-5'")
                .binding("high", "NUMBER '5'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "NUMBER '-6.00000000000000000000'")
                .binding("low", "NUMBER '-5'")
                .binding("high", "NUMBER '5'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "NUMBER '6.00000000000000000000'")
                .binding("low", "NUMBER '-5'")
                .binding("high", "NUMBER '5'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "NUMBER '333333333333333333.3'")
                .binding("low", "NUMBER '-111111111111111111'")
                .binding("high", "NUMBER '999999999999999999'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "NUMBER '1.00000000000000000000'")
                .binding("low", "NUMBER '-5'")
                .binding("high", "NUMBER '5.00000000000000000000' "))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "NUMBER '-6.00000000000000000000'")
                .binding("low", "NUMBER '-5'")
                .binding("high", "NUMBER '5.00000000000000000000' "))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "NUMBER '6.00000000000000000000'")
                .binding("low", "NUMBER '-5'")
                .binding("high", "NUMBER '5.00000000000000000000' "))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "NUMBER '333333333333333333.3'")
                .binding("low", "NUMBER '-111111111111111111'")
                .binding("high", "NUMBER '9999999999999999999'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "NUMBER '1.00000000000000000000'")
                .binding("low", "NUMBER '-5.00000000000000000000' ")
                .binding("high", "NUMBER '5'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "NUMBER '-6.00000000000000000000'")
                .binding("low", "NUMBER '-5.00000000000000000000' ")
                .binding("high", "NUMBER '5'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "NUMBER '6.00000000000000000000'")
                .binding("low", "NUMBER '-5.00000000000000000000' ")
                .binding("high", "NUMBER '5'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "NUMBER '333333333333333333.3'")
                .binding("low", "NUMBER '-111111111111111111.1'")
                .binding("high", "NUMBER '999999999999999999'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "NUMBER '1.00000000000000000000'")
                .binding("low", "NUMBER '-5.00000000000000000000' ")
                .binding("high", "NUMBER '5.00000000000000000000'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "NUMBER '-6.00000000000000000000'")
                .binding("low", "NUMBER '-5.00000000000000000000' ")
                .binding("high", "NUMBER '5.00000000000000000000'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "NUMBER '6.00000000000000000000'")
                .binding("low", "NUMBER '-5.00000000000000000000' ")
                .binding("high", "NUMBER '5.00000000000000000000'"))
                .isEqualTo(false);
    }

    @Test
    void testAddNumberBigint()
    {
        // number + bigint
        assertThat(assertions.operator(ADD, "NUMBER '123456789012345678'", "123456789012345678"))
                .isEqualTo(number("246913578024691356"));

        assertThat(assertions.operator(ADD, "NUMBER '.123456789012345678'", "123456789012345678"))
                .isEqualTo(number("123456789012345678.123456789012345678"));

        assertThat(assertions.operator(ADD, "NUMBER '-1234567890123456789'", "1234567890123456789"))
                .isEqualTo(number("0"));

        // bigint + number
        assertThat(assertions.operator(ADD, "123456789012345678", "NUMBER '123456789012345678'"))
                .isEqualTo(number("246913578024691356"));

        assertThat(assertions.operator(ADD, "123456789012345678", "NUMBER '.123456789012345678'"))
                .isEqualTo(number("123456789012345678.123456789012345678"));

        assertThat(assertions.operator(ADD, "1234567890123456789", "NUMBER '-1234567890123456789'"))
                .isEqualTo(number("0"));
    }

    @Test
    void testSubtractNumberBigint()
    {
        // number - bigint
        assertThat(assertions.operator(SUBTRACT, "NUMBER '1234567890123456789'", "1234567890123456789"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(SUBTRACT, "NUMBER '.1234567890123456789'", "1234567890123456789"))
                .isEqualTo(number("-1234567890123456788.8765432109876543211"));

        assertThat(assertions.operator(SUBTRACT, "NUMBER '-1234567890123456789'", "1234567890123456789"))
                .isEqualTo(number("-2469135780246913578"));

        // bigint - number
        assertThat(assertions.operator(SUBTRACT, "1234567890123456789", "NUMBER '1234567890123456789'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(SUBTRACT, "1234567890123456789", "NUMBER '.1234567890123456789'"))
                .isEqualTo(number("1234567890123456788.8765432109876543211"));

        assertThat(assertions.operator(SUBTRACT, "-1234567890123456789", "NUMBER '1234567890123456789'"))
                .isEqualTo(number("-2469135780246913578"));
    }

    @Test
    void testMultiplyNumberBigint()
    {
        // number bigint
        assertThat(assertions.operator(MULTIPLY, "NUMBER '12345678901234567'", "12345678901234567"))
                .isEqualTo(number("152415787532388345526596755677489"));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '-12345678901234567'", "12345678901234567"))
                .isEqualTo(number("-152415787532388345526596755677489"));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '-12345678901234567'", "-12345678901234567"))
                .isEqualTo(number("152415787532388345526596755677489"));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '.1234567890'", "BIGINT '3'"))
                .isEqualTo(number("0.370370367"));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '.1234567890'", "BIGINT '0'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MULTIPLY, "NUMBER '-.1234567890'", "BIGINT '0'"))
                .isEqualTo(number("0"));

        // bigint number
        assertThat(assertions.operator(MULTIPLY, "12345678901234567", "NUMBER '12345678901234567'"))
                .isEqualTo(number("152415787532388345526596755677489"));

        assertThat(assertions.operator(MULTIPLY, "12345678901234567", "NUMBER '-12345678901234567'"))
                .isEqualTo(number("-152415787532388345526596755677489"));

        assertThat(assertions.operator(MULTIPLY, "-12345678901234567", "NUMBER '-12345678901234567'"))
                .isEqualTo(number("152415787532388345526596755677489"));

        assertThat(assertions.operator(MULTIPLY, "BIGINT '3'", "NUMBER '.1234567890'"))
                .isEqualTo(number("0.370370367"));

        assertThat(assertions.operator(MULTIPLY, "BIGINT '3'", "NUMBER '.0000000000'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MULTIPLY, "BIGINT '-3'", "NUMBER '.0000000000'"))
                .isEqualTo(number("0"));
    }

    @Test
    void testDivideNumberBigint()
    {
        // bigint / number
        assertThat(assertions.operator(DIVIDE, "BIGINT '9'", "NUMBER '3.0'"))
                .isEqualTo(number("3"));

        assertThat(assertions.operator(DIVIDE, "BIGINT '-9'", "NUMBER '3.0'"))
                .isEqualTo(number("-3"));

        assertThat(assertions.operator(DIVIDE, "BIGINT '9'", "NUMBER '-3.0'"))
                .isEqualTo(number("-3"));

        assertThat(assertions.operator(DIVIDE, "BIGINT '-9'", "NUMBER '-3.0'"))
                .isEqualTo(number("3"));

        assertThat(assertions.operator(DIVIDE, "BIGINT '9'", "NUMBER '000000000000000003.0'"))
                .isEqualTo(number("3"));

        assertThat(assertions.operator(DIVIDE, "BIGINT '18'", "NUMBER '0.01'"))
                .isEqualTo(number("1.8E+3"));

        assertThat(assertions.operator(DIVIDE, "BIGINT '9'", "NUMBER '00000000000000000.1'"))
                .isEqualTo(number("9E+1"));

        assertThat(assertions.operator(DIVIDE, "BIGINT '9'", "NUMBER '300.0'"))
                .isEqualTo(number("0.03"));

        assertThat(assertions.operator(DIVIDE, "BIGINT '-9'", "NUMBER '300.0'"))
                .isEqualTo(number("-0.03"));

        assertThat(assertions.operator(DIVIDE, "BIGINT '9'", "NUMBER '-300.0'"))
                .isEqualTo(number("-0.03"));

        assertThat(assertions.operator(DIVIDE, "BIGINT '-9'", "NUMBER '-300.0'"))
                .isEqualTo(number("0.03"));

        // number / bigint
        assertThat(assertions.operator(DIVIDE, "NUMBER '9.0'", "BIGINT '3'"))
                .isEqualTo(number("3"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '-9.0'", "BIGINT '3'"))
                .isEqualTo(number("-3"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '9.0'", "BIGINT '-3'"))
                .isEqualTo(number("-3"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '-9.0'", "BIGINT '-3'"))
                .isEqualTo(number("3"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '0.018'", "BIGINT '9'"))
                .isEqualTo(number("0.002"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '-0.018'", "BIGINT '9'"))
                .isEqualTo(number("-0.002"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '0.018'", "BIGINT '-9'"))
                .isEqualTo(number("-0.002"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '-0.018'", "BIGINT '-9'"))
                .isEqualTo(number("0.002"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '.999'", "BIGINT '9'"))
                .isEqualTo(number("0.111"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '9.0'", "BIGINT '300'"))
                .isEqualTo(number("0.03"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '-9.0'", "BIGINT '300'"))
                .isEqualTo(number("-0.03"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '9.0'", "BIGINT '-300'"))
                .isEqualTo(number("-0.03"));

        assertThat(assertions.operator(DIVIDE, "NUMBER '-9.0'", "BIGINT '-300'"))
                .isEqualTo(number("0.03"));
    }

    @Test
    void testModulusNumberBigint()
    {
        // bigint % number
        assertThat(assertions.operator(MODULUS, "BIGINT '13'", "NUMBER '9.0'"))
                .isEqualTo(number("4"));

        assertThat(assertions.operator(MODULUS, "BIGINT '18'", "NUMBER '0.01'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MODULUS, "BIGINT '9'", "NUMBER '.1'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MODULUS, "BIGINT '-9'", "NUMBER '.1'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MODULUS, "BIGINT '9'", "NUMBER '-.1'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MODULUS, "BIGINT '-9'", "NUMBER '-.1'"))
                .isEqualTo(number("0"));

        // number % bigint
        assertThat(assertions.operator(MODULUS, "NUMBER '13.0'", "BIGINT '9'"))
                .isEqualTo(number("4"));

        assertThat(assertions.operator(MODULUS, "NUMBER '-13.0'", "BIGINT '9'"))
                .isEqualTo(number("-4"));

        assertThat(assertions.operator(MODULUS, "NUMBER '13.0'", "BIGINT '-9'"))
                .isEqualTo(number("4"));

        assertThat(assertions.operator(MODULUS, "NUMBER '-13.0'", "BIGINT '-9'"))
                .isEqualTo(number("-4"));

        assertThat(assertions.operator(MODULUS, "NUMBER '18.00'", "BIGINT '3'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MODULUS, "NUMBER '9.0'", "BIGINT '3'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MODULUS, "NUMBER '-9.0'", "BIGINT '3'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MODULUS, "NUMBER '9.0'", "BIGINT '-3'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MODULUS, "NUMBER '-9.0'", "BIGINT '-3'"))
                .isEqualTo(number("0"));

        assertThat(assertions.operator(MODULUS, "NUMBER '5.128'", "BIGINT '2'"))
                .isEqualTo(number("1.128"));
    }

    @Test
    void testMultiplyNumberDecimal()
    {
        // TODO All These fail because currently there is no coercion between DECIMAL and NUMBER

        // number decimal
        assertTrinoExceptionThrownBy(assertions.operator(MULTIPLY, "NUMBER '12345678901234567'", "DECIMAL '12345678901234567'")::evaluate)
                .hasErrorCode(FUNCTION_NOT_FOUND)
                .hasMessageContaining("Unexpected parameters (number, decimal(17,0)) for function $operator$multiply");

        assertTrinoExceptionThrownBy(assertions.operator(MULTIPLY, "NUMBER '-12345678901234567'", "DECIMAL '12345678901234567'")::evaluate)
                .hasErrorCode(FUNCTION_NOT_FOUND)
                .hasMessageContaining("Unexpected parameters (number, decimal(17,0)) for function $operator$multiply");

        assertTrinoExceptionThrownBy(assertions.operator(MULTIPLY, "NUMBER '-12345678901234567'", "DECIMAL '-12345678901234567'")::evaluate)
                .hasErrorCode(FUNCTION_NOT_FOUND)
                .hasMessageContaining("Unexpected parameters (number, decimal(17,0)) for function $operator$multiply");

        assertTrinoExceptionThrownBy(assertions.operator(MULTIPLY, "NUMBER '.1234567890'", "DECIMAL '3'")::evaluate)
                .hasErrorCode(FUNCTION_NOT_FOUND)
                .hasMessageContaining("Unexpected parameters (number, decimal(1,0)) for function $operator$multiply");

        assertTrinoExceptionThrownBy(assertions.operator(MULTIPLY, "NUMBER '.1234567890'", "DECIMAL '0'")::evaluate)
                .hasErrorCode(FUNCTION_NOT_FOUND)
                .hasMessageContaining("Unexpected parameters (number, decimal(1,0)) for function $operator$multiply");

        assertTrinoExceptionThrownBy(assertions.operator(MULTIPLY, "NUMBER '-.1234567890'", "DECIMAL '0'")::evaluate)
                .hasErrorCode(FUNCTION_NOT_FOUND)
                .hasMessageContaining("Unexpected parameters (number, decimal(1,0)) for function $operator$multiply");

        // decimal number
        assertTrinoExceptionThrownBy(assertions.operator(MULTIPLY, "DECIMAL '12345678901234567'", "NUMBER '12345678901234567'")::evaluate)
                .hasErrorCode(FUNCTION_NOT_FOUND)
                .hasMessageContaining("Unexpected parameters (decimal(17,0), number) for function $operator$multiply");

        assertTrinoExceptionThrownBy(assertions.operator(MULTIPLY, "DECIMAL '12345678901234567'", "NUMBER '-12345678901234567'")::evaluate)
                .hasErrorCode(FUNCTION_NOT_FOUND)
                .hasMessageContaining("Unexpected parameters (decimal(17,0), number) for function $operator$multiply");

        assertTrinoExceptionThrownBy(assertions.operator(MULTIPLY, "DECIMAL '-12345678901234567'", "NUMBER '-12345678901234567'")::evaluate)
                .hasErrorCode(FUNCTION_NOT_FOUND)
                .hasMessageContaining("Unexpected parameters (decimal(17,0), number) for function $operator$multiply");

        assertTrinoExceptionThrownBy(assertions.operator(MULTIPLY, "DECIMAL '3'", "NUMBER '.1234567890'")::evaluate)
                .hasErrorCode(FUNCTION_NOT_FOUND)
                .hasMessageContaining("Unexpected parameters (decimal(1,0), number) for function $operator$multiply");

        assertTrinoExceptionThrownBy(assertions.operator(MULTIPLY, "DECIMAL '3'", "NUMBER '.0000000000'")::evaluate)
                .hasErrorCode(FUNCTION_NOT_FOUND)
                .hasMessageContaining("Unexpected parameters (decimal(1,0), number) for function $operator$multiply");

        assertTrinoExceptionThrownBy(assertions.operator(MULTIPLY, "DECIMAL '-3'", "NUMBER '.0000000000'")::evaluate)
                .hasErrorCode(FUNCTION_NOT_FOUND)
                .hasMessageContaining("Unexpected parameters (decimal(1,0), number) for function $operator$multiply");
    }

    @Test
    void testIdentical()
    {
        assertThat(assertions.operator(IDENTICAL, "CAST(NULL AS number)", "CAST(NULL AS number)"))
                .isEqualTo(true);

        assertThat(assertions.operator(IDENTICAL, "NUMBER '37'", "NUMBER '37'"))
                .isEqualTo(true);

        assertThat(assertions.operator(IDENTICAL, "37", "38"))
                .isEqualTo(false);

        assertThat(assertions.operator(IDENTICAL, "NULL", "37"))
                .isEqualTo(false);

        assertThat(assertions.operator(IDENTICAL, "37", "NULL"))
                .isEqualTo(false);

        assertThat(assertions.operator(IDENTICAL, "NUMBER '-2'", "NUMBER '-3'"))
                .isEqualTo(false);

        assertThat(assertions.operator(IDENTICAL, "NUMBER '-2'", "CAST(NULL AS number)"))
                .isEqualTo(false);

        assertThat(assertions.operator(IDENTICAL, "NUMBER '-2'", "NUMBER '-2'"))
                .isEqualTo(true);

        assertThat(assertions.operator(IDENTICAL, "CAST(NULL AS number)", "NUMBER '-2'"))
                .isEqualTo(false);

        assertThat(assertions.operator(IDENTICAL, "NUMBER '12345678901234567.89012345678901234567'", "NUMBER '12345678901234567.8902345678901234567'"))
                .isEqualTo(false);

        assertThat(assertions.operator(IDENTICAL, "NUMBER '12345678901234567.89012345678901234567'", "CAST(NULL AS number)"))
                .isEqualTo(false);

        assertThat(assertions.operator(IDENTICAL, "CAST(NULL AS number)", "CAST(NULL AS number)"))
                .isEqualTo(true);

        assertThat(assertions.operator(IDENTICAL, "NUMBER '-12345678901234567.89012345678901234567'", "NUMBER '-12345678901234567.89012345678901234567'"))
                .isEqualTo(true);

        assertThat(assertions.operator(IDENTICAL, "CAST(NULL AS number)", "NUMBER '12345678901234567.89012345678901234567'"))
                .isEqualTo(false);

        assertThat(assertions.operator(IDENTICAL, "NUMBER '12345678901234567.89012345678901234567'", "NUMBER '-3'"))
                .isEqualTo(false);

        assertThat(assertions.operator(IDENTICAL, "NUMBER '12345678901234567.89012345678901234567'", "CAST(NULL AS number)"))
                .isEqualTo(false);

        assertThat(assertions.operator(IDENTICAL, "CAST(NULL AS number)", "CAST(NULL AS number)"))
                .isEqualTo(true);

        assertThat(assertions.operator(IDENTICAL, "NUMBER '00000000000000007.80000000000000000000'", "NUMBER '7.8'"))
                .isEqualTo(true);

        assertThat(assertions.operator(IDENTICAL, "CAST(NULL AS number)", "NUMBER '7.8'"))
                .isEqualTo(false);

        // with unknown
        assertThat(assertions.operator(IDENTICAL, "NULL", "NUMBER '-2'"))
                .isEqualTo(false);

        assertThat(assertions.operator(IDENTICAL, "NUMBER '-2'", "NULL"))
                .isEqualTo(false);

        assertThat(assertions.operator(IDENTICAL, "NULL", "NUMBER '12345678901234567.89012345678901234567'"))
                .isEqualTo(false);

        assertThat(assertions.operator(IDENTICAL, "NUMBER '12345678901234567.89012345678901234567'", "NULL"))
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
        assertThat(assertions.function("nullif", "NUMBER '-2'", "NUMBER '-3'"))
                .isEqualTo(number("-2"));

        assertThat(assertions.function("nullif", "NUMBER '-2'", "CAST(NULL AS number)"))
                .isEqualTo(number("-2"));

        assertThat(assertions.function("nullif", "NUMBER '-2'", "NUMBER '-2'"))
                .isNull(NUMBER);

        assertThat(assertions.function("nullif", "CAST(NULL AS number)", "NUMBER '-2'"))
                .isNull(NUMBER);

        assertThat(assertions.function("nullif", "CAST(NULL AS number)", "CAST(NULL AS number)"))
                .isNull(NUMBER);

        assertThat(assertions.function("nullif", "NUMBER '12345678901234567.89012345678901234567'", "NUMBER '12345678901234567.8902345678901234567'"))
                .isEqualTo(number("12345678901234567.89012345678901234567"));

        assertThat(assertions.function("nullif", "NUMBER '12345678901234567.89012345678901234567'", "CAST(NULL AS number)"))
                .isEqualTo(number("12345678901234567.89012345678901234567"));

        assertThat(assertions.function("nullif", "NUMBER '12345678901234567.89012345678901234567'", "NUMBER '12345678901234567.89012345678901234567'"))
                .isNull(NUMBER);

        assertThat(assertions.function("nullif", "CAST(NULL AS number)", "NUMBER '12345678901234567.89012345678901234567'"))
                .isNull(NUMBER);

        assertThat(assertions.function("nullif", "CAST(NULL AS number)", "CAST(NULL AS number)"))
                .isNull(NUMBER);

        assertThat(assertions.function("nullif", "NUMBER '12345678901234567.89012345678901234567'", "NUMBER '-3'"))
                .isEqualTo(number("12345678901234567.89012345678901234567"));

        assertThat(assertions.function("nullif", "NUMBER '12345678901234567.89012345678901234567'", "CAST(NULL AS number)"))
                .isEqualTo(number("12345678901234567.89012345678901234567"));

        assertThat(assertions.function("nullif", "NUMBER '12345678901234567.89012345678901234567'", "NUMBER '12345678901234567.89012345678901234567'"))
                .isNull(NUMBER);

        assertThat(assertions.function("nullif", "CAST(NULL AS number)", "NUMBER '12345678901234567.89012345678901234567'"))
                .isNull(NUMBER);

        // with unknown
        assertThat(assertions.function("nullif", "NULL", "NULL"))
                .isNull(UnknownType.UNKNOWN);

        assertThat(assertions.function("nullif", "NULL", "NUMBER '-2'"))
                .isNull(UnknownType.UNKNOWN);

        assertThat(assertions.function("nullif", "NUMBER '-2'", "NULL"))
                .isEqualTo(number("-2"));

        assertThat(assertions.function("nullif", "NULL", "NUMBER '12345678901234567.89012345678901234567'"))
                .isNull(UnknownType.UNKNOWN);

        assertThat(assertions.function("nullif", "NUMBER '12345678901234567.89012345678901234567'", "NULL"))
                .isEqualTo(number("12345678901234567.89012345678901234567"));
    }

    @Test
    void testCoalesce()
    {
        assertThat(assertions.function("coalesce", "NUMBER '2.1'", "null", "CAST(NULL AS number)"))
                .isEqualTo(number("2.1"));

        assertThat(assertions.function("coalesce", "CAST(NULL AS number)", "null", "CAST(NULL AS number)"))
                .isNull(NUMBER);

        assertThat(assertions.function("coalesce", "NUMBER '3'", "NUMBER '2.1'", "null", "CAST(NULL AS number)"))
                .isEqualTo(number("3"));

        assertThat(assertions.function("coalesce", "CAST(NULL AS number)", "null", "CAST(NULL AS number)"))
                .isNull(NUMBER);
    }

    @Test
    void testIndeterminate()
    {
        assertThat(assertions.operator(INDETERMINATE, "CAST(NULL AS number)"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "NUMBER '.999'"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "NUMBER '18'"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "NUMBER '9.0'"))
                .isEqualTo(false);

        assertThat(assertions.operator(INDETERMINATE, "NUMBER '12345678901234567.89012345678901234567'"))
                .isEqualTo(false);
    }

    @Test
    void testCastToTinyint()
    {
        assertThat(assertions.expression("cast(a as tinyint)")
                .binding("a", "CAST(NULL as number)"))
                .isNull(TINYINT);

        assertThat(assertions.expression("cast(a as tinyint)")
                .binding("a", "NUMBER '0'"))
                .hasType(TINYINT)
                .isEqualTo((byte) 0);

        assertThat(assertions.expression("cast(a as tinyint)")
                .binding("a", "NUMBER '127'"))
                .hasType(TINYINT)
                .isEqualTo((byte) 127);

        assertThat(assertions.expression("cast(a as tinyint)")
                .binding("a", "NUMBER '-128'"))
                .hasType(TINYINT)
                .isEqualTo((byte) -128);

        assertThat(assertions.expression("cast(a as tinyint)")
                .binding("a", "NUMBER '1.5'"))
                .hasType(TINYINT)
                .isEqualTo((byte) 2);

        assertThat(assertions.expression("cast(a as tinyint)")
                .binding("a", "NUMBER '2.5'"))
                .hasType(TINYINT)
                .isEqualTo((byte) 3);

        assertThat(assertions.expression("cast(a as tinyint)")
                .binding("a", "NUMBER '-2.5'"))
                .hasType(TINYINT)
                .isEqualTo((byte) -3);

        assertTrinoExceptionThrownBy(assertions.expression("cast(a as tinyint)")
                .binding("a", "NUMBER '128'")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        assertTrinoExceptionThrownBy(assertions.expression("cast(a as tinyint)")
                .binding("a", "NUMBER '-129'")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        assertTrinoExceptionThrownBy(assertions.expression("cast(a as tinyint)")
                .binding("a", "NUMBER '12345678901234567890'")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        assertTrinoExceptionThrownBy(assertions.expression("cast(a as tinyint)")
                .binding("a", "NUMBER 'NaN'")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("Cannot cast NUMBER 'NaN' to TINYINT");

        assertTrinoExceptionThrownBy(assertions.expression("cast(a as tinyint)")
                .binding("a", "NUMBER '+Infinity'")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("Cannot cast NUMBER '+Infinity' to TINYINT");

        assertTrinoExceptionThrownBy(assertions.expression("cast(a as tinyint)")
                .binding("a", "NUMBER '-Infinity'")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("Cannot cast NUMBER '-Infinity' to TINYINT");
    }

    @Test
    void testCastToSmallint()
    {
        assertThat(assertions.expression("cast(a as smallint)")
                .binding("a", "CAST(NULL as number)"))
                .isNull(SMALLINT);

        assertThat(assertions.expression("cast(a as smallint)")
                .binding("a", "NUMBER '0'"))
                .hasType(SMALLINT)
                .isEqualTo((short) 0);

        assertThat(assertions.expression("cast(a as smallint)")
                .binding("a", "NUMBER '32767'"))
                .hasType(SMALLINT)
                .isEqualTo((short) 32767);

        assertThat(assertions.expression("cast(a as smallint)")
                .binding("a", "NUMBER '-32768'"))
                .hasType(SMALLINT)
                .isEqualTo((short) -32768);

        assertThat(assertions.expression("cast(a as smallint)")
                .binding("a", "NUMBER '1.5'"))
                .hasType(SMALLINT)
                .isEqualTo((short) 2);

        assertThat(assertions.expression("cast(a as smallint)")
                .binding("a", "NUMBER '-2.5'"))
                .hasType(SMALLINT)
                .isEqualTo((short) -3);

        assertTrinoExceptionThrownBy(assertions.expression("cast(a as smallint)")
                .binding("a", "NUMBER '32768'")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        assertTrinoExceptionThrownBy(assertions.expression("cast(a as smallint)")
                .binding("a", "NUMBER '-32769'")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        assertTrinoExceptionThrownBy(assertions.expression("cast(a as smallint)")
                .binding("a", "NUMBER '12345678901234567890'")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        assertTrinoExceptionThrownBy(assertions.expression("cast(a as smallint)")
                .binding("a", "NUMBER 'NaN'")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("Cannot cast NUMBER 'NaN' to SMALLINT");

        assertTrinoExceptionThrownBy(assertions.expression("cast(a as smallint)")
                .binding("a", "NUMBER '+Infinity'")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("Cannot cast NUMBER '+Infinity' to SMALLINT");

        assertTrinoExceptionThrownBy(assertions.expression("cast(a as smallint)")
                .binding("a", "NUMBER '-Infinity'")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("Cannot cast NUMBER '-Infinity' to SMALLINT");
    }

    @Test
    void testCastToInteger()
    {
        assertThat(assertions.expression("cast(a as integer)")
                .binding("a", "CAST(NULL as number)"))
                .isNull(INTEGER);

        assertThat(assertions.expression("cast(a as integer)")
                .binding("a", "NUMBER '0'"))
                .hasType(INTEGER)
                .isEqualTo(0);

        assertThat(assertions.expression("cast(a as integer)")
                .binding("a", "NUMBER '2147483647'"))
                .hasType(INTEGER)
                .isEqualTo(2147483647);

        assertThat(assertions.expression("cast(a as integer)")
                .binding("a", "NUMBER '-2147483648'"))
                .hasType(INTEGER)
                .isEqualTo(-2147483648);

        assertThat(assertions.expression("cast(a as integer)")
                .binding("a", "NUMBER '1.5'"))
                .hasType(INTEGER)
                .isEqualTo(2);

        assertThat(assertions.expression("cast(a as integer)")
                .binding("a", "NUMBER '-2.5'"))
                .hasType(INTEGER)
                .isEqualTo(-3);

        assertTrinoExceptionThrownBy(assertions.expression("cast(a as integer)")
                .binding("a", "NUMBER '2147483648'")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        assertTrinoExceptionThrownBy(assertions.expression("cast(a as integer)")
                .binding("a", "NUMBER '-2147483649'")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        assertTrinoExceptionThrownBy(assertions.expression("cast(a as integer)")
                .binding("a", "NUMBER '12345678901234567890'")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        assertTrinoExceptionThrownBy(assertions.expression("cast(a as integer)")
                .binding("a", "NUMBER 'NaN'")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("Cannot cast NUMBER 'NaN' to INTEGER");

        assertTrinoExceptionThrownBy(assertions.expression("cast(a as integer)")
                .binding("a", "NUMBER '+Infinity'")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("Cannot cast NUMBER '+Infinity' to INTEGER");

        assertTrinoExceptionThrownBy(assertions.expression("cast(a as integer)")
                .binding("a", "NUMBER '-Infinity'")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("Cannot cast NUMBER '-Infinity' to INTEGER");
    }

    @Test
    void testCastToBigint()
    {
        assertThat(assertions.expression("cast(a as bigint)")
                .binding("a", "CAST(NULL as number)"))
                .isNull(BIGINT);

        assertThat(assertions.expression("cast(a as bigint)")
                .binding("a", "NUMBER '0'"))
                .hasType(BIGINT)
                .isEqualTo(0L);

        assertThat(assertions.expression("cast(a as bigint)")
                .binding("a", "NUMBER '9223372036854775807'"))
                .hasType(BIGINT)
                .isEqualTo(9223372036854775807L);

        assertThat(assertions.expression("cast(a as bigint)")
                .binding("a", "NUMBER '-9223372036854775808'"))
                .hasType(BIGINT)
                .isEqualTo(-9223372036854775808L);

        assertThat(assertions.expression("cast(a as bigint)")
                .binding("a", "NUMBER '1.5'"))
                .hasType(BIGINT)
                .isEqualTo(2L);

        assertThat(assertions.expression("cast(a as bigint)")
                .binding("a", "NUMBER '2.5'"))
                .hasType(BIGINT)
                .isEqualTo(3L);

        assertThat(assertions.expression("cast(a as bigint)")
                .binding("a", "NUMBER '-2.5'"))
                .hasType(BIGINT)
                .isEqualTo(-3L);

        assertTrinoExceptionThrownBy(assertions.expression("cast(a as bigint)")
                .binding("a", "NUMBER '9223372036854775808'")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        assertTrinoExceptionThrownBy(assertions.expression("cast(a as bigint)")
                .binding("a", "NUMBER '-9223372036854775809'")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        assertTrinoExceptionThrownBy(assertions.expression("cast(a as bigint)")
                .binding("a", "NUMBER '12345678901234567890'")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        assertTrinoExceptionThrownBy(assertions.expression("cast(a as bigint)")
                .binding("a", "NUMBER 'NaN'")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("Cannot cast NUMBER 'NaN' to BIGINT");

        assertTrinoExceptionThrownBy(assertions.expression("cast(a as bigint)")
                .binding("a", "NUMBER '+Infinity'")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("Cannot cast NUMBER '+Infinity' to BIGINT");

        assertTrinoExceptionThrownBy(assertions.expression("cast(a as bigint)")
                .binding("a", "NUMBER '-Infinity'")::evaluate)
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE)
                .hasMessage("Cannot cast NUMBER '-Infinity' to BIGINT");
    }

    @Test
    void testCastToReal()
    {
        assertThat(assertions.expression("cast(a as real)")
                .binding("a", "CAST(NULL as number)"))
                .isNull(REAL);

        assertThat(assertions.expression("cast(a as real)")
                .binding("a", "NUMBER '0'"))
                .hasType(REAL)
                .isEqualTo(0.0f);

        assertThat(assertions.expression("cast(a as real)")
                .binding("a", "NUMBER '1.5'"))
                .hasType(REAL)
                .isEqualTo(1.5f);

        assertThat(assertions.expression("cast(a as real)")
                .binding("a", "NUMBER '-2.5'"))
                .hasType(REAL)
                .isEqualTo(-2.5f);

        assertThat(assertions.expression("cast(a as real)")
                .binding("a", "NUMBER '3.14159'"))
                .hasType(REAL)
                .isEqualTo(3.14159f);

        assertThat(assertions.expression("cast(a as real)")
                .binding("a", "NUMBER '12345.6789'"))
                .hasType(REAL)
                .isEqualTo(12345.6789f);

        assertThat(assertions.expression("cast(a as real)")
                .binding("a", "NUMBER '9999e100'"))
                .hasType(REAL)
                .isEqualTo(Float.POSITIVE_INFINITY);

        assertThat(assertions.expression("cast(a as real)")
                .binding("a", "NUMBER 'NaN'"))
                .hasType(REAL)
                .isEqualTo(Float.NaN);

        assertThat(assertions.expression("cast(a as real)")
                .binding("a", "NUMBER '+Infinity'"))
                .hasType(REAL)
                .isEqualTo(Float.POSITIVE_INFINITY);

        assertThat(assertions.expression("cast(a as real)")
                .binding("a", "NUMBER '-Infinity'"))
                .hasType(REAL)
                .isEqualTo(Float.NEGATIVE_INFINITY);
    }

    @Test
    void testCastToDouble()
    {
        assertThat(assertions.expression("cast(a as double)")
                .binding("a", "CAST(NULL as number)"))
                .isNull(DOUBLE);

        assertThat(assertions.expression("cast(a as double)")
                .binding("a", "NUMBER '0'"))
                .hasType(DOUBLE)
                .isEqualTo(0.0);

        assertThat(assertions.expression("cast(a as double)")
                .binding("a", "NUMBER '1.5'"))
                .hasType(DOUBLE)
                .isEqualTo(1.5);

        assertThat(assertions.expression("cast(a as double)")
                .binding("a", "NUMBER '-2.5'"))
                .hasType(DOUBLE)
                .isEqualTo(-2.5);

        assertThat(assertions.expression("cast(a as double)")
                .binding("a", "NUMBER '3.14159'"))
                .hasType(DOUBLE)
                .isEqualTo(3.14159);

        assertThat(assertions.expression("cast(a as double)")
                .binding("a", "NUMBER '12345.6789'"))
                .hasType(DOUBLE)
                .isEqualTo(12345.6789);

        assertThat(assertions.expression("cast(a as double)")
                .binding("a", "NUMBER '1234567890123456'"))
                .hasType(DOUBLE)
                .isEqualTo(1234567890123456.0);

        assertThat(assertions.expression("cast(a as double)")
                .binding("a", "NUMBER '9999e500'"))
                .hasType(DOUBLE)
                .isEqualTo(Double.POSITIVE_INFINITY);

        assertThat(assertions.expression("cast(a as double)")
                .binding("a", "NUMBER 'NaN'"))
                .hasType(DOUBLE)
                .isEqualTo(Double.NaN);

        assertThat(assertions.expression("cast(a as double)")
                .binding("a", "NUMBER '+Infinity'"))
                .hasType(DOUBLE)
                .isEqualTo(Double.POSITIVE_INFINITY);

        assertThat(assertions.expression("cast(a as double)")
                .binding("a", "NUMBER '-Infinity'"))
                .hasType(DOUBLE)
                .isEqualTo(Double.NEGATIVE_INFINITY);
    }

    @Test
    void testCastToVarchar()
    {
        assertThat(assertions.expression("cast(a as varchar)")
                .binding("a", "NUMBER '0'"))
                .hasType(VARCHAR)
                .isEqualTo("0");

        assertThat(assertions.expression("cast(a as varchar)")
                .binding("a", "NUMBER '12345.6789'"))
                .hasType(VARCHAR)
                .isEqualTo("12345.6789");

        assertThat(assertions.expression("cast(a as varchar)")
                .binding("a", "NUMBER '-12345.6789'"))
                .hasType(VARCHAR)
                .isEqualTo("-12345.6789");

        assertThat(assertions.expression("cast(a as varchar)")
                .binding("a", "NUMBER '3.141592653589793238462643383279502884197169399375105820974944592307'"))
                .hasType(VARCHAR)
                .isEqualTo("3.141592653589793238462643383279502884197169399375105820974944592307");

        assertThat(assertions.expression("cast(a as varchar)")
                .binding("a", "NUMBER '123456789e-4'"))
                .hasType(VARCHAR)
                .isEqualTo("12345.6789");

        assertThat(assertions.expression("cast(a as varchar)")
                .binding("a", "NUMBER '123456789e+4'"))
                .hasType(VARCHAR)
                .isEqualTo("1.23456789E+12");

        assertThat(assertions.expression("cast(a as varchar)")
                .binding("a", "NUMBER 'NaN'"))
                .hasType(VARCHAR)
                .isEqualTo("NaN");

        assertThat(assertions.expression("cast(a as varchar)")
                .binding("a", "NUMBER '+Infinity'"))
                .hasType(VARCHAR)
                .isEqualTo("+Infinity");

        assertThat(assertions.expression("cast(a as varchar)")
                .binding("a", "NUMBER '-Infinity'"))
                .hasType(VARCHAR)
                .isEqualTo("-Infinity");

        assertThat(assertions.expression("cast(a as varchar(4))")
                .binding("a", "NUMBER '1234'"))
                .hasType(createVarcharType(4))
                .isEqualTo("1234");

        assertTrinoExceptionThrownBy(assertions.expression("cast(a as varchar(3))")
                .binding("a", "NUMBER '1234'")::evaluate)
                .hasMessage("Value 1234 cannot be represented as varchar(3)");

        assertTrinoExceptionThrownBy(assertions.expression("cast(a as varchar(3))")
                .binding("a", "NUMBER '1e9'")::evaluate)
                .hasMessage("Value 1E+9 cannot be represented as varchar(3)");
    }

    private enum TestedArithmeticOperator
    {
        ADD("+", BigDecimal::add),
        SUBTRACT("-", BigDecimal::subtract),
        MULTIPLY("*", BigDecimal::multiply),
        DIVIDE("/", (a, b) -> a.divide(b, max(6, a.stripTrailingZeros().scale() + b.stripTrailingZeros().precision() + 1), HALF_UP)),
        MODULUS("%", BigDecimal::remainder),
        /**/;

        private final String operator;
        private final BiFunction<BigDecimal, BigDecimal, BigDecimal> bigDecimalReferenceImplementation;

        TestedArithmeticOperator(String operator, BiFunction<BigDecimal, BigDecimal, BigDecimal> bigDecimalReferenceImplementation)
        {
            this.operator = operator;
            this.bigDecimalReferenceImplementation = bigDecimalReferenceImplementation;
        }
    }

    private enum TestedComparisonOperator
    {
        EQUAL("=", (a, b) -> a.compareTo(b) == 0),
        NOT_EQUAL("!=", (a, b) -> a.compareTo(b) != 0),
        LESS_THAN("<", (a, b) -> a.compareTo(b) < 0),
        LESS_THAN_OR_EQUAL("<=", (a, b) -> a.compareTo(b) <= 0),
        GREATER_THAN(">", (a, b) -> a.compareTo(b) > 0),
        GREATER_THAN_OR_EQUAL(">=", (a, b) -> a.compareTo(b) >= 0),
        /**/;

        private final String operator;
        private final BiPredicate<BigDecimal, BigDecimal> bigDecimalReferenceImplementation;

        TestedComparisonOperator(String operator, BiPredicate<BigDecimal, BigDecimal> bigDecimalReferenceImplementation)
        {
            this.operator = operator;
            this.bigDecimalReferenceImplementation = bigDecimalReferenceImplementation;
        }
    }

    private static SqlNumber number(String value)
    {
        return new SqlNumber(value);
    }

    private static SqlNumber number(double value)
    {
        if (Double.isNaN(value)) {
            return new SqlNumber(new TrinoNumber.NotANumber());
        }
        if (Double.isInfinite(value)) {
            return new SqlNumber(new TrinoNumber.Infinity(value == NEGATIVE_INFINITY));
        }
        BigDecimal bigDecimal = new BigDecimal(Double.toString(value));
        checkArgument(bigDecimal.doubleValue() == value, "Value %s cannot be represented as a BigDecimal losslessly", value);
        return new SqlNumber(new TrinoNumber.BigDecimalValue(bigDecimal.stripTrailingZeros()));
    }
}

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
package io.trino.sql.ir.optimizer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.TrinoNumber;
import io.trino.spi.type.TrinoNumber.NotANumber;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.optimizer.rule.RemoveRedundantArithmetic;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Optional;

import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.function.OperatorType.DIVIDE;
import static io.trino.spi.function.OperatorType.MODULO;
import static io.trino.spi.function.OperatorType.MULTIPLY;
import static io.trino.spi.function.OperatorType.SUBTRACT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.NumberType.NUMBER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.sql.planner.TestingSymbolAllocator.emptySymbolAllocator;
import static io.trino.testing.TestingSession.testSession;
import static io.trino.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static io.trino.type.Reals.toReal;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRemoveRedundantArithmetic
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();

    @Test
    void testAddZero()
    {
        for (Type type : ImmutableList.of(TINYINT, SMALLINT, INTEGER, BIGINT)) {
            Reference value = new Reference(type, "x");
            assertThat(optimize(operation(ADD, value, new Constant(type, 0L))))
                    .describedAs("%s + 0".formatted(type))
                    .isEqualTo(Optional.of(value));
            assertThat(optimize(operation(ADD, new Constant(type, 0L), value)))
                    .describedAs("0 + %s".formatted(type))
                    .isEqualTo(Optional.of(value));
        }
    }

    @Test
    void testSubtractZero()
    {
        for (Type type : ImmutableList.of(TINYINT, SMALLINT, INTEGER, BIGINT)) {
            Reference value = new Reference(type, "x");
            assertThat(optimize(operation(SUBTRACT, value, new Constant(type, 0L))))
                    .describedAs("%s - 0".formatted(type))
                    .isEqualTo(Optional.of(value));
            assertThat(optimize(operation(SUBTRACT, new Constant(type, 0L), value)))
                    .describedAs("0 - %s negates the value".formatted(type))
                    .isEmpty();
        }
    }

    @Test
    void testMultiplyOne()
    {
        for (Type type : ImmutableList.of(TINYINT, SMALLINT, INTEGER, BIGINT)) {
            Reference value = new Reference(type, "x");
            assertThat(optimize(operation(MULTIPLY, value, new Constant(type, 1L))))
                    .describedAs("%s * 1".formatted(type))
                    .isEqualTo(Optional.of(value));
            assertThat(optimize(operation(MULTIPLY, new Constant(type, 1L), value)))
                    .describedAs("1 * %s".formatted(type))
                    .isEqualTo(Optional.of(value));
        }

        Reference doubleValue = new Reference(DOUBLE, "x");
        assertThat(optimize(operation(MULTIPLY, doubleValue, new Constant(DOUBLE, 1.0))))
                .isEqualTo(Optional.of(doubleValue));
        assertThat(optimize(operation(MULTIPLY, new Constant(DOUBLE, 1.0), doubleValue)))
                .isEqualTo(Optional.of(doubleValue));

        Reference realValue = new Reference(REAL, "x");
        assertThat(optimize(operation(MULTIPLY, realValue, new Constant(REAL, toReal(1)))))
                .isEqualTo(Optional.of(realValue));
    }

    @Test
    void testDivideOne()
    {
        for (Type type : ImmutableList.of(TINYINT, SMALLINT, INTEGER, BIGINT)) {
            Reference value = new Reference(type, "x");
            assertThat(optimize(operation(DIVIDE, value, new Constant(type, 1L))))
                    .describedAs("%s / 1".formatted(type))
                    .isEqualTo(Optional.of(value));
            assertThat(optimize(operation(DIVIDE, new Constant(type, 1L), value)))
                    .describedAs("1 / %s is not the value".formatted(type))
                    .isEmpty();
        }

        Reference doubleValue = new Reference(DOUBLE, "x");
        assertThat(optimize(operation(DIVIDE, doubleValue, new Constant(DOUBLE, 1.0))))
                .isEqualTo(Optional.of(doubleValue));

        Reference realValue = new Reference(REAL, "x");
        assertThat(optimize(operation(DIVIDE, realValue, new Constant(REAL, toReal(1)))))
                .isEqualTo(Optional.of(realValue));
    }

    /**
     * {@code real} and {@code double} have two zeros, and adding or subtracting one of them can flip
     * the sign: {@code -0.0 + 0.0} and {@code -0.0 - -0.0} are both {@code 0.0}.
     */
    @Test
    void testKeepFloatingPointAdditionOfZero()
    {
        for (Expression zero : ImmutableList.of(new Constant(DOUBLE, 0.0), new Constant(DOUBLE, -0.0))) {
            Reference value = new Reference(DOUBLE, "x");
            assertThat(optimize(operation(ADD, value, zero)))
                    .describedAs("double + %s".formatted(zero))
                    .isEmpty();
            assertThat(optimize(operation(ADD, zero, value)))
                    .describedAs("%s + double".formatted(zero))
                    .isEmpty();
            assertThat(optimize(operation(SUBTRACT, value, zero)))
                    .describedAs("double - %s".formatted(zero))
                    .isEmpty();
        }

        Reference realValue = new Reference(REAL, "x");
        assertThat(optimize(operation(ADD, realValue, new Constant(REAL, toReal(0)))))
                .isEmpty();
        assertThat(optimize(operation(SUBTRACT, realValue, new Constant(REAL, toReal(-0.0f)))))
                .isEmpty();
    }

    /**
     * Scaling an interval by a {@code double} is lossy for large intervals, so it is not an identity:
     * {@code INTERVAL '999999999 00:00:00.001' DAY TO SECOND * 1e0} drops the millisecond.
     */
    @Test
    void testKeepIntervalScaling()
    {
        Reference value = new Reference(INTERVAL_DAY_TIME, "x");
        assertThat(optimize(operation(MULTIPLY, value, new Constant(DOUBLE, 1.0))))
                .isEmpty();
        assertThat(optimize(operation(MULTIPLY, new Constant(DOUBLE, 1.0), value)))
                .isEmpty();
        assertThat(optimize(operation(DIVIDE, value, new Constant(DOUBLE, 1.0))))
                .isEmpty();
        assertThat(optimize(operation(MULTIPLY, value, new Constant(BIGINT, 1L))))
                .isEmpty();
    }

    @Test
    void testShortDecimal()
    {
        // decimal(38,2) + decimal(10,0) is a decimal(38,2): precision is already saturated, so the operand type survives
        Reference value = new Reference(createDecimalType(38, 2), "x");
        assertThat(optimize(operation(ADD, value, new Constant(createDecimalType(10, 0), 0L))))
                .isEqualTo(Optional.of(value));
        assertThat(optimize(operation(SUBTRACT, value, new Constant(createDecimalType(10, 0), 0L))))
                .isEqualTo(Optional.of(value));
        assertThat(optimize(operation(MULTIPLY, value, new Constant(createDecimalType(2, 0), 1L))))
                .isEqualTo(Optional.of(value));

        // one at a non-zero scale is 10^scale unscaled
        Reference scaledValue = new Reference(createDecimalType(38, 6), "x");
        assertThat(optimize(operation(DIVIDE, scaledValue, new Constant(createDecimalType(10, 4), 10000L))))
                .describedAs("decimal(38,6) / 1.0000")
                .isEqualTo(Optional.of(scaledValue));
        assertThat(optimize(operation(DIVIDE, scaledValue, new Constant(createDecimalType(10, 4), 1L))))
                .describedAs("decimal(38,6) / 0.0001")
                .isEmpty();
    }

    @Test
    void testLongDecimal()
    {
        Reference value = new Reference(createDecimalType(38, 0), "x");
        assertThat(optimize(operation(ADD, value, new Constant(createDecimalType(38, 0), Int128.ZERO))))
                .isEqualTo(Optional.of(value));
        assertThat(optimize(operation(MULTIPLY, value, new Constant(createDecimalType(38, 0), Int128.valueOf(1)))))
                .isEqualTo(Optional.of(value));
        assertThat(optimize(operation(MULTIPLY, value, new Constant(createDecimalType(38, 0), Int128.valueOf(2)))))
                .isEmpty();
    }

    /**
     * Arithmetic on decimals widens the type, and the expression's type has to be preserved.
     */
    @Test
    void testKeepDecimalArithmeticThatWidensType()
    {
        Reference value = new Reference(createDecimalType(10, 2), "x");
        assertThat(optimize(operation(ADD, value, new Constant(createDecimalType(10, 0), 0L))))
                .describedAs("decimal(10,2) + 0 is a decimal(13,2)")
                .isEmpty();
        assertThat(optimize(operation(MULTIPLY, value, new Constant(createDecimalType(10, 0), 1L))))
                .describedAs("decimal(10,2) * 1 is a decimal(20,2)")
                .isEmpty();
        assertThat(optimize(operation(DIVIDE, value, new Constant(createDecimalType(10, 0), 1L))))
                .describedAs("decimal(10,2) / 1 is a decimal(21,12)")
                .isEmpty();
    }

    @Test
    void testNonIdentityConstant()
    {
        Reference value = new Reference(BIGINT, "x");
        assertThat(optimize(operation(ADD, value, new Constant(BIGINT, 1L)))).isEmpty();
        assertThat(optimize(operation(SUBTRACT, value, new Constant(BIGINT, 1L)))).isEmpty();
        assertThat(optimize(operation(MULTIPLY, value, new Constant(BIGINT, 0L)))).isEmpty();
        assertThat(optimize(operation(DIVIDE, value, new Constant(BIGINT, 2L)))).isEmpty();
        assertThat(optimize(operation(MODULO, value, new Constant(BIGINT, 1L))))
                .describedAs("modulo by one is zero, not the value")
                .isEmpty();
    }

    // handled by other optimizers
    @Test
    void testNullConstant()
    {
        Reference value = new Reference(BIGINT, "x");
        assertThat(optimize(operation(ADD, value, new Constant(BIGINT, null)))).isEmpty();
        assertThat(optimize(operation(MULTIPLY, value, new Constant(BIGINT, null)))).isEmpty();
    }

    @Test
    void testNonConstantOperand()
    {
        assertThat(optimize(operation(ADD, new Reference(BIGINT, "x"), new Reference(BIGINT, "y")))).isEmpty();
    }

    @Test
    void testNumber()
    {
        Reference value = new Reference(NUMBER, "x");
        assertThat(optimize(operation(ADD, value, number(BigDecimal.ZERO)))).isEqualTo(Optional.of(value));
        assertThat(optimize(operation(ADD, number(BigDecimal.ZERO), value))).isEqualTo(Optional.of(value));
        assertThat(optimize(operation(SUBTRACT, value, number(BigDecimal.ZERO)))).isEqualTo(Optional.of(value));
        assertThat(optimize(operation(MULTIPLY, value, number(BigDecimal.ONE)))).isEqualTo(Optional.of(value));
        assertThat(optimize(operation(MULTIPLY, number(BigDecimal.ONE), value))).isEqualTo(Optional.of(value));
        assertThat(optimize(operation(DIVIDE, value, number(BigDecimal.ONE)))).isEqualTo(Optional.of(value));

        assertThat(optimize(operation(ADD, value, number(new BigDecimal("0.000")))))
                .describedAs("a number is normalized, so zero at any scale is the additive identity")
                .isEqualTo(Optional.of(value));
        assertThat(optimize(operation(MULTIPLY, value, number(new BigDecimal("1.000")))))
                .describedAs("1.000 is the multiplicative identity")
                .isEqualTo(Optional.of(value));

        assertThat(optimize(operation(ADD, value, number(BigDecimal.ONE)))).isEmpty();
        assertThat(optimize(operation(SUBTRACT, number(BigDecimal.ZERO), value)))
                .describedAs("0 - number negates the value")
                .isEmpty();
        assertThat(optimize(operation(MULTIPLY, value, number(BigDecimal.TEN)))).isEmpty();
        assertThat(optimize(operation(DIVIDE, number(BigDecimal.ONE), value)))
                .describedAs("1 / number is not the value")
                .isEmpty();
        assertThat(optimize(operation(MULTIPLY, value, new Constant(NUMBER, TrinoNumber.from(new NotANumber())))))
                .describedAs("NaN is not the multiplicative identity")
                .isEmpty();
    }

    private static Constant number(BigDecimal value)
    {
        return new Constant(NUMBER, TrinoNumber.from(value));
    }

    private static Call operation(OperatorType operator, Expression left, Expression right)
    {
        return new Call(
                FUNCTIONS.resolveOperator(operator, ImmutableList.of(left.type(), right.type())),
                ImmutableList.of(left, right));
    }

    private static Optional<Expression> optimize(Expression expression)
    {
        return new RemoveRedundantArithmetic().apply(expression, testSession(), emptySymbolAllocator(), ImmutableMap.of());
    }
}

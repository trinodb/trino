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
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.ComparisonOperator;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Let;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.optimizer.rule.UnwrapMatchingCastsInComparison;
import io.trino.sql.planner.Symbol;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.ir.TestingIr.between;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.ir.optimizer.IrExpressionOptimizer.newOptimizer;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.sql.planner.TestingSymbolAllocator.emptySymbolAllocator;
import static io.trino.testing.TestingSession.testSession;
import static io.trino.type.UnknownType.UNKNOWN;
import static org.assertj.core.api.Assertions.assertThat;

public class TestUnwrapMatchingCastsInComparison
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction RANDOM = FUNCTIONS.resolveFunction("random", ImmutableList.of());

    private static final Type SHORT_DECIMAL = createDecimalType(18, 2);
    private static final Type LONG_DECIMAL = createDecimalType(19, 2);

    @Test
    public void testExactWidening()
    {
        for (ComparisonOperator operator : ComparisonOperator.values()) {
            assertUnwraps(operator, TINYINT, SMALLINT);
            assertUnwraps(operator, SMALLINT, INTEGER);
            assertUnwraps(operator, INTEGER, BIGINT);
            assertUnwraps(operator, createDecimalType(9, 2), createDecimalType(10, 2));
            assertUnwraps(operator, createDecimalType(18, 2), createDecimalType(19, 2));
            assertUnwraps(operator, createDecimalType(20, 2), createDecimalType(38, 2));
            assertUnwraps(operator, createDecimalType(3, 0), createDecimalType(38, 0));
            assertUnwraps(operator, createDecimalType(18, 2), createDecimalType(19, 3));
            assertUnwraps(operator, createDecimalType(18, 2), createDecimalType(38, 22));
            assertUnwraps(operator, TINYINT, createDecimalType(3, 0));
            assertUnwraps(operator, SMALLINT, createDecimalType(5, 0));
            assertUnwraps(operator, INTEGER, createDecimalType(10, 0));
            assertUnwraps(operator, INTEGER, createDecimalType(12, 2));
            assertUnwraps(operator, BIGINT, createDecimalType(19, 0));
            assertUnwraps(operator, BIGINT, createDecimalType(38, 19));
            assertUnwraps(operator, createDecimalType(2, 0), TINYINT);
            assertUnwraps(operator, createDecimalType(4, 0), SMALLINT);
            assertUnwraps(operator, createDecimalType(9, 0), INTEGER);
            assertUnwraps(operator, createDecimalType(18, 0), BIGINT);
            assertUnwraps(operator, TINYINT, REAL);
            assertUnwraps(operator, SMALLINT, REAL);
            assertUnwraps(operator, INTEGER, DOUBLE);
            assertUnwraps(operator, createDecimalType(7, 0), REAL);
            assertUnwraps(operator, createDecimalType(15, 0), DOUBLE);
            assertUnwraps(operator, REAL, DOUBLE);
        }
    }

    @Test
    public void testInexactCast()
    {
        for (ComparisonOperator operator : ComparisonOperator.values()) {
            assertDoesNotUnwrap(operator, createDecimalType(18, 2), DOUBLE);
            assertDoesNotUnwrap(operator, INTEGER, REAL);
            assertDoesNotUnwrap(operator, BIGINT, DOUBLE);
            assertDoesNotUnwrap(operator, createDecimalType(8, 0), REAL);
            assertDoesNotUnwrap(operator, createDecimalType(16, 0), DOUBLE);
            assertDoesNotUnwrap(operator, createDecimalType(5, 1), DOUBLE);
            assertDoesNotUnwrap(operator, DOUBLE, REAL);
            assertDoesNotUnwrap(operator, REAL, createDecimalType(38, 10));
            assertDoesNotUnwrap(operator, INTEGER, createDecimalType(9, 0));
            assertDoesNotUnwrap(operator, BIGINT, createDecimalType(38, 20));
            assertDoesNotUnwrap(operator, createDecimalType(38, 0), createDecimalType(38, 2));
            assertDoesNotUnwrap(operator, createDecimalType(20, 0), createDecimalType(38, 19));
            assertDoesNotUnwrap(operator, createDecimalType(18, 2), createDecimalType(19, 1));
            assertDoesNotUnwrap(operator, createDecimalType(19, 2), createDecimalType(18, 2));
            assertDoesNotUnwrap(operator, createDecimalType(1, 1), TINYINT);
            assertDoesNotUnwrap(operator, createDecimalType(3, 0), TINYINT);
            assertDoesNotUnwrap(operator, createDecimalType(5, 0), SMALLINT);
            assertDoesNotUnwrap(operator, createDecimalType(10, 0), INTEGER);
            assertDoesNotUnwrap(operator, createDecimalType(19, 0), BIGINT);
            assertDoesNotUnwrap(operator, BIGINT, INTEGER);
            assertDoesNotUnwrap(operator, createVarcharType(3), VARCHAR);
            assertDoesNotUnwrap(operator, createCharType(3), VARCHAR);
            assertDoesNotUnwrap(operator, DATE, createTimestampType(3));
            assertDoesNotUnwrap(operator, createTimestampType(3), createTimestampType(6));
            assertDoesNotUnwrap(operator, UNKNOWN, BIGINT);
        }
    }

    @Test
    public void testNonMatchingOperands()
    {
        assertThat(optimize(comparison(EQUAL, new Cast(new Reference(INTEGER, "a"), BIGINT), new Cast(new Reference(SMALLINT, "b"), BIGINT))))
                .describedAs("different source types")
                .isEqualTo(Optional.empty());

        assertThat(optimize(comparison(LESS_THAN, new Cast(new Reference(SHORT_DECIMAL, "a"), LONG_DECIMAL), new Reference(LONG_DECIMAL, "b"))))
                .describedAs("one cast operand")
                .isEqualTo(Optional.empty());

        assertThat(optimize(comparison(LESS_THAN, new Cast(new Reference(SHORT_DECIMAL, "a"), LONG_DECIMAL), new Constant(LONG_DECIMAL, Int128.valueOf(100)))))
                .describedAs("constant operand")
                .isEqualTo(Optional.empty());
    }

    @Test
    public void testBetween()
    {
        Reference value = new Reference(SHORT_DECIMAL, "a");
        Reference lower = new Reference(SHORT_DECIMAL, "b");
        Reference upper = new Reference(SHORT_DECIMAL, "c");
        Reference longLower = new Reference(LONG_DECIMAL, "d");
        Reference longUpper = new Reference(LONG_DECIMAL, "e");

        assertThat(optimize(between(new Cast(value, LONG_DECIMAL), new Cast(lower, LONG_DECIMAL), longUpper)))
                .describedAs("lower bound")
                .isEqualTo(Optional.of(new Logical(AND, ImmutableList.of(
                        comparison(GREATER_THAN_OR_EQUAL, value, lower),
                        comparison(LESS_THAN_OR_EQUAL, new Cast(value, LONG_DECIMAL), longUpper)))));

        assertThat(optimize(between(new Cast(value, LONG_DECIMAL), longLower, new Cast(upper, LONG_DECIMAL))))
                .describedAs("upper bound")
                .isEqualTo(Optional.of(new Logical(AND, ImmutableList.of(
                        comparison(GREATER_THAN_OR_EQUAL, new Cast(value, LONG_DECIMAL), longLower),
                        comparison(LESS_THAN_OR_EQUAL, value, upper)))));

        assertThat(optimize(between(new Cast(value, LONG_DECIMAL), new Cast(lower, LONG_DECIMAL), new Cast(upper, LONG_DECIMAL))))
                .describedAs("both bounds")
                .isEqualTo(Optional.of(between(value, lower, upper)));
    }

    @Test
    public void testBetweenNoEffect()
    {
        assertThat(optimize(between(new Cast(new Reference(BIGINT, "a"), DOUBLE), new Cast(new Reference(BIGINT, "b"), DOUBLE), new Reference(DOUBLE, "c"))))
                .describedAs("inexact cast")
                .isEqualTo(Optional.empty());

        Type decimal = createDecimalType(19, 0);
        assertThat(optimize(between(new Cast(new Reference(BIGINT, "a"), decimal), new Cast(new Reference(INTEGER, "b"), decimal), new Cast(new Reference(INTEGER, "c"), decimal))))
                .describedAs("different source types")
                .isEqualTo(Optional.empty());

        assertThat(optimize(between(new Cast(new Reference(SHORT_DECIMAL, "a"), LONG_DECIMAL), new Reference(LONG_DECIMAL, "b"), new Constant(LONG_DECIMAL, null))))
                .describedAs("no cast bounds")
                .isEqualTo(Optional.empty());
    }

    @Test
    public void testBetweenNonTrivialValue()
    {
        Expression value = new Cast(new Call(RANDOM, ImmutableList.of()), SHORT_DECIMAL);
        Reference lower = new Reference(SHORT_DECIMAL, "b");
        Reference upper = new Reference(LONG_DECIMAL, "c");
        Symbol symbol = new Symbol(SHORT_DECIMAL, "between");

        assertThat(optimize(between(new Cast(value, LONG_DECIMAL), new Cast(lower, LONG_DECIMAL), upper)))
                .isEqualTo(Optional.of(new Let(symbol, value, new Logical(AND, ImmutableList.of(
                        comparison(GREATER_THAN_OR_EQUAL, symbol.toSymbolReference(), lower),
                        comparison(LESS_THAN_OR_EQUAL, new Cast(symbol.toSymbolReference(), LONG_DECIMAL), upper))))));
    }

    @Test
    public void testRegisteredInOptimizer()
    {
        Reference left = new Reference(SHORT_DECIMAL, "a");
        Reference right = new Reference(SHORT_DECIMAL, "b");
        assertThat(newOptimizer(PLANNER_CONTEXT).process(comparison(LESS_THAN, new Cast(left, LONG_DECIMAL), new Cast(right, LONG_DECIMAL)), testSession(), emptySymbolAllocator(), ImmutableMap.of()))
                .isEqualTo(Optional.of(comparison(LESS_THAN, left, right)));
    }

    private static void assertUnwraps(ComparisonOperator operator, Type sourceType, Type targetType)
    {
        Reference left = new Reference(sourceType, "a");
        Reference right = new Reference(sourceType, "b");
        assertThat(optimize(comparison(operator, new Cast(left, targetType), new Cast(right, targetType))))
                .describedAs("%s %s %s", sourceType, operator, targetType)
                .isEqualTo(Optional.of(comparison(operator, left, right)));
    }

    private static void assertDoesNotUnwrap(ComparisonOperator operator, Type sourceType, Type targetType)
    {
        assertThat(optimize(comparison(operator, new Cast(new Reference(sourceType, "a"), targetType), new Cast(new Reference(sourceType, "b"), targetType))))
                .describedAs("%s %s %s", sourceType, operator, targetType)
                .isEqualTo(Optional.empty());
    }

    private static Optional<Expression> optimize(Expression expression)
    {
        return new UnwrapMatchingCastsInComparison(PLANNER_CONTEXT).apply(expression, testSession(), emptySymbolAllocator(), ImmutableMap.of());
    }
}

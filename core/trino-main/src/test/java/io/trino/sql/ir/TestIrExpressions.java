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
package io.trino.sql.ir;

import com.google.common.collect.ImmutableList;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.sql.ir.IrExpressions.Between;
import io.trino.sql.ir.IrExpressions.Comparison;
import io.trino.sql.ir.IrExpressions.NullIf;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import org.junit.jupiter.api.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.function.OperatorType.NEGATION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.ComparisonOperator.IDENTICAL;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.ComparisonOperator.NOT_EQUAL;
import static io.trino.sql.ir.IrExpressions.constantNull;
import static io.trino.sql.ir.IrExpressions.ifExpression;
import static io.trino.sql.ir.IrExpressions.matchBetween;
import static io.trino.sql.ir.IrExpressions.matchComparison;
import static io.trino.sql.ir.IrExpressions.matchNullIf;
import static io.trino.sql.ir.IrExpressions.mayBeNull;
import static io.trino.sql.ir.IrExpressions.mayReturnNullOnNonNullInput;
import static io.trino.sql.ir.IrExpressions.not;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.ir.TestingIr.between;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.ir.TestingIr.nullIf;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static io.trino.type.JsonType.JSON;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIrExpressions
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution(createTestTransactionManager(), PLANNER_CONTEXT);
    private static final ResolvedFunction LENGTH = FUNCTIONS.resolveFunction("length", fromTypes(VARCHAR));
    private static final ResolvedFunction SPLIT_PART = FUNCTIONS.resolveFunction("split_part", fromTypes(VARCHAR, VARCHAR, BIGINT));
    private static final ResolvedFunction NEGATION_BIGINT = new TestingFunctionResolution().resolveOperator(NEGATION, ImmutableList.of(BIGINT));
    private static final ResolvedFunction RANDOM = new TestingFunctionResolution().resolveFunction("random", fromTypes());

    @Test
    public void testMayBeNullConstantsAndReferences()
    {
        assertThat(mayBeNull(PLANNER_CONTEXT, new Constant(BIGINT, 1L))).isFalse();
        assertThat(mayBeNull(PLANNER_CONTEXT, constantNull(BIGINT))).isTrue();
        assertThat(mayBeNull(PLANNER_CONTEXT, new Reference(BIGINT, "x"))).isTrue();
        assertThat(mayBeNull(PLANNER_CONTEXT, new IsNull(new Reference(BIGINT, "x")))).isFalse();
    }

    @Test
    public void testMayBeNullCall()
    {
        assertThat(mayBeNull(PLANNER_CONTEXT, not(PLANNER_CONTEXT.getMetadata(), TRUE))).isFalse();
        assertThat(mayBeNull(PLANNER_CONTEXT, not(PLANNER_CONTEXT.getMetadata(), constantNull(BOOLEAN)))).isTrue();

        assertThat(mayBeNull(PLANNER_CONTEXT, new Call(LENGTH, ImmutableList.of(new Constant(VARCHAR, utf8Slice("hello")))))).isFalse();
        assertThat(mayBeNull(PLANNER_CONTEXT, new Call(LENGTH, ImmutableList.of(constantNull(VARCHAR))))).isTrue();
        assertThat(mayBeNull(PLANNER_CONTEXT, new Call(SPLIT_PART, ImmutableList.of(
                new Constant(VARCHAR, utf8Slice("hello")),
                new Constant(VARCHAR, utf8Slice("x")),
                new Constant(BIGINT, 1L))))).isTrue();
    }

    @Test
    public void testMayBeNullCast()
    {
        assertThat(mayBeNull(PLANNER_CONTEXT, new Cast(new Constant(INTEGER, 1L), BIGINT))).isFalse();
        assertThat(mayBeNull(PLANNER_CONTEXT, new Cast(constantNull(INTEGER), BIGINT))).isTrue();
        assertThat(mayBeNull(PLANNER_CONTEXT, new Cast(new Constant(JSON, utf8Slice("null")), BIGINT))).isTrue();
    }

    @Test
    public void testMayBeNullStructuralExpressions()
    {
        assertThat(mayBeNull(PLANNER_CONTEXT, new Coalesce(new Constant(BIGINT, 1L), constantNull(BIGINT)))).isFalse();
        assertThat(mayBeNull(PLANNER_CONTEXT, new Coalesce(constantNull(BIGINT), constantNull(BIGINT)))).isTrue();

        assertThat(mayBeNull(PLANNER_CONTEXT, new Case(
                ImmutableList.of(new WhenClause(new Reference(BOOLEAN, "condition"), new Constant(BIGINT, 1L))),
                new Constant(BIGINT, 2L)))).isFalse();
        assertThat(mayBeNull(PLANNER_CONTEXT, new Case(
                ImmutableList.of(new WhenClause(TRUE, constantNull(BIGINT))),
                new Constant(BIGINT, 1L)))).isTrue();

        assertThat(mayBeNull(PLANNER_CONTEXT, comparison(EQUAL, new Reference(BIGINT, "x"), new Constant(BIGINT, 1L)))).isTrue();
        assertThat(mayBeNull(PLANNER_CONTEXT, comparison(IDENTICAL, new Reference(BIGINT, "x"), constantNull(BIGINT)))).isFalse();
    }

    @Test
    public void testMayReturnNullOnNonNullInput()
    {
        assertThat(mayReturnNullOnNonNullInput(PLANNER_CONTEXT, new Reference(BIGINT, "x"))).isFalse();
        assertThat(mayReturnNullOnNonNullInput(PLANNER_CONTEXT, comparison(EQUAL, new Reference(BIGINT, "x"), new Constant(BIGINT, 1L)))).isFalse();
        assertThat(mayReturnNullOnNonNullInput(PLANNER_CONTEXT, new Cast(new Reference(INTEGER, "x"), BIGINT))).isFalse();
        assertThat(mayReturnNullOnNonNullInput(PLANNER_CONTEXT, new Coalesce(new Reference(BIGINT, "x"), new Constant(BIGINT, 1L)))).isFalse();
        assertThat(mayReturnNullOnNonNullInput(PLANNER_CONTEXT, nullIf(new SymbolAllocator(), new Reference(BIGINT, "x"), new Constant(BIGINT, 1L)))).isTrue();
        assertThat(mayReturnNullOnNonNullInput(PLANNER_CONTEXT, new Cast(new Constant(JSON, utf8Slice("null")), BIGINT))).isTrue();
    }

    @Test
    public void testComparisonRoundTrip()
    {
        Metadata metadata = PLANNER_CONTEXT.getMetadata();
        Reference left = new Reference(BIGINT, "x");
        Constant right = new Constant(BIGINT, 1L);

        // Operators with a dedicated operator function keep their operands as-is.
        assertThat(matchComparison(comparison(EQUAL, left, right))).isEqualTo(new Comparison.Equal(left, right));
        assertThat(matchComparison(comparison(LESS_THAN, left, right))).isEqualTo(new Comparison.LessThan(left, right));
        assertThat(matchComparison(comparison(LESS_THAN_OR_EQUAL, left, right))).isEqualTo(new Comparison.LessThanOrEqual(left, right));
        assertThat(matchComparison(comparison(IDENTICAL, left, right))).isEqualTo(new Comparison.Identical(left, right));

        // NOT_EQUAL is canonicalized to $not of EQUAL, and decoded back to NOT_EQUAL.
        assertThat(matchComparison(comparison(NOT_EQUAL, left, right))).isEqualTo(new Comparison.NotEqual(left, right));

        // Greater-than is canonicalized to less-than with flipped operands.
        assertThat(comparison(GREATER_THAN, left, right)).isEqualTo(comparison(LESS_THAN, right, left));
        assertThat(matchComparison(comparison(GREATER_THAN, left, right))).isEqualTo(new Comparison.LessThan(right, left));
        assertThat(comparison(GREATER_THAN_OR_EQUAL, left, right)).isEqualTo(comparison(LESS_THAN_OR_EQUAL, right, left));
        assertThat(matchComparison(comparison(GREATER_THAN_OR_EQUAL, left, right))).isEqualTo(new Comparison.LessThanOrEqual(right, left));

        // Non-comparison expressions decode to null.
        assertThat(matchComparison(new Reference(BIGINT, "x"))).isNull();
        assertThat(matchComparison(not(metadata, TRUE))).isNull();
        assertThat(matchComparison(new Call(LENGTH, ImmutableList.of(new Constant(VARCHAR, utf8Slice("hello")))))).isNull();
    }

    @Test
    public void testAsBetween()
    {
        // Round-trips the factory's trivial-value form
        assertThat(matchBetween(between(new Reference(BIGINT, "x"), new Constant(BIGINT, 1L), new Constant(BIGINT, 2L))))
                .isEqualTo(new Between(new Reference(BIGINT, "x"), new Constant(BIGINT, 1L), new Constant(BIGINT, 2L)));

        // Round-trips the factory's Let-wrapped form
        Expression value = new Call(NEGATION_BIGINT, ImmutableList.of(new Reference(BIGINT, "x")));
        assertThat(matchBetween(between(value, new Constant(BIGINT, 1L), new Constant(BIGINT, 2L))))
                .isEqualTo(new Between(value, new Constant(BIGINT, 1L), new Constant(BIGINT, 2L)));

        // The bound symbol must not escape through min/max
        assertThat(matchBetween(new Let(
                new Symbol(BIGINT, "s"),
                value,
                new Logical(AND, ImmutableList.of(
                        comparison(GREATER_THAN_OR_EQUAL, new Reference(BIGINT, "s"), new Reference(BIGINT, "s")),
                        comparison(LESS_THAN_OR_EQUAL, new Reference(BIGINT, "s"), new Constant(BIGINT, 10L)))))))
                .isNull();

        // A non-deterministic value evaluated independently in each conjunct is not a BETWEEN
        Expression random = new Call(RANDOM, ImmutableList.of());
        assertThat(matchBetween(new Logical(AND, ImmutableList.of(
                comparison(GREATER_THAN_OR_EQUAL, random, new Constant(DOUBLE, 0.1)),
                comparison(LESS_THAN_OR_EQUAL, random, new Constant(DOUBLE, 0.2))))))
                .isNull();
    }

    @Test
    public void testAsNullIf()
    {
        // Round-trips the factory's trivial-value form
        assertThat(matchNullIf(nullIf(new SymbolAllocator(), new Reference(BIGINT, "x"), new Constant(BIGINT, 1L))))
                .isEqualTo(new NullIf(new Reference(BIGINT, "x"), new Constant(BIGINT, 1L)));

        // Round-trips the factory's Let-wrapped form
        Expression first = new Call(NEGATION_BIGINT, ImmutableList.of(new Reference(BIGINT, "x")));
        assertThat(matchNullIf(nullIf(new SymbolAllocator(), first, new Constant(BIGINT, 1L))))
                .isEqualTo(new NullIf(first, new Constant(BIGINT, 1L)));

        // The bound symbol must not escape through second
        assertThat(matchNullIf(new Let(
                new Symbol(BIGINT, "s"),
                first,
                ifExpression(
                        comparison(EQUAL, new Reference(BIGINT, "s"), new Reference(BIGINT, "s")),
                        constantNull(BIGINT),
                        new Reference(BIGINT, "s")))))
                .isNull();

        // A non-deterministic first operand evaluated independently in the condition and the
        // default is not a NULLIF
        Expression random = new Call(RANDOM, ImmutableList.of());
        assertThat(matchNullIf(ifExpression(
                comparison(EQUAL, random, new Constant(DOUBLE, 0.1)),
                constantNull(DOUBLE),
                random)))
                .isNull();
    }
}

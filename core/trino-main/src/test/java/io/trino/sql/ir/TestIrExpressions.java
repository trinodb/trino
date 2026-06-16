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
import io.trino.sql.ir.IrExpressions.ComparisonView;
import org.junit.jupiter.api.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
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
import static io.trino.sql.ir.IrExpressions.comparison;
import static io.trino.sql.ir.IrExpressions.constantNull;
import static io.trino.sql.ir.IrExpressions.matchComparison;
import static io.trino.sql.ir.IrExpressions.mayBeNull;
import static io.trino.sql.ir.IrExpressions.mayReturnNullOnNonNullInput;
import static io.trino.sql.ir.IrExpressions.not;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static io.trino.type.JsonType.JSON;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIrExpressions
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution(createTestTransactionManager(), PLANNER_CONTEXT);
    private static final ResolvedFunction LENGTH = FUNCTIONS.resolveFunction("length", fromTypes(VARCHAR));
    private static final ResolvedFunction SPLIT_PART = FUNCTIONS.resolveFunction("split_part", fromTypes(VARCHAR, VARCHAR, BIGINT));

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

        assertThat(mayBeNull(PLANNER_CONTEXT, new Comparison(EQUAL, new Reference(BIGINT, "x"), new Constant(BIGINT, 1L)))).isTrue();
        assertThat(mayBeNull(PLANNER_CONTEXT, new Comparison(IDENTICAL, new Reference(BIGINT, "x"), constantNull(BIGINT)))).isFalse();
    }

    @Test
    public void testMayReturnNullOnNonNullInput()
    {
        assertThat(mayReturnNullOnNonNullInput(PLANNER_CONTEXT, new Reference(BIGINT, "x"))).isFalse();
        assertThat(mayReturnNullOnNonNullInput(PLANNER_CONTEXT, new Comparison(EQUAL, new Reference(BIGINT, "x"), new Constant(BIGINT, 1L)))).isFalse();
        assertThat(mayReturnNullOnNonNullInput(PLANNER_CONTEXT, new Cast(new Reference(INTEGER, "x"), BIGINT))).isFalse();
        assertThat(mayReturnNullOnNonNullInput(PLANNER_CONTEXT, new Coalesce(new Reference(BIGINT, "x"), new Constant(BIGINT, 1L)))).isFalse();
        assertThat(mayReturnNullOnNonNullInput(PLANNER_CONTEXT, new NullIf(new Reference(BIGINT, "x"), new Constant(BIGINT, 1L)))).isTrue();
        assertThat(mayReturnNullOnNonNullInput(PLANNER_CONTEXT, new Cast(new Constant(JSON, utf8Slice("null")), BIGINT))).isTrue();
    }

    @Test
    public void testComparisonRoundTrip()
    {
        Metadata metadata = PLANNER_CONTEXT.getMetadata();
        Reference left = new Reference(BIGINT, "x");
        Constant right = new Constant(BIGINT, 1L);

        // Operators with a dedicated operator function keep their operands as-is.
        assertThat(matchComparison(comparison(metadata, EQUAL, left, right))).isEqualTo(new ComparisonView.Equal(left, right));
        assertThat(matchComparison(comparison(metadata, LESS_THAN, left, right))).isEqualTo(new ComparisonView.LessThan(left, right));
        assertThat(matchComparison(comparison(metadata, LESS_THAN_OR_EQUAL, left, right))).isEqualTo(new ComparisonView.LessThanOrEqual(left, right));
        assertThat(matchComparison(comparison(metadata, IDENTICAL, left, right))).isEqualTo(new ComparisonView.Identical(left, right));

        // NOT_EQUAL is canonicalized to $not of EQUAL, and decoded back to NOT_EQUAL.
        assertThat(matchComparison(comparison(metadata, NOT_EQUAL, left, right))).isEqualTo(new ComparisonView.NotEqual(left, right));

        // Greater-than is canonicalized to less-than with flipped operands.
        assertThat(comparison(metadata, GREATER_THAN, left, right)).isEqualTo(comparison(metadata, LESS_THAN, right, left));
        assertThat(matchComparison(comparison(metadata, GREATER_THAN, left, right))).isEqualTo(new ComparisonView.LessThan(right, left));
        assertThat(comparison(metadata, GREATER_THAN_OR_EQUAL, left, right)).isEqualTo(comparison(metadata, LESS_THAN_OR_EQUAL, right, left));
        assertThat(matchComparison(comparison(metadata, GREATER_THAN_OR_EQUAL, left, right))).isEqualTo(new ComparisonView.LessThanOrEqual(right, left));

        // Non-comparison expressions decode to null.
        assertThat(matchComparison(new Reference(BIGINT, "x"))).isNull();
        assertThat(matchComparison(not(metadata, TRUE))).isNull();
        assertThat(matchComparison(new Call(LENGTH, ImmutableList.of(new Constant(VARCHAR, utf8Slice("hello")))))).isNull();
    }
}

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
package io.trino.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.function.OperatorType;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IsNull;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ApplyNode;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.DataOrganizationSpecification;
import io.trino.sql.planner.plan.IndexJoinNode;
import io.trino.sql.planner.plan.JoinNode.EquiJoinClause;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.SpatialJoinNode;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.type.CharVarcharCoercion;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Set;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.IrExpressions.not;
import static io.trino.sql.ir.IrUtils.and;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.sql.planner.optimizations.NonNullDerivation.deriveNonNullSymbols;
import static io.trino.sql.planner.plan.JoinType.FULL;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.sql.planner.plan.JoinType.LEFT;
import static io.trino.sql.planner.plan.WindowNode.Frame.DEFAULT_FRAME;
import static io.trino.testing.TestingSession.testSession;
import static org.assertj.core.api.Assertions.assertThat;

public class TestNonNullDerivation
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final CharVarcharCoercion CHAR_VARCHAR_COERCION = getCharVarcharCoercion(TEST_SESSION);

    private final PlanBuilder planBuilder = new PlanBuilder(new PlanNodeIdAllocator(), PLANNER_CONTEXT, testSession());

    @Test
    public void testFilterNullRejection()
    {
        PlanBuilder p = planBuilder;
        Symbol a = p.symbol("a", BIGINT);
        Symbol b = p.symbol("b", BIGINT);

        assertThat(derive(p.filter(
                comparison(GREATER_THAN, a.toSymbolReference(), new Constant(BIGINT, 5L)),
                p.values(1, a, b))))
                .describedAs("comparison rejects null")
                .containsExactly(a);

        assertThat(derive(p.filter(
                not(PLANNER_CONTEXT.getMetadata(), CHAR_VARCHAR_COERCION, new IsNull(a.toSymbolReference())),
                p.values(1, a, b))))
                .describedAs("IS NOT NULL rejects null")
                .containsExactly(a);

        assertThat(derive(p.filter(
                new IsNull(a.toSymbolReference()),
                p.values(1, a, b))))
                .describedAs("IS NULL keeps null rows")
                .isEmpty();

        Symbol d = p.symbol("d", DOUBLE);
        assertThat(derive(p.filter(
                and(
                        comparison(GREATER_THAN, a.toSymbolReference(), new Constant(BIGINT, 5L)),
                        comparison(GREATER_THAN, d.toSymbolReference(), random())),
                p.values(1, a, d))))
                .describedAs("non-deterministic conjunct is ignored")
                .containsExactly(a);
    }

    @Test
    public void testProject()
    {
        PlanBuilder p = planBuilder;
        Symbol a = p.symbol("a", BIGINT);
        Symbol b = p.symbol("b", BIGINT);
        Symbol sum = p.symbol("sum", BIGINT);
        Symbol defaulted = p.symbol("defaulted", BIGINT);
        Symbol passthrough = p.symbol("passthrough", BIGINT);

        PlanNode source = p.filter(
                comparison(GREATER_THAN, a.toSymbolReference(), new Constant(BIGINT, 5L)),
                p.values(1, a, b));

        assertThat(derive(p.project(
                Assignments.builder()
                        .put(sum, add(a))
                        .put(defaulted, new Coalesce(b.toSymbolReference(), new Constant(BIGINT, 0L)))
                        .put(passthrough, b.toSymbolReference())
                        .build(),
                source)))
                .containsExactlyInAnyOrder(sum, defaulted);
    }

    @Test
    public void testJoin()
    {
        PlanBuilder p = planBuilder;
        Symbol a = p.symbol("a", BIGINT);
        Symbol b = p.symbol("b", BIGINT);

        PlanNode left = p.filter(
                comparison(GREATER_THAN, a.toSymbolReference(), new Constant(BIGINT, 5L)),
                p.values(1, a));
        PlanNode right = p.values(1, b);

        assertThat(derive(p.join(INNER, left, right, new EquiJoinClause(a, b))))
                .describedAs("inner join: both sides, equi-criteria reject nulls")
                .containsExactlyInAnyOrder(a, b);

        assertThat(derive(p.join(LEFT, left, right, new EquiJoinClause(a, b))))
                .describedAs("left join: right side is null-extended")
                .containsExactly(a);

        assertThat(derive(p.join(FULL, left, right, new EquiJoinClause(a, b))))
                .describedAs("full join: both sides are null-extended")
                .isEmpty();

        assertThat(derive(p.join(INNER, left, right, comparison(GREATER_THAN, b.toSymbolReference(), a.toSymbolReference()))))
                .describedAs("inner join filter rejects nulls like a filter predicate")
                .containsExactlyInAnyOrder(a, b);

        assertThat(derive(p.join(
                INNER,
                left,
                right,
                ImmutableList.of(new EquiJoinClause(a, b)),
                ImmutableList.of(),
                ImmutableList.of(b),
                Optional.empty())))
                .describedAs("result is restricted to the node's output symbols")
                .containsExactly(b);
    }

    @Test
    public void testAggregation()
    {
        PlanBuilder p = planBuilder;
        Symbol key = p.symbol("key", BIGINT);
        Symbol value = p.symbol("value", BIGINT);
        Symbol count = p.symbol("count", BIGINT);
        Symbol sum = p.symbol("sum", BIGINT);

        PlanNode source = p.filter(
                comparison(GREATER_THAN, key.toSymbolReference(), new Constant(BIGINT, 5L)),
                p.values(1, key, value));

        assertThat(derive(p.aggregation(aggregation -> aggregation
                .singleGroupingSet(key)
                .addAggregation(count, new AggregationNode.Aggregation(
                        FUNCTIONS.resolveFunction("count", fromTypes(BIGINT)),
                        ImmutableList.of(value.toSymbolReference()),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()))
                .addAggregation(sum, new AggregationNode.Aggregation(
                        FUNCTIONS.resolveFunction("sum", fromTypes(BIGINT)),
                        ImmutableList.of(value.toSymbolReference()),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()))
                .source(source))))
                .describedAs("count is never null, sum is null on empty input, non-null key passes through")
                .containsExactlyInAnyOrder(key, count);
    }

    @Test
    public void testGroupId()
    {
        PlanBuilder p = planBuilder;
        Symbol a = p.symbol("a", BIGINT);
        Symbol b = p.symbol("b", BIGINT);
        Symbol argument = p.symbol("argument", BIGINT);
        Symbol groupId = p.symbol("group_id", BIGINT);

        PlanNode source = p.filter(
                and(
                        comparison(GREATER_THAN, a.toSymbolReference(), new Constant(BIGINT, 5L)),
                        comparison(GREATER_THAN, b.toSymbolReference(), new Constant(BIGINT, 5L)),
                        comparison(GREATER_THAN, argument.toSymbolReference(), new Constant(BIGINT, 5L))),
                p.values(1, a, b, argument));

        assertThat(derive(p.groupId(
                ImmutableList.of(ImmutableList.of(a, b), ImmutableList.of(a)),
                ImmutableList.of(argument),
                groupId,
                source)))
                .describedAs("b is nulled out in the grouping set that omits it")
                .containsExactlyInAnyOrder(a, argument, groupId);
    }

    @Test
    public void testUnion()
    {
        PlanBuilder p = planBuilder;
        Symbol a = p.symbol("a", BIGINT);
        Symbol b = p.symbol("b", BIGINT);
        Symbol output = p.symbol("output", BIGINT);

        PlanNode nonNullSource = p.filter(
                comparison(GREATER_THAN, a.toSymbolReference(), new Constant(BIGINT, 5L)),
                p.values(1, a));
        PlanNode nonNullSecond = p.filter(
                comparison(GREATER_THAN, b.toSymbolReference(), new Constant(BIGINT, 5L)),
                p.values(1, b));
        PlanNode nullableSecond = p.values(1, b);

        assertThat(derive(p.union(
                ImmutableListMultimap.of(output, a, output, b),
                ImmutableList.of(nonNullSource, nonNullSecond))))
                .describedAs("non-null in every source")
                .containsExactly(output);

        assertThat(derive(p.union(
                ImmutableListMultimap.of(output, a, output, b),
                ImmutableList.of(nonNullSource, nullableSecond))))
                .describedAs("nullable in one source")
                .isEmpty();
    }

    @Test
    public void testValues()
    {
        PlanBuilder p = planBuilder;
        Symbol a = p.symbol("a", BIGINT);
        Symbol b = p.symbol("b", BIGINT);

        assertThat(derive(p.values(
                ImmutableList.of(a, b),
                ImmutableList.of(
                        ImmutableList.of(new Constant(BIGINT, 1L), new Constant(BIGINT, 2L)),
                        ImmutableList.of(new Constant(BIGINT, 3L), new Constant(BIGINT, (Long) null))))))
                .describedAs("column with a null literal is excluded")
                .containsExactly(a);
    }

    @Test
    public void testSemiJoin()
    {
        PlanBuilder p = planBuilder;
        Symbol a = p.symbol("a", BIGINT);
        Symbol b = p.symbol("b", BIGINT);
        Symbol match = p.symbol("match", BIGINT);

        PlanNode source = p.filter(
                comparison(GREATER_THAN, a.toSymbolReference(), new Constant(BIGINT, 5L)),
                p.values(1, a));

        assertThat(derive(p.semiJoin(a, b, match, source, p.values(1, b))))
                .describedAs("source symbols pass through, the match symbol may be null")
                .containsExactly(a);
    }

    @Test
    public void testCorrelatedJoin()
    {
        PlanBuilder p = planBuilder;
        Symbol a = p.symbol("a", BIGINT);
        Symbol b = p.symbol("b", BIGINT);

        PlanNode input = p.filter(
                comparison(GREATER_THAN, a.toSymbolReference(), new Constant(BIGINT, 5L)),
                p.values(1, a));
        PlanNode subquery = p.filter(
                comparison(GREATER_THAN, b.toSymbolReference(), new Constant(BIGINT, 5L)),
                p.values(1, b));

        assertThat(derive(p.correlatedJoin(ImmutableList.of(a), input, INNER, TRUE, subquery)))
                .describedAs("inner correlated join: both sides")
                .containsExactlyInAnyOrder(a, b);

        assertThat(derive(p.correlatedJoin(ImmutableList.of(a), input, LEFT, TRUE, subquery)))
                .describedAs("left correlated join: subquery side is null-extended")
                .containsExactly(a);
    }

    @Test
    public void testApply()
    {
        PlanBuilder p = planBuilder;
        Symbol a = p.symbol("a", BIGINT);
        Symbol b = p.symbol("b", BIGINT);
        Symbol exists = p.symbol("exists", BOOLEAN);
        Symbol in = p.symbol("in", BOOLEAN);

        PlanNode input = p.filter(
                comparison(GREATER_THAN, a.toSymbolReference(), new Constant(BIGINT, 5L)),
                p.values(1, a));

        assertThat(derive(p.apply(
                ImmutableMap.of(exists, new ApplyNode.Exists(), in, new ApplyNode.In(a, b)),
                ImmutableList.of(),
                input,
                p.values(1, b))))
                .describedAs("input symbols pass through, EXISTS is never null, IN may be null")
                .containsExactlyInAnyOrder(a, exists);
    }

    @Test
    public void testWindow()
    {
        PlanBuilder p = planBuilder;
        Symbol a = p.symbol("a", BIGINT);
        Symbol rowNumber = p.symbol("row_number", BIGINT);
        Symbol lag = p.symbol("lag", BIGINT);

        PlanNode source = p.filter(
                comparison(GREATER_THAN, a.toSymbolReference(), new Constant(BIGINT, 5L)),
                p.values(1, a));

        assertThat(derive(p.window(
                new DataOrganizationSpecification(ImmutableList.of(), Optional.empty()),
                ImmutableMap.of(
                        rowNumber, windowFunction(FUNCTIONS.resolveFunction("row_number", fromTypes())),
                        lag, windowFunction(FUNCTIONS.resolveFunction("lag", fromTypes(BIGINT)), a.toSymbolReference())),
                source)))
                .describedAs("row_number declares a non-null result, lag does not")
                .containsExactlyInAnyOrder(a, rowNumber);
    }

    @Test
    public void testSpatialJoin()
    {
        PlanBuilder p = planBuilder;
        Symbol a = p.symbol("a", BIGINT);
        Symbol b = p.symbol("b", BIGINT);

        PlanNode left = p.filter(
                comparison(GREATER_THAN, a.toSymbolReference(), new Constant(BIGINT, 5L)),
                p.values(1, a));
        PlanNode right = p.filter(
                comparison(GREATER_THAN, b.toSymbolReference(), new Constant(BIGINT, 5L)),
                p.values(1, b));

        assertThat(derive(p.spatialJoin(SpatialJoinNode.Type.INNER, left, right, ImmutableList.of(a, b), TRUE)))
                .describedAs("inner spatial join: both sides")
                .containsExactlyInAnyOrder(a, b);

        assertThat(derive(p.spatialJoin(SpatialJoinNode.Type.LEFT, left, right, ImmutableList.of(a, b), TRUE)))
                .describedAs("left spatial join: right side is null-extended")
                .containsExactly(a);
    }

    @Test
    public void testIndexJoin()
    {
        PlanBuilder p = planBuilder;
        Symbol a = p.symbol("a", BIGINT);
        Symbol b = p.symbol("b", BIGINT);

        PlanNode probe = p.filter(
                comparison(GREATER_THAN, a.toSymbolReference(), new Constant(BIGINT, 5L)),
                p.values(1, a));
        PlanNode index = p.values(1, b);

        assertThat(derive(p.indexJoin(
                IndexJoinNode.Type.INNER,
                probe,
                index,
                ImmutableList.of(new IndexJoinNode.EquiJoinClause(a, b)))))
                .describedAs("inner index join: probe side facts, equi-criteria reject nulls")
                .containsExactlyInAnyOrder(a, b);

        assertThat(derive(p.indexJoin(
                IndexJoinNode.Type.SOURCE_OUTER,
                probe,
                index,
                ImmutableList.of(new IndexJoinNode.EquiJoinClause(a, b)))))
                .describedAs("source-outer index join: index side is null-extended")
                .containsExactly(a);
    }

    @Test
    public void testEnforceSingleRow()
    {
        PlanBuilder p = planBuilder;
        Symbol a = p.symbol("a", BIGINT);

        PlanNode source = p.filter(
                comparison(GREATER_THAN, a.toSymbolReference(), new Constant(BIGINT, 5L)),
                p.values(1, a));

        assertThat(derive(p.enforceSingleRow(source)))
                .describedAs("an empty source produces a single all-null row")
                .isEmpty();
    }

    @Test
    public void testUnhandledNodeIsSound()
    {
        PlanBuilder p = planBuilder;
        Symbol a = p.symbol("a", BIGINT);

        PlanNode source = p.filter(
                comparison(GREATER_THAN, a.toSymbolReference(), new Constant(BIGINT, 5L)),
                p.values(1, a));

        assertThat(derive(p.tableWriter(ImmutableList.of(a), ImmutableList.of("a"), source))).isEmpty();
    }

    private Set<Symbol> derive(PlanNode node)
    {
        return deriveNonNullSymbols(PLANNER_CONTEXT, testSession(), node);
    }

    private static Expression add(Symbol symbol)
    {
        return new Call(
                FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(BIGINT, BIGINT)),
                ImmutableList.of(symbol.toSymbolReference(), new Constant(BIGINT, 1L)));
    }

    private static Expression random()
    {
        return new Call(FUNCTIONS.resolveFunction("random", ImmutableList.of()), ImmutableList.of());
    }

    private static WindowNode.Function windowFunction(ResolvedFunction function, Expression... arguments)
    {
        return new WindowNode.Function(
                function,
                ImmutableList.copyOf(arguments),
                Optional.empty(),
                DEFAULT_FRAME,
                false,
                false);
    }
}

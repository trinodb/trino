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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.block.Block;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.SqlRow;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.In;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import io.trino.sql.planner.PlanOptimizers;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.SpatialJoinNode;
import io.trino.sql.planner.plan.TableScanNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Optional;
import java.util.stream.Stream;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.JoinType.INNER;

class TestPredicatePushdownRules
        extends BaseRuleTest
{
    @Test
    public void testPushThroughSymbolProjection()
    {
        tester().assertThat(new PushFilterThroughProject(tester().getPlannerContext(), false, false))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.filter(greaterThanTen(b), p.project(Assignments.of(b, a.toSymbolReference()), p.values(a)));
                })
                .matches(project(
                        ImmutableMap.of("b", expression(new Reference(BIGINT, "a"))),
                        filter(greaterThanTen(new Symbol(BIGINT, "a")), values("a"))));
    }

    @Test
    public void testRepeatedExpressionIsNotInlined()
    {
        tester().assertThat(new PushFilterThroughProject(tester().getPlannerContext(), false, false))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    var add = new TestingFunctionResolution().resolveOperator(OperatorType.ADD, ImmutableList.of(BIGINT, BIGINT));
                    return p.filter(
                            comparison(GREATER_THAN, b.toSymbolReference(), b.toSymbolReference()),
                            p.project(Assignments.of(b, new Call(add, ImmutableList.of(a.toSymbolReference(), new Constant(BIGINT, 1L)))), p.values(a)));
                })
                .doesNotFire();
    }

    @ParameterizedTest
    @MethodSource("predicatePushdownRules")
    public void testLimitIsBarrier(Rule<?> rule)
    {
        tester().assertThat(rule)
                .on(p -> {
                    Symbol a = p.symbol("a");
                    return p.filter(greaterThanTen(a), p.limit(5, p.values(a)));
                })
                .doesNotFire();
    }

    @Test
    public void testGlobalAggregationIsBarrier()
    {
        tester().assertThat(new PushFilterThroughAggregation(tester().getPlannerContext(), false, false))
                .on(p -> p.filter(FALSE, p.aggregation(aggregation -> aggregation.globalGrouping().source(p.values()))))
                .doesNotFire();
    }

    @Test
    public void testPushGroupingKeyThroughAggregation()
    {
        tester().assertThat(new PushFilterThroughAggregation(tester().getPlannerContext(), false, false))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    return p.filter(greaterThanTen(a), p.aggregation(aggregation -> aggregation
                            .singleGroupingSet(a)
                            .source(p.tableScan(ImmutableList.of(a), false))));
                })
                .matches(node(AggregationNode.class, filter(
                        greaterThanTen(new Symbol(BIGINT, "a")),
                        node(TableScanNode.class).withAlias("a", (node, _, _, _) -> Optional.of(node.getOutputSymbols().getFirst())))));
    }

    @Test
    public void testAlreadyConstrainedJoinThroughProjectionDoesNotFire()
    {
        tester().assertThat(new PushJoinPredicates(tester().getPlannerContext(), false, false))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol c = p.symbol("c");
                    var add = new TestingFunctionResolution().resolveOperator(OperatorType.ADD, ImmutableList.of(BIGINT, BIGINT));
                    Expression assignment = new Call(add, ImmutableList.of(c.toSymbolReference(), new Constant(BIGINT, 1L)));
                    return p.join(
                            INNER,
                            p.filter(greaterThanTen(a), p.tableScan(ImmutableList.of(a), false)),
                            p.project(Assignments.of(b, assignment), p.filter(
                                    comparison(GREATER_THAN, assignment, new Constant(BIGINT, 10L)),
                                    p.tableScan(ImmutableList.of(c), false))),
                            new JoinNode.EquiJoinClause(a, b));
                })
                .doesNotFire();
    }

    @Test
    public void testAlreadyConstrainedJoinDoesNotFire()
    {
        tester().assertThat(new PushJoinPredicates(tester().getPlannerContext(), false, false))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.join(
                            INNER,
                            p.filter(greaterThanTen(a), p.tableScan(ImmutableList.of(a), false)),
                            p.filter(greaterThanTen(b), p.tableScan(ImmutableList.of(b), false)),
                            new JoinNode.EquiJoinClause(a, b));
                })
                .doesNotFire();
    }

    @Test
    public void testAlreadyConstrainedJoinWithSimplifiablePredicateDoesNotFire()
    {
        tester().assertThat(new PushJoinPredicates(tester().getPlannerContext(), false, false))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    ImmutableList<Expression> values = ImmutableList.of(new Constant(BIGINT, 1L), new Constant(BIGINT, 2L));
                    return p.join(
                            INNER,
                            p.filter(new In(a.toSymbolReference(), values), p.tableScan(ImmutableList.of(a), false)),
                            p.filter(new In(b.toSymbolReference(), values), p.tableScan(ImmutableList.of(b), false)),
                            new JoinNode.EquiJoinClause(a, b));
                })
                .doesNotFire();
    }

    @Test
    public void testAlreadyConstrainedJoinWithArrayConstantsDoesNotFire()
    {
        tester().assertThat(new PushJoinPredicates(tester().getPlannerContext(), false, false))
                .on(p -> {
                    ArrayType type = new ArrayType(BIGINT);
                    Symbol a = p.symbol("a", type);
                    Symbol b = p.symbol("b", type);
                    return p.join(
                            INNER,
                            p.filter(new In(a.toSymbolReference(), ImmutableList.of(arrayConstant(1), arrayConstant(2))), p.tableScan(ImmutableList.of(a), false)),
                            p.filter(new In(b.toSymbolReference(), ImmutableList.of(arrayConstant(1), arrayConstant(2))), p.tableScan(ImmutableList.of(b), false)),
                            new JoinNode.EquiJoinClause(a, b));
                })
                .doesNotFire();
    }

    @ParameterizedTest
    @EnumSource(value = JoinType.class, names = {"LEFT", "RIGHT", "FULL"})
    public void testInputPredicateDoesNotConstrainNullExtendedOutput(JoinType joinType)
    {
        var constrainedInput = filter(
                greaterThanTen(new Symbol(BIGINT, "c")),
                node(TableScanNode.class).withAlias("c", (node, _, _, _) -> Optional.of(node.getOutputSymbols().getFirst())));
        var otherInput = node(TableScanNode.class);

        tester().assertThat(new PushFilterThroughJoin(tester().getPlannerContext(), false, false))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b", DOUBLE);
                    Symbol c = p.symbol("c");
                    Symbol d = p.symbol("d");
                    var constrained = p.filter(greaterThanTen(c), p.tableScan(ImmutableList.of(c), false));
                    var other = p.tableScan(ImmutableList.of(d), false);
                    var nested = p.join(
                            joinType,
                            joinType == JoinType.RIGHT ? constrained : other,
                            joinType == JoinType.RIGHT ? other : constrained,
                            joinType == JoinType.RIGHT ? new JoinNode.EquiJoinClause(c, d) : new JoinNode.EquiJoinClause(d, c));
                    return p.filter(
                            comparison(GREATER_THAN, b.toSymbolReference(), new Constant(DOUBLE, 10.0)),
                            p.join(
                                    INNER,
                                    p.tableScan(ImmutableList.of(a), false),
                                    p.project(Assignments.of(b, new Cast(c.toSymbolReference(), DOUBLE), d, d.toSymbolReference()), nested),
                                    new JoinNode.EquiJoinClause(a, d)));
                })
                .matches(node(JoinNode.class,
                        node(TableScanNode.class),
                        filter(
                                comparison(GREATER_THAN, new Reference(DOUBLE, "b"), new Constant(DOUBLE, 10.0)),
                                node(ProjectNode.class,
                                        node(JoinNode.class,
                                                joinType == JoinType.RIGHT ? constrainedInput : otherInput,
                                                joinType == JoinType.RIGHT ? otherInput : constrainedInput))
                                        .withAlias("b", (node, _, _, _) -> Optional.of(node.getOutputSymbols().getFirst())))));
    }

    @ParameterizedTest
    @EnumSource(value = JoinType.class, names = {"LEFT", "RIGHT", "FULL"})
    public void testJoinFilterDoesNotConstrainNullExtendedOutput(JoinType joinType)
    {
        tester().assertThat(new PushFilterThroughJoin(tester().getPlannerContext(), false, false))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b", DOUBLE);
                    Symbol c = p.symbol("c");
                    Symbol d = p.symbol("d");
                    Symbol joinKey = joinType == JoinType.RIGHT ? d : c;
                    Expression coalesce = new Coalesce(c.toSymbolReference(), d.toSymbolReference());
                    var nested = p.join(
                            joinType,
                            p.tableScan(ImmutableList.of(c), false),
                            p.tableScan(ImmutableList.of(d), false),
                            comparison(GREATER_THAN, coalesce, new Constant(BIGINT, 10L)),
                            new JoinNode.EquiJoinClause(c, d));
                    return p.filter(
                            comparison(GREATER_THAN, b.toSymbolReference(), new Constant(DOUBLE, 10.0)),
                            p.join(
                                    INNER,
                                    p.tableScan(ImmutableList.of(a), false),
                                    p.project(Assignments.of(b, new Cast(coalesce, DOUBLE), joinKey, joinKey.toSymbolReference()), nested),
                                    new JoinNode.EquiJoinClause(a, joinKey)));
                })
                .matches(node(JoinNode.class,
                        node(TableScanNode.class),
                        filter(
                                comparison(GREATER_THAN, new Reference(DOUBLE, "b"), new Constant(DOUBLE, 10.0)),
                                node(ProjectNode.class, node(JoinNode.class, node(TableScanNode.class), node(TableScanNode.class)))
                                        .withAlias("b", (node, _, _, _) -> Optional.of(node.getOutputSymbols().getFirst())))));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testSpatialJoinPredicateDoesNotConstrainNullExtendedOutput(boolean predicateInJoinFilter)
    {
        tester().assertThat(new PushFilterThroughJoin(tester().getPlannerContext(), false, false))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b", DOUBLE);
                    Symbol c = p.symbol("c");
                    Symbol d = p.symbol("d");
                    Expression projection = predicateInJoinFilter ? new Coalesce(c.toSymbolReference(), d.toSymbolReference()) : c.toSymbolReference();
                    Expression predicate = comparison(GREATER_THAN, projection, new Constant(BIGINT, 10L));
                    var right = p.tableScan(ImmutableList.of(c), false);
                    var nested = p.spatialJoin(
                            SpatialJoinNode.Type.LEFT,
                            p.tableScan(ImmutableList.of(d), false),
                            predicateInJoinFilter ? right : p.filter(predicate, right),
                            ImmutableList.of(c, d),
                            predicateInJoinFilter ? predicate : TRUE);
                    return p.filter(
                            comparison(GREATER_THAN, b.toSymbolReference(), new Constant(DOUBLE, 10.0)),
                            p.join(
                                    INNER,
                                    p.tableScan(ImmutableList.of(a), false),
                                    p.project(Assignments.of(b, new Cast(projection, DOUBLE), d, d.toSymbolReference()), nested),
                                    new JoinNode.EquiJoinClause(a, d)));
                })
                .matches(node(JoinNode.class,
                        node(TableScanNode.class),
                        filter(
                                comparison(GREATER_THAN, new Reference(DOUBLE, "b"), new Constant(DOUBLE, 10.0)),
                                node(ProjectNode.class,
                                        node(SpatialJoinNode.class,
                                                node(TableScanNode.class),
                                                predicateInJoinFilter ? node(TableScanNode.class) : node(FilterNode.class, node(TableScanNode.class))))
                                        .withAlias("b", (node, _, _, _) -> Optional.of(node.getOutputSymbols().getFirst())))));
    }

    private static Constant arrayConstant(long value)
    {
        return new Constant(new ArrayType(BIGINT), new LongArrayBlock(1, Optional.empty(), new long[] {value}));
    }

    @Test
    public void testCanonicalJoinFilterDoesNotFire()
    {
        tester().assertThat(new PushJoinPredicates(tester().getPlannerContext(), false, false))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.join(
                            JoinType.LEFT,
                            p.tableScan(ImmutableList.of(a), false),
                            p.tableScan(ImmutableList.of(b), false),
                            comparison(EQUAL, a.toSymbolReference(), new Constant(BIGINT, 3L)));
                })
                .doesNotFire();
    }

    @ParameterizedTest
    @CsvSource({"false, true", "true, true", "false, false", "true, false"})
    public void testPredicateSatisfiedByValues(boolean constantRows, boolean allRowsMatch)
    {
        tester().assertThat(new PushJoinPredicates(tester().getPlannerContext(), false, false))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.join(
                            INNER,
                            p.filter(remainderGreaterThanFive(a), p.tableScan(ImmutableList.of(a), false)),
                            p.valuesOfExpressions(ImmutableList.of(b), ImmutableList.of(
                                    valuesRow(17, constantRows),
                                    valuesRow(allRowsMatch ? 18 : 12, constantRows))),
                            new JoinNode.EquiJoinClause(a, b));
                })
                .matches(node(JoinNode.class,
                        // MergeFilters combines the inferred filter with the existing filter in a separate rule application.
                        node(FilterNode.class, node(FilterNode.class, node(TableScanNode.class))),
                        allRowsMatch ? values("b") : filter(remainderGreaterThanFive(new Symbol(BIGINT, "b")), values("b"))));
    }

    private static Expression valuesRow(long value, boolean constantRow)
    {
        if (constantRow) {
            return new Constant(RowType.anonymous(ImmutableList.of(BIGINT)), new SqlRow(0, new Block[] {new LongArrayBlock(1, Optional.empty(), new long[] {value})}));
        }
        return new Row(ImmutableList.of(new Constant(BIGINT, value)));
    }

    private static Expression remainderGreaterThanFive(Symbol symbol)
    {
        var modulus = new TestingFunctionResolution().resolveOperator(OperatorType.MODULO, ImmutableList.of(BIGINT, BIGINT));
        return comparison(GREATER_THAN, new Call(modulus, ImmutableList.of(symbol.toSymbolReference(), new Constant(BIGINT, 10L))), new Constant(BIGINT, 5L));
    }

    private Stream<Rule<?>> predicatePushdownRules()
    {
        return PlanOptimizers.predicatePushdownRules(tester().getPlannerContext(), false, false).stream();
    }

    private static Expression greaterThanTen(Symbol symbol)
    {
        return comparison(GREATER_THAN, symbol.toSymbolReference(), new Constant(BIGINT, 10L));
    }
}

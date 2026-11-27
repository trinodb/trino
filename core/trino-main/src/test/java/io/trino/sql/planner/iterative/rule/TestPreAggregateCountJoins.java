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
import com.google.common.collect.ImmutableSet;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.cost.SymbolStatsEstimate;
import io.trino.operator.aggregation.CountJoinAggregation;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.iterative.rule.test.RuleBuilder;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.JoinNode.EquiJoinClause;
import io.trino.sql.planner.plan.PlanNodeId;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Optional;

import static io.trino.SystemSessionProperties.PRE_AGGREGATE_COUNT_JOINS;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregationFunction;
import static io.trino.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.sql.planner.plan.JoinType.LEFT;
import static io.trino.type.CharVarcharCoercion.SQL_STANDARD;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPreAggregateCountJoins
        extends BaseRuleTest
{
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testRewrite(boolean global)
    {
        rule(2, 1000)
                .on(p -> plan(p, global ? "global" : "grouped"))
                .matches(aggregation(
                        global ? singleGroupingSet() : singleGroupingSet("a", "b"),
                        ImmutableMap.of(Optional.of("result"), aggregationFunction(CountJoinAggregation.NAME, ImmutableList.of("lc", "rc"))),
                        Optional.empty(),
                        SINGLE,
                        join(INNER, builder -> builder
                                .equiCriteria(ImmutableList.of(equiJoinClause("lk", "rk"), equiJoinClause("lk2", "rk2")))
                                .left(aggregation(
                                        global ? singleGroupingSet("lk", "lk2") : singleGroupingSet("lk", "lk2", "a"),
                                        ImmutableMap.of(Optional.of("lc"), aggregationFunction("count", ImmutableList.of())),
                                        Optional.empty(),
                                        SINGLE,
                                        values("lk", "lk2", "a", "mask")))
                                .right(aggregation(
                                        global ? singleGroupingSet("rk", "rk2") : singleGroupingSet("rk", "rk2", "b"),
                                        ImmutableMap.of(Optional.of("rc"), aggregationFunction("count", ImmutableList.of())),
                                        Optional.empty(),
                                        SINGLE,
                                        values("rk", "rk2", "b"))))));
    }

    @ParameterizedTest
    @ValueSource(strings = {"outer", "cross", "residual", "partial", "distinct", "filter", "mask", "argument", "multiple", "sets", "rewritten"})
    public void testUnsupported(String shape)
    {
        rule(2, 1000).on(p -> plan(p, shape)).doesNotFire();
    }

    @ParameterizedTest
    @ValueSource(doubles = {Double.NaN, 0, 100, 110})
    public void testNoExpansion(double joinRows)
    {
        rule(2, joinRows).on(p -> plan(p, "grouped")).doesNotFire();
    }

    @ParameterizedTest
    @ValueSource(doubles = {Double.NaN, 4, 100})
    public void testInsufficientCompression(double distinct)
    {
        rule(distinct, 1000).on(p -> plan(p, "grouped")).doesNotFire();
    }

    @ParameterizedTest
    @ValueSource(strings = {"global", "grouped"})
    public void testDisabledByDefault(String shape)
    {
        assertThat(new PreAggregateCountJoins(tester().getMetadata()).isEnabled(tester().getSession())).isFalse();
        rule(2, 1000).setSystemProperty(PRE_AGGREGATE_COUNT_JOINS, "false").on(p -> plan(p, shape)).doesNotFire();
    }

    private RuleBuilder rule(double distinct, double joinRows)
    {
        PlanNodeStatsEstimate.Builder left = PlanNodeStatsEstimate.builder().setOutputRowCount(100);
        PlanNodeStatsEstimate.Builder right = PlanNodeStatsEstimate.builder().setOutputRowCount(100);
        SymbolStatsEstimate stats = SymbolStatsEstimate.builder().setDistinctValuesCount(distinct).setNullsFraction(0).build();
        for (String name : ImmutableList.of("lk", "lk2", "a")) {
            left.addSymbolStatistics(new Symbol(BIGINT, name), stats);
        }
        for (String name : ImmutableList.of("rk", "rk2", "b")) {
            right.addSymbolStatistics(new Symbol(BIGINT, name), stats);
        }
        return tester().assertThat(new PreAggregateCountJoins(tester().getMetadata()))
                .setSystemProperty(PRE_AGGREGATE_COUNT_JOINS, "true")
                .overrideStats("left", left.build())
                .overrideStats("right", right.build())
                .overrideStats("join", PlanNodeStatsEstimate.builder().setOutputRowCount(joinRows).build());
    }

    private AggregationNode plan(PlanBuilder p, String shape)
    {
        Symbol lk = p.symbol("lk", BIGINT);
        Symbol lk2 = p.symbol("lk2", BIGINT);
        Symbol a = p.symbol("a", BIGINT);
        Symbol mask = p.symbol("mask", BOOLEAN);
        Symbol rk = p.symbol("rk", BIGINT);
        Symbol rk2 = p.symbol("rk2", BIGINT);
        Symbol b = p.symbol("b", BIGINT);
        var left = p.values(new PlanNodeId("left"), lk, lk2, a, mask);
        var right = p.values(new PlanNodeId("right"), rk, rk2, b);
        var source = p.join(new PlanNodeId("join"),
                shape.equals("outer") ? LEFT : INNER,
                left,
                right,
                shape.equals("cross") ? ImmutableList.of() : ImmutableList.of(new EquiJoinClause(lk, rk), new EquiJoinClause(lk2, rk2)),
                left.getOutputSymbols(),
                right.getOutputSymbols(),
                shape.equals("residual") ? Optional.of(comparison(LESS_THAN, a.toSymbolReference(), b.toSymbolReference())) : Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());
        String function = shape.equals("rewritten") ? CountJoinAggregation.NAME : "count";
        var arguments = switch (shape) {
            case "argument" -> ImmutableList.of(a.toSymbolReference());
            case "rewritten" -> ImmutableList.of(a.toSymbolReference(), b.toSymbolReference());
            default -> ImmutableList.<Reference>of();
        };
        var resolved = tester().getMetadata().resolveBuiltinFunction(SQL_STANDARD, function, arguments.stream().map(Reference::type).toList());
        return p.aggregation(builder -> {
            builder.source(source)
                    .addAggregation(p.symbol("result", BIGINT), new AggregationNode.Aggregation(
                            resolved,
                            ImmutableList.copyOf(arguments),
                            shape.equals("distinct"),
                            shape.equals("filter") ? Optional.of(mask) : Optional.empty(),
                            Optional.empty(),
                            shape.equals("mask") ? Optional.of(mask) : Optional.empty()))
                    .step(shape.equals("partial") ? PARTIAL : SINGLE);
            if (shape.equals("global")) {
                builder.globalGrouping();
            }
            else if (shape.equals("sets")) {
                builder.groupingSets(new AggregationNode.GroupingSetDescriptor(ImmutableList.of(a, b), 2, ImmutableSet.of(0)));
            }
            else {
                builder.singleGroupingSet(a, b);
            }
            if (shape.equals("multiple")) {
                builder.addAggregation(p.symbol("another", BIGINT), PlanBuilder.aggregation("count", ImmutableList.of()), ImmutableList.of());
            }
        });
    }
}

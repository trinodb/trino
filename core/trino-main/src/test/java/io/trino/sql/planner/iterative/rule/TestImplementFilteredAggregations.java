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
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.SymbolReference;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregationFunction;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.globalAggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.tree.LogicalExpression.Operator.AND;

public class TestImplementFilteredAggregations
        extends BaseRuleTest
{
    @Test
    public void testFilterToMask()
    {
        tester().assertThat(new ImplementFilteredAggregations(tester().getMetadata()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol g = p.symbol("g");
                    Symbol filter = p.symbol("filter", BOOLEAN);
                    return p.aggregation(builder -> builder
                            .singleGroupingSet(g)
                            .addAggregation(
                                    p.symbol("sum"),
                                    PlanBuilder.aggregation("sum", ImmutableList.of(a.toSymbolReference()), filter),
                                    ImmutableList.of(BIGINT))
                            .source(p.values(a, g, filter)));
                })
                .matches(
                        aggregation(
                                singleGroupingSet("g"),
                                ImmutableMap.of(Optional.of("sum"), aggregationFunction("sum", ImmutableList.of("a"))),
                                ImmutableList.of(),
                                ImmutableList.of("filter"),
                                Optional.empty(),
                                AggregationNode.Step.SINGLE,
                                filter(
                                        TRUE_LITERAL,
                                        project(
                                                ImmutableMap.of("a", expression(new SymbolReference("a")), "g", expression(new SymbolReference("g")), "filter", expression(new SymbolReference("filter"))),
                                                values("a", "g", "filter")))));
    }

    @Test
    public void testCombineMaskAndFilter()
    {
        tester().assertThat(new ImplementFilteredAggregations(tester().getMetadata()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol g = p.symbol("g");
                    Symbol mask = p.symbol("mask", BOOLEAN);
                    Symbol filter = p.symbol("filter", BOOLEAN);
                    return p.aggregation(builder -> builder
                            .singleGroupingSet(g)
                            .addAggregation(
                                    p.symbol("sum"),
                                    PlanBuilder.aggregation("sum", ImmutableList.of(a.toSymbolReference()), filter),
                                    ImmutableList.of(BIGINT),
                                    mask)
                            .source(p.values(a, g, mask, filter)));
                })
                .matches(
                        aggregation(
                                singleGroupingSet("g"),
                                ImmutableMap.of(Optional.of("sum"), aggregationFunction("sum", ImmutableList.of("a"))),
                                ImmutableList.of(),
                                ImmutableList.of("new_mask"),
                                Optional.empty(),
                                AggregationNode.Step.SINGLE,
                                filter(
                                        TRUE_LITERAL,
                                        project(
                                                ImmutableMap.of(
                                                        "a", expression(new SymbolReference("a")),
                                                        "g", expression(new SymbolReference("g")),
                                                        "mask", expression(new SymbolReference("mask")),
                                                        "filter", expression(new SymbolReference("filter")),
                                                        "new_mask", expression(new LogicalExpression(AND, ImmutableList.of(new SymbolReference("mask"), new SymbolReference("filter"))))),
                                                values("a", "g", "mask", "filter")))));
    }

    @Test
    public void testWithFilterPushdown()
    {
        tester().assertThat(new ImplementFilteredAggregations(tester().getMetadata()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol g = p.symbol("g");
                    Symbol filter = p.symbol("filter", BOOLEAN);
                    return p.aggregation(builder -> builder
                            .globalGrouping()
                            .addAggregation(
                                    p.symbol("sum"),
                                    PlanBuilder.aggregation("sum", ImmutableList.of(a.toSymbolReference()), filter),
                                    ImmutableList.of(BIGINT))
                            .source(p.values(a, g, filter)));
                })
                .matches(
                        aggregation(
                                globalAggregation(),
                                ImmutableMap.of(Optional.of("sum"), aggregationFunction("sum", ImmutableList.of("a"))),
                                ImmutableList.of(),
                                ImmutableList.of("filter"),
                                Optional.empty(),
                                AggregationNode.Step.SINGLE,
                                filter(
                                        new SymbolReference("filter"),
                                        project(
                                                ImmutableMap.of("a", expression(new SymbolReference("a")), "g", expression(new SymbolReference("g")), "filter", expression(new SymbolReference("filter"))),
                                                values("a", "g", "filter")))));
    }

    @Test
    public void testWithMultipleAggregations()
    {
        tester().assertThat(new ImplementFilteredAggregations(tester().getMetadata()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol g = p.symbol("g");
                    Symbol filter = p.symbol("filter", BOOLEAN);
                    return p.aggregation(builder -> builder
                            .globalGrouping()
                            .addAggregation(
                                    p.symbol("sum"),
                                    PlanBuilder.aggregation("sum", ImmutableList.of(a.toSymbolReference()), filter),
                                    ImmutableList.of(BIGINT))
                            .addAggregation(
                                    p.symbol("avg"),
                                    PlanBuilder.aggregation("avg", ImmutableList.of(a.toSymbolReference())),
                                    ImmutableList.of(BIGINT))
                            .source(p.values(a, g, filter)));
                })
                .matches(
                        aggregation(
                                globalAggregation(),
                                ImmutableMap.of(Optional.of("sum"), aggregationFunction("sum", ImmutableList.of("a")), Optional.of("avg"), aggregationFunction("avg", ImmutableList.of("a"))),
                                ImmutableList.of(),
                                ImmutableList.of("filter"),
                                Optional.empty(),
                                AggregationNode.Step.SINGLE,
                                filter(
                                        TRUE_LITERAL,
                                        project(
                                                ImmutableMap.of("a", expression(new SymbolReference("a")), "g", expression(new SymbolReference("g")), "filter", expression(new SymbolReference("filter"))),
                                                values("a", "g", "filter")))));
    }

    private FunctionCall functionWithFilter(String name, Symbol argument, Optional<Symbol> filter)
    {
        return new FunctionCall(
                Optional.empty(),
                QualifiedName.of(name),
                Optional.empty(),
                filter.map(Symbol::toSymbolReference),
                Optional.empty(),
                false,
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(argument.toSymbolReference()));
    }
}

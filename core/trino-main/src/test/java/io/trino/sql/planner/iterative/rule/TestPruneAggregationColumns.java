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
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.ProjectNode;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import static com.google.common.base.Predicates.alwaysTrue;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.functionCall;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;

public class TestPruneAggregationColumns
        extends BaseRuleTest
{
    @Test
    public void testNotAllAggregationsReferenced()
    {
        tester().assertThat(new PruneAggregationColumns())
                .on(p -> buildProjectedAggregation(p, ImmutableList.of(p.symbol("a"), p.symbol("b")), symbol -> ImmutableSet.of("key", "b").contains(symbol.getName())))
                .matches(
                        project(
                                ImmutableMap.of("b", expression("b")),
                                aggregation(
                                        singleGroupingSet("key"),
                                        ImmutableMap.of(
                                                Optional.of("b"),
                                                functionCall("count", false, ImmutableList.of())),
                                        ImmutableList.of(),
                                        ImmutableList.of(),
                                        Optional.empty(),
                                        SINGLE,
                                        Optional.of(ImmutableList.of("key", "b")),
                                        values("key"))));
    }

    @Test
    public void testNotAllGroupingKeysReferenced()
    {
        tester().assertThat(new PruneAggregationColumns())
                .on(p -> buildProjectedAggregation(p, ImmutableList.of(p.symbol("a")), symbol -> ImmutableSet.of("a").contains(symbol.getName())))
                .matches(
                        project(
                                ImmutableMap.of("a", expression("a")),
                                aggregation(
                                        singleGroupingSet("key"),
                                        ImmutableMap.of(
                                                Optional.of("a"),
                                                functionCall("count", false, ImmutableList.of())),
                                        ImmutableList.of(),
                                        ImmutableList.of(),
                                        Optional.empty(),
                                        SINGLE,
                                        Optional.of(ImmutableList.of("a")),
                                        values("key"))));
    }

    @Test
    public void testNotAllAggregationsAndGroupingKeysReferenced()
    {
        tester().assertThat(new PruneAggregationColumns())
                .on(p -> buildProjectedAggregation(p, ImmutableList.of(p.symbol("a"), p.symbol("b")), symbol -> ImmutableSet.of("a").contains(symbol.getName())))
                .matches(
                        project(
                                ImmutableMap.of("a", expression("a")),
                                aggregation(
                                        singleGroupingSet("key"),
                                        ImmutableMap.of(
                                                Optional.of("a"),
                                                functionCall("count", false, ImmutableList.of())),
                                        ImmutableList.of(),
                                        ImmutableList.of(),
                                        Optional.empty(),
                                        SINGLE,
                                        Optional.of(ImmutableList.of("a")),
                                        values("key"))));
    }

    @Test
    public void testRuleDoesNotFire()
    {
        tester().assertThat(new PruneAggregationColumns())
                .on(p -> buildProjectedAggregation(p, ImmutableList.of(p.symbol("a"), p.symbol("b")), alwaysTrue()))
                .doesNotFire();
    }

    private ProjectNode buildProjectedAggregation(PlanBuilder planBuilder, List<Symbol> aggregationSymbols, Predicate<Symbol> projectionFilter)
    {
        Symbol key = planBuilder.symbol("key");
        ImmutableList<Symbol> allSymbols = ImmutableList.<Symbol>builder().add(key).addAll(aggregationSymbols).build();

        return planBuilder.project(
                Assignments.identity(allSymbols.stream().filter(projectionFilter).collect(toImmutableSet())),
                planBuilder.aggregation(aggregationBuilder -> {
                    PlanBuilder.AggregationBuilder builder = aggregationBuilder
                            .source(planBuilder.values(key))
                            .singleGroupingSet(key);
                    aggregationSymbols.forEach(symbol -> builder.addAggregation(symbol, PlanBuilder.expression("count()"), ImmutableList.of()));
                }));
    }
}

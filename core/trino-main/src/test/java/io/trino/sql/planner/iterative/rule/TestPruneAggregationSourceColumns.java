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
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.AggregationNode;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import static com.google.common.base.Predicates.alwaysTrue;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregationFunction;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;

public class TestPruneAggregationSourceColumns
        extends BaseRuleTest
{
    @Test
    public void testNotAllInputsReferenced()
    {
        tester().assertThat(new PruneAggregationSourceColumns())
                .on(p -> buildAggregation(p, alwaysTrue()))
                .matches(
                        aggregation(
                                singleGroupingSet("key"),
                                ImmutableMap.of(
                                        Optional.of("avg"),
                                        aggregationFunction("avg", ImmutableList.of("input"))),
                                ImmutableList.of(),
                                ImmutableList.of("mask"),
                                Optional.empty(),
                                SINGLE,
                                strictProject(
                                        ImmutableMap.of(
                                                "input", expression(new Reference(BIGINT, "input")),
                                                "key", expression(new Reference(BIGINT, "key")),
                                                "mask", expression(new Reference(BOOLEAN, "mask"))),
                                        values("input", "key", "mask", "unused"))));
    }

    @Test
    public void testAllInputsReferenced()
    {
        tester().assertThat(new PruneAggregationSourceColumns())
                .on(p -> buildAggregation(p, symbol -> !symbol.name().equals("unused")))
                .doesNotFire();
    }

    private AggregationNode buildAggregation(PlanBuilder planBuilder, Predicate<Symbol> sourceSymbolFilter)
    {
        Symbol avg = planBuilder.symbol("avg");
        Symbol input = planBuilder.symbol("input");
        Symbol key = planBuilder.symbol("key");
        Symbol mask = planBuilder.symbol("mask");
        Symbol unused = planBuilder.symbol("unused");
        List<Symbol> sourceSymbols = ImmutableList.of(input, key, mask, unused);
        return planBuilder.aggregation(aggregationBuilder -> aggregationBuilder
                .singleGroupingSet(key)
                .addAggregation(avg, PlanBuilder.aggregation("avg", ImmutableList.of(new Reference(BIGINT, "input"))), ImmutableList.of(BIGINT), mask)
                .source(
                        planBuilder.values(
                                sourceSymbols.stream()
                                        .filter(sourceSymbolFilter)
                                        .collect(toImmutableList()),
                                ImmutableList.of())));
    }
}

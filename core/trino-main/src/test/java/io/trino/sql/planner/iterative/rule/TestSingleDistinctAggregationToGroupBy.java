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
import io.trino.sql.planner.assertions.AggregationFunction;
import io.trino.sql.planner.assertions.ExpectedValueProvider;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.SymbolReference;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregationFunction;
import static io.trino.sql.planner.assertions.PlanMatchPattern.globalAggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN;

public class TestSingleDistinctAggregationToGroupBy
        extends BaseRuleTest
{
    @Test
    public void testNoDistinct()
    {
        tester().assertThat(new SingleDistinctAggregationToGroupBy())
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .addAggregation(p.symbol("output1"), PlanBuilder.aggregation("count", ImmutableList.of(new SymbolReference("input1"))), ImmutableList.of(BIGINT))
                        .source(
                                p.values(
                                        p.symbol("input1"),
                                        p.symbol("input2")))))
                .doesNotFire();
    }

    @Test
    public void testMultipleDistincts()
    {
        tester().assertThat(new SingleDistinctAggregationToGroupBy())
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .addAggregation(p.symbol("output1"), PlanBuilder.aggregation("count", true, ImmutableList.of(new SymbolReference("input1"))), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("output2"), PlanBuilder.aggregation("count", true, ImmutableList.of(new SymbolReference("input2"))), ImmutableList.of(BIGINT))
                        .source(
                                p.values(
                                        p.symbol("input1"),
                                        p.symbol("input2")))))
                .doesNotFire();
    }

    @Test
    public void testMixedDistinctAndNonDistinct()
    {
        tester().assertThat(new SingleDistinctAggregationToGroupBy())
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .addAggregation(p.symbol("output1"), PlanBuilder.aggregation("count", true, ImmutableList.of(new SymbolReference("input1"))), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("output2"), PlanBuilder.aggregation("count", ImmutableList.of(new SymbolReference("input2"))), ImmutableList.of(BIGINT))
                        .source(
                                p.values(
                                        p.symbol("input1"),
                                        p.symbol("input2")))))
                .doesNotFire();
    }

    @Test
    public void testDistinctWithFilter()
    {
        tester().assertThat(new SingleDistinctAggregationToGroupBy())
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .addAggregation(p.symbol("output"), PlanBuilder.aggregation("count", true, ImmutableList.of(new SymbolReference("input1")), new Symbol("filter1")), ImmutableList.of(BIGINT))
                        .source(
                                p.project(
                                        Assignments.builder()
                                                .putIdentity(p.symbol("input1"))
                                                .putIdentity(p.symbol("input2"))
                                                .put(p.symbol("filter1"), new ComparisonExpression(GREATER_THAN, new SymbolReference("input2"), new LongLiteral("0")))
                                                .build(),
                                        p.values(
                                                p.symbol("input1"),
                                                p.symbol("input2"))))))
                .doesNotFire();
    }

    @Test
    public void testSingleAggregation()
    {
        tester().assertThat(new SingleDistinctAggregationToGroupBy())
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .addAggregation(p.symbol("output"), PlanBuilder.aggregation("count", true, ImmutableList.of(new SymbolReference("input"))), ImmutableList.of(BIGINT))
                        .source(
                                p.values(p.symbol("input")))))
                .matches(
                        aggregation(
                                globalAggregation(),
                                ImmutableMap.of(
                                        Optional.of("output"),
                                        aggregationFunction("count", ImmutableList.of("input"))),
                                Optional.empty(),
                                SINGLE,
                                aggregation(
                                        singleGroupingSet("input"),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        SINGLE,
                                        values("input"))));
    }

    @Test
    public void testMultipleAggregations()
    {
        tester().assertThat(new SingleDistinctAggregationToGroupBy())
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .addAggregation(p.symbol("output1"), PlanBuilder.aggregation("count", true, ImmutableList.of(new SymbolReference("input"))), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("output2"), PlanBuilder.aggregation("sum", true, ImmutableList.of(new SymbolReference("input"))), ImmutableList.of(BIGINT))
                        .source(
                                p.values(p.symbol("input")))))
                .matches(
                        aggregation(
                                globalAggregation(),
                                ImmutableMap.<Optional<String>, ExpectedValueProvider<AggregationFunction>>builder()
                                        .put(Optional.of("output1"), aggregationFunction("count", ImmutableList.of("input")))
                                        .put(Optional.of("output2"), aggregationFunction("sum", ImmutableList.of("input")))
                                        .buildOrThrow(),
                                Optional.empty(),
                                SINGLE,
                                aggregation(
                                        singleGroupingSet("input"),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        SINGLE,
                                        values("input"))));
    }

    @Test
    public void testMultipleInputs()
    {
        tester().assertThat(new SingleDistinctAggregationToGroupBy())
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .addAggregation(p.symbol("output1"), PlanBuilder.aggregation("corr", true, ImmutableList.of(new SymbolReference("x"), new SymbolReference("y"))), ImmutableList.of(REAL, REAL))
                        .addAggregation(p.symbol("output2"), PlanBuilder.aggregation("corr", true, ImmutableList.of(new SymbolReference("y"), new SymbolReference("x"))), ImmutableList.of(REAL, REAL))
                        .source(
                                p.values(p.symbol("x"), p.symbol("y")))))
                .matches(
                        aggregation(
                                globalAggregation(),
                                ImmutableMap.<Optional<String>, ExpectedValueProvider<AggregationFunction>>builder()
                                        .put(Optional.of("output1"), aggregationFunction("corr", ImmutableList.of("x", "y")))
                                        .put(Optional.of("output2"), aggregationFunction("corr", ImmutableList.of("y", "x")))
                                        .buildOrThrow(),
                                Optional.empty(),
                                SINGLE,
                                aggregation(
                                        singleGroupingSet("x", "y"),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        SINGLE,
                                        values("x", "y"))));
    }
}

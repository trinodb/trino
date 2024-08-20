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
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.function.OperatorType;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.PlanNode;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;

public class TestRemoveRedundantDistinctAggregation
        extends BaseRuleTest
{
    private static final ResolvedFunction ADD_INTEGER = new TestingFunctionResolution().resolveOperator(OperatorType.ADD, ImmutableList.of(INTEGER, INTEGER));

    @Test
    public void testRemoveDistinctAggregationForGroupedAggregation()
    {
        tester().assertThat(new RemoveRedundantDistinctAggregation())
                .on(TestRemoveRedundantDistinctAggregation::distinctWithGroupBy)
                .matches(
                        aggregation(
                                singleGroupingSet("value"),
                                ImmutableMap.of(),
                                Optional.empty(),
                                SINGLE,
                                values("value")));
    }

    @Test
    public void testRemoveDistinctAggregationForGroupedAggregationDoesNotFireOnGroupingKeyNotPresentInDistinct()
    {
        tester().assertThat(new RemoveRedundantDistinctAggregation())
                .on(TestRemoveRedundantDistinctAggregation::distinctWithGroupByWithNonMatchingKeySubset)
                .doesNotFire();
    }

    @Test
    public void testRemoveDistinctAggregationForGroupedAggregationWithProjectAndFilter()
    {
        tester().assertThat(new RemoveRedundantDistinctAggregation())
                .on(TestRemoveRedundantDistinctAggregation::distinctWithFilterWithProjectWithGroupBy)
                .matches(
                        filter(new Comparison(GREATER_THAN, new Constant(INTEGER, 6L), new Constant(INTEGER, 5L)),
                                project(aggregation(singleGroupingSet("value"),
                                ImmutableMap.of(),
                                Optional.empty(),
                                SINGLE,
                                values("value")))));
    }

    @Test
    public void testRemoveDistinctAggregationForGroupedAggregationDoesNotFireForNonIdentityProjectionsOnChildGroupingKeys()
    {
        tester().assertThat(new RemoveRedundantDistinctAggregation())
                .on(TestRemoveRedundantDistinctAggregation::distinctWithFilterWithNonIdentityProjectOnChildGroupingKeys)
                .doesNotFire();
    }

    @Test
    public void testRemoveDistinctAggregationForGroupedAggregationWithSymbolReference()
    {
        tester().assertThat(new RemoveRedundantDistinctAggregation())
                .on(TestRemoveRedundantDistinctAggregation::distinctWithSymbolReferenceWithGroupBy)
                .matches(
                        filter(new Comparison(GREATER_THAN, new Constant(INTEGER, 6L), new Constant(INTEGER, 5L)),
                                project(aggregation(singleGroupingSet("value"),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        SINGLE,
                                        values("value")))));
    }

    private static PlanNode distinctWithGroupBy(PlanBuilder p)
    {
        Symbol value = p.symbol("value", INTEGER);
        return p.aggregation(b -> b
                .singleGroupingSet(value)
                .source(p.aggregation(builder -> builder
                        .singleGroupingSet(value)
                        .source(p.values(value)))));
    }

    private static PlanNode distinctWithGroupByWithNonMatchingKeySubset(PlanBuilder p)
    {
        Symbol value = p.symbol("value", INTEGER);
        Symbol value2 = p.symbol("value2", INTEGER);
        return p.aggregation(b -> b
                .singleGroupingSet(value)
                .source(p.aggregation(builder -> builder
                        .singleGroupingSet(value, value2)
                        .source(p.values(value, value2)))));
    }

    private static PlanNode distinctWithFilterWithProjectWithGroupBy(PlanBuilder p)
    {
        Symbol value = p.symbol("value", INTEGER);
        return p.aggregation(b -> b
                .singleGroupingSet(value)
                .source(p.filter(new Comparison(GREATER_THAN, new Constant(INTEGER, 6L), new Constant(INTEGER, 5L)),
                        p.project(Assignments.builder().putIdentity(value).build(),
                                p.aggregation(builder -> builder
                                        .singleGroupingSet(value)
                                        .source(p.values(value)))))));
    }

    private static PlanNode distinctWithFilterWithNonIdentityProjectOnChildGroupingKeys(PlanBuilder p)
    {
        Symbol value = p.symbol("value", INTEGER);
        return p.aggregation(b -> b
                .singleGroupingSet(value)
                .source(p.filter(new Comparison(GREATER_THAN, new Constant(INTEGER, 6L), new Constant(INTEGER, 5L)),
                        p.project(Assignments.builder().put(value, new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "x"), new Constant(INTEGER, 2L)))).build(),
                                p.aggregation(builder -> builder
                                        .singleGroupingSet(value)
                                        .source(p.values(value)))))));
    }

    private static PlanNode distinctWithSymbolReferenceWithGroupBy(PlanBuilder p)
    {
        Symbol value = p.symbol("value", INTEGER);
        Symbol value2 = p.symbol("value2", INTEGER);
        return p.aggregation(b -> b
                .singleGroupingSet(value2)
                .source(p.filter(new Comparison(GREATER_THAN, new Constant(INTEGER, 6L), new Constant(INTEGER, 5L)),
                        p.project(Assignments.builder().put(value2, value.toSymbolReference()).build(),
                                p.aggregation(builder -> builder
                                        .singleGroupingSet(value)
                                        .source(p.values(value)))))));
    }
}

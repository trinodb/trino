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
import io.trino.sql.planner.plan.AggregationNode;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expression;

public class TestPruneDistinctAggregation
        extends BaseRuleTest
{
    @Test
    public void testPruning()
    {
        tester().assertThat(new PruneDistinctAggregation())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    AggregationNode child = p.aggregation(aggregationBuilder ->
                            aggregationBuilder.globalGrouping()
                                    .source(p.values(1, a)));
                    return p.aggregation(aggregationBuilder ->
                            aggregationBuilder.globalGrouping()
                                    .source(child));
                })
                .matches(
                        aggregation(ImmutableMap.of(), values("a")));
    }

    @Test
    public void testNonPruning()
    {
        tester().assertThat(new PruneDistinctAggregation())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    AggregationNode child = p.aggregation(aggregationBuilder ->
                            aggregationBuilder.globalGrouping()
                                    .addAggregation(p.symbol("sum", BIGINT), expression("sum(a)"), ImmutableList.of(BIGINT))
                                    .source(p.values(1, a)));
                    return p.aggregation(aggregationBuilder ->
                            aggregationBuilder.globalGrouping()
                                    .source(child));
                })
                .doesNotFire();
    }
}

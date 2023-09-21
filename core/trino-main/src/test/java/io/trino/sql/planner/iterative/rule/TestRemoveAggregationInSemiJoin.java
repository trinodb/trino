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
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.PlanNode;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expression;

public class TestRemoveAggregationInSemiJoin
        extends BaseRuleTest
{
    @Test
    public void test()
    {
        tester().assertThat(new RemoveAggregationInSemiJoin())
                .on(TestRemoveAggregationInSemiJoin::semiJoinWithDistinctAsFilteringSource)
                .matches(
                        semiJoin("leftKey", "rightKey", "match",
                                values("leftKey"),
                                values("rightKey")));
    }

    @Test
    public void testDoesNotFire()
    {
        tester().assertThat(new RemoveAggregationInSemiJoin())
                .on(TestRemoveAggregationInSemiJoin::semiJoinWithAggregationAsFilteringSource)
                .doesNotFire();
    }

    private static PlanNode semiJoinWithDistinctAsFilteringSource(PlanBuilder p)
    {
        Symbol leftKey = p.symbol("leftKey");
        Symbol rightKey = p.symbol("rightKey");
        return p.semiJoin(
                leftKey,
                rightKey,
                p.symbol("match"),
                Optional.empty(),
                Optional.empty(),
                p.values(leftKey),
                p.aggregation(builder -> builder
                        .singleGroupingSet(rightKey)
                        .source(p.values(rightKey))));
    }

    private static PlanNode semiJoinWithAggregationAsFilteringSource(PlanBuilder p)
    {
        Symbol leftKey = p.symbol("leftKey");
        Symbol rightKey = p.symbol("rightKey");
        return p.semiJoin(
                leftKey,
                rightKey,
                p.symbol("match"),
                Optional.empty(),
                Optional.empty(),
                p.values(leftKey),
                p.aggregation(builder -> builder
                        .globalGrouping()
                        .addAggregation(rightKey, expression("count(rightValue)"), ImmutableList.of(BIGINT))
                        .source(p.values(p.symbol("rightValue")))));
    }
}

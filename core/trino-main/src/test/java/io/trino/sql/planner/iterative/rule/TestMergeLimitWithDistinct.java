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
import io.trino.sql.planner.plan.DistinctLimitNode;
import io.trino.sql.planner.plan.ValuesNode;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expression;

public class TestMergeLimitWithDistinct
        extends BaseRuleTest
{
    @Test
    public void test()
    {
        tester().assertThat(new MergeLimitWithDistinct())
                .on(p ->
                        p.limit(
                                1,
                                p.aggregation(builder -> builder
                                        .singleGroupingSet(p.symbol("foo"))
                                        .source(p.values(p.symbol("foo"))))))
                .matches(
                        node(DistinctLimitNode.class,
                                node(ValuesNode.class)));
    }

    @Test
    public void testDoesNotFire()
    {
        tester().assertThat(new MergeLimitWithDistinct())
                .on(p ->
                        p.limit(
                                1,
                                p.aggregation(builder -> builder
                                        .addAggregation(p.symbol("c"), expression("count(foo)"), ImmutableList.of(BIGINT))
                                        .globalGrouping()
                                        .source(p.values(p.symbol("foo"))))))
                .doesNotFire();

        tester().assertThat(new MergeLimitWithDistinct())
                .on(p ->
                        p.limit(
                                1,
                                p.aggregation(builder -> builder
                                        .globalGrouping()
                                        .source(p.values(p.symbol("foo"))))))
                .doesNotFire();
    }

    @Test
    public void testDoNotMergeLimitWithTies()
    {
        tester().assertThat(new MergeLimitWithDistinct())
                .on(p -> {
                    Symbol foo = p.symbol("foo");
                    return p.limit(
                            1,
                            ImmutableList.of(foo),
                            p.aggregation(builder -> builder
                                    .singleGroupingSet(foo)
                                    .source(p.values(foo))));
                })
                .doesNotFire();
    }
}

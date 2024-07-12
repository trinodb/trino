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
import org.junit.jupiter.api.Test;

import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.sort;
import static io.trino.sql.planner.assertions.PlanMatchPattern.topN;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.planner.plan.ExchangeNode.Type.GATHER;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.trino.sql.planner.plan.TopNNode.Step.PARTIAL;
import static io.trino.sql.tree.SortItem.NullOrdering.FIRST;
import static io.trino.sql.tree.SortItem.Ordering.ASCENDING;

public class TestGatherPartialTopN
        extends BaseRuleTest
{
    @Test
    public void testPartialTopNGather()
    {
        tester().assertThat(new GatherPartialTopN())
                .on(p ->
                {
                    Symbol orderBy = p.symbol("a");
                    return p.exchange(exchange -> exchange
                            .scope(REMOTE)
                            .singleDistributionPartitioningScheme(orderBy)
                            .addInputsSet(orderBy)
                            .addSource(p.topN(10, ImmutableList.of(orderBy), PARTIAL, p.values(orderBy))));
                }).matches(exchange(REMOTE,
                        topN(
                                10,
                                ImmutableList.of(sort("a", ASCENDING, FIRST)),
                                PARTIAL,
                                exchange(LOCAL, GATHER,
                                        topN(
                                                10,
                                                ImmutableList.of(sort("a", ASCENDING, FIRST)),
                                                PARTIAL,
                                                exchange(LOCAL, REPARTITION, FIXED_ARBITRARY_DISTRIBUTION,
                                                        topN(
                                                                10,
                                                                ImmutableList.of(sort("a", ASCENDING, FIRST)),
                                                                PARTIAL,
                                                                values("a"))))))));
    }

    @Test
    public void testRuleDoesNotFireTwice()
    {
        tester().assertThat(new GatherPartialTopN())
                .on(p ->
                {
                    Symbol orderBy = p.symbol("a");
                    return p.exchange(exchange -> exchange
                            .scope(REMOTE)
                            .singleDistributionPartitioningScheme(orderBy)
                            .addInputsSet(orderBy)
                            .addSource(p.topN(
                                    10,
                                    ImmutableList.of(orderBy),
                                    PARTIAL,
                                    p.exchange(localExchange -> localExchange
                                            .scope(LOCAL)
                                            .type(GATHER)
                                            .singleDistributionPartitioningScheme(orderBy)
                                            .addInputsSet(orderBy)
                                            .addSource(p.topN(
                                                    10,
                                                    ImmutableList.of(orderBy),
                                                    PARTIAL,
                                                    p.values(orderBy)))))));
                }).doesNotFire();
    }
}

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
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.UnnestNode;
import org.testng.annotations.Test;

import static io.trino.SystemSessionProperties.JOIN_PARTITIONED_BUILD_MIN_ROW_COUNT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.planner.plan.ExchangeNode.Type.GATHER;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static java.lang.Double.NaN;

public class TestUseNonPartitionedJoinLookupSource
        extends BaseRuleTest
{
    @Test
    public void testLocalGatheringExchangeNotChanged()
    {
        tester().assertThat(new UseNonPartitionedJoinLookupSource())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.join(
                            INNER,
                            p.values(a),
                            p.gatheringExchange(LOCAL, p.values(b)));
                })
                .doesNotFire();
    }

    @Test
    public void testLocalRepartitioningExchangeChangedToGather()
    {
        tester().assertThat(new UseNonPartitionedJoinLookupSource())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.join(
                            INNER,
                            p.values(a),
                            repartitioningExchange(p, b, p.values(b)));
                })
                .matches(
                        join(INNER, builder -> builder
                                .left(values("a"))
                                .right(exchange(LOCAL, GATHER, values("b")))));
    }

    @Test
    public void testLeftRepartitionNotChanged()
    {
        tester().assertThat(new UseNonPartitionedJoinLookupSource())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.join(
                            INNER,
                            repartitioningExchange(p, a, p.values(a)),
                            p.values(b));
                })
                .doesNotFire();
    }

    @Test
    public void testRepartitioningExchangeNotChangedIfBuildSideTooBig()
    {
        tester()
                .assertThat(new UseNonPartitionedJoinLookupSource())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.join(
                            INNER,
                            p.values(a),
                            repartitioningExchange(p, b, p.values(new PlanNodeId("source"), b)));
                })
                .overrideStats("source", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(5_000_001)
                        .build())
                .doesNotFire();
    }

    @Test
    public void testRepartitioningExchangeNotChangedIfRuleDisabled()
    {
        tester().assertThat(new UseNonPartitionedJoinLookupSource())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.join(
                            INNER,
                            p.values(a),
                            repartitioningExchange(p, b, p.values(b)));
                })
                .setSystemProperty(JOIN_PARTITIONED_BUILD_MIN_ROW_COUNT, "0")
                .doesNotFire();
    }

    @Test
    public void testRepartitioningExchangeNotChangedIfBuildSideContainsJoin()
    {
        tester()
                .assertThat(new UseNonPartitionedJoinLookupSource())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol c = p.symbol("c");
                    return p.join(
                            INNER,
                            p.values(a),
                            repartitioningExchange(p, b, p.join(INNER, p.values(c), p.values(b))));
                })
                .doesNotFire();
    }

    @Test
    public void testRepartitioningExchangeNotChangedIfBuildSideContainsUnnest()
    {
        tester()
                .assertThat(new UseNonPartitionedJoinLookupSource())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol c = p.symbol("c");
                    return p.join(
                            INNER,
                            p.values(a),
                            repartitioningExchange(p, b, p.unnest(
                                    ImmutableList.of(),
                                    ImmutableList.of(new UnnestNode.Mapping(c, ImmutableList.of(b))),
                                    p.values(c))));
                })
                .doesNotFire();
    }

    @Test
    public void testRepartitioningExchangeNotChangedIfBuildSideRowCountUnknown()
    {
        tester()
                .assertThat(new UseNonPartitionedJoinLookupSource())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.join(
                            INNER,
                            p.values(a),
                            repartitioningExchange(p, b, p.values(new PlanNodeId("source"), b)));
                })
                .overrideStats("source", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(NaN)
                        .build())
                .doesNotFire();
    }

    private ExchangeNode repartitioningExchange(PlanBuilder p, Symbol symbol, PlanNode source)
    {
        return p.exchange(builder -> builder
                .type(REPARTITION)
                .scope(LOCAL)
                .fixedHashDistributionPartitioningScheme(ImmutableList.of(symbol), ImmutableList.of(symbol))
                .addSource(source)
                .addInputsSet(symbol));
    }
}

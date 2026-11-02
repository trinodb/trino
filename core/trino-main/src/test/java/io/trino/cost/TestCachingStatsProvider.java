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
package io.trino.cost;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.statistics.TableStatistics;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinNode.EquiJoinClause;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ValuesNode;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static io.trino.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.sql.planner.plan.JoinType.LEFT;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCachingStatsProvider
{
    @Test
    public void testEquivalentJoinCopiesShareOneEstimate()
    {
        AtomicInteger calculations = new AtomicInteger();
        StatsCalculator countingCalculator = (_, _) -> PlanNodeStatsEstimate.builder()
                .setOutputRowCount(calculations.incrementAndGet())
                .build();
        CachingStatsProvider provider = new CachingStatsProvider(countingCalculator, TEST_SESSION, _ -> TableStatistics.empty());

        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        Symbol a = new Symbol(BIGINT, "a");
        Symbol b = new Symbol(BIGINT, "b");
        PlanNode left = new ValuesNode(idAllocator.getNextId(), ImmutableList.of(a), ImmutableList.of());
        PlanNode right = new ValuesNode(idAllocator.getNextId(), ImmutableList.of(b), ImmutableList.of());
        JoinNode inner = join(idAllocator, INNER, left, right, a, b);

        PlanNodeStatsEstimate expected = provider.getStats(inner);
        assertThat(provider.getStats(inner.withDistributionType(PARTITIONED))).isEqualTo(expected);
        assertThat(provider.getStats(inner.withDistributionType(REPLICATED))).isEqualTo(expected);
        assertThat(provider.getStats(inner.flipChildren())).isEqualTo(expected);
        assertThat(provider.getStats(join(idAllocator, INNER, right, left, b, a))).isEqualTo(expected);
        assertThat(calculations).hasValue(1);

        JoinNode outer = join(idAllocator, LEFT, left, right, a, b);
        provider.getStats(outer);
        provider.getStats(outer.withDistributionType(PARTITIONED));
        assertThat(calculations).hasValue(2);
        provider.getStats(join(idAllocator, LEFT, right, left, b, a));
        assertThat(calculations).hasValue(3);
    }

    private static JoinNode join(PlanNodeIdAllocator idAllocator, JoinType type, PlanNode left, PlanNode right, Symbol leftKey, Symbol rightKey)
    {
        return new JoinNode(
                idAllocator.getNextId(),
                type,
                left,
                right,
                ImmutableList.of(new EquiJoinClause(leftKey, rightKey)),
                left.getOutputSymbols(),
                right.getOutputSymbols(),
                false,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                Optional.empty());
    }
}

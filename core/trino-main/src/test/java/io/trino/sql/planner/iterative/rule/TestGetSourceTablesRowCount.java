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
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.cost.StatsProvider;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.testing.TestingMetadata.TestingColumnHandle;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.cost.PlanNodeStatsEstimate.unknown;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.sql.planner.iterative.Lookup.noLookup;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestGetSourceTablesRowCount
{
    @Test
    public void testMissingSourceStats()
    {
        PlanBuilder planBuilder = planBuilder();
        Symbol symbol = planBuilder.symbol("col");

        assertThat(getSourceTablesRowCount(
                planBuilder.tableScan(
                        tableScan -> tableScan
                                .setSymbols(ImmutableList.of(symbol))
                                .setAssignments(ImmutableMap.of(symbol, new TestingColumnHandle("col")))
                                .setStatistics(Optional.of(unknown()))))).isNaN();
    }

    @Test
    public void testTwoSourcePlanNodes()
    {
        PlanBuilder planBuilder = planBuilder();
        Symbol symbol = planBuilder.symbol("col");
        Symbol sourceSymbol1 = planBuilder.symbol("source1");
        Symbol sourceSymbol2 = planBuilder.symbol("soruce2");

        assertThat(getSourceTablesRowCount(
                planBuilder.union(
                        ImmutableListMultimap.<Symbol, Symbol>builder()
                                .put(symbol, sourceSymbol1)
                                .put(symbol, sourceSymbol2)
                                .build(),
                        ImmutableList.of(
                                planBuilder.tableScan(
                                        tableScan -> tableScan
                                                .setSymbols(ImmutableList.of(sourceSymbol1))
                                                .setAssignments(ImmutableMap.of(sourceSymbol1, new TestingColumnHandle("col")))
                                                .setStatistics(Optional.of(stats(10)))),
                                planBuilder.values(new PlanNodeId("valuesNode"), 20, sourceSymbol2))))).isEqualTo(30.0);
    }

    @Test
    public void testJoinNode()
    {
        PlanBuilder planBuilder = planBuilder();
        Symbol sourceSymbol1 = planBuilder.symbol("source1");
        Symbol sourceSymbol2 = planBuilder.symbol("soruce2");

        assertThat(getSourceTablesRowCount(
                planBuilder.join(
                        INNER,
                        planBuilder.values(sourceSymbol1),
                        planBuilder.values(sourceSymbol2)))).isNaN();
    }

    private double getSourceTablesRowCount(PlanNode planNode)
    {
        return UseNonPartitionedJoinLookupSource.getSourceTablesRowCount(
                planNode,
                noLookup(),
                testStatsProvider());
    }

    private PlanBuilder planBuilder()
    {
        return new PlanBuilder(new PlanNodeIdAllocator(), PLANNER_CONTEXT, testSessionBuilder().build());
    }

    private static StatsProvider testStatsProvider()
    {
        return node -> {
            if (node instanceof TableScanNode tableScanNode) {
                return tableScanNode.getStatistics().orElse(unknown());
            }

            if (node instanceof ValuesNode valuesNode) {
                return stats(valuesNode.getRowCount());
            }

            return unknown();
        };
    }

    private static PlanNodeStatsEstimate stats(int rowCount)
    {
        return PlanNodeStatsEstimate.builder()
                .setOutputRowCount(rowCount)
                .build();
    }
}

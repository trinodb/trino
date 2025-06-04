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
package io.trino.execution.scheduler.policy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.cost.StatsAndCosts;
import io.trino.operator.RetryPolicy;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.RemoteSourceNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.TestingMetadata;

import java.util.Optional;

import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static io.trino.sql.planner.plan.AggregationNode.Step.FINAL;
import static io.trino.sql.planner.plan.AggregationNode.singleGroupingSet;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static io.trino.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static io.trino.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.testing.TestingHandles.TEST_TABLE_HANDLE;
import static io.trino.type.UnknownType.UNKNOWN;

final class PlanUtils
{
    private PlanUtils() {}

    static PlanFragment createAggregationFragment(String name, PlanFragment sourceFragment)
    {
        RemoteSourceNode source = new RemoteSourceNode(new PlanNodeId("source_id"), sourceFragment.getId(), ImmutableList.of(), Optional.empty(), REPARTITION, RetryPolicy.NONE);
        PlanNode planNode = new AggregationNode(
                new PlanNodeId(name + "_id"),
                source,
                ImmutableMap.of(),
                singleGroupingSet(ImmutableList.of()),
                ImmutableList.of(),
                FINAL,
                Optional.empty(),
                Optional.empty());

        return createFragment(planNode);
    }

    static PlanFragment createBroadcastJoinPlanFragment(String name, PlanFragment buildFragment)
    {
        Symbol symbol = new Symbol(UNKNOWN, "column");
        PlanNode tableScan = new TableScanNode(
                new PlanNodeId(name),
                TEST_TABLE_HANDLE,
                ImmutableList.of(symbol),
                ImmutableMap.of(symbol, new TestingMetadata.TestingColumnHandle("column")),
                TupleDomain.all(),
                Optional.empty(),
                false,
                Optional.empty());

        RemoteSourceNode remote = new RemoteSourceNode(new PlanNodeId("build_id"), buildFragment.getId(), ImmutableList.of(), Optional.empty(), REPLICATE, RetryPolicy.NONE);
        PlanNode join = new JoinNode(
                new PlanNodeId(name + "_id"),
                INNER,
                tableScan,
                remote,
                ImmutableList.of(),
                tableScan.getOutputSymbols(),
                remote.getOutputSymbols(),
                false,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(REPLICATED),
                Optional.empty(),
                ImmutableMap.of(),
                Optional.empty());

        return createFragment(join);
    }

    static PlanFragment createJoinPlanFragment(JoinType joinType, String name, PlanFragment buildFragment, PlanFragment probeFragment)
    {
        return createJoinPlanFragment(joinType, PARTITIONED, name, buildFragment, probeFragment);
    }

    static PlanFragment createJoinPlanFragment(JoinType joinType, JoinNode.DistributionType distributionType, String name, PlanFragment buildFragment, PlanFragment probeFragment)
    {
        RemoteSourceNode probe = new RemoteSourceNode(new PlanNodeId("probe_id"), probeFragment.getId(), ImmutableList.of(), Optional.empty(), REPARTITION, RetryPolicy.NONE);
        RemoteSourceNode build = new RemoteSourceNode(new PlanNodeId("build_id"), buildFragment.getId(), ImmutableList.of(), Optional.empty(), REPARTITION, RetryPolicy.NONE);
        PlanNode planNode = new JoinNode(
                new PlanNodeId(name + "_id"),
                joinType,
                probe,
                build,
                ImmutableList.of(),
                probe.getOutputSymbols(),
                build.getOutputSymbols(),
                false,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(distributionType),
                Optional.empty(),
                ImmutableMap.of(),
                Optional.empty());
        return createFragment(planNode);
    }

    static PlanFragment createBroadcastAndPartitionedJoinPlanFragment(
            String name,
            PlanFragment broadcastBuildFragment,
            PlanFragment partitionedBuildFragment,
            PlanFragment probeFragment)
    {
        RemoteSourceNode probe = new RemoteSourceNode(new PlanNodeId("probe_id"), probeFragment.getId(), ImmutableList.of(), Optional.empty(), REPARTITION, RetryPolicy.NONE);
        RemoteSourceNode broadcastBuild = new RemoteSourceNode(new PlanNodeId("broadcast_build_id"), broadcastBuildFragment.getId(), ImmutableList.of(), Optional.empty(), REPLICATE, RetryPolicy.NONE);
        RemoteSourceNode partitionedBuild = new RemoteSourceNode(new PlanNodeId("partitioned_build_id"), partitionedBuildFragment.getId(), ImmutableList.of(), Optional.empty(), REPARTITION, RetryPolicy.NONE);
        PlanNode broadcastPlanNode = new JoinNode(
                new PlanNodeId(name + "_broadcast_id"),
                INNER,
                probe,
                broadcastBuild,
                ImmutableList.of(),
                probe.getOutputSymbols(),
                broadcastBuild.getOutputSymbols(),
                false,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(REPLICATED),
                Optional.empty(),
                ImmutableMap.of(),
                Optional.empty());
        PlanNode partitionedPlanNode = new JoinNode(
                new PlanNodeId(name + "_partitioned_id"),
                INNER,
                broadcastPlanNode,
                partitionedBuild,
                ImmutableList.of(),
                broadcastPlanNode.getOutputSymbols(),
                partitionedBuild.getOutputSymbols(),
                false,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(PARTITIONED),
                Optional.empty(),
                ImmutableMap.of(),
                Optional.empty());

        return createFragment(partitionedPlanNode);
    }

    static PlanFragment createTableScanPlanFragment(String name)
    {
        Symbol symbol = new Symbol(UNKNOWN, "column");
        PlanNode planNode = new TableScanNode(
                new PlanNodeId(name),
                TEST_TABLE_HANDLE,
                ImmutableList.of(symbol),
                ImmutableMap.of(symbol, new TestingMetadata.TestingColumnHandle("column")),
                TupleDomain.all(),
                Optional.empty(),
                false,
                Optional.empty());

        return createFragment(planNode);
    }

    private static PlanFragment createFragment(PlanNode planNode)
    {
        return new PlanFragment(
                new PlanFragmentId(planNode.getId() + "_fragment_id"),
                planNode,
                ImmutableSet.copyOf(planNode.getOutputSymbols()),
                SOURCE_DISTRIBUTION,
                Optional.empty(),
                ImmutableList.of(planNode.getId()),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), planNode.getOutputSymbols()),
                StatsAndCosts.empty(),
                ImmutableList.of(),
                ImmutableMap.of(),
                Optional.empty());
    }
}

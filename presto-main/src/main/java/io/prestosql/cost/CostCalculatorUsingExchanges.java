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

package io.prestosql.cost;

import com.google.common.collect.ImmutableList;
import io.prestosql.Session;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.iterative.GroupReference;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.AssignUniqueId;
import io.prestosql.sql.planner.plan.EnforceSingleRowNode;
import io.prestosql.sql.planner.plan.ExchangeNode;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.LimitNode;
import io.prestosql.sql.planner.plan.OutputNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.PlanVisitor;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.RowNumberNode;
import io.prestosql.sql.planner.plan.SemiJoinNode;
import io.prestosql.sql.planner.plan.SpatialJoinNode;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.sql.planner.plan.UnionNode;
import io.prestosql.sql.planner.plan.ValuesNode;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.cost.CostCalculatorWithEstimatedExchanges.calculateJoinInputCost;
import static io.prestosql.cost.CostCalculatorWithEstimatedExchanges.calculateLocalRepartitionCost;
import static io.prestosql.cost.CostCalculatorWithEstimatedExchanges.calculateRemoteGatherCost;
import static io.prestosql.cost.CostCalculatorWithEstimatedExchanges.calculateRemoteRepartitionCost;
import static io.prestosql.cost.CostCalculatorWithEstimatedExchanges.calculateRemoteReplicateCost;
import static io.prestosql.sql.planner.plan.AggregationNode.Step.FINAL;
import static io.prestosql.sql.planner.plan.AggregationNode.Step.SINGLE;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

/**
 * Simple implementation of CostCalculator. It assumes that ExchangeNodes are already in the plan.
 */
@ThreadSafe
public class CostCalculatorUsingExchanges
        implements CostCalculator
{
    private final TaskCountEstimator taskCountEstimator;

    @Inject
    public CostCalculatorUsingExchanges(TaskCountEstimator taskCountEstimator)
    {
        this.taskCountEstimator = requireNonNull(taskCountEstimator, "taskCountEstimator is null");
    }

    @Override
    public PlanCostEstimate calculateCost(PlanNode node, StatsProvider stats, CostProvider sourcesCosts, Session session, TypeProvider types)
    {
        CostEstimator costEstimator = new CostEstimator(stats, sourcesCosts, types, taskCountEstimator);
        return node.accept(costEstimator, null);
    }

    private static class CostEstimator
            extends PlanVisitor<PlanCostEstimate, Void>
    {
        private final StatsProvider stats;
        private final CostProvider sourcesCosts;
        private final TypeProvider types;
        private final TaskCountEstimator taskCountEstimator;

        CostEstimator(StatsProvider stats, CostProvider sourcesCosts, TypeProvider types, TaskCountEstimator taskCountEstimator)
        {
            this.stats = requireNonNull(stats, "stats is null");
            this.sourcesCosts = requireNonNull(sourcesCosts, "sourcesCosts is null");
            this.types = requireNonNull(types, "types is null");
            this.taskCountEstimator = requireNonNull(taskCountEstimator, "taskCountEstimator is null");
        }

        @Override
        protected PlanCostEstimate visitPlan(PlanNode node, Void context)
        {
            // TODO implement cost estimates for all plan nodes
            return PlanCostEstimate.unknown();
        }

        @Override
        public PlanCostEstimate visitGroupReference(GroupReference node, Void context)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public PlanCostEstimate visitAssignUniqueId(AssignUniqueId node, Void context)
        {
            PlanNodeLocalCostEstimate localCost = PlanNodeLocalCostEstimate.ofCpu(getStats(node).getOutputSizeInBytes(ImmutableList.of(node.getIdColumn()), types));
            return costForStreaming(node, localCost);
        }

        @Override
        public PlanCostEstimate visitRowNumber(RowNumberNode node, Void context)
        {
            List<Symbol> symbols = node.getOutputSymbols();
            // when maxRowCountPerPartition is set, the RowNumberOperator
            // copies values for all the columns into a page builder
            if (!node.getMaxRowCountPerPartition().isPresent()) {
                symbols = ImmutableList.<Symbol>builder()
                        .addAll(node.getPartitionBy())
                        .add(node.getRowNumberSymbol())
                        .build();
            }
            PlanNodeStatsEstimate stats = getStats(node);
            double cpuCost = stats.getOutputSizeInBytes(symbols, types);
            double memoryCost = node.getPartitionBy().isEmpty() ? 0 : stats.getOutputSizeInBytes(node.getSource().getOutputSymbols(), types);
            PlanNodeLocalCostEstimate localCost = PlanNodeLocalCostEstimate.of(cpuCost, memoryCost, 0);
            return costForStreaming(node, localCost);
        }

        @Override
        public PlanCostEstimate visitOutput(OutputNode node, Void context)
        {
            return costForStreaming(node, PlanNodeLocalCostEstimate.zero());
        }

        @Override
        public PlanCostEstimate visitTableScan(TableScanNode node, Void context)
        {
            // TODO: add network cost, based on input size in bytes? Or let connector provide this cost?
            PlanNodeLocalCostEstimate localCost = PlanNodeLocalCostEstimate.ofCpu(getStats(node).getOutputSizeInBytes(node.getOutputSymbols(), types));
            return costForSource(node, localCost);
        }

        @Override
        public PlanCostEstimate visitFilter(FilterNode node, Void context)
        {
            PlanNodeLocalCostEstimate localCost = PlanNodeLocalCostEstimate.ofCpu(getStats(node.getSource()).getOutputSizeInBytes(node.getOutputSymbols(), types));
            return costForStreaming(node, localCost);
        }

        @Override
        public PlanCostEstimate visitProject(ProjectNode node, Void context)
        {
            PlanNodeLocalCostEstimate localCost = PlanNodeLocalCostEstimate.ofCpu(getStats(node).getOutputSizeInBytes(node.getOutputSymbols(), types));
            return costForStreaming(node, localCost);
        }

        @Override
        public PlanCostEstimate visitAggregation(AggregationNode node, Void context)
        {
            if (node.getStep() != FINAL && node.getStep() != SINGLE) {
                return PlanCostEstimate.unknown();
            }
            PlanNodeStatsEstimate aggregationStats = getStats(node);
            PlanNodeStatsEstimate sourceStats = getStats(node.getSource());
            double cpuCost = sourceStats.getOutputSizeInBytes(node.getSource().getOutputSymbols(), types);
            double memoryCost = aggregationStats.getOutputSizeInBytes(node.getOutputSymbols(), types);
            PlanNodeLocalCostEstimate localCost = PlanNodeLocalCostEstimate.of(cpuCost, memoryCost, 0);
            return costForAccumulation(node, localCost);
        }

        @Override
        public PlanCostEstimate visitJoin(JoinNode node, Void context)
        {
            PlanNodeLocalCostEstimate localCost = calculateJoinCost(
                    node,
                    node.getLeft(),
                    node.getRight(),
                    Objects.equals(node.getDistributionType(), Optional.of(JoinNode.DistributionType.REPLICATED)));
            return costForLookupJoin(node, localCost);
        }

        private PlanNodeLocalCostEstimate calculateJoinCost(PlanNode join, PlanNode probe, PlanNode build, boolean replicated)
        {
            PlanNodeLocalCostEstimate joinInputCost = calculateJoinInputCost(
                    probe,
                    build,
                    stats,
                    types,
                    replicated,
                    taskCountEstimator.estimateSourceDistributedTaskCount());
            PlanNodeLocalCostEstimate joinOutputCost = calculateJoinOutputCost(join);
            return joinInputCost.add(joinOutputCost);
        }

        private PlanNodeLocalCostEstimate calculateJoinOutputCost(PlanNode join)
        {
            PlanNodeStatsEstimate outputStats = getStats(join);
            double joinOutputSize = outputStats.getOutputSizeInBytes(join.getOutputSymbols(), types);
            return PlanNodeLocalCostEstimate.ofCpu(joinOutputSize);
        }

        @Override
        public PlanCostEstimate visitExchange(ExchangeNode node, Void context)
        {
            return costForStreaming(node, calculateExchangeCost(node));
        }

        private PlanNodeLocalCostEstimate calculateExchangeCost(ExchangeNode node)
        {
            double inputSizeInBytes = getStats(node).getOutputSizeInBytes(node.getOutputSymbols(), types);
            switch (node.getScope()) {
                case LOCAL:
                    switch (node.getType()) {
                        case GATHER:
                            return PlanNodeLocalCostEstimate.zero();
                        case REPARTITION:
                            return calculateLocalRepartitionCost(inputSizeInBytes);
                        case REPLICATE:
                            return PlanNodeLocalCostEstimate.zero();
                        default:
                            throw new IllegalArgumentException("Unexpected type: " + node.getType());
                    }
                case REMOTE:
                    switch (node.getType()) {
                        case GATHER:
                            return calculateRemoteGatherCost(inputSizeInBytes);
                        case REPARTITION:
                            return calculateRemoteRepartitionCost(inputSizeInBytes);
                        case REPLICATE:
                            // assuming that destination is always source distributed
                            // it is true as now replicated exchange is used for joins only
                            // for replicated join probe side is usually source distributed
                            return calculateRemoteReplicateCost(inputSizeInBytes, taskCountEstimator.estimateSourceDistributedTaskCount());
                        default:
                            throw new IllegalArgumentException("Unexpected type: " + node.getType());
                    }
                default:
                    throw new IllegalArgumentException("Unexpected scope: " + node.getScope());
            }
        }

        @Override
        public PlanCostEstimate visitSemiJoin(SemiJoinNode node, Void context)
        {
            PlanNodeLocalCostEstimate localCost = calculateJoinCost(
                    node,
                    node.getSource(),
                    node.getFilteringSource(),
                    node.getDistributionType().orElse(SemiJoinNode.DistributionType.PARTITIONED).equals(SemiJoinNode.DistributionType.REPLICATED));
            return costForLookupJoin(node, localCost);
        }

        @Override
        public PlanCostEstimate visitSpatialJoin(SpatialJoinNode node, Void context)
        {
            PlanNodeLocalCostEstimate localCost = calculateJoinCost(
                    node,
                    node.getLeft(),
                    node.getRight(),
                    node.getDistributionType() == SpatialJoinNode.DistributionType.REPLICATED);
            return costForLookupJoin(node, localCost);
        }

        @Override
        public PlanCostEstimate visitValues(ValuesNode node, Void context)
        {
            return costForSource(node, PlanNodeLocalCostEstimate.zero());
        }

        @Override
        public PlanCostEstimate visitEnforceSingleRow(EnforceSingleRowNode node, Void context)
        {
            return costForAccumulation(node, PlanNodeLocalCostEstimate.zero());
        }

        @Override
        public PlanCostEstimate visitLimit(LimitNode node, Void context)
        {
            // This is just a wild guess. First of all, LimitNode is rather rare except as a top node of a query plan,
            // so proper cost estimation is not that important. Second, since LimitNode can lead to incomplete evaluation
            // of the source, true cost estimation should be implemented as a "constraint" enforced on a sub-tree and
            // evaluated in context of actual source node type (and their sources).
            PlanNodeLocalCostEstimate localCost = PlanNodeLocalCostEstimate.ofCpu(getStats(node).getOutputSizeInBytes(node.getOutputSymbols(), types));
            return costForStreaming(node, localCost);
        }

        @Override
        public PlanCostEstimate visitUnion(UnionNode node, Void context)
        {
            // Cost will be accounted either in CostCalculatorUsingExchanges#CostEstimator#visitExchange
            // or in CostCalculatorWithEstimatedExchanges#CostEstimator#visitUnion
            // This stub is needed just to avoid the cumulative cost being set to unknown
            return costForStreaming(node, PlanNodeLocalCostEstimate.zero());
        }

        private PlanCostEstimate costForSource(PlanNode node, PlanNodeLocalCostEstimate localCost)
        {
            verify(node.getSources().isEmpty(), "Unexpected sources for %s: %s", node, node.getSources());
            return localCost.toPlanCost();
        }

        private PlanCostEstimate costForAccumulation(PlanNode node, PlanNodeLocalCostEstimate localCost)
        {
            PlanCostEstimate sourcesCost = getSourcesEstimations(node).stream()
                    .reduce(PlanCostEstimate.zero(), CostCalculatorUsingExchanges::addSiblingsCost);
            return new PlanCostEstimate(
                    sourcesCost.getCpuCost() + localCost.getCpuCost(),
                    max(
                            sourcesCost.getMaxMemory(),
                            sourcesCost.getMaxMemoryWhenOutputting() + localCost.getMemoryUsage()),
                    localCost.getMemoryUsage(),
                    sourcesCost.getNetworkCost() + localCost.getNetworkCost());
        }

        private PlanCostEstimate costForStreaming(PlanNode node, PlanNodeLocalCostEstimate localCost)
        {
            PlanCostEstimate sourcesCost = getSourcesEstimations(node).stream()
                    .reduce(PlanCostEstimate.zero(), CostCalculatorUsingExchanges::addSiblingsCost);
            return new PlanCostEstimate(
                    sourcesCost.getCpuCost() + localCost.getCpuCost(),
                    max(
                            sourcesCost.getMaxMemory(),
                            sourcesCost.getMaxMemoryWhenOutputting() + localCost.getMemoryUsage()),
                    sourcesCost.getMaxMemoryWhenOutputting() + localCost.getMemoryUsage(),
                    sourcesCost.getNetworkCost() + localCost.getNetworkCost());
        }

        private PlanCostEstimate costForLookupJoin(PlanNode node, PlanNodeLocalCostEstimate localCost)
        {
            verify(node.getSources().size() == 2, "Unexpected number of sources for %s: %s", node, node.getSources());
            List<PlanCostEstimate> sourcesCosts = getSourcesEstimations(node);
            verify(sourcesCosts.size() == 2);
            PlanCostEstimate probeCost = sourcesCosts.get(0);
            PlanCostEstimate buildCost = sourcesCosts.get(1);

            return new PlanCostEstimate(
                    probeCost.getCpuCost() + buildCost.getCpuCost() + localCost.getCpuCost(),
                    max(
                            probeCost.getMaxMemory() + buildCost.getMaxMemory(),
                            probeCost.getMaxMemoryWhenOutputting() + buildCost.getMaxMemoryWhenOutputting() + localCost.getMemoryUsage()),
                    probeCost.getMaxMemoryWhenOutputting() + localCost.getMemoryUsage(),
                    probeCost.getNetworkCost() + buildCost.getNetworkCost() + localCost.getNetworkCost());
        }

        private PlanNodeStatsEstimate getStats(PlanNode node)
        {
            return stats.getStats(node);
        }

        private List<PlanCostEstimate> getSourcesEstimations(PlanNode node)
        {
            return node.getSources().stream()
                    .map(sourcesCosts::getCost)
                    .collect(toImmutableList());
        }
    }

    private static PlanCostEstimate addSiblingsCost(PlanCostEstimate a, PlanCostEstimate b)
    {
        return new PlanCostEstimate(
                a.getCpuCost() + b.getCpuCost(),
                a.getMaxMemory() + b.getMaxMemory(),
                a.getMaxMemoryWhenOutputting() + b.getMaxMemoryWhenOutputting(),
                a.getNetworkCost() + b.getNetworkCost());
    }
}

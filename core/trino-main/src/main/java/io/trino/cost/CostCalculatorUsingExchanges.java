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
import com.google.errorprone.annotations.ThreadSafe;
import com.google.inject.Inject;
import io.trino.Session;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.GroupReference;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AssignUniqueId;
import io.trino.sql.planner.plan.ChooseAlternativeNode;
import io.trino.sql.planner.plan.EnforceSingleRowNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.RowNumberNode;
import io.trino.sql.planner.plan.SemiJoinNode;
import io.trino.sql.planner.plan.SpatialJoinNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.UnionNode;
import io.trino.sql.planner.plan.ValuesNode;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.cost.CostCalculatorWithEstimatedExchanges.adjustReplicatedJoinLocalExchangeCost;
import static io.trino.cost.CostCalculatorWithEstimatedExchanges.calculateJoinInputCost;
import static io.trino.cost.CostCalculatorWithEstimatedExchanges.calculateLocalRepartitionCost;
import static io.trino.cost.CostCalculatorWithEstimatedExchanges.calculateRemoteGatherCost;
import static io.trino.cost.CostCalculatorWithEstimatedExchanges.calculateRemoteRepartitionCost;
import static io.trino.cost.CostCalculatorWithEstimatedExchanges.calculateRemoteReplicateCost;
import static io.trino.cost.LocalCostEstimate.addPartialComponents;
import static io.trino.sql.planner.plan.AggregationNode.Step.FINAL;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
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
    public PlanCostEstimate calculateCost(PlanNode node, StatsProvider stats, CostProvider sourcesCosts, Session session)
    {
        CostEstimator costEstimator = new CostEstimator(stats, sourcesCosts, taskCountEstimator, session);
        return node.accept(costEstimator, null);
    }

    private static class CostEstimator
            extends PlanVisitor<PlanCostEstimate, Void>
    {
        private final StatsProvider stats;
        private final CostProvider sourcesCosts;
        private final TaskCountEstimator taskCountEstimator;
        private final Session session;

        CostEstimator(StatsProvider stats, CostProvider sourcesCosts, TaskCountEstimator taskCountEstimator, Session session)
        {
            this.stats = requireNonNull(stats, "stats is null");
            this.sourcesCosts = requireNonNull(sourcesCosts, "sourcesCosts is null");
            this.taskCountEstimator = requireNonNull(taskCountEstimator, "taskCountEstimator is null");
            this.session = requireNonNull(session, "session is null");
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
        public PlanCostEstimate visitChooseAlternativeNode(ChooseAlternativeNode node, Void context)
        {
            // It's unknown which alternatives will get executed. The first alternative should be
            // the most pessimistic one and therefore probably incurs the maximal cost.
            // Note that there is no local cost of ChooseAlternativeNode, because there is no actual execution for it.
            return sourcesCosts.getCost(node.getSources().get(0));
        }

        @Override
        public PlanCostEstimate visitAssignUniqueId(AssignUniqueId node, Void context)
        {
            LocalCostEstimate localCost = LocalCostEstimate.ofCpu(getStats(node).getOutputSizeInBytes(ImmutableList.of(node.getIdColumn())));
            return costForStreaming(node, localCost);
        }

        @Override
        public PlanCostEstimate visitRowNumber(RowNumberNode node, Void context)
        {
            List<Symbol> symbols = node.getOutputSymbols();
            // when maxRowCountPerPartition is set, the RowNumberOperator
            // copies values for all the columns into a page builder
            if (node.getMaxRowCountPerPartition().isEmpty()) {
                symbols = ImmutableList.<Symbol>builder()
                        .addAll(node.getPartitionBy())
                        .add(node.getRowNumberSymbol())
                        .build();
            }
            PlanNodeStatsEstimate stats = getStats(node);
            double cpuCost = stats.getOutputSizeInBytes(symbols);
            double memoryCost = node.getPartitionBy().isEmpty() ? 0 : stats.getOutputSizeInBytes(node.getSource().getOutputSymbols());
            LocalCostEstimate localCost = LocalCostEstimate.of(cpuCost, memoryCost, 0);
            return costForStreaming(node, localCost);
        }

        @Override
        public PlanCostEstimate visitOutput(OutputNode node, Void context)
        {
            return costForStreaming(node, LocalCostEstimate.zero());
        }

        @Override
        public PlanCostEstimate visitTableScan(TableScanNode node, Void context)
        {
            // TODO: add network cost, based on input size in bytes? Or let connector provide this cost?
            LocalCostEstimate localCost = LocalCostEstimate.ofCpu(getStats(node).getOutputSizeInBytes(node.getOutputSymbols()));
            return costForSource(node, localCost);
        }

        @Override
        public PlanCostEstimate visitFilter(FilterNode node, Void context)
        {
            LocalCostEstimate localCost = LocalCostEstimate.ofCpu(getStats(node.getSource()).getOutputSizeInBytes(node.getOutputSymbols()));
            return costForStreaming(node, localCost);
        }

        @Override
        public PlanCostEstimate visitProject(ProjectNode node, Void context)
        {
            LocalCostEstimate localCost = LocalCostEstimate.ofCpu(getStats(node).getOutputSizeInBytes(node.getOutputSymbols()));
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
            double cpuCost = sourceStats.getOutputSizeInBytes(node.getSource().getOutputSymbols());
            double memoryCost = aggregationStats.getOutputSizeInBytes(node.getOutputSymbols());
            LocalCostEstimate localCost = LocalCostEstimate.of(cpuCost, memoryCost, 0);
            return costForAccumulation(node, localCost);
        }

        @Override
        public PlanCostEstimate visitJoin(JoinNode node, Void context)
        {
            LocalCostEstimate localCost = calculateJoinCost(
                    node,
                    node.getLeft(),
                    node.getRight(),
                    Objects.equals(node.getDistributionType(), Optional.of(JoinNode.DistributionType.REPLICATED)));
            return costForLookupJoin(node, localCost);
        }

        private LocalCostEstimate calculateJoinCost(PlanNode join, PlanNode probe, PlanNode build, boolean replicated)
        {
            int estimatedSourceDistributedTaskCount = taskCountEstimator.estimateSourceDistributedTaskCount(session);
            LocalCostEstimate joinInputCost = calculateJoinInputCost(
                    probe,
                    build,
                    stats,
                    replicated,
                    estimatedSourceDistributedTaskCount);
            // TODO: Use traits (https://github.com/trinodb/trino/issues/4763) instead, to correctly estimate
            // local exchange cost for replicated join in CostCalculatorUsingExchanges#visitExchange
            LocalCostEstimate adjustedLocalExchangeCost = adjustReplicatedJoinLocalExchangeCost(
                    build,
                    stats,
                    replicated,
                    estimatedSourceDistributedTaskCount);
            LocalCostEstimate joinOutputCost = calculateJoinOutputCost(join);
            return addPartialComponents(joinInputCost, adjustedLocalExchangeCost, joinOutputCost);
        }

        private LocalCostEstimate calculateJoinOutputCost(PlanNode join)
        {
            PlanNodeStatsEstimate outputStats = getStats(join);
            double joinOutputSize = outputStats.getOutputSizeInBytes(join.getOutputSymbols());
            return LocalCostEstimate.ofCpu(joinOutputSize);
        }

        @Override
        public PlanCostEstimate visitExchange(ExchangeNode node, Void context)
        {
            return costForStreaming(node, calculateExchangeCost(node));
        }

        private LocalCostEstimate calculateExchangeCost(ExchangeNode node)
        {
            double inputSizeInBytes = getStats(node).getOutputSizeInBytes(node.getOutputSymbols());
            return switch (node.getScope()) {
                case LOCAL -> switch (node.getType()) {
                    case GATHER -> LocalCostEstimate.zero();
                    case REPARTITION -> calculateLocalRepartitionCost(inputSizeInBytes);
                    case REPLICATE -> LocalCostEstimate.zero();
                };
                case REMOTE -> switch (node.getType()) {
                    case GATHER -> calculateRemoteGatherCost(inputSizeInBytes);
                    case REPARTITION -> calculateRemoteRepartitionCost(inputSizeInBytes);
                    // assuming that destination is always source distributed
                    // it is true as now replicated exchange is used for joins only
                    // for replicated join probe side is usually source distributed
                    case REPLICATE -> calculateRemoteReplicateCost(inputSizeInBytes, taskCountEstimator.estimateSourceDistributedTaskCount(session));
                };
            };
        }

        @Override
        public PlanCostEstimate visitSemiJoin(SemiJoinNode node, Void context)
        {
            LocalCostEstimate localCost = calculateJoinCost(
                    node,
                    node.getSource(),
                    node.getFilteringSource(),
                    node.getDistributionType().orElse(SemiJoinNode.DistributionType.PARTITIONED) == SemiJoinNode.DistributionType.REPLICATED);
            return costForLookupJoin(node, localCost);
        }

        @Override
        public PlanCostEstimate visitSpatialJoin(SpatialJoinNode node, Void context)
        {
            LocalCostEstimate localCost = calculateJoinCost(
                    node,
                    node.getLeft(),
                    node.getRight(),
                    node.getDistributionType() == SpatialJoinNode.DistributionType.REPLICATED);
            return costForLookupJoin(node, localCost);
        }

        @Override
        public PlanCostEstimate visitValues(ValuesNode node, Void context)
        {
            return costForSource(node, LocalCostEstimate.zero());
        }

        @Override
        public PlanCostEstimate visitEnforceSingleRow(EnforceSingleRowNode node, Void context)
        {
            return costForAccumulation(node, LocalCostEstimate.zero());
        }

        @Override
        public PlanCostEstimate visitLimit(LimitNode node, Void context)
        {
            // This is just a wild guess. First of all, LimitNode is rather rare except as a top node of a query plan,
            // so proper cost estimation is not that important. Second, since LimitNode can lead to incomplete evaluation
            // of the source, true cost estimation should be implemented as a "constraint" enforced on a sub-tree and
            // evaluated in context of actual source node type (and their sources).
            LocalCostEstimate localCost = LocalCostEstimate.ofCpu(getStats(node).getOutputSizeInBytes(node.getOutputSymbols()));
            return costForStreaming(node, localCost);
        }

        @Override
        public PlanCostEstimate visitUnion(UnionNode node, Void context)
        {
            // Cost will be accounted either in CostCalculatorUsingExchanges#CostEstimator#visitExchange
            // or in CostCalculatorWithEstimatedExchanges#CostEstimator#visitUnion
            // This stub is needed just to avoid the cumulative cost being set to unknown
            return costForStreaming(node, LocalCostEstimate.zero());
        }

        private PlanCostEstimate costForSource(PlanNode node, LocalCostEstimate localCost)
        {
            verify(node.getSources().isEmpty(), "Unexpected sources for %s: %s", node, node.getSources());
            return new PlanCostEstimate(localCost.getCpuCost(), localCost.getMaxMemory(), localCost.getMaxMemory(), localCost.getNetworkCost(), localCost);
        }

        private PlanCostEstimate costForAccumulation(PlanNode node, LocalCostEstimate localCost)
        {
            PlanCostEstimate sourcesCost = getSourcesEstimations(node)
                    .reduce(PlanCostEstimate.zero(), CostCalculatorUsingExchanges::addParallelSiblingsCost);
            return new PlanCostEstimate(
                    sourcesCost.getCpuCost() + localCost.getCpuCost(),
                    max(
                            sourcesCost.getMaxMemory(), // Accumulating operator allocates insignificant amount of memory (usually none) before first input page is received
                            sourcesCost.getMaxMemoryWhenOutputting() + localCost.getMaxMemory()),
                    localCost.getMaxMemory(), // Source freed its memory allocations when finished its output
                    sourcesCost.getNetworkCost() + localCost.getNetworkCost(),
                    localCost);
        }

        private PlanCostEstimate costForStreaming(PlanNode node, LocalCostEstimate localCost)
        {
            PlanCostEstimate sourcesCost = getSourcesEstimations(node)
                    .reduce(PlanCostEstimate.zero(), CostCalculatorUsingExchanges::addParallelSiblingsCost);
            return new PlanCostEstimate(
                    sourcesCost.getCpuCost() + localCost.getCpuCost(),
                    max(
                            sourcesCost.getMaxMemory(), // Streaming operator allocates insignificant amount of memory (usually none) before first input page is received
                            sourcesCost.getMaxMemoryWhenOutputting() + localCost.getMaxMemory()),
                    sourcesCost.getMaxMemoryWhenOutputting() + localCost.getMaxMemory(),
                    sourcesCost.getNetworkCost() + localCost.getNetworkCost(),
                    localCost);
        }

        private PlanCostEstimate costForLookupJoin(PlanNode node, LocalCostEstimate localCost)
        {
            verify(node.getSources().size() == 2, "Unexpected number of sources for %s: %s", node, node.getSources());
            List<PlanCostEstimate> sourcesCosts = getSourcesEstimations(node).collect(toImmutableList());
            verify(sourcesCosts.size() == 2);
            PlanCostEstimate probeCost = sourcesCosts.get(0);
            PlanCostEstimate buildCost = sourcesCosts.get(1);

            return new PlanCostEstimate(
                    probeCost.getCpuCost() + buildCost.getCpuCost() + localCost.getCpuCost(),
                    max(
                            probeCost.getMaxMemory() + buildCost.getMaxMemory(), // Probe and build execute independently, so their max memory allocations can be realized at the same time
                            probeCost.getMaxMemory() + buildCost.getMaxMemoryWhenOutputting() + localCost.getMaxMemory()),
                    probeCost.getMaxMemoryWhenOutputting() + localCost.getMaxMemory(), // Build side finished and freed its memory allocations
                    probeCost.getNetworkCost() + buildCost.getNetworkCost() + localCost.getNetworkCost(),
                    localCost);
        }

        private PlanNodeStatsEstimate getStats(PlanNode node)
        {
            return stats.getStats(node);
        }

        private Stream<PlanCostEstimate> getSourcesEstimations(PlanNode node)
        {
            return node.getSources().stream()
                    .map(sourcesCosts::getCost);
        }
    }

    private static PlanCostEstimate addParallelSiblingsCost(PlanCostEstimate a, PlanCostEstimate b)
    {
        return new PlanCostEstimate(
                a.getCpuCost() + b.getCpuCost(),
                a.getMaxMemory() + b.getMaxMemory(),
                a.getMaxMemoryWhenOutputting() + b.getMaxMemoryWhenOutputting(),
                a.getNetworkCost() + b.getNetworkCost());
    }
}

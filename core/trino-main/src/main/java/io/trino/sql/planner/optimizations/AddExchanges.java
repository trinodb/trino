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
package io.trino.sql.planner.optimizations;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.cost.CachingStatsProvider;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.cost.StatsCalculator;
import io.trino.cost.StatsProvider;
import io.trino.cost.TableStatsProvider;
import io.trino.cost.TaskCountEstimator;
import io.trino.execution.querystats.PlanOptimizersStatsCollector;
import io.trino.execution.warnings.WarningCollector;
import io.trino.operator.RetryPolicy;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.GroupingProperty;
import io.trino.spi.connector.LocalProperty;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.DomainTranslator;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.SystemPartitioningHandle;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.iterative.rule.PushPredicateIntoTableScan;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ApplyNode;
import io.trino.sql.planner.plan.AssignUniqueId;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.ChildReplacer;
import io.trino.sql.planner.plan.CorrelatedJoinNode;
import io.trino.sql.planner.plan.DistinctLimitNode;
import io.trino.sql.planner.plan.EnforceSingleRowNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.ExplainAnalyzeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.GroupIdNode;
import io.trino.sql.planner.plan.IndexJoinNode;
import io.trino.sql.planner.plan.IndexSourceNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.MarkDistinctNode;
import io.trino.sql.planner.plan.MergeWriterNode;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PatternRecognitionNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.RefreshMaterializedViewNode;
import io.trino.sql.planner.plan.RowNumberNode;
import io.trino.sql.planner.plan.SemiJoinNode;
import io.trino.sql.planner.plan.SimpleTableExecuteNode;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.planner.plan.SpatialJoinNode;
import io.trino.sql.planner.plan.StatisticsWriterNode;
import io.trino.sql.planner.plan.TableDeleteNode;
import io.trino.sql.planner.plan.TableExecuteNode;
import io.trino.sql.planner.plan.TableFinishNode;
import io.trino.sql.planner.plan.TableFunctionNode;
import io.trino.sql.planner.plan.TableFunctionProcessorNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TableWriterNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.TopNRankingNode;
import io.trino.sql.planner.plan.UnionNode;
import io.trino.sql.planner.plan.UnnestNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.SymbolReference;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.SystemSessionProperties.getMaxWriterTaskCount;
import static io.trino.SystemSessionProperties.getRetryPolicy;
import static io.trino.SystemSessionProperties.getTaskConcurrency;
import static io.trino.SystemSessionProperties.ignoreDownStreamPreferences;
import static io.trino.SystemSessionProperties.isColocatedJoinEnabled;
import static io.trino.SystemSessionProperties.isDistributedSortEnabled;
import static io.trino.SystemSessionProperties.isForceSingleNodeOutput;
import static io.trino.SystemSessionProperties.isUseCostBasedPartitioning;
import static io.trino.SystemSessionProperties.isUseExactPartitioning;
import static io.trino.SystemSessionProperties.isUsePartialDistinctLimit;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SCALED_WRITER_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SCALED_WRITER_ROUND_ROBIN_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.optimizations.ActualProperties.Global.partitionedOn;
import static io.trino.sql.planner.optimizations.ActualProperties.Global.singlePartition;
import static io.trino.sql.planner.optimizations.LocalProperties.grouped;
import static io.trino.sql.planner.optimizations.PreferredProperties.partitionedWithLocal;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.planner.plan.ExchangeNode.Type.GATHER;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.trino.sql.planner.plan.ExchangeNode.gatheringExchange;
import static io.trino.sql.planner.plan.ExchangeNode.mergingExchange;
import static io.trino.sql.planner.plan.ExchangeNode.partitionedExchange;
import static io.trino.sql.planner.plan.ExchangeNode.replicatedExchange;
import static io.trino.sql.planner.plan.ExchangeNode.roundRobinExchange;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class AddExchanges
        implements PlanOptimizer
{
    private final PlannerContext plannerContext;
    private final TypeAnalyzer typeAnalyzer;
    private final StatsCalculator statsCalculator;
    private final TaskCountEstimator taskCountEstimator;

    public AddExchanges(PlannerContext plannerContext, TypeAnalyzer typeAnalyzer, StatsCalculator statsCalculator, TaskCountEstimator taskCountEstimator)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
        this.taskCountEstimator = requireNonNull(taskCountEstimator, "taskCountEstimator is null");
    }

    @Override
    public PlanNode optimize(
            PlanNode plan,
            Session session,
            TypeProvider types,
            SymbolAllocator symbolAllocator,
            PlanNodeIdAllocator idAllocator,
            WarningCollector warningCollector,
            PlanOptimizersStatsCollector planOptimizersStatsCollector,
            TableStatsProvider tableStatsProvider)
    {
        PlanWithProperties result = plan.accept(new Rewriter(idAllocator, symbolAllocator, session, tableStatsProvider), PreferredProperties.any());
        return result.getNode();
    }

    private class Rewriter
            extends PlanVisitor<PlanWithProperties, PreferredProperties>
    {
        // using parent partitioning may limit parallelism if the estimate of NDV is wrong or partitions are skewed.
        // this constant should be small enough to cover as many cases as possible but also big enough to provide some buffer for the above issues.
        private static final int PREFER_PARENT_PARTITIONING_MIN_PARTITIONS_PER_DRIVER_MULTIPLIER = 128;
        private final PlanNodeIdAllocator idAllocator;
        private final SymbolAllocator symbolAllocator;
        private final TypeProvider types;
        private final StatsProvider statsProvider;
        private final Session session;
        private final DomainTranslator domainTranslator;
        private final boolean redistributeWrites;
        private final boolean scaleWriters;

        public Rewriter(PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Session session, TableStatsProvider tableStatsProvider)
        {
            this.idAllocator = idAllocator;
            this.symbolAllocator = symbolAllocator;
            this.types = symbolAllocator.getTypes();
            this.statsProvider = new CachingStatsProvider(statsCalculator, session, types, tableStatsProvider);
            this.session = session;
            this.domainTranslator = new DomainTranslator(plannerContext);
            this.redistributeWrites = SystemSessionProperties.isRedistributeWrites(session);
            this.scaleWriters = SystemSessionProperties.isScaleWriters(session);
        }

        @Override
        protected PlanWithProperties visitPlan(PlanNode node, PreferredProperties preferredProperties)
        {
            return rebaseAndDeriveProperties(node, planChild(node, preferredProperties));
        }

        @Override
        public PlanWithProperties visitProject(ProjectNode node, PreferredProperties preferredProperties)
        {
            Map<Symbol, Symbol> identities = computeIdentityTranslations(node.getAssignments());
            PreferredProperties translatedPreferred = preferredProperties.translate(symbol -> Optional.ofNullable(identities.get(symbol)));

            return rebaseAndDeriveProperties(node, planChild(node, translatedPreferred));
        }

        @Override
        public PlanWithProperties visitOutput(OutputNode node, PreferredProperties preferredProperties)
        {
            PlanWithProperties child = planChild(node, PreferredProperties.undistributed());

            if (!child.getProperties().isSingleNode() && isForceSingleNodeOutput(session)) {
                child = withDerivedProperties(
                        gatheringExchange(idAllocator.getNextId(), REMOTE, child.getNode()),
                        child.getProperties());
            }

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitEnforceSingleRow(EnforceSingleRowNode node, PreferredProperties preferredProperties)
        {
            PlanWithProperties child = planChild(node, PreferredProperties.any());

            if (!child.getProperties().isSingleNode()) {
                child = withDerivedProperties(
                        gatheringExchange(idAllocator.getNextId(), REMOTE, child.getNode()),
                        child.getProperties());
            }

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitAggregation(AggregationNode node, PreferredProperties parentPreferredProperties)
        {
            Set<Symbol> partitioningRequirement = ImmutableSet.copyOf(node.getGroupingKeys());

            boolean preferSingleNode = node.hasSingleNodeExecutionPreference(session, plannerContext.getMetadata());
            PreferredProperties preferredProperties = preferSingleNode ? PreferredProperties.undistributed() : PreferredProperties.any();

            if (!node.getGroupingKeys().isEmpty()) {
                preferredProperties = computePreference(
                        partitionedWithLocal(
                                partitioningRequirement,
                                grouped(node.getGroupingKeys())),
                        parentPreferredProperties);
            }

            PlanWithProperties child = planChild(node, preferredProperties);

            if (child.getProperties().isSingleNode()) {
                // If already unpartitioned, just drop the single aggregation back on
                return rebaseAndDeriveProperties(node, child);
            }

            if (preferSingleNode) {
                child = withDerivedProperties(
                        gatheringExchange(idAllocator.getNextId(), REMOTE, child.getNode()),
                        child.getProperties());
            }
            else if (!isNodePartitionedOn(child.getProperties(), partitioningRequirement) || node.hasEmptyGroupingSet()) {
                List<Symbol> partitioningKeys = parentPreferredProperties.getGlobalProperties()
                        .flatMap(PreferredProperties.Global::getPartitioningProperties)
                        .map(PreferredProperties.PartitioningProperties::getPartitioningColumns)
                        .flatMap(partitioningColumns -> useParentPreferredPartitioning(node, partitioningColumns))
                        .orElse(node.getGroupingKeys());
                child = withDerivedProperties(
                        partitionedExchange(idAllocator.getNextId(), REMOTE, child.getNode(), partitioningKeys, node.getHashSymbol()),
                        child.getProperties());
            }
            return rebaseAndDeriveProperties(node, child);
        }

        /**
         * For cases where parent prefers to be partitioned by a subset of the current node {@link AggregationNode#getGroupingKeys()},
         * and the preferred partitioning columns have enough distinct values to provide good parallelism, we want to partition
         * just by parent preferred partitioning columns to avoid another data shuffle required by the parent.
         */
        private Optional<List<Symbol>> useParentPreferredPartitioning(AggregationNode node, Set<Symbol> parentPreferredPartitioningColumns)
        {
            if (isUseExactPartitioning(session) || !isUseCostBasedPartitioning(session)) {
                return Optional.empty();
            }
            if (parentPreferredPartitioningColumns.isEmpty()) {
                return Optional.empty();
            }
            if (!ImmutableSet.copyOf(node.getGroupingKeys()).containsAll(parentPreferredPartitioningColumns)) {
                // parent wants to be partitioned by at least one different column
                return Optional.empty();
            }
            double parentPartitioningNDV = getMinDistinctValueCountEstimate(statsProvider.getStats(node), parentPreferredPartitioningColumns);
            if (Double.isNaN(parentPartitioningNDV)) {
                // unknown estimate, fallback to partitioning by all grouping keys
                return Optional.empty();
            }
            int maxConcurrentPartitionsCount = taskCountEstimator.estimateHashedTaskCount(session) * getTaskConcurrency(session);

            if (parentPartitioningNDV <= PREFER_PARENT_PARTITIONING_MIN_PARTITIONS_PER_DRIVER_MULTIPLIER * maxConcurrentPartitionsCount) {
                // small parentPartitioningNDV reduces the parallelism, also because the partitioning may be skewed.
                // This makes query to underutilize the cluster CPU but also to possibly concentrate memory on few nodes.
                // Fallback to partitioning by all grouping keys to increase the chance of higher parallelism.
                return Optional.empty();
            }
            List<Symbol> newGroupingKeys = ImmutableList.copyOf(parentPreferredPartitioningColumns);
            return Optional.of(newGroupingKeys);
        }

        private static double getMinDistinctValueCountEstimate(PlanNodeStatsEstimate nodeStatsEstimate, Set<Symbol> groupingKeys)
        {
            double min = Double.NaN;
            for (Symbol groupingKey : groupingKeys) {
                double distinctValuesCount = nodeStatsEstimate.getSymbolStatistics(groupingKey).getDistinctValuesCount();
                if (Double.isNaN(distinctValuesCount)) {
                    return Double.NaN;
                }
                if (Double.isNaN(min) || distinctValuesCount < min) {
                    min = distinctValuesCount;
                }
            }

            return min;
        }

        @Override
        public PlanWithProperties visitGroupId(GroupIdNode node, PreferredProperties preferredProperties)
        {
            PreferredProperties childPreference = preferredProperties.translate(translateGroupIdSymbols(node));
            PlanWithProperties child = planChild(node, childPreference);
            return rebaseAndDeriveProperties(node, child);
        }

        private Function<Symbol, Optional<Symbol>> translateGroupIdSymbols(GroupIdNode node)
        {
            return symbol -> {
                if (node.getAggregationArguments().contains(symbol)) {
                    return Optional.of(symbol);
                }

                if (node.getCommonGroupingColumns().contains(symbol)) {
                    return Optional.of(node.getGroupingColumns().get(symbol));
                }

                return Optional.empty();
            };
        }

        @Override
        public PlanWithProperties visitMarkDistinct(MarkDistinctNode node, PreferredProperties preferredProperties)
        {
            PreferredProperties preferredChildProperties = computePreference(
                    partitionedWithLocal(ImmutableSet.copyOf(node.getDistinctSymbols()), grouped(node.getDistinctSymbols())),
                    preferredProperties);

            PlanWithProperties child = node.getSource().accept(this, preferredChildProperties);

            if (child.getProperties().isSingleNode() || !isNodePartitionedOn(child.getProperties(), node.getDistinctSymbols())) {
                child = withDerivedProperties(
                        partitionedExchange(
                                idAllocator.getNextId(),
                                REMOTE,
                                child.getNode(),
                                node.getDistinctSymbols(),
                                node.getHashSymbol()),
                        child.getProperties());
            }

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitWindow(WindowNode node, PreferredProperties preferredProperties)
        {
            List<LocalProperty<Symbol>> desiredProperties = new ArrayList<>();
            if (!node.getPartitionBy().isEmpty()) {
                desiredProperties.add(new GroupingProperty<>(node.getPartitionBy()));
            }
            node.getOrderingScheme().ifPresent(orderingScheme -> desiredProperties.addAll(orderingScheme.toLocalProperties()));

            PlanWithProperties child = planChild(
                    node,
                    computePreference(
                            partitionedWithLocal(ImmutableSet.copyOf(node.getPartitionBy()), desiredProperties),
                            preferredProperties));

            if (!isNodePartitionedOn(child.getProperties(), node.getPartitionBy())) {
                if (node.getPartitionBy().isEmpty()) {
                    child = withDerivedProperties(
                            gatheringExchange(idAllocator.getNextId(), REMOTE, child.getNode()),
                            child.getProperties());
                }
                else {
                    child = withDerivedProperties(
                            partitionedExchange(idAllocator.getNextId(), REMOTE, child.getNode(), node.getPartitionBy(), node.getHashSymbol()),
                            child.getProperties());
                }
            }

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitPatternRecognition(PatternRecognitionNode node, PreferredProperties preferredProperties)
        {
            List<LocalProperty<Symbol>> desiredProperties = new ArrayList<>();
            if (!node.getPartitionBy().isEmpty()) {
                desiredProperties.add(new GroupingProperty<>(node.getPartitionBy()));
            }
            node.getOrderingScheme().ifPresent(orderingScheme -> desiredProperties.addAll(orderingScheme.toLocalProperties()));

            PlanWithProperties child = planChild(
                    node,
                    computePreference(
                            partitionedWithLocal(ImmutableSet.copyOf(node.getPartitionBy()), desiredProperties),
                            preferredProperties));

            if (!isNodePartitionedOn(child.getProperties(), node.getPartitionBy())) {
                if (node.getPartitionBy().isEmpty()) {
                    child = withDerivedProperties(
                            gatheringExchange(idAllocator.getNextId(), REMOTE, child.getNode()),
                            child.getProperties());
                }
                else {
                    child = withDerivedProperties(
                            partitionedExchange(idAllocator.getNextId(), REMOTE, child.getNode(), node.getPartitionBy(), node.getHashSymbol()),
                            child.getProperties());
                }
            }

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitTableFunction(TableFunctionNode node, PreferredProperties preferredProperties)
        {
            throw new IllegalStateException(format("Unexpected node: TableFunctionNode (%s)", node.getName()));
        }

        @Override
        public PlanWithProperties visitTableFunctionProcessor(TableFunctionProcessorNode node, PreferredProperties preferredProperties)
        {
            if (node.getSource().isEmpty()) {
                return new PlanWithProperties(node, deriveProperties(node, ImmutableList.of()));
            }

            if (node.getSpecification().isEmpty()) {
                // node.getSpecification.isEmpty() indicates that there were no sources or a single source with row semantics.
                // The case of no sources was addressed above.
                // The case of a single source with row semantics is addressed here. A single source with row semantics can be distributed arbitrarily.
                PlanWithProperties child = planChild(node, PreferredProperties.any());
                return rebaseAndDeriveProperties(node, child);
            }

            List<Symbol> partitionBy = node.getSpecification().orElseThrow().getPartitionBy();
            List<LocalProperty<Symbol>> desiredProperties = new ArrayList<>();
            if (!partitionBy.isEmpty()) {
                desiredProperties.add(new GroupingProperty<>(partitionBy));
            }
            node.getSpecification().orElseThrow().getOrderingScheme().ifPresent(orderingScheme -> desiredProperties.addAll(orderingScheme.toLocalProperties()));

            PlanWithProperties child = planChild(node, partitionedWithLocal(ImmutableSet.copyOf(partitionBy), desiredProperties));

            // TODO do not gather if already gathered
            if (!node.isPruneWhenEmpty()) {
                child = withDerivedProperties(
                        gatheringExchange(idAllocator.getNextId(), REMOTE, child.getNode()),
                        child.getProperties());
            }
            else if (!isNodePartitionedOn(child.getProperties(), partitionBy)) {
                if (partitionBy.isEmpty()) {
                    child = withDerivedProperties(
                            gatheringExchange(idAllocator.getNextId(), REMOTE, child.getNode()),
                            child.getProperties());
                }
                else {
                    child = withDerivedProperties(
                            partitionedExchange(idAllocator.getNextId(), REMOTE, child.getNode(), partitionBy, node.getHashSymbol()),
                            child.getProperties());
                }
            }

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitRowNumber(RowNumberNode node, PreferredProperties preferredProperties)
        {
            if (node.getPartitionBy().isEmpty()) {
                PlanWithProperties child = planChild(node, PreferredProperties.undistributed());

                if (!child.getProperties().isSingleNode()) {
                    child = withDerivedProperties(
                            gatheringExchange(idAllocator.getNextId(), REMOTE, child.getNode()),
                            child.getProperties());
                }

                return rebaseAndDeriveProperties(node, child);
            }

            PlanWithProperties child = planChild(
                    node,
                    computePreference(partitionedWithLocal(ImmutableSet.copyOf(node.getPartitionBy()), grouped(node.getPartitionBy())),
                            preferredProperties));

            // TODO: add config option/session property to force parallel plan if child is unpartitioned and window has a PARTITION BY clause
            if (!isNodePartitionedOn(child.getProperties(), node.getPartitionBy())) {
                child = withDerivedProperties(
                        partitionedExchange(
                                idAllocator.getNextId(),
                                REMOTE,
                                child.getNode(),
                                node.getPartitionBy(),
                                node.getHashSymbol()),
                        child.getProperties());
            }

            // TODO: streaming

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitTopNRanking(TopNRankingNode node, PreferredProperties preferredProperties)
        {
            PreferredProperties preferredChildProperties;
            Function<PlanNode, PlanNode> addExchange;

            if (node.getPartitionBy().isEmpty()) {
                preferredChildProperties = PreferredProperties.any();
                addExchange = partial -> gatheringExchange(idAllocator.getNextId(), REMOTE, partial);
            }
            else {
                preferredChildProperties = computePreference(
                        partitionedWithLocal(ImmutableSet.copyOf(node.getPartitionBy()), grouped(node.getPartitionBy())),
                        preferredProperties);
                addExchange = partial -> partitionedExchange(idAllocator.getNextId(), REMOTE, partial, node.getPartitionBy(), node.getHashSymbol());
            }

            PlanWithProperties child = planChild(node, preferredChildProperties);
            if (!isNodePartitionedOn(child.getProperties(), node.getPartitionBy())) {
                // add exchange + push function to child
                child = withDerivedProperties(
                        new TopNRankingNode(
                                idAllocator.getNextId(),
                                child.getNode(),
                                node.getSpecification(),
                                node.getRankingType(),
                                node.getRankingSymbol(),
                                node.getMaxRankingPerPartition(),
                                true,
                                node.getHashSymbol()),
                        child.getProperties());

                child = withDerivedProperties(addExchange.apply(child.getNode()), child.getProperties());
            }

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitTopN(TopNNode node, PreferredProperties preferredProperties)
        {
            return switch (node.getStep()) {
                case SINGLE, FINAL -> {
                    PlanWithProperties child = planChild(node, PreferredProperties.undistributed());
                    if (!child.getProperties().isSingleNode()) {
                        child = withDerivedProperties(
                                gatheringExchange(idAllocator.getNextId(), REMOTE, child.getNode()),
                                child.getProperties());
                    }
                    yield rebaseAndDeriveProperties(node, child);
                }
                case PARTIAL -> {
                    PlanWithProperties child = planChild(node, PreferredProperties.any());
                    // If source is pre-sorted, partial topN can be replaced with partial limit N.
                    // We record the pre-sorted symbols in LimitNode to avoid pushdown of such replaced LimitNode
                    // through the source which was producing ordered input.
                    List<LocalProperty<Symbol>> desiredProperties = node.getOrderingScheme().toLocalProperties();
                    boolean sortingSatisfied = LocalProperties.match(child.getProperties().getLocalProperties(), desiredProperties).stream()
                            .allMatch(Optional::isEmpty);
                    if (sortingSatisfied) {
                        yield withDerivedProperties(
                                new LimitNode(
                                        node.getId(),
                                        child.getNode(),
                                        node.getCount(),
                                        Optional.empty(),
                                        true,
                                        node.getOrderingScheme().getOrderBy()),
                                child.getProperties());
                    }
                    yield rebaseAndDeriveProperties(node, child);
                }
            };
        }

        @Override
        public PlanWithProperties visitSort(SortNode node, PreferredProperties preferredProperties)
        {
            PlanWithProperties child = planChild(node, PreferredProperties.undistributed());

            if (child.getProperties().isSingleNode()) {
                // current plan so far is single node, so local properties are effectively global properties
                // skip the SortNode if the local properties guarantee ordering on Sort keys
                // TODO: This should be extracted as a separate optimizer once the planner is able to reason about the ordering of each operator
                List<LocalProperty<Symbol>> desiredProperties = node.getOrderingScheme().toLocalProperties();

                if (LocalProperties.match(child.getProperties().getLocalProperties(), desiredProperties).stream()
                        .noneMatch(Optional::isPresent)) {
                    return child;
                }
            }

            if (isDistributedSortEnabled(session)) {
                child = planChild(node, PreferredProperties.any());
                // insert round robin exchange to eliminate skewness issues
                PlanNode source = roundRobinExchange(idAllocator.getNextId(), REMOTE, child.getNode());
                return withDerivedProperties(
                        mergingExchange(
                                idAllocator.getNextId(),
                                REMOTE,
                                new SortNode(
                                        idAllocator.getNextId(),
                                        source,
                                        node.getOrderingScheme(),
                                        true),
                                node.getOrderingScheme()),
                        child.getProperties());
            }

            if (!child.getProperties().isSingleNode()) {
                child = withDerivedProperties(
                        gatheringExchange(idAllocator.getNextId(), REMOTE, child.getNode()),
                        child.getProperties());
            }

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitLimit(LimitNode node, PreferredProperties preferredProperties)
        {
            if (node.isWithTies()) {
                throw new IllegalStateException("Unexpected node: LimitNode with ties");
            }

            PlanWithProperties child = planChild(node, PreferredProperties.any());

            if (!child.getProperties().isSingleNode()) {
                child = withDerivedProperties(
                        new LimitNode(idAllocator.getNextId(), child.getNode(), node.getCount(), true),
                        child.getProperties());

                child = withDerivedProperties(
                        gatheringExchange(idAllocator.getNextId(), REMOTE, child.getNode()),
                        child.getProperties());
            }

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitDistinctLimit(DistinctLimitNode node, PreferredProperties preferredProperties)
        {
            PlanWithProperties child = planChild(node, PreferredProperties.any());

            if (!child.getProperties().isSingleNode() && isUsePartialDistinctLimit(session)) {
                child = withDerivedProperties(
                        gatheringExchange(
                                idAllocator.getNextId(),
                                REMOTE,
                                new DistinctLimitNode(idAllocator.getNextId(), child.getNode(), node.getLimit(), true, node.getDistinctSymbols(), node.getHashSymbol())),
                        child.getProperties());
            }

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitFilter(FilterNode node, PreferredProperties preferredProperties)
        {
            if (node.getSource() instanceof TableScanNode) {
                Optional<PlanNode> plan = PushPredicateIntoTableScan.pushFilterIntoTableScan(
                        node,
                        (TableScanNode) node.getSource(),
                        true,
                        session,
                        symbolAllocator,
                        plannerContext,
                        typeAnalyzer,
                        statsProvider,
                        domainTranslator);
                if (plan.isPresent()) {
                    return new PlanWithProperties(plan.get(), derivePropertiesRecursively(plan.get()));
                }
            }

            return rebaseAndDeriveProperties(node, planChild(node, preferredProperties));
        }

        @Override
        public PlanWithProperties visitTableScan(TableScanNode node, PreferredProperties preferredProperties)
        {
            return new PlanWithProperties(node, deriveProperties(node, ImmutableList.of()));
        }

        @Override
        public PlanWithProperties visitRefreshMaterializedView(RefreshMaterializedViewNode node, PreferredProperties preferredProperties)
        {
            return new PlanWithProperties(node, deriveProperties(node, ImmutableList.of()));
        }

        @Override
        public PlanWithProperties visitTableWriter(TableWriterNode node, PreferredProperties preferredProperties)
        {
            return visitTableWriter(node, node.getPartitioningScheme(), node.getSource(), preferredProperties, node.getTarget());
        }

        @Override
        public PlanWithProperties visitTableExecute(TableExecuteNode node, PreferredProperties preferredProperties)
        {
            return visitTableWriter(node, node.getPartitioningScheme(), node.getSource(), preferredProperties, node.getTarget());
        }

        @Override
        public PlanWithProperties visitSimpleTableExecuteNode(SimpleTableExecuteNode node, PreferredProperties context)
        {
            return new PlanWithProperties(
                    node,
                    ActualProperties.builder()
                            .global(singlePartition())
                            .build());
        }

        private PlanWithProperties visitTableWriter(PlanNode node, Optional<PartitioningScheme> partitioningScheme, PlanNode source, PreferredProperties preferredProperties, TableWriterNode.WriterTarget writerTarget)
        {
            PlanWithProperties newSource = source.accept(this, preferredProperties);
            PlanWithProperties partitionedSource = getWriterPlanWithProperties(partitioningScheme, newSource, writerTarget);

            return rebaseAndDeriveProperties(node, partitionedSource);
        }

        @Override
        public PlanWithProperties visitMergeWriter(MergeWriterNode node, PreferredProperties preferredProperties)
        {
            PlanWithProperties source = node.getSource().accept(this, preferredProperties);

            Optional<PartitioningScheme> partitioningScheme = node.getPartitioningScheme();
            PlanWithProperties partitionedSource = getWriterPlanWithProperties(partitioningScheme, source, node.getTarget());

            return rebaseAndDeriveProperties(node, partitionedSource);
        }

        private PlanWithProperties getWriterPlanWithProperties(Optional<PartitioningScheme> partitioningScheme, PlanWithProperties newSource, TableWriterNode.WriterTarget writerTarget)
        {
            if (partitioningScheme.isEmpty()) {
                // use maxWritersTasks to set PartitioningScheme.partitionCount field to limit number of tasks that will take part in executing writing stage
                int maxWriterTasks = writerTarget.getMaxWriterTasks(plannerContext.getMetadata(), session).orElse(getMaxWriterTaskCount(session));
                Optional<Integer> maxWritersNodesCount = getRetryPolicy(session) != RetryPolicy.TASK
                        ? Optional.of(Math.min(maxWriterTasks, getMaxWriterTaskCount(session)))
                        : Optional.empty();
                if (scaleWriters && writerTarget.supportsReportingWrittenBytes(plannerContext.getMetadata(), session)) {
                    partitioningScheme = Optional.of(new PartitioningScheme(Partitioning.create(SCALED_WRITER_ROUND_ROBIN_DISTRIBUTION, ImmutableList.of()), newSource.getNode().getOutputSymbols(), Optional.empty(), false, Optional.empty(), maxWritersNodesCount));
                }
                else if (redistributeWrites) {
                    partitioningScheme = Optional.of(new PartitioningScheme(Partitioning.create(FIXED_ARBITRARY_DISTRIBUTION, ImmutableList.of()), newSource.getNode().getOutputSymbols(), Optional.empty(), false, Optional.empty(), maxWritersNodesCount));
                }
            }
            else if (scaleWriters
                    && writerTarget.supportsReportingWrittenBytes(plannerContext.getMetadata(), session)
                    && writerTarget.supportsMultipleWritersPerPartition(plannerContext.getMetadata(), session)
                    // do not insert an exchange if partitioning is compatible
                    && !newSource.getProperties().isCompatibleTablePartitioningWith(partitioningScheme.get().getPartitioning(), false, plannerContext.getMetadata(), session)) {
                if (partitioningScheme.get().getPartitioning().getHandle().equals(FIXED_HASH_DISTRIBUTION)) {
                    partitioningScheme = Optional.of(partitioningScheme.get().withPartitioningHandle(SCALED_WRITER_HASH_DISTRIBUTION));
                }
                else {
                    PartitioningHandle partitioningHandle = partitioningScheme.get().getPartitioning().getHandle();
                    verify(!(partitioningHandle.getConnectorHandle() instanceof SystemPartitioningHandle));
                    verify(
                            partitioningScheme.get().getPartitioning().getArguments().stream().noneMatch(Partitioning.ArgumentBinding::isConstant),
                            "Table writer partitioning has constant arguments");
                    partitioningScheme = Optional.of(partitioningScheme.get().withPartitioningHandle(
                            new PartitioningHandle(
                                    partitioningHandle.getCatalogHandle(),
                                    partitioningHandle.getTransactionHandle(),
                                    partitioningHandle.getConnectorHandle(),
                                    true)));
                }
            }
            if (partitioningScheme.isPresent() && !newSource.getProperties().isCompatibleTablePartitioningWith(partitioningScheme.get().getPartitioning(), false, plannerContext.getMetadata(), session)) {
                newSource = withDerivedProperties(
                        partitionedExchange(
                                idAllocator.getNextId(),
                                REMOTE,
                                newSource.getNode(),
                                partitioningScheme.get()),
                        newSource.getProperties());
            }
            return newSource;
        }

        @Override
        public PlanWithProperties visitValues(ValuesNode node, PreferredProperties preferredProperties)
        {
            return new PlanWithProperties(
                    node,
                    ActualProperties.builder()
                            .global(singlePartition())
                            .build());
        }

        @Override
        public PlanWithProperties visitTableDelete(TableDeleteNode node, PreferredProperties context)
        {
            return new PlanWithProperties(
                    node,
                    ActualProperties.builder()
                            .global(singlePartition())
                            .build());
        }

        @Override
        public PlanWithProperties visitExplainAnalyze(ExplainAnalyzeNode node, PreferredProperties preferredProperties)
        {
            PlanWithProperties child = planChild(node, PreferredProperties.any());

            // if the child is already a gathering exchange, don't add another
            if ((child.getNode() instanceof ExchangeNode) && ((ExchangeNode) child.getNode()).getType() == ExchangeNode.Type.GATHER) {
                return rebaseAndDeriveProperties(node, child);
            }

            // Always add an exchange because ExplainAnalyze should be in its own stage
            child = withDerivedProperties(
                    gatheringExchange(idAllocator.getNextId(), REMOTE, child.getNode()),
                    child.getProperties());

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitStatisticsWriterNode(StatisticsWriterNode node, PreferredProperties context)
        {
            PlanWithProperties child = planChild(node, PreferredProperties.any());

            // if the child is already a gathering exchange, don't add another
            if ((child.getNode() instanceof ExchangeNode) && ((ExchangeNode) child.getNode()).getType() == GATHER) {
                return rebaseAndDeriveProperties(node, child);
            }

            if (!child.getProperties().isCoordinatorOnly()) {
                child = withDerivedProperties(
                        gatheringExchange(idAllocator.getNextId(), REMOTE, child.getNode()),
                        child.getProperties());
            }

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitTableFinish(TableFinishNode node, PreferredProperties preferredProperties)
        {
            PlanWithProperties child = planChild(node, PreferredProperties.any());

            // if the child is already a gathering exchange, don't add another
            if ((child.getNode() instanceof ExchangeNode) && ((ExchangeNode) child.getNode()).getType() == GATHER) {
                return rebaseAndDeriveProperties(node, child);
            }

            if (!child.getProperties().isCoordinatorOnly()) {
                child = withDerivedProperties(
                        gatheringExchange(idAllocator.getNextId(), REMOTE, child.getNode()),
                        child.getProperties());
            }

            return rebaseAndDeriveProperties(node, child);
        }

        private <T> SetMultimap<T, T> createMapping(List<T> keys, List<T> values)
        {
            checkArgument(keys.size() == values.size(), "Inputs must have the same size");
            ImmutableSetMultimap.Builder<T, T> builder = ImmutableSetMultimap.builder();
            for (int i = 0; i < keys.size(); i++) {
                builder.put(keys.get(i), values.get(i));
            }
            return builder.build();
        }

        private <T> Function<T, Optional<T>> createTranslator(SetMultimap<T, T> inputToOutput)
        {
            return input -> inputToOutput.get(input).stream().findAny();
        }

        private <T> Function<T, T> createDirectTranslator(SetMultimap<T, T> inputToOutput)
        {
            return input -> inputToOutput.get(input).iterator().next();
        }

        @Override
        public PlanWithProperties visitJoin(JoinNode node, PreferredProperties preferredProperties)
        {
            List<Symbol> leftSymbols = node.getCriteria().stream()
                    .map(JoinNode.EquiJoinClause::getLeft)
                    .collect(toImmutableList());
            List<Symbol> rightSymbols = node.getCriteria().stream()
                    .map(JoinNode.EquiJoinClause::getRight)
                    .collect(toImmutableList());

            JoinNode.DistributionType distributionType = node.getDistributionType().orElseThrow(() -> new IllegalArgumentException("distributionType not yet set"));

            if (distributionType == JoinNode.DistributionType.REPLICATED) {
                PlanWithProperties left = node.getLeft().accept(this, PreferredProperties.any());

                // use partitioned join if probe side is naturally partitioned on join symbols (e.g: because of aggregation)
                if (!node.getCriteria().isEmpty()
                        && isNodePartitionedOn(left.getProperties(), leftSymbols) && !left.getProperties().isSingleNode()) {
                    return planPartitionedJoin(node, leftSymbols, rightSymbols, left);
                }

                return planReplicatedJoin(node, left);
            }
            return planPartitionedJoin(node, leftSymbols, rightSymbols);
        }

        private PlanWithProperties planPartitionedJoin(JoinNode node, List<Symbol> leftSymbols, List<Symbol> rightSymbols)
        {
            return planPartitionedJoin(node, leftSymbols, rightSymbols, node.getLeft().accept(this, PreferredProperties.partitioned(ImmutableSet.copyOf(leftSymbols))));
        }

        private PlanWithProperties planPartitionedJoin(JoinNode node, List<Symbol> leftSymbols, List<Symbol> rightSymbols, PlanWithProperties left)
        {
            SetMultimap<Symbol, Symbol> rightToLeft = createMapping(rightSymbols, leftSymbols);
            SetMultimap<Symbol, Symbol> leftToRight = createMapping(leftSymbols, rightSymbols);

            PlanWithProperties right;

            if (isNodePartitionedOn(left.getProperties(), leftSymbols) && !left.getProperties().isSingleNode()) {
                Partitioning rightPartitioning = left.getProperties().translate(createTranslator(leftToRight)).getNodePartitioning().get();
                right = node.getRight().accept(this, PreferredProperties.partitioned(rightPartitioning));
                if (!right.getProperties().isCompatibleTablePartitioningWith(left.getProperties(), rightToLeft::get, plannerContext.getMetadata(), session)) {
                    right = withDerivedProperties(
                            partitionedExchange(idAllocator.getNextId(), REMOTE, right.getNode(), new PartitioningScheme(rightPartitioning, right.getNode().getOutputSymbols())),
                            right.getProperties());
                }
            }
            else {
                right = node.getRight().accept(this, PreferredProperties.partitioned(ImmutableSet.copyOf(rightSymbols)));

                if (isNodePartitionedOn(right.getProperties(), rightSymbols) && !right.getProperties().isSingleNode()) {
                    Partitioning leftPartitioning = right.getProperties().translate(createTranslator(rightToLeft)).getNodePartitioning().get();
                    left = withDerivedProperties(
                            partitionedExchange(idAllocator.getNextId(), REMOTE, left.getNode(), new PartitioningScheme(leftPartitioning, left.getNode().getOutputSymbols())),
                            left.getProperties());
                }
                else {
                    left = withDerivedProperties(
                            partitionedExchange(idAllocator.getNextId(), REMOTE, left.getNode(), leftSymbols, Optional.empty()),
                            left.getProperties());
                    right = withDerivedProperties(
                            partitionedExchange(idAllocator.getNextId(), REMOTE, right.getNode(), rightSymbols, Optional.empty()),
                            right.getProperties());
                }
            }

            verify(left.getProperties().isCompatibleTablePartitioningWith(right.getProperties(), leftToRight::get, plannerContext.getMetadata(), session));

            // if colocated joins are disabled, force redistribute when using a custom partitioning
            if (!isColocatedJoinEnabled(session) && hasMultipleSources(left.getNode(), right.getNode())) {
                Partitioning rightPartitioning = left.getProperties().translate(createTranslator(leftToRight)).getNodePartitioning().get();
                right = withDerivedProperties(
                        partitionedExchange(idAllocator.getNextId(), REMOTE, right.getNode(), new PartitioningScheme(rightPartitioning, right.getNode().getOutputSymbols())),
                        right.getProperties());
            }

            return buildJoin(node, left, right, JoinNode.DistributionType.PARTITIONED);
        }

        private PlanWithProperties planReplicatedJoin(JoinNode node, PlanWithProperties left)
        {
            // Broadcast Join
            PlanWithProperties right = node.getRight().accept(this, PreferredProperties.any());

            if (left.getProperties().isSingleNode()) {
                if (!right.getProperties().isSingleNode() ||
                        (!isColocatedJoinEnabled(session) && hasMultipleSources(left.getNode(), right.getNode()))) {
                    right = withDerivedProperties(
                            gatheringExchange(idAllocator.getNextId(), REMOTE, right.getNode()),
                            right.getProperties());
                }
            }
            else {
                right = withDerivedProperties(
                        replicatedExchange(idAllocator.getNextId(), REMOTE, right.getNode()),
                        right.getProperties());
            }

            return buildJoin(node, left, right, JoinNode.DistributionType.REPLICATED);
        }

        private PlanWithProperties buildJoin(JoinNode node, PlanWithProperties newLeft, PlanWithProperties newRight, JoinNode.DistributionType newDistributionType)
        {
            JoinNode result = new JoinNode(
                    node.getId(),
                    node.getType(),
                    newLeft.getNode(),
                    newRight.getNode(),
                    node.getCriteria(),
                    node.getLeftOutputSymbols(),
                    node.getRightOutputSymbols(),
                    node.isMaySkipOutputDuplicates(),
                    node.getFilter(),
                    node.getLeftHashSymbol(),
                    node.getRightHashSymbol(),
                    Optional.of(newDistributionType),
                    node.isSpillable(),
                    node.getDynamicFilters(),
                    node.getReorderJoinStatsAndCost());

            return new PlanWithProperties(result, deriveProperties(result, ImmutableList.of(newLeft.getProperties(), newRight.getProperties())));
        }

        @Override
        public PlanWithProperties visitSpatialJoin(SpatialJoinNode node, PreferredProperties preferredProperties)
        {
            SpatialJoinNode.DistributionType distributionType = node.getDistributionType();

            PlanWithProperties left = node.getLeft().accept(this, PreferredProperties.any());
            PlanWithProperties right = node.getRight().accept(this, PreferredProperties.any());

            if (distributionType == SpatialJoinNode.DistributionType.REPLICATED) {
                if (left.getProperties().isSingleNode()) {
                    if (!right.getProperties().isSingleNode()) {
                        right = withDerivedProperties(
                                gatheringExchange(idAllocator.getNextId(), REMOTE, right.getNode()),
                                right.getProperties());
                    }
                }
                else {
                    right = withDerivedProperties(
                            replicatedExchange(idAllocator.getNextId(), REMOTE, right.getNode()),
                            right.getProperties());
                }
            }
            else {
                left = withDerivedProperties(
                        partitionedExchange(idAllocator.getNextId(), REMOTE, left.getNode(), ImmutableList.of(node.getLeftPartitionSymbol().get()), Optional.empty()),
                        left.getProperties());
                right = withDerivedProperties(
                        partitionedExchange(idAllocator.getNextId(), REMOTE, right.getNode(), ImmutableList.of(node.getRightPartitionSymbol().get()), Optional.empty()),
                        right.getProperties());
            }

            PlanNode newJoinNode = node.replaceChildren(ImmutableList.of(left.getNode(), right.getNode()));
            return new PlanWithProperties(newJoinNode, deriveProperties(newJoinNode, ImmutableList.of(left.getProperties(), right.getProperties())));
        }

        @Override
        public PlanWithProperties visitUnnest(UnnestNode node, PreferredProperties preferredProperties)
        {
            PreferredProperties translatedPreferred = preferredProperties.translate(symbol -> node.getReplicateSymbols().contains(symbol) ? Optional.of(symbol) : Optional.empty());

            return rebaseAndDeriveProperties(node, planChild(node, translatedPreferred));
        }

        @Override
        public PlanWithProperties visitSemiJoin(SemiJoinNode node, PreferredProperties preferredProperties)
        {
            PlanWithProperties source;
            PlanWithProperties filteringSource;

            SemiJoinNode.DistributionType distributionType = node.getDistributionType().orElseThrow(() -> new IllegalArgumentException("distributionType not yet set"));
            if (distributionType == SemiJoinNode.DistributionType.PARTITIONED) {
                List<Symbol> sourceSymbols = ImmutableList.of(node.getSourceJoinSymbol());
                List<Symbol> filteringSourceSymbols = ImmutableList.of(node.getFilteringSourceJoinSymbol());

                SetMultimap<Symbol, Symbol> sourceToFiltering = createMapping(sourceSymbols, filteringSourceSymbols);
                SetMultimap<Symbol, Symbol> filteringToSource = createMapping(filteringSourceSymbols, sourceSymbols);

                source = node.getSource().accept(this, PreferredProperties.partitioned(ImmutableSet.copyOf(sourceSymbols)));

                if (isNodePartitionedOn(source.getProperties(), sourceSymbols) && !source.getProperties().isSingleNode()) {
                    Partitioning filteringPartitioning = source.getProperties().translate(createTranslator(sourceToFiltering)).getNodePartitioning().get();
                    filteringSource = node.getFilteringSource().accept(this, PreferredProperties.partitionedWithNullsAndAnyReplicated(filteringPartitioning));
                    if (!source.getProperties().withReplicatedNulls(true).isCompatibleTablePartitioningWith(filteringSource.getProperties(), sourceToFiltering::get, plannerContext.getMetadata(), session)) {
                        filteringSource = withDerivedProperties(
                                partitionedExchange(idAllocator.getNextId(), REMOTE, filteringSource.getNode(), new PartitioningScheme(
                                        filteringPartitioning,
                                        filteringSource.getNode().getOutputSymbols(),
                                        Optional.empty(),
                                        true,
                                        Optional.empty(),
                                        Optional.empty())),
                                filteringSource.getProperties());
                    }
                }
                else {
                    filteringSource = node.getFilteringSource().accept(this, PreferredProperties.partitionedWithNullsAndAnyReplicated(ImmutableSet.copyOf(filteringSourceSymbols)));

                    if (filteringSource.getProperties().isNodePartitionedOn(filteringSourceSymbols, true, isUseExactPartitioning(session)) && !filteringSource.getProperties().isSingleNode()) {
                        Partitioning sourcePartitioning = filteringSource.getProperties().translate(createTranslator(filteringToSource)).getNodePartitioning().get();
                        source = withDerivedProperties(
                                partitionedExchange(idAllocator.getNextId(), REMOTE, source.getNode(), new PartitioningScheme(sourcePartitioning, source.getNode().getOutputSymbols())),
                                source.getProperties());
                    }
                    else {
                        source = withDerivedProperties(
                                partitionedExchange(idAllocator.getNextId(), REMOTE, source.getNode(), sourceSymbols, Optional.empty()),
                                source.getProperties());
                        filteringSource = withDerivedProperties(
                                partitionedExchange(idAllocator.getNextId(), REMOTE, filteringSource.getNode(), filteringSourceSymbols, Optional.empty(), true),
                                filteringSource.getProperties());
                    }
                }

                verify(source.getProperties().withReplicatedNulls(true).isCompatibleTablePartitioningWith(filteringSource.getProperties(), sourceToFiltering::get, plannerContext.getMetadata(), session));

                // if colocated joins are disabled, force redistribute when using a custom partitioning
                if (!isColocatedJoinEnabled(session) && hasMultipleSources(source.getNode(), filteringSource.getNode())) {
                    Partitioning filteringPartitioning = source.getProperties().translate(createTranslator(sourceToFiltering)).getNodePartitioning().get();
                    filteringSource = withDerivedProperties(
                            partitionedExchange(idAllocator.getNextId(), REMOTE, filteringSource.getNode(), new PartitioningScheme(
                                    filteringPartitioning,
                                    filteringSource.getNode().getOutputSymbols(),
                                    Optional.empty(),
                                    true,
                                    Optional.empty(),
                                    Optional.empty())),
                            filteringSource.getProperties());
                }
            }
            else {
                source = node.getSource().accept(this, PreferredProperties.any());
                // Delete operator works fine even if TableScans on the filtering (right) side is not co-located with itself. It only cares about the corresponding TableScan,
                // which is always on the source (left) side. Therefore, hash-partitioned semi-join is always allowed on the filtering side.
                filteringSource = node.getFilteringSource().accept(this, PreferredProperties.any());

                // make filtering source match requirements of source
                if (source.getProperties().isSingleNode()) {
                    if (!filteringSource.getProperties().isSingleNode() ||
                            (!isColocatedJoinEnabled(session) && hasMultipleSources(source.getNode(), filteringSource.getNode()))) {
                        filteringSource = withDerivedProperties(
                                gatheringExchange(idAllocator.getNextId(), REMOTE, filteringSource.getNode()),
                                filteringSource.getProperties());
                    }
                }
                else {
                    filteringSource = withDerivedProperties(
                            replicatedExchange(idAllocator.getNextId(), REMOTE, filteringSource.getNode()),
                            filteringSource.getProperties());
                }
            }

            return rebaseAndDeriveProperties(node, ImmutableList.of(source, filteringSource));
        }

        @Override
        public PlanWithProperties visitIndexJoin(IndexJoinNode node, PreferredProperties preferredProperties)
        {
            List<Symbol> joinColumns = node.getCriteria().stream()
                    .map(IndexJoinNode.EquiJoinClause::getProbe)
                    .collect(toImmutableList());

            // Only prefer grouping on join columns if no parent local property preferences
            List<LocalProperty<Symbol>> desiredLocalProperties = preferredProperties.getLocalProperties().isEmpty() ? grouped(joinColumns) : ImmutableList.of();

            PlanWithProperties probeSource = node.getProbeSource().accept(
                    this,
                    computePreference(
                            partitionedWithLocal(ImmutableSet.copyOf(joinColumns), desiredLocalProperties),
                            preferredProperties));

            PlanWithProperties indexSource = node.getIndexSource().accept(this, PreferredProperties.any());

            // TODO: if input is grouped, create streaming join

            // index side is really a nested-loops plan, so don't add exchanges
            PlanNode result = ChildReplacer.replaceChildren(node, ImmutableList.of(probeSource.getNode(), node.getIndexSource()));
            return new PlanWithProperties(result, deriveProperties(result, ImmutableList.of(probeSource.getProperties(), indexSource.getProperties())));
        }

        @Override
        public PlanWithProperties visitIndexSource(IndexSourceNode node, PreferredProperties preferredProperties)
        {
            return new PlanWithProperties(
                    node,
                    ActualProperties.builder()
                            .global(singlePartition())
                            .build());
        }

        private Function<Symbol, Optional<Symbol>> outputToInputTranslator(UnionNode node, int sourceIndex)
        {
            return symbol -> Optional.of(node.getSymbolMapping().get(symbol).get(sourceIndex));
        }

        private Partitioning selectUnionPartitioning(UnionNode node, PreferredProperties.PartitioningProperties parentPreference)
        {
            // Use the parent's requested partitioning if available
            if (parentPreference.getPartitioning().isPresent()) {
                return parentPreference.getPartitioning().get();
            }

            // Try planning the children to see if any of them naturally produce a partitioning (for now, just select the first)
            boolean nullsAndAnyReplicated = parentPreference.isNullsAndAnyReplicated();
            for (int sourceIndex = 0; sourceIndex < node.getSources().size(); sourceIndex++) {
                PreferredProperties.PartitioningProperties childPartitioning = parentPreference.translate(outputToInputTranslator(node, sourceIndex)).get();
                PreferredProperties childPreferred = PreferredProperties.builder()
                        .global(PreferredProperties.Global.distributed(childPartitioning.withNullsAndAnyReplicated(nullsAndAnyReplicated)))
                        .build();
                PlanWithProperties child = node.getSources().get(sourceIndex).accept(this, childPreferred);
                // Don't select a single node partitioning so that we maintain query parallelism
                // Theoretically, if all children are single partitioned on the same node we could choose a single
                // partitioning, but as this only applies to a union of two values nodes, it isn't worth the added complexity
                if (child.getProperties().isNodePartitionedOn(childPartitioning.getPartitioningColumns(), nullsAndAnyReplicated, isUseExactPartitioning(session)) && !child.getProperties().isSingleNode()) {
                    Function<Symbol, Optional<Symbol>> childToParent = createTranslator(createMapping(node.sourceOutputLayout(sourceIndex), node.getOutputSymbols()));
                    return child.getProperties().translate(childToParent).getNodePartitioning().get();
                }
            }

            // Otherwise, choose an arbitrary partitioning over the columns
            return Partitioning.create(FIXED_HASH_DISTRIBUTION, ImmutableList.copyOf(parentPreference.getPartitioningColumns()));
        }

        @Override
        public PlanWithProperties visitUnion(UnionNode node, PreferredProperties parentPreference)
        {
            Optional<PreferredProperties.Global> parentGlobal = parentPreference.getGlobalProperties();
            if (parentGlobal.isPresent() && parentGlobal.get().isDistributed() && parentGlobal.get().getPartitioningProperties().isPresent()) {
                PreferredProperties.PartitioningProperties parentPartitioningPreference = parentGlobal.get().getPartitioningProperties().get();
                boolean nullsAndAnyReplicated = parentPartitioningPreference.isNullsAndAnyReplicated();
                Partitioning desiredParentPartitioning = selectUnionPartitioning(node, parentPartitioningPreference);

                ImmutableList.Builder<PlanNode> partitionedSources = ImmutableList.builder();
                ImmutableListMultimap.Builder<Symbol, Symbol> outputToSourcesMapping = ImmutableListMultimap.builder();

                for (int sourceIndex = 0; sourceIndex < node.getSources().size(); sourceIndex++) {
                    Partitioning childPartitioning = desiredParentPartitioning.translate(createDirectTranslator(createMapping(node.getOutputSymbols(), node.sourceOutputLayout(sourceIndex))));

                    PreferredProperties childPreferred = PreferredProperties.builder()
                            .global(PreferredProperties.Global.distributed(PreferredProperties.PartitioningProperties.partitioned(childPartitioning)
                                    .withNullsAndAnyReplicated(nullsAndAnyReplicated)))
                            .build();

                    PlanWithProperties source = node.getSources().get(sourceIndex).accept(this, childPreferred);
                    if (!source.getProperties().isCompatibleTablePartitioningWith(childPartitioning, nullsAndAnyReplicated, plannerContext.getMetadata(), session)) {
                        source = withDerivedProperties(
                                partitionedExchange(
                                        idAllocator.getNextId(),
                                        REMOTE,
                                        source.getNode(),
                                        new PartitioningScheme(
                                                childPartitioning,
                                                source.getNode().getOutputSymbols(),
                                                Optional.empty(),
                                                nullsAndAnyReplicated,
                                                Optional.empty(),
                                                Optional.empty())),
                                source.getProperties());
                    }
                    partitionedSources.add(source.getNode());

                    for (int column = 0; column < node.getOutputSymbols().size(); column++) {
                        outputToSourcesMapping.put(node.getOutputSymbols().get(column), node.sourceOutputLayout(sourceIndex).get(column));
                    }
                }
                UnionNode newNode = new UnionNode(
                        node.getId(),
                        partitionedSources.build(),
                        outputToSourcesMapping.build(),
                        ImmutableList.copyOf(outputToSourcesMapping.build().keySet()));

                return new PlanWithProperties(
                        newNode,
                        ActualProperties.builder()
                                .global(partitionedOn(desiredParentPartitioning))
                                .build()
                                .withReplicatedNulls(parentPartitioningPreference.isNullsAndAnyReplicated()));
            }

            // first, classify children into partitioned and unpartitioned
            List<PlanNode> unpartitionedChildren = new ArrayList<>();
            List<List<Symbol>> unpartitionedOutputLayouts = new ArrayList<>();

            List<PlanNode> partitionedChildren = new ArrayList<>();
            List<List<Symbol>> partitionedOutputLayouts = new ArrayList<>();

            for (int i = 0; i < node.getSources().size(); i++) {
                PlanWithProperties child = node.getSources().get(i).accept(this, PreferredProperties.any());
                if (child.getProperties().isSingleNode()) {
                    unpartitionedChildren.add(child.getNode());
                    unpartitionedOutputLayouts.add(node.sourceOutputLayout(i));
                }
                else {
                    partitionedChildren.add(child.getNode());
                    // union may drop or duplicate symbols from the input so we must provide an exact mapping
                    partitionedOutputLayouts.add(node.sourceOutputLayout(i));
                }
            }

            PlanNode result;
            if (!partitionedChildren.isEmpty() && unpartitionedChildren.isEmpty()) {
                // parent does not have preference or prefers some partitioning without any explicit partitioning - just use
                // children partitioning and don't GATHER partitioned inputs
                // TODO: add FIXED_ARBITRARY_DISTRIBUTION support on non empty unpartitionedChildren
                if (parentGlobal.isEmpty() || parentGlobal.get().isDistributed()) {
                    return arbitraryDistributeUnion(node, partitionedChildren, partitionedOutputLayouts);
                }

                // add a gathering exchange above partitioned inputs
                result = new ExchangeNode(
                        idAllocator.getNextId(),
                        GATHER,
                        REMOTE,
                        new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), node.getOutputSymbols()),
                        partitionedChildren,
                        partitionedOutputLayouts,
                        Optional.empty());
            }
            else if (!unpartitionedChildren.isEmpty()) {
                if (!partitionedChildren.isEmpty()) {
                    // add a gathering exchange above partitioned inputs and fold it into the set of unpartitioned inputs
                    // NOTE: new symbols for ExchangeNode output are required in order to keep plan logically correct with new local union below

                    List<Symbol> exchangeOutputLayout = node.getOutputSymbols().stream()
                            .map(outputSymbol -> symbolAllocator.newSymbol(outputSymbol.getName(), types.get(outputSymbol)))
                            .collect(toImmutableList());

                    result = new ExchangeNode(
                            idAllocator.getNextId(),
                            GATHER,
                            REMOTE,
                            new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), exchangeOutputLayout),
                            partitionedChildren,
                            partitionedOutputLayouts,
                            Optional.empty());

                    unpartitionedChildren.add(result);
                    unpartitionedOutputLayouts.add(result.getOutputSymbols());
                }

                ImmutableListMultimap.Builder<Symbol, Symbol> mappings = ImmutableListMultimap.builder();
                for (int i = 0; i < node.getOutputSymbols().size(); i++) {
                    for (List<Symbol> outputLayout : unpartitionedOutputLayouts) {
                        mappings.put(node.getOutputSymbols().get(i), outputLayout.get(i));
                    }
                }

                // add local union for all unpartitioned inputs
                result = new UnionNode(node.getId(), unpartitionedChildren, mappings.build(), ImmutableList.copyOf(mappings.build().keySet()));
            }
            else {
                throw new IllegalStateException("both unpartitionedChildren partitionedChildren are empty");
            }

            return new PlanWithProperties(
                    result,
                    ActualProperties.builder()
                            .global(singlePartition())
                            .build());
        }

        private PlanWithProperties arbitraryDistributeUnion(
                UnionNode unionNode,
                List<PlanNode> partitionedChildren,
                List<List<Symbol>> partitionedOutputLayouts)
        {
            // TODO: can we insert LOCAL exchange for one child SOURCE distributed and another HASH distributed?
            if (countSources(partitionedChildren) == 0) {
                // No source distributed child, we can use insert LOCAL exchange
                // TODO: if all children have the same partitioning, pass this partitioning to the parent
                // instead of "arbitraryPartition".
                return new PlanWithProperties(unionNode.replaceChildren(partitionedChildren));
            }

            int repartitionedRemoteExchangeNodesCount = partitionedChildren.stream().mapToInt(AddExchanges::countRepartitionedRemoteExchangeNodes).sum();
            int partitionedConnectorSourceCount = partitionedChildren.stream().mapToInt(AddExchanges::countPartitionedConnectorSource).sum();
            long uniqueSourceCatalogCount = partitionedChildren.stream().flatMap(AddExchanges::collectSourceCatalogs).distinct().count();

            // MultiSourcePartitionedScheduler does not support node partitioning. Both partitioned remote exchanges and
            // partitioned connector sources require node partitioning.
            if (repartitionedRemoteExchangeNodesCount == 0
                    && partitionedConnectorSourceCount == 0
                    && uniqueSourceCatalogCount == 1) {
                return new PlanWithProperties(unionNode.replaceChildren(partitionedChildren));
            }
            // If there is at least one not source distributed source or one of sources is connector partitioned
            // we have to insert REMOTE exchange with FIXED_ARBITRARY_DISTRIBUTION instead of local exchange
            return new PlanWithProperties(
                    new ExchangeNode(
                            idAllocator.getNextId(),
                            REPARTITION,
                            REMOTE,
                            new PartitioningScheme(Partitioning.create(FIXED_ARBITRARY_DISTRIBUTION, ImmutableList.of()), unionNode.getOutputSymbols()),
                            partitionedChildren,
                            partitionedOutputLayouts,
                            Optional.empty()));
        }

        @Override
        public PlanWithProperties visitApply(ApplyNode node, PreferredProperties preferredProperties)
        {
            throw new IllegalStateException("Unexpected node: " + node.getClass().getName());
        }

        @Override
        public PlanWithProperties visitCorrelatedJoin(CorrelatedJoinNode node, PreferredProperties preferredProperties)
        {
            throw new IllegalStateException("Unexpected node: " + node.getClass().getName());
        }

        private PlanWithProperties planChild(PlanNode node, PreferredProperties preferredProperties)
        {
            return getOnlyElement(node.getSources()).accept(this, preferredProperties);
        }

        private PlanWithProperties rebaseAndDeriveProperties(PlanNode node, PlanWithProperties child)
        {
            return withDerivedProperties(
                    ChildReplacer.replaceChildren(node, ImmutableList.of(child.getNode())),
                    child.getProperties());
        }

        @Override
        public PlanWithProperties visitAssignUniqueId(AssignUniqueId node, PreferredProperties preferredProperties)
        {
            PreferredProperties translatedPreferred = preferredProperties.translate(symbol -> node.getIdColumn().equals(symbol) ? Optional.empty() : Optional.of(symbol));

            return rebaseAndDeriveProperties(node, planChild(node, translatedPreferred));
        }

        private PlanWithProperties rebaseAndDeriveProperties(PlanNode node, List<PlanWithProperties> children)
        {
            PlanNode result = node.replaceChildren(
                    children.stream()
                            .map(PlanWithProperties::getNode)
                            .collect(toList()));
            return new PlanWithProperties(result, deriveProperties(result, children.stream().map(PlanWithProperties::getProperties).collect(toList())));
        }

        private PlanWithProperties withDerivedProperties(PlanNode node, ActualProperties inputProperties)
        {
            return new PlanWithProperties(node, deriveProperties(node, inputProperties));
        }

        private ActualProperties deriveProperties(PlanNode result, ActualProperties inputProperties)
        {
            return deriveProperties(result, ImmutableList.of(inputProperties));
        }

        private ActualProperties deriveProperties(PlanNode result, List<ActualProperties> inputProperties)
        {
            // TODO: move this logic to PlanSanityChecker once PropertyDerivations.deriveProperties fully supports local exchanges
            ActualProperties outputProperties = PropertyDerivations.deriveProperties(result, inputProperties, plannerContext, session, types, typeAnalyzer);
            verify(result instanceof SemiJoinNode || inputProperties.stream().noneMatch(ActualProperties::isNullsAndAnyReplicated) || outputProperties.isNullsAndAnyReplicated(),
                    "SemiJoinNode is the only node that can strip null replication");
            return outputProperties;
        }

        private ActualProperties derivePropertiesRecursively(PlanNode result)
        {
            return PropertyDerivations.derivePropertiesRecursively(result, plannerContext, session, types, typeAnalyzer);
        }

        private PreferredProperties computePreference(PreferredProperties preferredProperties, PreferredProperties parentPreferredProperties)
        {
            if (!ignoreDownStreamPreferences(session)) {
                return preferredProperties.mergeWithParent(parentPreferredProperties);
            }
            return preferredProperties;
        }

        private boolean isNodePartitionedOn(ActualProperties properties, Collection<Symbol> columns)
        {
            return properties.isNodePartitionedOn(columns, isUseExactPartitioning(session));
        }
    }

    private static Map<Symbol, Symbol> computeIdentityTranslations(Assignments assignments)
    {
        Map<Symbol, Symbol> outputToInput = new HashMap<>();
        for (Map.Entry<Symbol, Expression> assignment : assignments.getMap().entrySet()) {
            if (assignment.getValue() instanceof SymbolReference) {
                outputToInput.put(assignment.getKey(), Symbol.from(assignment.getValue()));
            }
        }
        return outputToInput;
    }

    private static int countRepartitionedRemoteExchangeNodes(PlanNode root)
    {
        return PlanNodeSearcher
                .searchFrom(root)
                .where(node -> node instanceof ExchangeNode exchangeNode && exchangeNode.getScope() == REMOTE && exchangeNode.getType() == REPARTITION)
                .recurseOnlyWhen(AddExchanges::isNotRemoteExchange)
                .findAll()
                .size();
    }

    private static int countPartitionedConnectorSource(PlanNode root)
    {
        return PlanNodeSearcher
                .searchFrom(root)
                .where(node -> node instanceof TableScanNode tableScanNode && tableScanNode.getUseConnectorNodePartitioning().orElse(false))
                .recurseOnlyWhen(AddExchanges::isNotRemoteExchange)
                .findAll()
                .size();
    }

    private static boolean hasMultipleSources(PlanNode... nodes)
    {
        return countSources(nodes) > 1;
    }

    private static int countSources(PlanNode... nodes)
    {
        return countSources(Arrays.asList(nodes));
    }

    private static int countSources(List<PlanNode> nodes)
    {
        return nodes
                .stream()
                .mapToInt(node -> PlanNodeSearcher
                        .searchFrom(node)
                        .where(TableScanNode.class::isInstance)
                        .recurseOnlyWhen(AddExchanges::isNotRemoteExchange)
                        .findAll()
                        .size())
                .sum();
    }

    private static Stream<CatalogHandle> collectSourceCatalogs(PlanNode root)
    {
        return PlanNodeSearcher
                .searchFrom(root)
                .where(node -> node instanceof TableScanNode)
                .recurseOnlyWhen(AddExchanges::isNotRemoteExchange)
                .findAll()
                .stream()
                .map(TableScanNode.class::cast)
                .map(node -> node.getTable().getCatalogHandle());
    }

    private static boolean isNotRemoteExchange(PlanNode node)
    {
        return !(node instanceof ExchangeNode exchangeNode && exchangeNode.getScope() == REMOTE);
    }

    @VisibleForTesting
    static class PlanWithProperties
    {
        private final PlanNode node;
        private final ActualProperties properties;

        public PlanWithProperties(PlanNode node)
        {
            this(node, ActualProperties.builder().build());
        }

        public PlanWithProperties(PlanNode node, ActualProperties properties)
        {
            this.node = node;
            this.properties = properties;
        }

        public PlanNode getNode()
        {
            return node;
        }

        public ActualProperties getProperties()
        {
            return properties;
        }
    }
}

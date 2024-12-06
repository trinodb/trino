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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.opentelemetry.api.trace.Span;
import io.trino.Session;
import io.trino.cache.CacheSplitSource;
import io.trino.cache.ConnectorAwareAddressProvider;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.metadata.TableHandle;
import io.trino.server.DynamicFilterService;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.TupleDomain;
import io.trino.split.SampledSplitSource;
import io.trino.split.SplitManager;
import io.trino.split.SplitSource;
import io.trino.sql.DynamicFilters;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.plan.AdaptivePlanNode;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AssignUniqueId;
import io.trino.sql.planner.plan.ChooseAlternativeNode;
import io.trino.sql.planner.plan.DistinctLimitNode;
import io.trino.sql.planner.plan.DynamicFilterSourceNode;
import io.trino.sql.planner.plan.EnforceSingleRowNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.ExplainAnalyzeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.GroupIdNode;
import io.trino.sql.planner.plan.IndexJoinNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.LoadCachedDataPlanNode;
import io.trino.sql.planner.plan.MarkDistinctNode;
import io.trino.sql.planner.plan.MergeProcessorNode;
import io.trino.sql.planner.plan.MergeWriterNode;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PatternRecognitionNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.RefreshMaterializedViewNode;
import io.trino.sql.planner.plan.RemoteSourceNode;
import io.trino.sql.planner.plan.RowNumberNode;
import io.trino.sql.planner.plan.SampleNode;
import io.trino.sql.planner.plan.SemiJoinNode;
import io.trino.sql.planner.plan.SimpleTableExecuteNode;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.planner.plan.SpatialJoinNode;
import io.trino.sql.planner.plan.StatisticsWriterNode;
import io.trino.sql.planner.plan.TableDeleteNode;
import io.trino.sql.planner.plan.TableExecuteNode;
import io.trino.sql.planner.plan.TableFinishNode;
import io.trino.sql.planner.plan.TableFunctionProcessorNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TableUpdateNode;
import io.trino.sql.planner.plan.TableWriterNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.TopNRankingNode;
import io.trino.sql.planner.plan.UnionNode;
import io.trino.sql.planner.plan.UnnestNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.planner.plan.WindowNode;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.cache.CacheCommonSubqueries.getLoadCachedDataPlanNode;
import static io.trino.cache.CacheCommonSubqueries.isCacheChooseAlternativeNode;
import static io.trino.spi.connector.Constraint.alwaysTrue;
import static io.trino.spi.connector.DynamicFilter.EMPTY;
import static io.trino.sql.ir.IrUtils.filterConjuncts;
import static java.util.Objects.requireNonNull;

public class SplitSourceFactory
{
    private static final Logger log = Logger.get(SplitSourceFactory.class);

    private final SplitManager splitManager;
    private final PlannerContext plannerContext;
    private final DynamicFilterService dynamicFilterService;
    private final ConnectorAwareAddressProvider connectorAwareAddressProvider;
    private final NodeInfo nodeInfo;
    private final boolean schedulerIncludeCoordinator;

    @Inject
    public SplitSourceFactory(
            SplitManager splitManager,
            PlannerContext plannerContext,
            DynamicFilterService dynamicFilterService,
            ConnectorAwareAddressProvider connectorAwareAddressProvider,
            NodeInfo nodeInfo,
            NodeSchedulerConfig nodeSchedulerConfig)
    {
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.plannerContext = requireNonNull(plannerContext, "metadata is null");
        this.dynamicFilterService = requireNonNull(dynamicFilterService, "dynamicFilterService is null");
        this.connectorAwareAddressProvider = requireNonNull(connectorAwareAddressProvider, "connectorAwareAddressProvider is null");
        this.nodeInfo = requireNonNull(nodeInfo, "nodeInfo is null");
        this.schedulerIncludeCoordinator = requireNonNull(nodeSchedulerConfig, "nodeSchedulerConfig is null").isIncludeCoordinator();
    }

    public Map<PlanNodeId, SplitSource> createSplitSources(Session session, Span stageSpan, PlanFragment fragment)
    {
        ImmutableList.Builder<SplitSource> allSplitSources = ImmutableList.builder();
        try {
            // get splits for this fragment, this is lazy so split assignments aren't actually calculated here
            return fragment.getRoot().accept(
                    new Visitor(session, stageSpan, allSplitSources),
                    null);
        }
        catch (Throwable t) {
            allSplitSources.build().forEach(SplitSourceFactory::closeSplitSource);
            throw t;
        }
    }

    private static void closeSplitSource(SplitSource source)
    {
        try {
            source.close();
        }
        catch (Throwable t) {
            log.warn(t, "Error closing split source");
        }
    }

    private final class Visitor
            extends PlanVisitor<Map<PlanNodeId, SplitSource>, Void>
    {
        private final Session session;
        private final Span stageSpan;
        private final ImmutableList.Builder<SplitSource> splitSources;

        private Visitor(
                Session session,
                Span stageSpan,
                ImmutableList.Builder<SplitSource> allSplitSources)
        {
            this.session = session;
            this.stageSpan = stageSpan;
            this.splitSources = allSplitSources;
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitExplainAnalyze(ExplainAnalyzeNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitTableScan(TableScanNode node, Void context)
        {
            SplitSource splitSource = createSplitSource(node.getTable(), node.getAssignments(), Optional.empty());

            splitSources.add(splitSource);

            return ImmutableMap.of(node.getId(), splitSource);
        }

        private SplitSource createSplitSource(TableHandle table, Map<Symbol, ColumnHandle> assignments, Optional<Expression> filterPredicate)
        {
            List<DynamicFilters.Descriptor> dynamicFilters = filterPredicate
                    .map(DynamicFilters::extractDynamicFilters)
                    .map(DynamicFilters.ExtractResult::getDynamicConjuncts)
                    .orElse(ImmutableList.of());

            DynamicFilter dynamicFilter = EMPTY;
            if (!dynamicFilters.isEmpty()) {
                log.debug("Dynamic filters: %s", dynamicFilters);
                dynamicFilter = dynamicFilterService.createDynamicFilter(session.getQueryId(), dynamicFilters, assignments);
            }

            Constraint constraint = filterPredicate
                    .map(predicate -> filterConjuncts(predicate, expression -> !DynamicFilters.isDynamicFilter(expression)))
                    .map(predicate -> new LayoutConstraintEvaluator(plannerContext, session, assignments, predicate))
                    .map(evaluator -> new Constraint(TupleDomain.all(), evaluator::isCandidate, evaluator.getArguments())) // we are interested only in functional predicate here, so we set the summary to ALL.
                    .orElse(alwaysTrue());

            // get dataSource for table
            return splitManager.getSplits(
                    session,
                    stageSpan,
                    table,
                    dynamicFilter,
                    constraint);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitJoin(JoinNode node, Void context)
        {
            Map<PlanNodeId, SplitSource> leftSplits = node.getLeft().accept(this, context);
            Map<PlanNodeId, SplitSource> rightSplits = node.getRight().accept(this, context);
            return ImmutableMap.<PlanNodeId, SplitSource>builder()
                    .putAll(leftSplits)
                    .putAll(rightSplits)
                    .buildOrThrow();
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitSemiJoin(SemiJoinNode node, Void context)
        {
            Map<PlanNodeId, SplitSource> sourceSplits = node.getSource().accept(this, context);
            Map<PlanNodeId, SplitSource> filteringSourceSplits = node.getFilteringSource().accept(this, context);
            return ImmutableMap.<PlanNodeId, SplitSource>builder()
                    .putAll(sourceSplits)
                    .putAll(filteringSourceSplits)
                    .buildOrThrow();
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitSpatialJoin(SpatialJoinNode node, Void context)
        {
            Map<PlanNodeId, SplitSource> leftSplits = node.getLeft().accept(this, context);
            Map<PlanNodeId, SplitSource> rightSplits = node.getRight().accept(this, context);
            return ImmutableMap.<PlanNodeId, SplitSource>builder()
                    .putAll(leftSplits)
                    .putAll(rightSplits)
                    .buildOrThrow();
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitIndexJoin(IndexJoinNode node, Void context)
        {
            return node.getProbeSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitDynamicFilterSource(DynamicFilterSourceNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitRemoteSource(RemoteSourceNode node, Void context)
        {
            // remote source node does not have splits
            return ImmutableMap.of();
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitValues(ValuesNode node, Void context)
        {
            // values node does not have splits
            return ImmutableMap.of();
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitFilter(FilterNode node, Void context)
        {
            if (node.getSource() instanceof TableScanNode scan) {
                SplitSource splitSource = createSplitSource(scan.getTable(), scan.getAssignments(), Optional.of(node.getPredicate()));

                splitSources.add(splitSource);

                return ImmutableMap.of(scan.getId(), splitSource);
            }

            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitSample(SampleNode node, Void context)
        {
            return switch (node.getSampleType()) {
                case BERNOULLI -> node.getSource().accept(this, context);
                case SYSTEM -> {
                    Map<PlanNodeId, SplitSource> nodeSplits = node.getSource().accept(this, context);
                    // TODO: when this happens we should switch to either BERNOULLI or page sampling
                    if (nodeSplits.size() == 1) {
                        PlanNodeId planNodeId = getOnlyElement(nodeSplits.keySet());
                        SplitSource sampledSplitSource = new SampledSplitSource(nodeSplits.get(planNodeId), node.getSampleRatio());
                        yield ImmutableMap.of(planNodeId, sampledSplitSource);
                    }
                    // table sampling on a sub query without splits is meaningless
                    yield nodeSplits;
                }
            };
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitAggregation(AggregationNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitGroupId(GroupIdNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitMarkDistinct(MarkDistinctNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitWindow(WindowNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitPatternRecognition(PatternRecognitionNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitTableFunctionProcessor(TableFunctionProcessorNode node, Void context)
        {
            if (node.getSource().isEmpty()) {
                // this is a source node, so produce splits
                SplitSource splitSource = splitManager.getSplits(session, stageSpan, node.getHandle());
                splitSources.add(splitSource);

                return ImmutableMap.of(node.getId(), splitSource);
            }

            return node.getSource().orElseThrow().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitChooseAlternativeNode(ChooseAlternativeNode node, Void context)
        {
            checkArgument(isCacheChooseAlternativeNode(node));

            TableHandle originalTableHandle = node.getOriginalTableScan().tableHandle();
            SplitSource splitSource = createSplitSource(
                    originalTableHandle,
                    node.getOriginalTableScan().assignments(),
                    node.getOriginalTableScan().filterPredicate());
            LoadCachedDataPlanNode loadCachedDataNode = getLoadCachedDataPlanNode(node);
            CacheSplitSource cacheSplitSource = new CacheSplitSource(
                    loadCachedDataNode.getPlanSignature().signature(),
                    splitManager.getConnectorSplitManager(originalTableHandle),
                    splitSource,
                    connectorAwareAddressProvider,
                    nodeInfo,
                    schedulerIncludeCoordinator);

            splitSources.add(cacheSplitSource);

            return ImmutableMap.of(node.getId(), cacheSplitSource);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitRowNumber(RowNumberNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitTopNRanking(TopNRankingNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitProject(ProjectNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitUnnest(UnnestNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitTopN(TopNNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitOutput(OutputNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitEnforceSingleRow(EnforceSingleRowNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitAssignUniqueId(AssignUniqueId node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitLimit(LimitNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitDistinctLimit(DistinctLimitNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitSort(SortNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitRefreshMaterializedView(RefreshMaterializedViewNode node, Void context)
        {
            // RefreshMaterializedViewNode does not have splits
            return ImmutableMap.of();
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitTableWriter(TableWriterNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitTableFinish(TableFinishNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitStatisticsWriterNode(StatisticsWriterNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitMergeWriter(MergeWriterNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitMergeProcessor(MergeProcessorNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitTableDelete(TableDeleteNode node, Void context)
        {
            // node does not have splits
            return ImmutableMap.of();
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitTableUpdate(TableUpdateNode node, Void context)
        {
            // node does not have splits
            return ImmutableMap.of();
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitTableExecute(TableExecuteNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitSimpleTableExecuteNode(SimpleTableExecuteNode node, Void context)
        {
            // node does not have splits
            return ImmutableMap.of();
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitUnion(UnionNode node, Void context)
        {
            return processSources(node.getSources(), context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitExchange(ExchangeNode node, Void context)
        {
            return processSources(node.getSources(), context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitAdaptivePlanNode(AdaptivePlanNode node, Void context)
        {
            return processSources(node.getSources(), context);
        }

        private Map<PlanNodeId, SplitSource> processSources(List<PlanNode> sources, Void context)
        {
            ImmutableMap.Builder<PlanNodeId, SplitSource> result = ImmutableMap.builder();
            for (PlanNode child : sources) {
                result.putAll(child.accept(this, context));
            }

            return result.buildOrThrow();
        }

        @Override
        protected Map<PlanNodeId, SplitSource> visitPlan(PlanNode node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }
    }
}

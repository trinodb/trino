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

import io.airlift.log.Logger;
import io.prestosql.Session;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.iterative.GroupReference;
import io.prestosql.sql.planner.iterative.Memo;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.ApplyNode;
import io.prestosql.sql.planner.plan.AssignUniqueId;
import io.prestosql.sql.planner.plan.DeleteNode;
import io.prestosql.sql.planner.plan.DistinctLimitNode;
import io.prestosql.sql.planner.plan.EnforceSingleRowNode;
import io.prestosql.sql.planner.plan.ExceptNode;
import io.prestosql.sql.planner.plan.ExchangeNode;
import io.prestosql.sql.planner.plan.ExplainAnalyzeNode;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.GroupIdNode;
import io.prestosql.sql.planner.plan.IndexJoinNode;
import io.prestosql.sql.planner.plan.IndexSourceNode;
import io.prestosql.sql.planner.plan.IntersectNode;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.LateralJoinNode;
import io.prestosql.sql.planner.plan.LimitNode;
import io.prestosql.sql.planner.plan.MarkDistinctNode;
import io.prestosql.sql.planner.plan.MetadataDeleteNode;
import io.prestosql.sql.planner.plan.OutputNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.PlanVisitor;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.RemoteSourceNode;
import io.prestosql.sql.planner.plan.RowNumberNode;
import io.prestosql.sql.planner.plan.SampleNode;
import io.prestosql.sql.planner.plan.SemiJoinNode;
import io.prestosql.sql.planner.plan.SortNode;
import io.prestosql.sql.planner.plan.SpatialJoinNode;
import io.prestosql.sql.planner.plan.TableFinishNode;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.sql.planner.plan.TableWriterNode;
import io.prestosql.sql.planner.plan.TopNNode;
import io.prestosql.sql.planner.plan.TopNRowNumberNode;
import io.prestosql.sql.planner.plan.UnionNode;
import io.prestosql.sql.planner.plan.UnnestNode;
import io.prestosql.sql.planner.plan.ValuesNode;
import io.prestosql.sql.planner.plan.WindowNode;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.SystemSessionProperties.isEnableStatsCalculator;
import static io.prestosql.SystemSessionProperties.isIgnoreStatsCalculatorFailures;
import static io.prestosql.util.MoreLists.mappedCopy;
import static java.lang.Double.NaN;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

public class CachingCostProvider
        implements CostProvider
{
    private static final Logger log = Logger.get(CachingCostProvider.class);

    private final CostCalculator costCalculator;
    private final StatsProvider statsProvider;
    private final Optional<Memo> memo;
    private final Session session;
    private final TypeProvider types;

    private final Map<PlanNode, PlanCostEstimate> cache = new IdentityHashMap<>();

    public CachingCostProvider(CostCalculator costCalculator, StatsProvider statsProvider, Session session, TypeProvider types)
    {
        this(costCalculator, statsProvider, Optional.empty(), session, types);
    }

    public CachingCostProvider(CostCalculator costCalculator, StatsProvider statsProvider, Optional<Memo> memo, Session session, TypeProvider types)
    {
        this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
        this.statsProvider = requireNonNull(statsProvider, "statsProvider is null");
        this.memo = requireNonNull(memo, "memo is null");
        this.session = requireNonNull(session, "session is null");
        this.types = requireNonNull(types, "types is null");
    }

    @Override
    public PlanNodeCostEstimate getCumulativeCost(PlanNode node)
    {
        if (!isEnableStatsCalculator(session)) {
            return PlanNodeCostEstimate.unknown();
        }

        return getPlanCost(node).toPlanNodeCostEstimate();
    }

    private PlanCostEstimate getPlanCost(PlanNode node)
    {
        requireNonNull(node, "node is null");

        try {
            if (node instanceof GroupReference) {
                return getGroupCost((GroupReference) node);
            }

            PlanCostEstimate cumulativeCost = cache.get(node);
            if (cumulativeCost == null) {
                cumulativeCost = calculateCumulativeCost(node);
                verify(cache.put(node, cumulativeCost) == null, "Cost already set");
            }
            return cumulativeCost;
        }
        catch (RuntimeException e) {
            if (isIgnoreStatsCalculatorFailures(session)) {
                log.error(e, "Error occurred when computing cost for query %s", session.getQueryId());
                return PlanCostEstimate.unknown();
            }
            throw e;
        }
    }

    private PlanCostEstimate getGroupCost(GroupReference groupReference)
    {
        int group = groupReference.getGroupId();
        Memo memo = this.memo.orElseThrow(() -> new IllegalStateException("CachingCostProvider without memo cannot handle GroupReferences"));

        Optional<PlanCostEstimate> cost = memo.getCumulativeCost(group);
        if (cost.isPresent()) {
            return cost.get();
        }

        PlanCostEstimate cumulativeCost = calculateCumulativeCost(memo.getNode(group));
        verify(!memo.getCumulativeCost(group).isPresent(), "Group cost already set");
        memo.storeCumulativeCost(group, cumulativeCost);
        return cumulativeCost;
    }

    private PlanCostEstimate calculateCumulativeCost(PlanNode node)
    {
        PlanNodeCostEstimate localCost = costCalculator.calculateCost(node, statsProvider, session, types);

        List<PlanCostEstimate> sourcesCosts = mappedCopy(node.getSources(), this::getPlanCost);
        List<MemoryEstimate> sourcesEstimations = mappedCopy(sourcesCosts, planCostEstimate ->
                new MemoryEstimate(planCostEstimate.getMaxMemory(), planCostEstimate.getMaxMemoryWhenOutputting()));
        PlanCostEstimate sourcesCost = sourcesCosts.stream()
                .reduce(PlanCostEstimate.zero(), CachingCostProvider::addSiblingsCost);

        MemoryEstimate memoryEstimate = node.accept(new PeakMemoryEstimator(), new EstimationContext(localCost.getMemoryCost(), sourcesEstimations));
        requireNonNull(memoryEstimate, "memoryEstimate is null");
        return new PlanCostEstimate(
                localCost.getCpuCost() + sourcesCost.getCpuCost(),
                max(sourcesCost.getMaxMemory(), max(memoryEstimate.maxMemoryBeforeOutputting, memoryEstimate.maxMemoryWhenOutputting)),
                memoryEstimate.maxMemoryWhenOutputting,
                localCost.getNetworkCost() + sourcesCost.getNetworkCost());
    }

    private static PlanCostEstimate addSiblingsCost(PlanCostEstimate a, PlanCostEstimate b)
    {
        return new PlanCostEstimate(
                a.getCpuCost() + b.getCpuCost(),
                a.getMaxMemory() + b.getMaxMemory(),
                a.getMaxMemoryWhenOutputting() + b.getMaxMemoryWhenOutputting(),
                a.getNetworkCost() + b.getNetworkCost());
    }

    private static class EstimationContext
    {
        private final double localMaxMemory;
        private final List<MemoryEstimate> sourcesEstimations;

        public EstimationContext(double localMaxMemory, List<MemoryEstimate> sourcesEstimations)
        {
            this.localMaxMemory = localMaxMemory;
            this.sourcesEstimations = requireNonNull(sourcesEstimations, "sourcesEstimations is null");
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("localMaxMemory", localMaxMemory)
                    .add("sourcesEstimations", sourcesEstimations)
                    .toString();
        }
    }

    private static class MemoryEstimate
    {
        private final double maxMemoryBeforeOutputting;
        private final double maxMemoryWhenOutputting;

        static MemoryEstimate zero()
        {
            return new MemoryEstimate(0, 0);
        }

        MemoryEstimate(double maxMemoryBeforeOutputting, double maxMemoryWhenOutputting)
        {
            this.maxMemoryBeforeOutputting = maxMemoryBeforeOutputting;
            this.maxMemoryWhenOutputting = maxMemoryWhenOutputting;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("maxMemoryBeforeOutputting", maxMemoryBeforeOutputting)
                    .add("maxMemoryWhenOutputting", maxMemoryWhenOutputting)
                    .toString();
        }

        /**
         * Add {@link MemoryEstimate} as if {@code this} and {@code that} were independent (e.g. siblings in the query plan).
         */
        MemoryEstimate add(MemoryEstimate that)
        {
            return new MemoryEstimate(
                    this.maxMemoryBeforeOutputting + that.maxMemoryBeforeOutputting,
                    this.maxMemoryWhenOutputting + that.maxMemoryWhenOutputting);
        }

        /**
         * Add individual {@link MemoryEstimate}s as if they were independent (e.g. siblings in the query plan).
         */
        static MemoryEstimate sum(List<MemoryEstimate> estimations)
        {
            return estimations.stream()
                    .reduce(MemoryEstimate.zero(), MemoryEstimate::add);
        }
    }

    private static class PeakMemoryEstimator
            extends PlanVisitor<MemoryEstimate, EstimationContext>
    {
        @Override
        protected MemoryEstimate visitPlan(PlanNode node, EstimationContext context)
        {
            throw new UnsupportedOperationException("Unsupported plan node type: " + node.getClass().getName());
        }

        @Override
        public MemoryEstimate visitGroupReference(GroupReference node, EstimationContext context)
        {
            throw new IllegalStateException("Unexpected GroupReference");
        }

        @Override
        public MemoryEstimate visitRemoteSource(RemoteSourceNode node, EstimationContext context)
        {
            return memoryEstimateForStreaming(context);
        }

        @Override
        public MemoryEstimate visitAggregation(AggregationNode node, EstimationContext context)
        {
            if (node.getStep() != AggregationNode.Step.SINGLE && node.getStep() != AggregationNode.Step.FINAL) {
                return memoryEstimateForStreaming(context);
            }
            else {
                return memoryEstimateForAccumulation(context);
            }
        }

        @Override
        public MemoryEstimate visitFilter(FilterNode node, EstimationContext context)
        {
            return memoryEstimateForStreaming(context);
        }

        @Override
        public MemoryEstimate visitProject(ProjectNode node, EstimationContext context)
        {
            return memoryEstimateForStreaming(context);
        }

        @Override
        public MemoryEstimate visitTopN(TopNNode node, EstimationContext context)
        {
            return memoryEstimateForAccumulation(context);
        }

        @Override
        public MemoryEstimate visitOutput(OutputNode node, EstimationContext context)
        {
            return memoryEstimateForStreaming(context);
        }

        @Override
        public MemoryEstimate visitLimit(LimitNode node, EstimationContext context)
        {
            return memoryEstimateForStreaming(context);
        }

        @Override
        public MemoryEstimate visitDistinctLimit(DistinctLimitNode node, EstimationContext context)
        {
            if (node.isPartial()) {
                return memoryEstimateForStreaming(context);
            }
            else {
                return memoryEstimateForAccumulation(context);
            }
        }

        @Override
        public MemoryEstimate visitSample(SampleNode node, EstimationContext context)
        {
            // TODO
            return new MemoryEstimate(NaN, NaN);
        }

        @Override
        public MemoryEstimate visitTableScan(TableScanNode node, EstimationContext context)
        {
            return memoryEstimateForSource(node, context);
        }

        @Override
        public MemoryEstimate visitExplainAnalyze(ExplainAnalyzeNode node, EstimationContext context)
        {
            return memoryEstimateForStreaming(context);
        }

        @Override
        public MemoryEstimate visitValues(ValuesNode node, EstimationContext context)
        {
            return memoryEstimateForSource(node, context);
        }

        @Override
        public MemoryEstimate visitIndexSource(IndexSourceNode node, EstimationContext context)
        {
            return memoryEstimateForSource(node, context);
        }

        @Override
        public MemoryEstimate visitJoin(JoinNode node, EstimationContext context)
        {
            return memoryEstimateForLookupJoin(node, context);
        }

        @Override
        public MemoryEstimate visitSemiJoin(SemiJoinNode node, EstimationContext context)
        {
            return memoryEstimateForLookupJoin(node, context);
        }

        @Override
        public MemoryEstimate visitSpatialJoin(SpatialJoinNode node, EstimationContext context)
        {
            return memoryEstimateForLookupJoin(node, context);
        }

        @Override
        public MemoryEstimate visitIndexJoin(IndexJoinNode node, EstimationContext context)
        {
            MemoryEstimate sourcesMemory = MemoryEstimate.sum(context.sourcesEstimations);
            return new MemoryEstimate(
                    sourcesMemory.maxMemoryWhenOutputting,
                    context.localMaxMemory + sourcesMemory.maxMemoryWhenOutputting);
        }

        @Override
        public MemoryEstimate visitSort(SortNode node, EstimationContext context)
        {
            return memoryEstimateForAccumulation(context);
        }

        @Override
        public MemoryEstimate visitWindow(WindowNode node, EstimationContext context)
        {
            return memoryEstimateForStreaming(context);
        }

        @Override
        public MemoryEstimate visitTableWriter(TableWriterNode node, EstimationContext context)
        {
            return memoryEstimateForStreaming(context);
        }

        @Override
        public MemoryEstimate visitDelete(DeleteNode node, EstimationContext context)
        {
            return memoryEstimateForStreaming(context);
        }

        @Override
        public MemoryEstimate visitMetadataDelete(MetadataDeleteNode node, EstimationContext context)
        {
            return memoryEstimateForStreaming(context);
        }

        @Override
        public MemoryEstimate visitTableFinish(TableFinishNode node, EstimationContext context)
        {
            return memoryEstimateForStreaming(context);
        }

        @Override
        public MemoryEstimate visitUnion(UnionNode node, EstimationContext context)
        {
            return memoryEstimateForStreaming(context);
        }

        @Override
        public MemoryEstimate visitIntersect(IntersectNode node, EstimationContext context)
        {
            return memoryEstimateForLogicalOperator();
        }

        @Override
        public MemoryEstimate visitExcept(ExceptNode node, EstimationContext context)
        {
            return memoryEstimateForLogicalOperator();
        }

        @Override
        public MemoryEstimate visitUnnest(UnnestNode node, EstimationContext context)
        {
            return memoryEstimateForStreaming(context);
        }

        @Override
        public MemoryEstimate visitMarkDistinct(MarkDistinctNode node, EstimationContext context)
        {
            return memoryEstimateForStreaming(context);
        }

        @Override
        public MemoryEstimate visitGroupId(GroupIdNode node, EstimationContext context)
        {
            return memoryEstimateForStreaming(context);
        }

        @Override
        public MemoryEstimate visitRowNumber(RowNumberNode node, EstimationContext context)
        {
            return memoryEstimateForStreaming(context);
        }

        @Override
        public MemoryEstimate visitTopNRowNumber(TopNRowNumberNode node, EstimationContext context)
        {
            if (node.isPartial()) {
                return memoryEstimateForStreaming(context);
            }
            else {
                return memoryEstimateForAccumulation(context);
            }
        }

        @Override
        public MemoryEstimate visitExchange(ExchangeNode node, EstimationContext context)
        {
            return memoryEstimateForStreaming(context);
        }

        @Override
        public MemoryEstimate visitEnforceSingleRow(EnforceSingleRowNode node, EstimationContext context)
        {
            // EnforceSingleRowOperator returns output only after finish() has been called
            return memoryEstimateForAccumulation(context);
        }

        @Override
        public MemoryEstimate visitApply(ApplyNode node, EstimationContext context)
        {
            return new MemoryEstimate(NaN, NaN);
        }

        @Override
        public MemoryEstimate visitAssignUniqueId(AssignUniqueId node, EstimationContext context)
        {
            return memoryEstimateForStreaming(context);
        }

        @Override
        public MemoryEstimate visitLateralJoin(LateralJoinNode node, EstimationContext context)
        {
            return memoryEstimateForLogicalOperator();
        }

        private static MemoryEstimate memoryEstimateForLogicalOperator()
        {
            return new MemoryEstimate(NaN, NaN);
        }

        private static MemoryEstimate memoryEstimateForSource(PlanNode node, EstimationContext context)
        {
            verify(context.sourcesEstimations.isEmpty(), "Unexpected number of sources for %s: %s", node, context.sourcesEstimations.size());
            return new MemoryEstimate(0, context.localMaxMemory);
        }

        private static MemoryEstimate memoryEstimateForAccumulation(EstimationContext context)
        {
            MemoryEstimate sourcesMemory = MemoryEstimate.sum(context.sourcesEstimations);
            return new MemoryEstimate(
                    sourcesMemory.maxMemoryWhenOutputting + context.localMaxMemory,
                    context.localMaxMemory);
        }

        private static MemoryEstimate memoryEstimateForStreaming(EstimationContext context)
        {
            MemoryEstimate sourcesMemory = MemoryEstimate.sum(context.sourcesEstimations);
            return new MemoryEstimate(
                    // Assumption is made that context.localMaxMemory is allocated lazily, i.e. no memory is allocated before current node gets any data.
                    context.localMaxMemory + sourcesMemory.maxMemoryWhenOutputting,
                    context.localMaxMemory + sourcesMemory.maxMemoryWhenOutputting);
        }

        private static MemoryEstimate memoryEstimateForLookupJoin(PlanNode node, EstimationContext context)
        {
            verify(context.sourcesEstimations.size() == 2, "Unexpected number of sources for %s: %s", node, context.sourcesEstimations.size());
            double localMaxMemory = context.localMaxMemory;
            double probeMaxMemoryWhenOutputting = context.sourcesEstimations.get(0).maxMemoryWhenOutputting;
            double buildMaxMemoryWhenOutputting = context.sourcesEstimations.get(1).maxMemoryWhenOutputting;

            return new MemoryEstimate(
                    localMaxMemory + probeMaxMemoryWhenOutputting + buildMaxMemoryWhenOutputting,
                    localMaxMemory + probeMaxMemoryWhenOutputting);
        }

        private static double getOnlySourceMaxMemoryWhenOutputting(PlanNode forNode, EstimationContext context)
        {
            verify(context.sourcesEstimations.size() == 1, "Unexpected number of sources for %s: %s", forNode, context.sourcesEstimations.size());
            return getOnlyElement(context.sourcesEstimations).maxMemoryWhenOutputting;
        }
    }
}

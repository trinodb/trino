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
import com.google.common.collect.ImmutableSet;
import com.google.common.graph.Traverser;
import io.trino.Session;
import io.trino.cost.CachingTableStatsProvider;
import io.trino.cost.RuntimeInfoProvider;
import io.trino.cost.StatsAndCosts;
import io.trino.execution.querystats.PlanOptimizersStatsCollector;
import io.trino.execution.warnings.WarningCollector;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.optimizations.AdaptivePlanOptimizer;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.planner.plan.AdaptivePlanNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.RemoteSourceNode;
import io.trino.sql.planner.plan.SimplePlanRewriter;
import io.trino.sql.planner.sanity.PlanSanityChecker;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.planner.plan.SimplePlanRewriter.rewriteWith;
import static io.trino.tracing.ScopedSpan.scopedSpan;
import static java.util.Objects.requireNonNull;

/**
 * This class is responsible for re-optimizing the plan using exchange statistics in FTE. For example,
 * reordering of join or mitigation of skewness. It will significantly impact cost and
 * performance if the plan chosen by the static optimiser isn’t the best due to the
 * underestimation of statistics or lack of statistics.
 * <p>
 * Re-planning Steps:
 * 1. It first merges all SubPlans into a single PlanNode where RemoteSourceNode for stages
 * that haven’t finished will get replaced with Remote Exchanges. On the other hand,
 * RemoteSourceNode for finished stages will remain as it is in the plan.
 * <p>
 * 2. Once we have a merged plan which contains all the unfinished parts, we will reoptimize it using
 * a set of PlanOptimizers.
 * <p>
 * 3. During re-optimization, it is possible that some new exchanges need to be added due to the change
 * in partitioning strategy. For instance, if a rule changes the distribution type of the join
 * from BROADCAST to PARTITIONED. It is also possible that some remote exchanges are removed.
 * For example, while changing the order of the join.
 * <p>
 * 4. Ultimately, the planner will fragment the optimized PlanNode again and generate the SubPlans with
 * new PlanFragmentIds. The re-fragmentation will only happen if the old plan and the new
 * plan have some differences. To check these differences, we rely on
 * PlanOptimizer#optimizeAndMarkPlanChanges api which also returns changed plan ids.
 * <p>
 * Note: We do not change the fragment ids which have no changes and are not downstream of the
 * changed plan nodes. This optimization is done to avoid unnecessary stage restart due to speculative execution.
 */
public class AdaptivePlanner
{
    private final Session session;
    private final PlannerContext plannerContext;
    private final List<AdaptivePlanOptimizer> planOptimizers;
    private final PlanFragmenter planFragmenter;
    private final PlanSanityChecker planSanityChecker;
    private final WarningCollector warningCollector;
    private final PlanOptimizersStatsCollector planOptimizersStatsCollector;
    private final CachingTableStatsProvider tableStatsProvider;

    public AdaptivePlanner(
            Session session,
            PlannerContext plannerContext,
            List<AdaptivePlanOptimizer> planOptimizers,
            PlanFragmenter planFragmenter,
            PlanSanityChecker planSanityChecker,
            WarningCollector warningCollector,
            PlanOptimizersStatsCollector planOptimizersStatsCollector,
            CachingTableStatsProvider tableStatsProvider)
    {
        this.session = requireNonNull(session, "session is null");
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.planOptimizers = requireNonNull(planOptimizers, "planOptimizers is null");
        this.planFragmenter = requireNonNull(planFragmenter, "planFragmenter is null");
        this.planSanityChecker = requireNonNull(planSanityChecker, "planSanityChecker is null");
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        this.planOptimizersStatsCollector = requireNonNull(planOptimizersStatsCollector, "planOptimizersStatsCollector is null");
        this.tableStatsProvider = requireNonNull(tableStatsProvider, "tableStatsProvider is null");
    }

    public SubPlan optimize(SubPlan root, RuntimeInfoProvider runtimeInfoProvider)
    {
        // No need to run optimizer since the root is already finished or its stats are almost accurate based on
        // estimate by progress.
        // TODO: We need add an ability to re-plan fragment whose stats are estimated by progress.
        if (runtimeInfoProvider.getRuntimeOutputStats(root.getFragment().getId()).isAccurate()) {
            return root;
        }

        List<SubPlan> subPlans = traverse(root).collect(toImmutableList());

        // create a new fragment id allocator and symbol allocator
        PlanFragmentIdAllocator fragmentIdAllocator = new PlanFragmentIdAllocator(getMaxPlanFragmentId(subPlans) + 1);
        SymbolAllocator symbolAllocator = createSymbolAllocator(subPlans);

        // rewrite remote source nodes to exchange nodes, except for fragments which are finisher or whose stats are
        // estimated by progress.
        ReplaceRemoteSourcesWithExchanges rewriter = new ReplaceRemoteSourcesWithExchanges(runtimeInfoProvider);
        PlanNode currentAdaptivePlan = rewriteWith(rewriter, root.getFragment().getRoot(), root.getChildren());

        // Remove the adaptive plan node and replace it with initial plan
        PlanNode initialPlan = getInitialPlan(currentAdaptivePlan);
        // Remove the adaptive plan node and replace it with current plan
        PlanNode currentPlan = getCurrentPlan(currentAdaptivePlan);

        // Collect the sub plans for each remote exchange and remote source node. We will use this map during
        // re-fragmentation as a cache for all unchanged sub plans.
        ExchangeSourceIdToSubPlanCollector exchangeSourceIdToSubPlanCollector = new ExchangeSourceIdToSubPlanCollector();
        currentPlan.accept(exchangeSourceIdToSubPlanCollector, subPlans);
        Map<ExchangeSourceId, SubPlan> exchangeSourceIdToSubPlan = exchangeSourceIdToSubPlanCollector.getExchangeSourceIdToSubPlan();

        // optimize the current plan
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator(getMaxPlanId(currentPlan) + 1);
        AdaptivePlanOptimizer.Result optimizationResult = optimizePlan(currentPlan, symbolAllocator, runtimeInfoProvider, idAllocator);

        // Check whether there are some changes in the plan after optimization
        if (optimizationResult.changedPlanNodes().isEmpty()) {
            return root;
        }

        // Add the adaptive plan node recursively where initialPlan remain as it is and optimizedPlan as new currentPlan
        PlanNode adaptivePlan = addAdaptivePlanNode(idAllocator, initialPlan, optimizationResult.plan(), optimizationResult.changedPlanNodes());
        // validate the adaptive plan
        try (var _ = scopedSpan(plannerContext.getTracer(), "validate-adaptive-plan")) {
            planSanityChecker.validateAdaptivePlan(adaptivePlan, session, plannerContext, warningCollector);
        }

        // Fragment the adaptive plan
        return planFragmenter.createSubPlans(
                session,
                new Plan(adaptivePlan, StatsAndCosts.empty()),
                false,
                warningCollector,
                fragmentIdAllocator,
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), adaptivePlan.getOutputSymbols()),
                // We do not change the subPlans which have no changes and are not downstream of the
                // changed plan nodes. This optimization is done to avoid unnecessary stage restart due to speculative
                // execution.
                getUnchangedSubPlans(adaptivePlan, optimizationResult.changedPlanNodes(), exchangeSourceIdToSubPlan));
    }

    private AdaptivePlanOptimizer.Result optimizePlan(
            PlanNode plan,
            SymbolAllocator symbolAllocator,
            RuntimeInfoProvider runtimeInfoProvider,
            PlanNodeIdAllocator idAllocator)
    {
        AdaptivePlanOptimizer.Result result = new AdaptivePlanOptimizer.Result(plan, Set.of());
        ImmutableSet.Builder<PlanNodeId> changedPlanNodes = ImmutableSet.builder();
        for (AdaptivePlanOptimizer optimizer : planOptimizers) {
            result = optimizer.optimizeAndMarkPlanChanges(
                    result.plan(),
                    new PlanOptimizer.Context(
                            session,
                            symbolAllocator,
                            idAllocator,
                            warningCollector,
                            planOptimizersStatsCollector,
                            tableStatsProvider,
                            runtimeInfoProvider));
            changedPlanNodes.addAll(result.changedPlanNodes());
        }
        return new AdaptivePlanOptimizer.Result(result.plan(), changedPlanNodes.build());
    }

    private PlanNode addAdaptivePlanNode(
            PlanNodeIdAllocator idAllocator,
            PlanNode initialPlan,
            PlanNode optimizedPlanNode,
            Set<PlanNodeId> changedPlanNodes)
    {
        // We should check optimizedPlanNode here instead of initialPlan since it is possible that new
        // nodes have been added, and they aren't part of initialPlan. However, we should put the adaptive plan node
        // above them.
        if (changedPlanNodes.contains(optimizedPlanNode.getId())) {
            return new AdaptivePlanNode(
                    idAllocator.getNextId(),
                    initialPlan,
                    SymbolsExtractor.extractOutputSymbols(initialPlan),
                    optimizedPlanNode);
        }

        // This condition should always be true because if a plan node is changed, then it should be captured in the
        // changedPlanNodes set based on the semantics of PlanOptimizer#optimizeAndMarkPlanChanges.
        verify(initialPlan.getSources().size() == optimizedPlanNode.getSources().size());
        ImmutableList.Builder<PlanNode> sources = ImmutableList.builder();
        for (int i = 0; i < initialPlan.getSources().size(); i++) {
            PlanNode initialSource = initialPlan.getSources().get(i);
            PlanNode optimizedSource = optimizedPlanNode.getSources().get(i);
            sources.add(addAdaptivePlanNode(idAllocator, initialSource, optimizedSource, changedPlanNodes));
        }
        return optimizedPlanNode.replaceChildren(sources.build());
    }

    private Map<ExchangeSourceId, SubPlan> getUnchangedSubPlans(
            PlanNode adaptivePlan, Set<PlanNodeId> changedPlanIds, Map<ExchangeSourceId, SubPlan> exchangeSourceIdToSubPlan)
    {
        Set<PlanNodeId> changedPlanIdsWithDownstream = new HashSet<>();
        for (PlanNodeId changedId : changedPlanIds) {
            changedPlanIdsWithDownstream.addAll(getDownstreamPlanNodeIds(adaptivePlan, changedId));
        }

        return exchangeSourceIdToSubPlan.entrySet().stream()
                .filter(entry -> !changedPlanIdsWithDownstream.contains(entry.getKey().exchangeId())
                        && !changedPlanIdsWithDownstream.contains(entry.getKey().sourceId()))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private Set<PlanNodeId> getDownstreamPlanNodeIds(PlanNode root, PlanNodeId id)
    {
        if (root.getId().equals(id)) {
            return ImmutableSet.of(id);
        }
        Set<PlanNodeId> upstreamNodes = new HashSet<>();
        root.getSources().stream()
                .map(source -> getDownstreamPlanNodeIds(source, id))
                .forEach(upstreamNodes::addAll);
        if (!upstreamNodes.isEmpty()) {
            upstreamNodes.add(root.getId());
        }
        return upstreamNodes;
    }

    private PlanNode getCurrentPlan(PlanNode node)
    {
        return rewriteWith(new CurrentPlanRewriter(), node);
    }

    private PlanNode getInitialPlan(PlanNode node)
    {
        return rewriteWith(new InitialPlanRewriter(), node);
    }

    private SymbolAllocator createSymbolAllocator(List<SubPlan> subPlans)
    {
        return new SymbolAllocator(
                subPlans.stream()
                        .map(SubPlan::getFragment)
                        .map(PlanFragment::getSymbols)
                        .flatMap(Set::stream)
                        .collect(toImmutableSet()));
    }

    private int getMaxPlanFragmentId(List<SubPlan> subPlans)
    {
        return subPlans.stream()
                .map(SubPlan::getFragment)
                .map(PlanFragment::getId)
                .mapToInt(fragmentId -> Integer.parseInt(fragmentId.toString()))
                .max()
                .orElseThrow();
    }

    private int getMaxPlanId(PlanNode node)
    {
        return traverse(node)
                .map(PlanNode::getId)
                .mapToInt(planNodeId -> Integer.parseInt(planNodeId.toString()))
                .max()
                .orElseThrow();
    }

    private Stream<PlanNode> traverse(PlanNode node)
    {
        Iterable<PlanNode> iterable = Traverser.forTree(PlanNode::getSources).depthFirstPreOrder(node);
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    private Stream<SubPlan> traverse(SubPlan subPlan)
    {
        Iterable<SubPlan> iterable = Traverser.forTree(SubPlan::getChildren).depthFirstPreOrder(subPlan);
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    private static class ReplaceRemoteSourcesWithExchanges
            extends SimplePlanRewriter<List<SubPlan>>
    {
        private final RuntimeInfoProvider runtimeInfoProvider;

        private ReplaceRemoteSourcesWithExchanges(RuntimeInfoProvider runtimeInfoProvider)
        {
            this.runtimeInfoProvider = requireNonNull(runtimeInfoProvider, "runtimeInfoProvider is null");
        }

        @Override
        public PlanNode visitAdaptivePlanNode(AdaptivePlanNode node, RewriteContext<List<SubPlan>> context)
        {
            // It is possible that the initial plan also contains remote source nodes, therefore we need to
            // rewrite them as well.
            PlanNode initialPlan = context.rewrite(node.getInitialPlan(), context.get());
            PlanNode currentPlan = context.rewrite(node.getCurrentPlan(), context.get());
            return new AdaptivePlanNode(node.getId(), initialPlan, SymbolsExtractor.extractOutputSymbols(initialPlan), currentPlan);
        }

        @Override
        public PlanNode visitRemoteSource(RemoteSourceNode node, RewriteContext<List<SubPlan>> context)
        {
            // We won't run optimizer rules on sub plans which are either finished or their stats are almost accurate
            // based are estimated by progress.
            // TODO: We need add an ability to re-plan fragment whose stats are estimated by progress.
            if (node.getSourceFragmentIds().stream()
                    .anyMatch(planFragmentId -> runtimeInfoProvider.getRuntimeOutputStats(planFragmentId).isAccurate())) {
                return node;
            }

            List<SubPlan> sourceSubPlans = context.get().stream()
                    .filter(subPlan -> node.getSourceFragmentIds().contains(subPlan.getFragment().getId()))
                    .collect(toImmutableList());

            ImmutableList.Builder<PlanNode> sourceNodesBuilder = ImmutableList.builder();
            for (SubPlan sourceSubPlan : sourceSubPlans) {
                PlanNode sourceNode = context.rewrite(sourceSubPlan.getFragment().getRoot(), sourceSubPlan.getChildren());
                sourceNodesBuilder.add(sourceNode);
            }

            List<PlanNode> sourceNodes = sourceNodesBuilder.build();

            // Find the input symbols for the exchange node
            List<PartitioningScheme> outputPartitioningSchemes = node.getSourceFragmentIds().stream()
                    .map(runtimeInfoProvider::getPlanFragment)
                    .map(PlanFragment::getOutputPartitioningScheme)
                    .collect(toImmutableList());
            verify(outputPartitioningSchemes.size() == sourceNodes.size(), "Output partitioning schemes size does not match source nodes size");
            List<List<Symbol>> inputs = outputPartitioningSchemes.stream()
                    .map(PartitioningScheme::getOutputLayout)
                    .collect(toImmutableList());

            return new ExchangeNode(
                    node.getId(),
                    node.getExchangeType(),
                    REMOTE,
                    // We need to translate the output layout of the partitioning scheme to the output layout
                    // of the RemoteSourceNode.
                    outputPartitioningSchemes.getFirst().translateOutputLayout(node.getOutputSymbols()),
                    sourceNodes,
                    inputs,
                    node.getOrderingScheme());
        }
    }

    private static class ExchangeSourceIdToSubPlanCollector
            extends SimplePlanVisitor<List<SubPlan>>
    {
        private final Map<ExchangeSourceId, SubPlan> exchangeSourceIdToSubPlan = new HashMap<>();

        @Override
        public Void visitExchange(ExchangeNode node, List<SubPlan> context)
        {
            // Process the source nodes first
            visitPlan(node, context);

            // No need to process the exchange node if it is not a remote exchange
            if (node.getScope() != REMOTE) {
                return null;
            }

            // Find the sub plans for this exchange node
            List<PlanNodeId> sourceIds = node.getSources().stream().map(PlanNode::getId).collect(toImmutableList());
            List<SubPlan> sourceSubPlans = context.stream()
                    .filter(subPlan -> sourceIds.contains(subPlan.getFragment().getRoot().getId()))
                    .collect(toImmutableList());
            verify(
                    sourceSubPlans.size() == sourceIds.size(),
                    "Source subPlans not found for exchange node");

            for (SubPlan sourceSubPlan : sourceSubPlans) {
                PlanNodeId sourceId = sourceSubPlan.getFragment().getRoot().getId();
                exchangeSourceIdToSubPlan.put(new ExchangeSourceId(node.getId(), sourceId), sourceSubPlan);
            }
            return null;
        }

        @Override
        public Void visitRemoteSource(RemoteSourceNode node, List<SubPlan> context)
        {
            List<SubPlan> sourceSubPlans = context.stream()
                    .filter(subPlan -> node.getSourceFragmentIds().contains(subPlan.getFragment().getId()))
                    .collect(toImmutableList());

            for (SubPlan sourceSubPlan : sourceSubPlans) {
                PlanNodeId sourceId = sourceSubPlan.getFragment().getRoot().getId();
                exchangeSourceIdToSubPlan.put(new ExchangeSourceId(node.getId(), sourceId), sourceSubPlan);
            }
            return null;
        }

        public Map<ExchangeSourceId, SubPlan> getExchangeSourceIdToSubPlan()
        {
            return ImmutableMap.copyOf(exchangeSourceIdToSubPlan);
        }
    }

    private static class CurrentPlanRewriter
            extends SimplePlanRewriter<List<SubPlan>>
    {
        @Override
        public PlanNode visitAdaptivePlanNode(AdaptivePlanNode node, RewriteContext<List<SubPlan>> context)
        {
            verify(
                    !containsAdaptivePlanNode(node.getCurrentPlan()),
                    "Adaptive plan node cannot have a nested adaptive plan node");
            return node.getCurrentPlan();
        }
    }

    private static class InitialPlanRewriter
            extends SimplePlanRewriter<List<SubPlan>>
    {
        @Override
        public PlanNode visitAdaptivePlanNode(AdaptivePlanNode node, RewriteContext<List<SubPlan>> context)
        {
            verify(
                    !containsAdaptivePlanNode(node.getInitialPlan()),
                    "Adaptive plan node cannot have a nested adaptive plan node");
            return node.getInitialPlan();
        }
    }

    private static boolean containsAdaptivePlanNode(PlanNode node)
    {
        return PlanNodeSearcher.searchFrom(node)
                .whereIsInstanceOfAny(AdaptivePlanNode.class)
                .matches();
    }

    public record ExchangeSourceId(PlanNodeId exchangeId, PlanNodeId sourceId)
    {
        public ExchangeSourceId
        {
            requireNonNull(exchangeId, "exchangeId is null");
            requireNonNull(sourceId, "sourceId is null");
        }
    }
}

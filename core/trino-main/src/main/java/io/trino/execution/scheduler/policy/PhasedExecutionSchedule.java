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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.trino.execution.scheduler.StageExecution;
import io.trino.execution.scheduler.StageExecution.State;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.IndexJoinNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.RemoteSourceNode;
import io.trino.sql.planner.plan.SemiJoinNode;
import io.trino.sql.planner.plan.SpatialJoinNode;
import org.jgrapht.DirectedGraph;
import org.jgrapht.EdgeFactory;
import org.jgrapht.alg.StrongConnectivityInspector;
import org.jgrapht.graph.DefaultDirectedGraph;
import oshi.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.execution.scheduler.StageExecution.State.FLUSHING;
import static io.trino.execution.scheduler.StageExecution.State.RUNNING;
import static io.trino.execution.scheduler.StageExecution.State.SCHEDULED;
import static io.trino.sql.planner.plan.AggregationNode.Step.FINAL;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

/**
 * Schedules stages choosing to order to provide the best resource utilization.
 * This means that stages which output won't be consumed (e.g. join probe side) will
 * not be scheduled until dependent stages finish (e.g. join build source stages).
 * Contrary to {@link LegacyPhasedExecutionPolicy}, {@link PhasedExecutionSchedule} will
 * schedule multiple source stages in order to fully utilize IO.
 */
public class PhasedExecutionSchedule
        implements ExecutionSchedule
{
    /**
     * Graph representing a before -> after relationship between fragments.
     * Destination fragment should be started only when source stage is completed.
     */
    private final DirectedGraph<PlanFragmentId, FragmentsEdge> fragmentDependency;
    /**
     * Graph representing topology between fragments (e.g. child -> parent relationship).
     */
    private final DirectedGraph<PlanFragmentId, FragmentsEdge> fragmentTopology;
    /**
     * Fragments sorted using in-order tree scan where join build side
     * is visited before probe side.
     */
    private final List<PlanFragmentId> sortedFragments = new ArrayList<>();
    private final Map<PlanFragmentId, StageExecution> stagesByFragmentId;
    private final Set<StageExecution> activeStages = new LinkedHashSet<>();

    /**
     * Set by {@link PhasedExecutionSchedule#init(Collection)} method.
     */
    private Ordering<PlanFragmentId> fragmentOrdering;

    @GuardedBy("this")
    private SettableFuture<Void> rescheduleFuture = SettableFuture.create();

    public static PhasedExecutionSchedule forStages(Collection<StageExecution> stages)
    {
        PhasedExecutionSchedule schedule = new PhasedExecutionSchedule(stages);
        schedule.init(stages);
        return schedule;
    }

    private PhasedExecutionSchedule(Collection<StageExecution> stages)
    {
        fragmentDependency = new DefaultDirectedGraph<>(new FragmentsEdgeFactory());
        fragmentTopology = new DefaultDirectedGraph<>(new FragmentsEdgeFactory());
        stagesByFragmentId = stages.stream()
                .collect(toImmutableMap(stage -> stage.getFragment().getId(), identity()));
    }

    private void init(Collection<StageExecution> stages)
    {
        ImmutableSet.Builder<PlanFragmentId> fragmentsToExecute = ImmutableSet.builder();
        fragmentsToExecute.addAll(extractDependenciesAndReturnNonLazyFragments(
                stages.stream()
                        .map(StageExecution::getFragment)
                        .collect(toImmutableList())));
        // start stages without any dependencies
        fragmentDependency.vertexSet().stream()
                .filter(fragmentId -> fragmentDependency.inDegreeOf(fragmentId) == 0)
                .forEach(fragmentsToExecute::add);
        fragmentOrdering = Ordering.explicit(sortedFragments);
        selectForExecution(fragmentsToExecute.build());
    }

    @Override
    public StagesScheduleResult getStagesToSchedule()
    {
        // obtain reschedule future before actual scheduling, so that state change
        // notifications from previously started stages are not lost
        Optional<ListenableFuture<Void>> rescheduleFuture = getRescheduleFuture();
        schedule();
        return new StagesScheduleResult(activeStages, rescheduleFuture);
    }

    @Override
    public boolean isFinished()
    {
        // dependency graph contains both running and not started fragments
        return fragmentDependency.vertexSet().isEmpty();
    }

    @VisibleForTesting
    synchronized Optional<ListenableFuture<Void>> getRescheduleFuture()
    {
        return Optional.of(rescheduleFuture);
    }

    @VisibleForTesting
    void schedule()
    {
        ImmutableSet.Builder<PlanFragmentId> fragmentsToExecute = ImmutableSet.builder();
        fragmentsToExecute.addAll(removeCompletedStages());
        fragmentsToExecute.addAll(unblockStagesWithFullOutputBuffer());
        selectForExecution(fragmentsToExecute.build());
    }

    @VisibleForTesting
    List<PlanFragmentId> getSortedFragments()
    {
        return sortedFragments;
    }

    @VisibleForTesting
    DirectedGraph<PlanFragmentId, FragmentsEdge> getFragmentDependency()
    {
        return fragmentDependency;
    }

    @VisibleForTesting
    Set<StageExecution> getActiveStages()
    {
        return activeStages;
    }

    private Set<PlanFragmentId> removeCompletedStages()
    {
        // iterate over all stages, not only active ones; stages which are not yet active could have already been aborted
        Set<StageExecution> completedStages = stagesByFragmentId.values().stream()
                .filter(this::isStageCompleted)
                .collect(toImmutableSet());
        // remove completed stages outside of Java stream to prevent concurrent modification
        return completedStages.stream()
                .flatMap(stage -> removeCompletedStage(stage).stream())
                .collect(toImmutableSet());
    }

    private Set<PlanFragmentId> removeCompletedStage(StageExecution stage)
    {
        // start all stages that depend on completed stage
        PlanFragmentId fragmentId = stage.getFragment().getId();
        if (!fragmentDependency.containsVertex(fragmentId)) {
            // already gone
            return ImmutableSet.of();
        }
        Set<PlanFragmentId> fragmentsToExecute = fragmentDependency.outgoingEdgesOf(fragmentId).stream()
                .map(FragmentsEdge::getTarget)
                // filter stages that depend on completed stage only
                .filter(dependentFragmentId -> fragmentDependency.inDegreeOf(dependentFragmentId) == 1)
                .collect(toImmutableSet());
        fragmentDependency.removeVertex(fragmentId);
        fragmentTopology.removeVertex(fragmentId);
        activeStages.remove(stage);
        return fragmentsToExecute;
    }

    private Set<PlanFragmentId> unblockStagesWithFullOutputBuffer()
    {
        // find stages that are blocked on full task output buffer
        Set<PlanFragmentId> blockedFragments = activeStages.stream()
                .filter(StageExecution::isAnyTaskBlocked)
                .map(stage -> stage.getFragment().getId())
                .collect(toImmutableSet());
        // start immediate downstream stages so that data can be consumed
        return blockedFragments.stream()
                .flatMap(fragmentId -> fragmentTopology.outgoingEdgesOf(fragmentId).stream())
                .map(FragmentsEdge::getTarget)
                .collect(toImmutableSet());
    }

    private void selectForExecution(Set<PlanFragmentId> fragmentIds)
    {
        requireNonNull(fragmentOrdering, "fragmentOrdering is null");
        fragmentIds.stream()
                .sorted(fragmentOrdering)
                .map(stagesByFragmentId::get)
                .forEach(this::selectForExecution);
    }

    private void selectForExecution(StageExecution stage)
    {
        if (isStageCompleted(stage)) {
            // don't start completed stages (can happen when non-lazy stage is selected for
            // execution and stage is started immediately even with dependencies)
            return;
        }

        if (fragmentDependency.outDegreeOf(stage.getFragment().getId()) > 0) {
            // if there are any dependent stages then reschedule when stage is completed
            stage.addStateChangeListener(state -> {
                if (isStageCompleted(stage)) {
                    notifyReschedule();
                }
            });
        }
        activeStages.add(stage);
    }

    private void notifyReschedule()
    {
        SettableFuture<Void> rescheduleFuture;
        synchronized (this) {
            rescheduleFuture = this.rescheduleFuture;
            this.rescheduleFuture = SettableFuture.create();
        }
        // notify listeners outside of the critical section
        rescheduleFuture.set(null);
    }

    private boolean isStageCompleted(StageExecution stage)
    {
        State state = stage.getState();
        return state == SCHEDULED || state == RUNNING || state == FLUSHING || state.isDone();
    }

    private Set<PlanFragmentId> extractDependenciesAndReturnNonLazyFragments(Collection<PlanFragment> fragments)
    {
        // Build a graph where the plan fragments are vertexes and the edges represent
        // a before -> after relationship. Destination fragment should be started only
        // when source fragment is completed. For example, a join hash build has an edge
        // to the join probe.
        Visitor visitor = new Visitor(fragments);
        visitor.processAllFragments();

        // Make sure there are no strongly connected components as it would mean circular dependency between stages
        List<Set<PlanFragmentId>> components = new StrongConnectivityInspector<>(fragmentDependency).stronglyConnectedSets();
        verify(components.size() == fragmentDependency.vertexSet().size(), "circular dependency between stages");

        return visitor.getNonLazyFragments();
    }

    private class Visitor
            extends PlanVisitor<FragmentSubGraph, PlanFragmentId>
    {
        private final Map<PlanFragmentId, PlanFragment> fragments;
        private final ImmutableSet.Builder<PlanFragmentId> nonLazyFragments = ImmutableSet.builder();
        private final Map<PlanFragmentId, FragmentSubGraph> fragmentSubGraphs = new HashMap<>();

        public Visitor(Collection<PlanFragment> fragments)
        {
            this.fragments = requireNonNull(fragments, "fragments is null").stream()
                    .collect(toImmutableMap(PlanFragment::getId, identity()));
        }

        public Set<PlanFragmentId> getNonLazyFragments()
        {
            return nonLazyFragments.build();
        }

        public void processAllFragments()
        {
            fragments.forEach((fragmentId, fragment) -> {
                fragmentDependency.addVertex(fragmentId);
                fragmentTopology.addVertex(fragmentId);
            });

            // determine non-output fragments
            Set<PlanFragmentId> remoteSources = fragments.values().stream()
                    .map(PlanFragment::getRemoteSourceNodes)
                    .flatMap(Collection::stream)
                    .map(RemoteSourceNode::getSourceFragmentIds)
                    .flatMap(Collection::stream)
                    .collect(toImmutableSet());

            // process fragments (starting from root)
            fragments.keySet().stream()
                    .filter(fragmentId -> !remoteSources.contains(fragmentId))
                    .forEach(this::processFragment);
        }

        public FragmentSubGraph processFragment(PlanFragmentId planFragmentId)
        {
            if (fragmentSubGraphs.containsKey(planFragmentId)) {
                return fragmentSubGraphs.get(planFragmentId);
            }

            FragmentSubGraph subGraph = processFragment(fragments.get(planFragmentId));
            verify(fragmentSubGraphs.put(planFragmentId, subGraph) == null, "fragment %s was already processed", planFragmentId);
            sortedFragments.add(planFragmentId);
            return subGraph;
        }

        private FragmentSubGraph processFragment(PlanFragment fragment)
        {
            FragmentSubGraph subGraph = fragment.getRoot().accept(this, fragment.getId());
            // append current fragment to set of upstream fragments as it is no longer being visited
            Set<PlanFragmentId> upstreamFragments = ImmutableSet.<PlanFragmentId>builder()
                    .addAll(subGraph.getUpstreamFragments())
                    .add(fragment.getId())
                    .build();
            Set<PlanFragmentId> lazyUpstreamFragments;
            if (subGraph.isCurrentFragmentLazy()) {
                // append current fragment as a lazy fragment as it is no longer being visited
                lazyUpstreamFragments = ImmutableSet.<PlanFragmentId>builder()
                        .addAll(subGraph.getLazyUpstreamFragments())
                        .add(fragment.getId())
                        .build();
            }
            else {
                lazyUpstreamFragments = subGraph.getLazyUpstreamFragments();
                nonLazyFragments.add(fragment.getId());
            }
            return new FragmentSubGraph(
                    upstreamFragments,
                    lazyUpstreamFragments,
                    // no longer relevant as we have finished visiting given fragment
                    false);
        }

        @Override
        public FragmentSubGraph visitJoin(JoinNode node, PlanFragmentId currentFragmentId)
        {
            return processJoin(
                    node.getDistributionType().orElseThrow() == JoinNode.DistributionType.REPLICATED,
                    node.getLeft(),
                    node.getRight(),
                    currentFragmentId);
        }

        @Override
        public FragmentSubGraph visitSpatialJoin(SpatialJoinNode node, PlanFragmentId currentFragmentId)
        {
            return processJoin(
                    node.getDistributionType() == SpatialJoinNode.DistributionType.REPLICATED,
                    node.getLeft(),
                    node.getRight(),
                    currentFragmentId);
        }

        @Override
        public FragmentSubGraph visitSemiJoin(SemiJoinNode node, PlanFragmentId currentFragmentId)
        {
            return processJoin(
                    node.getDistributionType().orElseThrow() == SemiJoinNode.DistributionType.REPLICATED,
                    node.getSource(),
                    node.getFilteringSource(),
                    currentFragmentId);
        }

        @Override
        public FragmentSubGraph visitIndexJoin(IndexJoinNode node, PlanFragmentId currentFragmentId)
        {
            return processJoin(
                    true,
                    node.getProbeSource(),
                    node.getIndexSource(),
                    currentFragmentId);
        }

        private FragmentSubGraph processJoin(boolean replicated, PlanNode probe, PlanNode build, PlanFragmentId currentFragmentId)
        {
            FragmentSubGraph buildSubGraph = build.accept(this, currentFragmentId);
            FragmentSubGraph probeSubGraph = probe.accept(this, currentFragmentId);

            // start probe source stages after all build source stages finish
            addDependencyEdges(buildSubGraph.getUpstreamFragments(), probeSubGraph.getLazyUpstreamFragments());

            boolean currentFragmentLazy = probeSubGraph.isCurrentFragmentLazy() && buildSubGraph.isCurrentFragmentLazy();
            if (replicated && currentFragmentLazy) {
                // Do not start join stage (which can also be a source stage with table scans)
                // for replicated join until build source stage enters FLUSHING state.
                // Broadcast join limit for CBO is set in such a way that build source data should
                // fit into task output buffer.
                // In case build source stage is blocked on full task buffer then join stage
                // will be started automatically regardless od dependency. This is handled by
                // unblockStagesWithFullOutputBuffer method.
                addDependencyEdges(buildSubGraph.getUpstreamFragments(), ImmutableSet.of(currentFragmentId));
            }
            else {
                // start current fragment immediately since for partitioned join
                // build source data won't be able to fit into task output buffer.
                currentFragmentLazy = false;
            }

            return new FragmentSubGraph(
                    ImmutableSet.<PlanFragmentId>builder()
                            .addAll(probeSubGraph.getUpstreamFragments())
                            .addAll(buildSubGraph.getUpstreamFragments())
                            .build(),
                    // only probe source fragments can be considered lazy
                    // since build source stages should be started immediately
                    probeSubGraph.getLazyUpstreamFragments(),
                    currentFragmentLazy);
        }

        @Override
        public FragmentSubGraph visitAggregation(AggregationNode node, PlanFragmentId currentFragmentId)
        {
            FragmentSubGraph subGraph = node.getSource().accept(this, currentFragmentId);
            if (node.getStep() != FINAL && node.getStep() != SINGLE) {
                return subGraph;
            }

            // start current fragment immediately since final/single aggregation will fully
            // consume input before producing output data (aggregation shouldn't get blocked)
            return new FragmentSubGraph(
                    subGraph.getUpstreamFragments(),
                    ImmutableSet.of(),
                    false);
        }

        @Override
        public FragmentSubGraph visitRemoteSource(RemoteSourceNode node, PlanFragmentId currentFragmentId)
        {
            List<FragmentSubGraph> subGraphs = node.getSourceFragmentIds().stream()
                    .map(this::processFragment)
                    .collect(toImmutableList());
            node.getSourceFragmentIds()
                    .forEach(sourceFragmentId -> fragmentTopology.addEdge(sourceFragmentId, currentFragmentId));
            return new FragmentSubGraph(
                    subGraphs.stream()
                            .flatMap(source -> source.getUpstreamFragments().stream())
                            .collect(toImmutableSet()),
                    subGraphs.stream()
                            .flatMap(source -> source.getLazyUpstreamFragments().stream())
                            .collect(toImmutableSet()),
                    // initially current fragment is considered to be lazy unless there exist
                    // an operator that can fully consume input data without producing any output
                    // (e.g. final aggregation)
                    true);
        }

        @Override
        public FragmentSubGraph visitExchange(ExchangeNode node, PlanFragmentId currentFragmentId)
        {
            checkArgument(node.getScope() == LOCAL, "Only local exchanges are supported in the phased execution scheduler");
            return visitPlan(node, currentFragmentId);
        }

        @Override
        protected FragmentSubGraph visitPlan(PlanNode node, PlanFragmentId currentFragmentId)
        {
            List<FragmentSubGraph> sourceSubGraphs = node.getSources().stream()
                    .map(subPlanNode -> subPlanNode.accept(this, currentFragmentId))
                    .collect(toImmutableList());

            return new FragmentSubGraph(
                    sourceSubGraphs.stream()
                            .flatMap(source -> source.getUpstreamFragments().stream())
                            .collect(toImmutableSet()),
                    sourceSubGraphs.stream()
                            .flatMap(source -> source.getLazyUpstreamFragments().stream())
                            .collect(toImmutableSet()),
                    sourceSubGraphs.stream()
                            .allMatch(FragmentSubGraph::isCurrentFragmentLazy));
        }

        private void addDependencyEdges(Set<PlanFragmentId> sourceFragments, Set<PlanFragmentId> targetFragments)
        {
            for (PlanFragmentId targetFragment : targetFragments) {
                for (PlanFragmentId sourceFragment : sourceFragments) {
                    fragmentDependency.addEdge(sourceFragment, targetFragment);
                }
            }
        }
    }

    private static class FragmentSubGraph
    {
        /**
         * All upstream fragments (excluding currently visited fragment)
         */
        private final Set<PlanFragmentId> upstreamFragments;
        /**
         * All upstream lazy fragments (excluding currently visited fragment).
         * Lazy fragments don't have to be started immediately.
         */
        private final Set<PlanFragmentId> lazyUpstreamFragments;
        /**
         * Is currently visited fragment lazy?
         */
        private final boolean currentFragmentLazy;

        public FragmentSubGraph(
                Set<PlanFragmentId> upstreamFragments,
                Set<PlanFragmentId> lazyUpstreamFragments,
                boolean currentFragmentLazy)
        {
            this.upstreamFragments = requireNonNull(upstreamFragments, "upstreamFragments is null");
            this.lazyUpstreamFragments = requireNonNull(lazyUpstreamFragments, "lazyUpstreamFragments is null");
            this.currentFragmentLazy = currentFragmentLazy;
        }

        public Set<PlanFragmentId> getUpstreamFragments()
        {
            return upstreamFragments;
        }

        public Set<PlanFragmentId> getLazyUpstreamFragments()
        {
            return lazyUpstreamFragments;
        }

        public boolean isCurrentFragmentLazy()
        {
            return currentFragmentLazy;
        }
    }

    private static class FragmentsEdgeFactory
            implements EdgeFactory<PlanFragmentId, FragmentsEdge>
    {
        @Override
        public FragmentsEdge createEdge(PlanFragmentId sourceVertex, PlanFragmentId targetVertex)
        {
            return new FragmentsEdge(sourceVertex, targetVertex);
        }
    }

    @VisibleForTesting
    static class FragmentsEdge
    {
        private final PlanFragmentId source;
        private final PlanFragmentId target;

        public FragmentsEdge(PlanFragmentId source, PlanFragmentId target)
        {
            this.source = requireNonNull(source, "source is null");
            this.target = requireNonNull(target, "target is null");
        }

        public PlanFragmentId getSource()
        {
            return source;
        }

        public PlanFragmentId getTarget()
        {
            return target;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("source", source)
                    .add("target", target)
                    .toString();
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FragmentsEdge that = (FragmentsEdge) o;
            return source.equals(that.source) && target.equals(that.target);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(source, target);
        }
    }
}

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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.graph.EndpointPair;
import com.google.common.graph.Graph;
import com.google.common.util.concurrent.ListenableFuture;
import io.opentelemetry.api.trace.Span;
import io.trino.execution.DynamicFilterConfig;
import io.trino.execution.ExecutionFailureInfo;
import io.trino.execution.RemoteTask;
import io.trino.execution.StageId;
import io.trino.execution.StateMachine.StateChangeListener;
import io.trino.execution.TaskId;
import io.trino.execution.TaskStatus;
import io.trino.execution.scheduler.StageExecution;
import io.trino.execution.scheduler.TaskLifecycleListener;
import io.trino.metadata.InternalNode;
import io.trino.metadata.Split;
import io.trino.server.DynamicFilterService;
import io.trino.spi.QueryId;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.execution.scheduler.StageExecution.State.ABORTED;
import static io.trino.execution.scheduler.StageExecution.State.FINISHED;
import static io.trino.execution.scheduler.StageExecution.State.FLUSHING;
import static io.trino.execution.scheduler.StageExecution.State.SCHEDULED;
import static io.trino.execution.scheduler.policy.PlanUtils.createAggregationFragment;
import static io.trino.execution.scheduler.policy.PlanUtils.createBroadcastAndPartitionedJoinPlanFragment;
import static io.trino.execution.scheduler.policy.PlanUtils.createBroadcastJoinPlanFragment;
import static io.trino.execution.scheduler.policy.PlanUtils.createJoinPlanFragment;
import static io.trino.execution.scheduler.policy.PlanUtils.createTableScanPlanFragment;
import static io.trino.metadata.FunctionManager.createTestingFunctionManager;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static io.trino.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPhasedExecutionSchedule
{
    private final DynamicFilterService dynamicFilterService = new DynamicFilterService(
            createTestMetadataManager(),
            createTestingFunctionManager(),
            new TypeOperators(),
            new DynamicFilterConfig());

    @Test
    public void testPartitionedJoin()
    {
        PlanFragment buildFragment = createTableScanPlanFragment("build");
        PlanFragment probeFragment = createTableScanPlanFragment("probe");
        PlanFragment joinFragment = createJoinPlanFragment(INNER, PARTITIONED, "join", buildFragment, probeFragment);

        TestingStageExecution buildStage = new TestingStageExecution(buildFragment);
        TestingStageExecution probeStage = new TestingStageExecution(probeFragment);
        TestingStageExecution joinStage = new TestingStageExecution(joinFragment);

        PhasedExecutionSchedule schedule = PhasedExecutionSchedule.forStages(ImmutableSet.of(buildStage, probeStage, joinStage), dynamicFilterService);
        assertThat(schedule.getSortedFragments()).containsExactly(buildFragment.getId(), probeFragment.getId(), joinFragment.getId());

        // single dependency between build and probe stages
        Graph<PlanFragmentId> dependencies = schedule.getFragmentDependency();
        assertThat(dependencies.edges()).containsExactlyInAnyOrder(EndpointPair.ordered(buildFragment.getId(), probeFragment.getId()));

        // build and join stage should start immediately
        assertThat(getSchedulingFragments(schedule)).containsExactly(buildFragment.getId(), joinFragment.getId());

        // probe stage should start after build stage is completed
        ListenableFuture<Void> rescheduleFuture = schedule.getRescheduleFuture().orElseThrow();
        assertThat(rescheduleFuture).isNotDone();
        buildStage.setState(FLUSHING);
        assertThat(rescheduleFuture).isDone();
        schedule.schedule();
        assertThat(getSchedulingFragments(schedule)).containsExactly(joinFragment.getId(), probeFragment.getId());

        // make sure scheduler finishes
        rescheduleFuture = schedule.getRescheduleFuture().orElseThrow();
        assertThat(rescheduleFuture).isNotDone();
        probeStage.setState(FINISHED);
        assertThat(rescheduleFuture).isNotDone();
        joinStage.setState(FINISHED);
        schedule.schedule();
        assertThat(getSchedulingFragments(schedule)).isEmpty();
        assertThat(schedule.isFinished()).isTrue();
    }

    @Test
    public void testBroadcastSourceJoin()
    {
        PlanFragment buildFragment = createTableScanPlanFragment("build");
        PlanFragment joinSourceFragment = createBroadcastJoinPlanFragment("probe", buildFragment);

        TestingStageExecution buildStage = new TestingStageExecution(buildFragment);
        TestingStageExecution joinSourceStage = new TestingStageExecution(joinSourceFragment);

        PhasedExecutionSchedule schedule = PhasedExecutionSchedule.forStages(ImmutableSet.of(joinSourceStage, buildStage), dynamicFilterService);
        assertThat(schedule.getSortedFragments()).containsExactly(buildFragment.getId(), joinSourceFragment.getId());

        // single dependency between build and join stages
        Graph<PlanFragmentId> dependencies = schedule.getFragmentDependency();
        assertThat(dependencies.edges()).containsExactlyInAnyOrder(EndpointPair.ordered(buildFragment.getId(), joinSourceFragment.getId()));

        // build stage should start immediately
        assertThat(getSchedulingFragments(schedule)).containsExactly(buildFragment.getId());

        // join stage should start after build stage buffer is full
        buildStage.setAnyTaskBlocked(true);
        schedule.schedule();
        assertThat(getSchedulingFragments(schedule)).containsExactly(buildFragment.getId(), joinSourceFragment.getId());
    }

    @Test
    public void testAggregation()
    {
        PlanFragment sourceFragment = createTableScanPlanFragment("probe");
        PlanFragment aggregationFragment = createAggregationFragment("aggregation", sourceFragment);
        PlanFragment buildFragment = createTableScanPlanFragment("build");
        PlanFragment joinFragment = createJoinPlanFragment(INNER, REPLICATED, "join", buildFragment, aggregationFragment);

        TestingStageExecution sourceStage = new TestingStageExecution(sourceFragment);
        TestingStageExecution aggregationStage = new TestingStageExecution(aggregationFragment);
        TestingStageExecution buildStage = new TestingStageExecution(buildFragment);
        TestingStageExecution joinStage = new TestingStageExecution(joinFragment);

        PhasedExecutionSchedule schedule = PhasedExecutionSchedule.forStages(ImmutableSet.of(sourceStage, aggregationStage, buildStage, joinStage), dynamicFilterService);
        assertThat(schedule.getSortedFragments()).containsExactly(buildFragment.getId(), sourceFragment.getId(), aggregationFragment.getId(), joinFragment.getId());

        // aggregation and source stage should start immediately, join stage should wait for build stage to complete
        Graph<PlanFragmentId> dependencies = schedule.getFragmentDependency();
        assertThat(dependencies.edges()).containsExactly(EndpointPair.ordered(buildFragment.getId(), joinFragment.getId()));
        assertThat(getSchedulingFragments(schedule)).containsExactly(buildFragment.getId(), sourceFragment.getId(), aggregationFragment.getId());
    }

    @Test
    public void testDependentStageAbortedBeforeStarted()
    {
        PlanFragment sourceFragment = createTableScanPlanFragment("probe");
        PlanFragment aggregationFragment = createAggregationFragment("aggregation", sourceFragment);
        PlanFragment buildFragment = createTableScanPlanFragment("build");
        PlanFragment joinFragment = createJoinPlanFragment(INNER, REPLICATED, "join", buildFragment, aggregationFragment);

        TestingStageExecution sourceStage = new TestingStageExecution(sourceFragment);
        TestingStageExecution aggregationStage = new TestingStageExecution(aggregationFragment);
        TestingStageExecution buildStage = new TestingStageExecution(buildFragment);
        TestingStageExecution joinStage = new TestingStageExecution(joinFragment);

        PhasedExecutionSchedule schedule = PhasedExecutionSchedule.forStages(ImmutableSet.of(sourceStage, aggregationStage, buildStage, joinStage), dynamicFilterService);
        assertThat(schedule.getSortedFragments()).containsExactly(buildFragment.getId(), sourceFragment.getId(), aggregationFragment.getId(), joinFragment.getId());

        // aggregation and source stage should start immediately, join stage should wait for build stage to complete
        Graph<PlanFragmentId> dependencies = schedule.getFragmentDependency();
        assertThat(dependencies.edges()).containsExactly(EndpointPair.ordered(buildFragment.getId(), joinFragment.getId()));
        assertThat(getSchedulingFragments(schedule)).containsExactly(buildFragment.getId(), sourceFragment.getId(), aggregationFragment.getId());

        // abort non-scheduling join stage
        joinStage.setState(ABORTED);

        // dependencies finish
        buildStage.setState(FINISHED);
        aggregationStage.setState(FINISHED);
        sourceStage.setState(FINISHED);

        // join stage already aborted. Whole schedule should be marked as finished
        schedule.schedule();
        assertThat(schedule.isFinished()).isTrue();
    }

    @Test
    public void testStageWithBroadcastAndPartitionedJoin()
    {
        PlanFragment broadcastBuildFragment = createTableScanPlanFragment("broadcast_build");
        PlanFragment partitionedBuildFragment = createTableScanPlanFragment("partitioned_build");
        PlanFragment probeFragment = createTableScanPlanFragment("probe");
        PlanFragment joinFragment = createBroadcastAndPartitionedJoinPlanFragment("join", broadcastBuildFragment, partitionedBuildFragment, probeFragment);

        TestingStageExecution broadcastBuildStage = new TestingStageExecution(broadcastBuildFragment);
        TestingStageExecution partitionedBuildStage = new TestingStageExecution(partitionedBuildFragment);
        TestingStageExecution probeStage = new TestingStageExecution(probeFragment);
        TestingStageExecution joinStage = new TestingStageExecution(joinFragment);

        PhasedExecutionSchedule schedule = PhasedExecutionSchedule.forStages(ImmutableSet.of(
                broadcastBuildStage, partitionedBuildStage, probeStage, joinStage), dynamicFilterService);

        // join stage should start immediately because partitioned join forces that
        Graph<PlanFragmentId> dependencies = schedule.getFragmentDependency();
        assertThat(dependencies.edges()).containsExactlyInAnyOrder(
                EndpointPair.ordered(broadcastBuildFragment.getId(), probeFragment.getId()),
                EndpointPair.ordered(partitionedBuildFragment.getId(), probeFragment.getId()),
                EndpointPair.ordered(broadcastBuildFragment.getId(), joinFragment.getId()));
        assertThat(getSchedulingFragments(schedule)).containsExactly(partitionedBuildFragment.getId(), broadcastBuildFragment.getId(), joinFragment.getId());

        // completing single build dependency shouldn't cause probe stage to start
        broadcastBuildStage.setState(FLUSHING);
        schedule.schedule();
        assertThat(getSchedulingFragments(schedule)).containsExactly(partitionedBuildFragment.getId(), joinFragment.getId());

        // completing all build dependencies should cause probe stage to start
        partitionedBuildStage.setState(FLUSHING);
        schedule.schedule();
        assertThat(getSchedulingFragments(schedule)).containsExactly(joinFragment.getId(), probeFragment.getId());
    }

    @Test
    public void testSourceStageBroadcastJoinWithPartitionedJoinBuildSide()
    {
        PlanFragment nestedJoinBuildFragment = createTableScanPlanFragment("nested_join_build");
        PlanFragment nestedJoinProbeFragment = createTableScanPlanFragment("nested_join_probe");
        PlanFragment nestedJoinFragment = createJoinPlanFragment(INNER, "nested_join", nestedJoinBuildFragment, nestedJoinProbeFragment);
        PlanFragment joinSourceFragment = createBroadcastJoinPlanFragment("probe", nestedJoinFragment);

        TestingStageExecution nestedJoinBuildStage = new TestingStageExecution(nestedJoinBuildFragment);
        TestingStageExecution nestedJoinProbeStage = new TestingStageExecution(nestedJoinProbeFragment);
        TestingStageExecution nestedJoinStage = new TestingStageExecution(nestedJoinFragment);
        TestingStageExecution joinSourceStage = new TestingStageExecution(joinSourceFragment);

        PhasedExecutionSchedule schedule = PhasedExecutionSchedule.forStages(ImmutableSet.of(
                nestedJoinBuildStage, nestedJoinProbeStage, nestedJoinStage, joinSourceStage), dynamicFilterService);

        // nestedJoinStage and nestedJoinProbeStage should start immediately
        Graph<PlanFragmentId> dependencies = schedule.getFragmentDependency();
        assertThat(dependencies.edges()).containsExactlyInAnyOrder(
                EndpointPair.ordered(nestedJoinBuildFragment.getId(), joinSourceFragment.getId()),
                EndpointPair.ordered(nestedJoinFragment.getId(), joinSourceFragment.getId()),
                EndpointPair.ordered(nestedJoinBuildFragment.getId(), nestedJoinProbeFragment.getId()),
                EndpointPair.ordered(nestedJoinProbeFragment.getId(), joinSourceFragment.getId()));
        assertThat(getSchedulingFragments(schedule)).containsExactly(nestedJoinBuildFragment.getId(), nestedJoinFragment.getId());

        // Mark nestedJoinFragment and nestedJoinBuildFragment as scheduled.
        // joinSourceFragment still has dependency on nestedJoinProbeFragment
        nestedJoinStage.setState(SCHEDULED);
        nestedJoinBuildStage.setState(FINISHED);
        schedule.schedule();
        assertThat(getSchedulingFragments(schedule)).containsExactly(nestedJoinProbeFragment.getId());

        // mark nestedJoinFragment buffer as full, now joinSourceFragment is forced to be scheduled
        nestedJoinStage.setAnyTaskBlocked(true);
        schedule.schedule();
        assertThat(getSchedulingFragments(schedule)).containsExactly(nestedJoinProbeFragment.getId(), joinSourceFragment.getId());

        nestedJoinProbeStage.setState(FINISHED);
        schedule.schedule();
        assertThat(getSchedulingFragments(schedule)).containsExactly(joinSourceFragment.getId());
    }

    private Set<PlanFragmentId> getSchedulingFragments(PhasedExecutionSchedule schedule)
    {
        return schedule.getSchedulingStages().stream()
                .map(stage -> stage.getFragment().getId())
                .collect(toImmutableSet());
    }

    private static class TestingStageExecution
            implements StageExecution
    {
        private final PlanFragment fragment;
        private StateChangeListener<State> stateChangeListener;
        private boolean anyTaskBlocked;
        private State state = State.SCHEDULING;

        public TestingStageExecution(PlanFragment fragment)
        {
            this.fragment = requireNonNull(fragment, "fragment is null");
        }

        @Override
        public PlanFragment getFragment()
        {
            return fragment;
        }

        @Override
        public boolean isAnyTaskBlocked()
        {
            return anyTaskBlocked;
        }

        public void setAnyTaskBlocked(boolean anyTaskBlocked)
        {
            this.anyTaskBlocked = anyTaskBlocked;
        }

        public void setState(State state)
        {
            this.state = state;
            if (stateChangeListener != null) {
                stateChangeListener.stateChanged(state);
            }
        }

        @Override
        public State getState()
        {
            return state;
        }

        @Override
        public void addStateChangeListener(StateChangeListener<State> stateChangeListener)
        {
            this.stateChangeListener = requireNonNull(stateChangeListener, "stateChangeListener is null");
        }

        @Override
        public StageId getStageId()
        {
            return new StageId(new QueryId("id"), 0);
        }

        @Override
        public int getAttemptId()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Span getStageSpan()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void beginScheduling()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void transitionToSchedulingSplits()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public TaskLifecycleListener getTaskLifecycleListener()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void schedulingComplete()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void schedulingComplete(PlanNodeId partitionedSource)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void cancel()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void abort()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void recordGetSplitTime(long start)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<RemoteTask> scheduleTask(InternalNode node, int partition, Multimap<PlanNodeId, Split> initialSplits)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void failTask(TaskId taskId, Throwable failureCause)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<RemoteTask> getAllTasks()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<TaskStatus> getTaskStatuses()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<ExecutionFailureInfo> getFailureCause()
        {
            throw new UnsupportedOperationException();
        }
    }
}

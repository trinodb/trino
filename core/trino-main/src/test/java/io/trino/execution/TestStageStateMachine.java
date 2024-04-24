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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.Span;
import io.trino.cost.StatsAndCosts;
import io.trino.execution.scheduler.SplitSchedulerStats;
import io.trino.operator.PipelineContext;
import io.trino.operator.TaskStats;
import io.trino.operator.TestingOperatorContext;
import io.trino.spi.QueryId;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Row;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ValuesNode;
import org.joda.time.DateTime;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.tracing.Tracing.noopTracer;
import static io.airlift.units.DataSize.succinctBytes;
import static io.airlift.units.Duration.succinctDuration;
import static io.trino.execution.StageState.PLANNED;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestStageStateMachine
{
    private static final StageId STAGE_ID = new StageId("query", 0);
    private static final PlanFragment PLAN_FRAGMENT = createValuesPlan();
    private static final SQLException FAILED_CAUSE;

    static {
        FAILED_CAUSE = new SQLException("FAILED");
        FAILED_CAUSE.setStackTrace(new StackTraceElement[0]);
    }

    private ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));

    @AfterAll
    public void tearDown()
    {
        executor.shutdownNow();
        executor = null;
    }

    @Test
    public void testBasicStateChanges()
    {
        StageStateMachine stateMachine = createStageStateMachine();
        assertState(stateMachine, PLANNED);

        assertThat(stateMachine.transitionToScheduling()).isTrue();
        assertState(stateMachine, StageState.SCHEDULING);

        assertThat(stateMachine.transitionToRunning()).isTrue();
        assertState(stateMachine, StageState.RUNNING);

        assertThat(stateMachine.transitionToPending()).isTrue();
        assertState(stateMachine, StageState.PENDING);

        assertThat(stateMachine.transitionToRunning()).isTrue();
        assertState(stateMachine, StageState.RUNNING);

        assertThat(stateMachine.transitionToFinished()).isTrue();
        assertState(stateMachine, StageState.FINISHED);
    }

    @Test
    public void testPlanned()
    {
        StageStateMachine stateMachine = createStageStateMachine();
        assertState(stateMachine, PLANNED);

        stateMachine = createStageStateMachine();
        assertThat(stateMachine.transitionToScheduling()).isTrue();
        assertState(stateMachine, StageState.SCHEDULING);

        stateMachine = createStageStateMachine();
        assertThat(stateMachine.transitionToRunning()).isTrue();
        assertState(stateMachine, StageState.RUNNING);

        stateMachine = createStageStateMachine();
        assertThat(stateMachine.transitionToFinished()).isTrue();
        assertState(stateMachine, StageState.FINISHED);

        stateMachine = createStageStateMachine();
        assertThat(stateMachine.transitionToFailed(FAILED_CAUSE)).isTrue();
        assertState(stateMachine, StageState.FAILED);
    }

    @Test
    public void testScheduling()
    {
        StageStateMachine stateMachine = createStageStateMachine();
        assertThat(stateMachine.transitionToScheduling()).isTrue();
        assertState(stateMachine, StageState.SCHEDULING);

        assertThat(stateMachine.transitionToScheduling()).isFalse();
        assertState(stateMachine, StageState.SCHEDULING);

        stateMachine = createStageStateMachine();
        stateMachine.transitionToScheduling();
        assertThat(stateMachine.transitionToRunning()).isTrue();
        assertState(stateMachine, StageState.RUNNING);

        stateMachine = createStageStateMachine();
        stateMachine.transitionToScheduling();
        assertThat(stateMachine.transitionToFinished()).isTrue();
        assertState(stateMachine, StageState.FINISHED);

        stateMachine = createStageStateMachine();
        stateMachine.transitionToScheduling();
        assertThat(stateMachine.transitionToFailed(FAILED_CAUSE)).isTrue();
        assertState(stateMachine, StageState.FAILED);
    }

    @Test
    public void testRunning()
    {
        StageStateMachine stateMachine = createStageStateMachine();
        assertThat(stateMachine.transitionToRunning()).isTrue();
        assertState(stateMachine, StageState.RUNNING);

        assertThat(stateMachine.transitionToScheduling()).isFalse();
        assertState(stateMachine, StageState.RUNNING);

        assertThat(stateMachine.transitionToRunning()).isFalse();
        assertState(stateMachine, StageState.RUNNING);

        assertThat(stateMachine.transitionToPending()).isTrue();
        assertState(stateMachine, StageState.PENDING);

        assertThat(stateMachine.transitionToRunning()).isTrue();
        assertState(stateMachine, StageState.RUNNING);

        stateMachine = createStageStateMachine();
        stateMachine.transitionToRunning();
        assertThat(stateMachine.transitionToFinished()).isTrue();
        assertState(stateMachine, StageState.FINISHED);

        stateMachine = createStageStateMachine();
        stateMachine.transitionToRunning();
        assertThat(stateMachine.transitionToFailed(FAILED_CAUSE)).isTrue();
        assertState(stateMachine, StageState.FAILED);
    }

    @Test
    public void testFinished()
    {
        StageStateMachine stateMachine = createStageStateMachine();

        assertThat(stateMachine.transitionToFinished()).isTrue();
        assertFinalState(stateMachine, StageState.FINISHED);
    }

    @Test
    public void testFailed()
    {
        StageStateMachine stateMachine = createStageStateMachine();

        assertThat(stateMachine.transitionToFailed(FAILED_CAUSE)).isTrue();
        assertFinalState(stateMachine, StageState.FAILED);
    }

    @Test
    public void testGetBasicStageInfo()
    {
        ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1);
        StageStateMachine stateMachine = createStageStateMachine();
        StageId stageId = new StageId(new QueryId("0"), 0);
        PipelineContext pipeline0Context = TestingOperatorContext.createDriverContext(executorService).getPipelineContext();
        PipelineContext pipeline1Context = TestingOperatorContext.createDriverContext(executorService).getPipelineContext();
        int baseValue = 1;

        TaskInfo task0 = TaskInfo.createInitialTask(
                new TaskId(stageId, 0, 0),
                URI.create(""),
                "0",
                false,
                Optional.empty(),
                taskStats(ImmutableList.of(pipeline0Context, pipeline1Context), baseValue));
        TaskInfo task1 = task0.withTaskStatus(TaskStatus.failWith(task0.getTaskStatus(), TaskState.FAILED, ImmutableList.of()));
        List<TaskInfo> taskInfos = ImmutableList.of(task0, task1);
        int expectedStatsValue = baseValue * taskInfos.size();

        BasicStageInfo stageInfo = stateMachine.getBasicStageInfo(() -> taskInfos);
        assertThat(stageInfo.getStageId()).isEqualTo(STAGE_ID);
        assertThat(stageInfo.getState()).isEqualTo(PLANNED);
        assertThat(stageInfo.isCoordinatorOnly()).isFalse();
        assertThat(stageInfo.getSubStages()).isEmpty();
        assertThat(stageInfo.getTasks().size()).isEqualTo(taskInfos.size());

        BasicStageStats stats = stageInfo.getStageStats();

        assertThat(stats.isScheduled()).isFalse();
        assertThat(stats.getFailedTasks()).isEqualTo(1);
        assertThat(stats.getTotalDrivers()).isEqualTo(expectedStatsValue);
        assertThat(stats.getQueuedDrivers()).isEqualTo(expectedStatsValue);
        assertThat(stats.getRunningDrivers()).isEqualTo(expectedStatsValue);
        assertThat(stats.getCompletedDrivers()).isEqualTo(expectedStatsValue);
        assertThat(stats.getBlockedDrivers()).isEqualTo(expectedStatsValue);
        assertThat(stats.getPhysicalInputDataSize()).isEqualTo(succinctBytes(expectedStatsValue));
        assertThat(stats.getPhysicalWrittenDataSize()).isEqualTo(succinctBytes(expectedStatsValue));
        assertThat(stats.getInternalNetworkInputDataSize()).isEqualTo(succinctBytes(expectedStatsValue));
        assertThat(stats.getInternalNetworkInputPositions()).isEqualTo(expectedStatsValue);
        assertThat(stats.getPhysicalInputPositions()).isEqualTo(expectedStatsValue);
        assertThat(stats.getPhysicalInputReadTime()).isEqualTo(succinctDuration(expectedStatsValue, MILLISECONDS));
        assertThat(stats.getPhysicalInputPositions()).isEqualTo(expectedStatsValue);
        assertThat(stats.getRawInputDataSize()).isEqualTo(succinctBytes(0));
        assertThat(stats.getRawInputPositions()).isEqualTo(0);
        assertThat(stats.getCumulativeUserMemory()).isEqualTo(expectedStatsValue);
        assertThat(stats.getFailedCumulativeUserMemory()).isEqualTo(1);
        assertThat(stats.getTotalMemoryReservation()).isEqualTo(succinctBytes(expectedStatsValue * 2L));
        assertThat(stats.getUserMemoryReservation()).isEqualTo(succinctBytes(expectedStatsValue));
        assertThat(stats.isFullyBlocked()).isFalse();
        assertThat(stats.getBlockedReasons()).isEmpty();
        assertThat(stats.getTotalCpuTime()).isEqualTo(succinctDuration(expectedStatsValue, MILLISECONDS));
        assertThat(stats.getTotalScheduledTime()).isEqualTo(succinctDuration(expectedStatsValue, MILLISECONDS));
        assertThat(stats.getFailedCpuTime()).isEqualTo(succinctDuration(1, MILLISECONDS));
        assertThat(stats.getFailedScheduledTime()).isEqualTo(succinctDuration(1, MILLISECONDS));
        assertThat(stats.getRunningPercentage()).isEmpty();
        assertThat(stats.getProgressPercentage()).isEmpty();
        assertThat(stats.getSpilledDataSize()).isEqualTo(succinctBytes(0));
    }

    private static TaskStats taskStats(List<PipelineContext> pipelineContexts)
    {
        return taskStats(pipelineContexts, 0);
    }

    private static TaskStats taskStats(List<PipelineContext> pipelineContexts, int baseValue)
    {
        return new TaskStats(DateTime.now(),
                null,
                null,
                null,
                null,
                null,
                new Duration(baseValue, MILLISECONDS),
                new Duration(baseValue, MILLISECONDS),
                baseValue,
                baseValue,
                baseValue,
                baseValue,
                baseValue,
                baseValue,
                baseValue,
                baseValue,
                baseValue,
                baseValue,
                DataSize.ofBytes(baseValue),
                DataSize.ofBytes(baseValue),
                DataSize.ofBytes(baseValue),
                new Duration(baseValue, MILLISECONDS),
                new Duration(baseValue, MILLISECONDS),
                new Duration(baseValue, MILLISECONDS),
                false,
                ImmutableSet.of(),
                DataSize.ofBytes(baseValue),
                baseValue,
                new Duration(baseValue, MILLISECONDS),
                DataSize.ofBytes(baseValue),
                baseValue,
                DataSize.ofBytes(baseValue),
                baseValue,
                DataSize.ofBytes(baseValue),
                baseValue,
                new Duration(baseValue, MILLISECONDS),
                DataSize.ofBytes(baseValue),
                baseValue,
                new Duration(baseValue, MILLISECONDS),
                DataSize.ofBytes(baseValue),
                DataSize.ofBytes(baseValue),
                Optional.empty(),
                baseValue,
                new Duration(baseValue, MILLISECONDS),
                pipelineContexts.stream().map(PipelineContext::getPipelineStats).collect(toImmutableList()));
    }

    private static void assertFinalState(StageStateMachine stateMachine, StageState expectedState)
    {
        assertThat(expectedState.isDone()).isTrue();

        assertState(stateMachine, expectedState);

        assertThat(stateMachine.transitionToScheduling()).isFalse();
        assertState(stateMachine, expectedState);

        assertThat(stateMachine.transitionToPending()).isFalse();
        assertState(stateMachine, expectedState);

        assertThat(stateMachine.transitionToRunning()).isFalse();
        assertState(stateMachine, expectedState);

        assertThat(stateMachine.transitionToFinished()).isFalse();
        assertState(stateMachine, expectedState);

        assertThat(stateMachine.transitionToFailed(FAILED_CAUSE)).isFalse();
        assertState(stateMachine, expectedState);

        // attempt to fail with another exception, which will fail
        assertThat(stateMachine.transitionToFailed(new IOException("failure after finish"))).isFalse();
        assertState(stateMachine, expectedState);
    }

    private static void assertState(StageStateMachine stateMachine, StageState expectedState)
    {
        assertThat(stateMachine.getStageId()).isEqualTo(STAGE_ID);

        StageInfo stageInfo = stateMachine.getStageInfo(ImmutableList::of);
        assertThat(stageInfo.getStageId()).isEqualTo(STAGE_ID);
        assertThat(stageInfo.getSubStages()).isEqualTo(ImmutableList.of());
        assertThat(stageInfo.getTasks()).isEqualTo(ImmutableList.of());
        assertThat(stageInfo.getTypes()).isEqualTo(ImmutableList.of(VARCHAR));
        assertThat(stageInfo.getPlan()).isSameAs(PLAN_FRAGMENT);

        assertThat(stateMachine.getState()).isEqualTo(expectedState);
        assertThat(stageInfo.getState()).isEqualTo(expectedState);

        if (expectedState == StageState.FAILED) {
            ExecutionFailureInfo failure = stageInfo.getFailureCause();
            assertThat(failure.getMessage()).isEqualTo(FAILED_CAUSE.getMessage());
            assertThat(failure.getType()).isEqualTo(FAILED_CAUSE.getClass().getName());
        }
        else {
            assertThat(stageInfo.getFailureCause()).isNull();
        }
    }

    private StageStateMachine createStageStateMachine()
    {
        return new StageStateMachine(
                STAGE_ID,
                PLAN_FRAGMENT,
                ImmutableMap.of(),
                executor,
                noopTracer(),
                Span.getInvalid(),
                new SplitSchedulerStats());
    }

    private static PlanFragment createValuesPlan()
    {
        Symbol symbol = new Symbol(VARCHAR, "column");
        PlanNodeId valuesNodeId = new PlanNodeId("plan");
        PlanFragment planFragment = new PlanFragment(
                new PlanFragmentId("plan"),
                new ValuesNode(valuesNodeId,
                        ImmutableList.of(symbol),
                        ImmutableList.of(new Row(ImmutableList.of(new Constant(VARCHAR, Slices.utf8Slice("foo")))))),
                ImmutableSet.of(symbol),
                SOURCE_DISTRIBUTION,
                Optional.empty(),
                ImmutableList.of(valuesNodeId),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(symbol)),
                StatsAndCosts.empty(),
                ImmutableList.of(),
                ImmutableMap.of(),
                Optional.empty());

        return planFragment;
    }
}

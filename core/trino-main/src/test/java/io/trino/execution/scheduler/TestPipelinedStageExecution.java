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
package io.trino.execution.scheduler;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.Span;
import io.trino.execution.MockRemoteTaskFactory;
import io.trino.execution.MockRemoteTaskFactory.MockRemoteTask;
import io.trino.execution.NodeTaskMap;
import io.trino.execution.SqlStage;
import io.trino.execution.StageId;
import io.trino.execution.StageInfo;
import io.trino.execution.TaskInfo;
import io.trino.execution.TaskState;
import io.trino.execution.TaskStatus;
import io.trino.node.InternalNode;
import io.trino.node.TestingInternalNodeManager;
import io.trino.spi.NodeVersion;
import io.trino.spi.QueryId;
import io.trino.sql.planner.PlanFragment;
import io.trino.util.FinalizerService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.function.Consumer;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.tracing.Tracing.noopTracer;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.execution.SqlStage.createSqlStage;
import static io.trino.execution.TaskState.ABORTED;
import static io.trino.execution.TaskState.CANCELED;
import static io.trino.execution.TaskState.FINISHED;
import static io.trino.execution.TaskState.RUNNING;
import static io.trino.execution.TaskTestUtils.PLAN_FRAGMENT;
import static io.trino.execution.TaskTestUtils.SPLIT;
import static io.trino.execution.TaskTestUtils.TABLE_SCAN_NODE_ID;
import static io.trino.execution.scheduler.PipelinedStageExecution.createPipelinedStageExecution;
import static io.trino.metadata.AbstractMockMetadata.dummyMetadata;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestPipelinedStageExecution
{
    private static final Duration FINAL_TASK_INFO_TIMEOUT = new Duration(100, MILLISECONDS);
    // the stage sets the bucket to partition mapping itself
    private static final PlanFragment FRAGMENT = PLAN_FRAGMENT.withOutputPartitioning(Optional.empty(), OptionalInt.empty());
    private static final QueryId QUERY_ID = new QueryId("query");

    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;

    @BeforeAll
    void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduled-%s"));
    }

    @AfterAll
    void tearDown()
    {
        executor.shutdownNow();
        executor = null;
        scheduledExecutor.shutdownNow();
        scheduledExecutor = null;
    }

    @Test
    void testCanceledTaskWithoutFinalTaskInfoIsFinalizedLocally()
            throws Exception
    {
        try (RecordingScheduledExecutor checkExecutor = new RecordingScheduledExecutor()) {
            StageHarness stage = createStage(checkExecutor);
            MockRemoteTask task = stage.scheduleTask(0);
            task.withholdFinalTaskInfo();

            stage.execution().cancel();
            stage.sqlStage().finish();
            // a mock task completes its cancel once its splits are gone
            task.clearSplits();

            StageInfo stageInfo = stage.finalStageInfo().get(10, SECONDS);
            assertThat(taskStates(stageInfo)).containsExactly(CANCELED);
            assertThat(task.forcedFinalizations()).isEqualTo(1);
        }
    }

    @Test
    void testFinishedTaskWithoutFinalTaskInfoIsFinalizedLocally()
            throws Exception
    {
        try (RecordingScheduledExecutor checkExecutor = new RecordingScheduledExecutor()) {
            StageHarness stage = createStage(checkExecutor);
            MockRemoteTask task = stage.scheduleTask(0);
            task.withholdFinalTaskInfo();

            task.noMoreSplits(TABLE_SCAN_NODE_ID);
            stage.sqlStage().finish();

            StageInfo stageInfo = stage.finalStageInfo().get(10, SECONDS);
            assertThat(taskStates(stageInfo)).containsExactly(FINISHED);
            assertThat(task.forcedFinalizations()).isEqualTo(1);
        }
    }

    @Test
    void testTaskWithFinalTaskInfoIsNotFinalizedLocally()
            throws Exception
    {
        try (RecordingScheduledExecutor checkExecutor = new RecordingScheduledExecutor()) {
            StageHarness stage = createStage(checkExecutor);
            MockRemoteTask task = stage.scheduleTask(0);

            stage.execution().cancel();
            stage.sqlStage().finish();
            task.clearSplits();

            StageInfo stageInfo = stage.finalStageInfo().get(10, SECONDS);
            assertThat(taskStates(stageInfo)).containsExactly(CANCELED);

            // the final task info arrived before the check ran
            assertEventually(() -> assertThat(checkExecutor.delayedTasks()).hasSize(1));
            checkExecutor.delayedTasks().get(0).get(10, SECONDS);
            assertThat(task.forcedFinalizations()).isEqualTo(0);
        }
    }

    @Test
    void testOnlyTasksWithoutFinalTaskInfoAreFinalizedLocally()
            throws Exception
    {
        try (RecordingScheduledExecutor checkExecutor = new RecordingScheduledExecutor()) {
            StageHarness stage = createStage(checkExecutor);
            MockRemoteTask first = stage.scheduleTask(0);
            MockRemoteTask withheld = stage.scheduleTask(1);
            MockRemoteTask last = stage.scheduleTask(2);
            withheld.withholdFinalTaskInfo();

            stage.execution().cancel();
            stage.sqlStage().finish();
            first.clearSplits();
            withheld.clearSplits();
            last.clearSplits();

            StageInfo stageInfo = stage.finalStageInfo().get(10, SECONDS);
            assertThat(taskStates(stageInfo)).containsExactly(CANCELED, CANCELED, CANCELED);
            assertThat(first.forcedFinalizations()).isEqualTo(0);
            assertThat(withheld.forcedFinalizations()).isEqualTo(1);
            assertThat(last.forcedFinalizations()).isEqualTo(0);
        }
    }

    @Test
    void testCancelContinuesPastTaskWhoseCancelThrows()
            throws Exception
    {
        testTerminationContinuesPastFailingTask(PipelinedStageExecution::cancel, CANCELED);
    }

    @Test
    void testAbortContinuesPastTaskWhoseAbortThrows()
            throws Exception
    {
        testTerminationContinuesPastFailingTask(PipelinedStageExecution::abort, ABORTED);
    }

    @Test
    void testFailContinuesPastTaskWhoseAbortThrows()
            throws Exception
    {
        testTerminationContinuesPastFailingTask(execution -> execution.fail(new RuntimeException("stage failed")), ABORTED);
    }

    private void testTerminationContinuesPastFailingTask(Consumer<PipelinedStageExecution> terminate, TaskState expectedState)
            throws Exception
    {
        try (RecordingScheduledExecutor checkExecutor = new RecordingScheduledExecutor()) {
            StageHarness stage = createStage(checkExecutor);
            MockRemoteTask first = stage.scheduleTask(0);
            MockRemoteTask failing = stage.scheduleTask(1);
            MockRemoteTask last = stage.scheduleTask(2);
            failing.failTerminationWith(new RuntimeException("termination failed"));

            // one task's termination throws
            terminate.accept(stage.execution());

            // the other tasks are done once their final task info checks are scheduled, which happens before the executor closes
            assertEventually(() -> assertThat(checkExecutor.delayedTasks()).hasSize(2));
            assertThat(taskState(first)).isEqualTo(expectedState);
            assertThat(taskState(last)).isEqualTo(expectedState);
            assertThat(taskState(failing)).isEqualTo(RUNNING);
        }
    }

    private StageHarness createStage(ScheduledExecutorService checkExecutor)
    {
        SqlStage sqlStage = createSqlStage(
                dummyMetadata(),
                new StageId(QUERY_ID, 0),
                FRAGMENT,
                ImmutableMap.of(),
                new MockRemoteTaskFactory(executor, scheduledExecutor),
                TEST_SESSION,
                true,
                new NodeTaskMap(new FinalizerService()),
                executor,
                noopTracer(),
                Span.getInvalid(),
                new SplitSchedulerStats(),
                (_, _) -> OptionalInt.empty());
        SettableFuture<StageInfo> finalStageInfo = SettableFuture.create();
        sqlStage.addFinalStageInfoListener(finalStageInfo::set);
        PipelinedStageExecution execution = createPipelinedStageExecution(
                sqlStage,
                ImmutableMap.of(FRAGMENT.getId(), new PartitionedPipelinedOutputBufferManager(FIXED_HASH_DISTRIBUTION, 1)),
                TaskLifecycleListener.NO_OP,
                TestingInternalNodeManager.createDefault(),
                checkExecutor,
                FINAL_TASK_INFO_TIMEOUT,
                Optional.of(new int[] {0}),
                OptionalInt.empty(),
                0);
        return new StageHarness(sqlStage, execution, finalStageInfo);
    }

    private static List<TaskState> taskStates(StageInfo stageInfo)
    {
        return stageInfo.tasks().stream()
                .map(TaskInfo::taskStatus)
                .map(TaskStatus::state)
                .collect(toImmutableList());
    }

    private static TaskState taskState(MockRemoteTask task)
    {
        return task.getTaskStatus().state();
    }

    private record StageHarness(SqlStage sqlStage, PipelinedStageExecution execution, SettableFuture<StageInfo> finalStageInfo)
    {
        MockRemoteTask scheduleTask(int partition)
        {
            InternalNode node = new InternalNode("node" + partition, URI.create("http://node/" + partition), NodeVersion.UNKNOWN, false);
            return (MockRemoteTask) execution.scheduleTask(node, partition, ImmutableListMultimap.of(TABLE_SCAN_NODE_ID, SPLIT.split())).orElseThrow();
        }
    }

    /**
     * The executor the stage execution schedules its final task info checks on; records the delayed tasks
     * so a test can wait for a check to run.
     */
    private static class RecordingScheduledExecutor
            extends ScheduledThreadPoolExecutor
    {
        private final List<RunnableScheduledFuture<?>> delayedTasks = new CopyOnWriteArrayList<>();

        public RecordingScheduledExecutor()
        {
            super(2, daemonThreadsNamed("stage-execution-check-%s"));
            setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        }

        public List<RunnableScheduledFuture<?>> delayedTasks()
        {
            return delayedTasks;
        }

        @Override
        protected <V> RunnableScheduledFuture<V> decorateTask(Runnable runnable, RunnableScheduledFuture<V> task)
        {
            if (task.getDelay(NANOSECONDS) > 0) {
                delayedTasks.add(task);
            }
            return task;
        }
    }
}

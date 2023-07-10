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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.Span;
import io.trino.client.NodeVersion;
import io.trino.cost.StatsAndCosts;
import io.trino.execution.ExecutionFailureInfo;
import io.trino.execution.NodeTaskMap;
import io.trino.execution.RemoteTask;
import io.trino.execution.StageId;
import io.trino.execution.StateMachine;
import io.trino.execution.TaskId;
import io.trino.execution.TaskState;
import io.trino.execution.TaskStatus;
import io.trino.execution.buffer.OutputBufferStatus;
import io.trino.metadata.InMemoryNodeManager;
import io.trino.metadata.InternalNode;
import io.trino.metadata.Split;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.TestingMetadata;
import io.trino.util.FinalizerService;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicReference;

import static io.airlift.concurrent.Threads.threadsNamed;
import static io.trino.execution.TestingRemoteTaskFactory.TestingRemoteTask;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static io.trino.testing.TestingHandles.TEST_TABLE_HANDLE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;

public class TestScaledWriterScheduler
{
    private static final PlanNodeId TABLE_SCAN_NODE_ID = new PlanNodeId("plan_id");
    private static final InternalNode NODE_1 = new InternalNode("node-1", URI.create("http://127.0.0.1:11"), new NodeVersion("version-1"), false);
    private static final InternalNode NODE_2 = new InternalNode("node-2", URI.create("http://127.0.0.1:12"), new NodeVersion("version-1"), false);
    private static final InternalNode NODE_3 = new InternalNode("node-3", URI.create("http://127.0.0.1:13"), new NodeVersion("version-1"), false);

    @Test
    public void testGetNewTaskCountWithUnderutilizedTasksWithoutSkewness()
    {
        TaskStatus taskStatus1 = buildTaskStatus(true, 12345L);
        TaskStatus taskStatus2 = buildTaskStatus(false, 12345L);
        TaskStatus taskStatus3 = buildTaskStatus(false, 12345L);

        ScaledWriterScheduler scaledWriterScheduler = buildScaleWriterSchedulerWithInitialTasks(taskStatus1, taskStatus2, taskStatus3);
        assertEquals(scaledWriterScheduler.schedule().getNewTasks().size(), 0);
    }

    @Test
    public void testGetNewTaskCountWithOverutilizedTasksWithoutSkewness()
    {
        TaskStatus taskStatus1 = buildTaskStatus(true, 12345L);
        TaskStatus taskStatus2 = buildTaskStatus(true, 12345L);
        TaskStatus taskStatus3 = buildTaskStatus(false, 12345L);

        ScaledWriterScheduler scaledWriterScheduler = buildScaleWriterSchedulerWithInitialTasks(taskStatus1, taskStatus2, taskStatus3);
        assertEquals(scaledWriterScheduler.schedule().getNewTasks().size(), 1);
    }

    @Test
    public void testGetNewTaskCountWithOverutilizedSkewedTaskAndUnderutilizedNonSkewedTasks()
    {
        TaskStatus taskStatus1 = buildTaskStatus(true, 1234567L);
        TaskStatus taskStatus2 = buildTaskStatus(false, 12345L);
        TaskStatus taskStatus3 = buildTaskStatus(false, 123456L);

        ScaledWriterScheduler scaledWriterScheduler = buildScaleWriterSchedulerWithInitialTasks(taskStatus1, taskStatus2, taskStatus3);
        assertEquals(scaledWriterScheduler.schedule().getNewTasks().size(), 1);
    }

    @Test
    public void testGetNewTaskCountWithUnderutilizedSkewedTaskAndOverutilizedNonSkewedTasks()
    {
        TaskStatus taskStatus1 = buildTaskStatus(true, 12345L);
        TaskStatus taskStatus2 = buildTaskStatus(true, 123456L);
        TaskStatus taskStatus3 = buildTaskStatus(false, 1234567L);

        ScaledWriterScheduler scaledWriterScheduler = buildScaleWriterSchedulerWithInitialTasks(taskStatus1, taskStatus2, taskStatus3);
        assertEquals(scaledWriterScheduler.schedule().getNewTasks().size(), 1);
    }

    @Test
    public void testGetNewTaskCountWhenWriterDataProcessedIsGreaterThanMinForScaleUp()
    {
        TaskStatus taskStatus1 = buildTaskStatus(1, DataSize.of(32, DataSize.Unit.MEGABYTE));
        TaskStatus taskStatus2 = buildTaskStatus(1, DataSize.of(32, DataSize.Unit.MEGABYTE));
        TaskStatus taskStatus3 = buildTaskStatus(2, DataSize.of(64, DataSize.Unit.MEGABYTE));

        ScaledWriterScheduler scaledWriterScheduler = buildScaleWriterSchedulerWithInitialTasks(taskStatus1, taskStatus2, taskStatus3);
        // Scale up will happen
        assertEquals(scaledWriterScheduler.schedule().getNewTasks().size(), 1);
    }

    @Test
    public void testGetNewTaskCountWhenWriterDataProcessedIsLessThanMinForScaleUp()
    {
        TaskStatus taskStatus1 = buildTaskStatus(1, DataSize.of(32, DataSize.Unit.MEGABYTE));
        TaskStatus taskStatus2 = buildTaskStatus(1, DataSize.of(32, DataSize.Unit.MEGABYTE));
        TaskStatus taskStatus3 = buildTaskStatus(2, DataSize.of(32, DataSize.Unit.MEGABYTE));

        ScaledWriterScheduler scaledWriterScheduler = buildScaleWriterSchedulerWithInitialTasks(taskStatus1, taskStatus2, taskStatus3);
        // Scale up will not happen because for one of the task there are two local writers which makes the
        // minWrittenBytes for scaling up to (2 * writerScalingMinDataProcessed) that is greater than writerInputDataSize.
        assertEquals(scaledWriterScheduler.schedule().getNewTasks().size(), 0);
    }

    @Test
    public void testGetNewTaskCountWhenExistingWriterTaskMaxWriterCountIsEmpty()
    {
        TaskStatus taskStatus1 = buildTaskStatus(1, DataSize.of(32, DataSize.Unit.MEGABYTE));
        TaskStatus taskStatus2 = buildTaskStatus(2, DataSize.of(100, DataSize.Unit.MEGABYTE));
        TaskStatus taskStatus3 = buildTaskStatus(true, 12345L, Optional.empty(), DataSize.of(0, DataSize.Unit.MEGABYTE));

        ScaledWriterScheduler scaledWriterScheduler = buildScaleWriterSchedulerWithInitialTasks(taskStatus1, taskStatus2, taskStatus3);
        // Scale up will not happen because one of the existing writer task isn't initialized yet with maxWriterCount.
        assertEquals(scaledWriterScheduler.schedule().getNewTasks().size(), 0);
    }

    @Test
    public void testNewTaskCountWhenNodesUpperLimitIsNotExceeded()
    {
        TaskStatus taskStatus = buildTaskStatus(true, 123456L);
        AtomicReference<List<TaskStatus>> taskStatusProvider = new AtomicReference<>(ImmutableList.of(taskStatus));
        ScaledWriterScheduler scaledWriterScheduler = buildScaledWriterScheduler(taskStatusProvider, 2);

        scaledWriterScheduler.schedule();
        assertEquals(scaledWriterScheduler.schedule().getNewTasks().size(), 1);
    }

    @Test
    public void testNewTaskCountWhenNodesUpperLimitIsExceeded()
    {
        TaskStatus taskStatus = buildTaskStatus(true, 123456L);
        AtomicReference<List<TaskStatus>> taskStatusProvider = new AtomicReference<>(ImmutableList.of(taskStatus));
        ScaledWriterScheduler scaledWriterScheduler = buildScaledWriterScheduler(taskStatusProvider, 1);

        scaledWriterScheduler.schedule();
        assertEquals(scaledWriterScheduler.schedule().getNewTasks().size(), 0);
    }

    private ScaledWriterScheduler buildScaleWriterSchedulerWithInitialTasks(TaskStatus taskStatus1, TaskStatus taskStatus2, TaskStatus taskStatus3)
    {
        AtomicReference<List<TaskStatus>> taskStatusProvider = new AtomicReference<>(ImmutableList.of());
        ScaledWriterScheduler scaledWriterScheduler = buildScaledWriterScheduler(taskStatusProvider, 100);

        assertEquals(scaledWriterScheduler.schedule().getNewTasks().size(), 1);
        taskStatusProvider.set(ImmutableList.of(taskStatus1));

        assertEquals(scaledWriterScheduler.schedule().getNewTasks().size(), 1);
        taskStatusProvider.set(ImmutableList.of(taskStatus1, taskStatus2));

        assertEquals(scaledWriterScheduler.schedule().getNewTasks().size(), 1);
        taskStatusProvider.set(ImmutableList.of(taskStatus1, taskStatus2, taskStatus3));

        return scaledWriterScheduler;
    }

    private ScaledWriterScheduler buildScaledWriterScheduler(AtomicReference<List<TaskStatus>> taskStatusProvider, int maxWritersNodesCount)
    {
        return new ScaledWriterScheduler(
                new TestingStageExecution(createFragment()),
                taskStatusProvider::get,
                taskStatusProvider::get,
                new UniformNodeSelectorFactory(
                        new InMemoryNodeManager(NODE_1, NODE_2, NODE_3),
                        new NodeSchedulerConfig().setIncludeCoordinator(true),
                        new NodeTaskMap(new FinalizerService())).createNodeSelector(testSessionBuilder().build(), Optional.empty()),
                newScheduledThreadPool(10, threadsNamed("task-notification-%s")),
                DataSize.of(32, DataSize.Unit.MEGABYTE),
                maxWritersNodesCount);
    }

    private static TaskStatus buildTaskStatus(boolean isOutputBufferOverUtilized, long outputDataSize)
    {
        return buildTaskStatus(isOutputBufferOverUtilized, outputDataSize, Optional.of(1), DataSize.of(32, DataSize.Unit.MEGABYTE));
    }

    private static TaskStatus buildTaskStatus(int maxWriterCount, DataSize writerInputDataSize)
    {
        return buildTaskStatus(true, 12345L, Optional.of(maxWriterCount), writerInputDataSize);
    }

    private static TaskStatus buildTaskStatus(boolean isOutputBufferOverUtilized, long outputDataSize, Optional<Integer> maxWriterCount, DataSize writerInputDataSize)
    {
        return new TaskStatus(
                TaskId.valueOf("taskId"),
                "task-instance-id",
                0,
                TaskState.RUNNING,
                URI.create("fake://task/" + "taskId" + "/node/some_node"),
                "some_node",
                false,
                ImmutableList.of(),
                0,
                0,
                new OutputBufferStatus(OptionalLong.empty(), isOutputBufferOverUtilized, false),
                DataSize.ofBytes(outputDataSize),
                writerInputDataSize,
                DataSize.of(1, DataSize.Unit.MEGABYTE),
                maxWriterCount,
                DataSize.of(1, DataSize.Unit.MEGABYTE),
                DataSize.of(1, DataSize.Unit.MEGABYTE),
                DataSize.of(0, DataSize.Unit.MEGABYTE),
                0,
                Duration.valueOf("0s"),
                0,
                1,
                1);
    }

    private static class TestingStageExecution
            implements StageExecution
    {
        private final PlanFragment fragment;

        public TestingStageExecution(PlanFragment fragment)
        {
            this.fragment = requireNonNull(fragment, "fragment is null");
        }

        @Override
        public PlanFragment getFragment()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isAnyTaskBlocked()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public State getState()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addStateChangeListener(StateMachine.StateChangeListener<State> stateChangeListener)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public StageId getStageId()
        {
            throw new UnsupportedOperationException();
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
            return Optional.of(new TestingRemoteTask(TaskId.valueOf("taskId"), "nodeId", fragment));
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

    private static PlanFragment createFragment()
    {
        Symbol symbol = new Symbol("column");

        // table scan with splitCount splits
        TableScanNode tableScan = TableScanNode.newInstance(
                TABLE_SCAN_NODE_ID,
                TEST_TABLE_HANDLE,
                ImmutableList.of(symbol),
                ImmutableMap.of(symbol, new TestingMetadata.TestingColumnHandle("column")),
                false,
                Optional.empty());

        return new PlanFragment(
                new PlanFragmentId("plan_id"),
                tableScan,
                ImmutableMap.of(symbol, VARCHAR),
                SOURCE_DISTRIBUTION,
                Optional.empty(),
                ImmutableList.of(TABLE_SCAN_NODE_ID),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(symbol)),
                StatsAndCosts.empty(),
                ImmutableList.of(),
                Optional.empty());
    }
}

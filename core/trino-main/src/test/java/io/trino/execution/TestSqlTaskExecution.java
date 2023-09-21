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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.slice.Slice;
import io.airlift.stats.TestingGcMonitor;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.Span;
import io.trino.execution.buffer.BufferResult;
import io.trino.execution.buffer.BufferState;
import io.trino.execution.buffer.OutputBuffer;
import io.trino.execution.buffer.OutputBufferStateMachine;
import io.trino.execution.buffer.PagesSerdeFactory;
import io.trino.execution.buffer.PartitionedOutputBuffer;
import io.trino.execution.buffer.PipelinedOutputBuffers;
import io.trino.execution.buffer.PipelinedOutputBuffers.OutputBufferId;
import io.trino.execution.executor.TaskExecutor;
import io.trino.execution.executor.timesharing.TimeSharingTaskExecutor;
import io.trino.memory.MemoryPool;
import io.trino.memory.QueryContext;
import io.trino.memory.context.SimpleLocalMemoryContext;
import io.trino.metadata.Split;
import io.trino.operator.DriverContext;
import io.trino.operator.DriverFactory;
import io.trino.operator.OperatorContext;
import io.trino.operator.SourceOperator;
import io.trino.operator.SourceOperatorFactory;
import io.trino.operator.TaskContext;
import io.trino.operator.output.TaskOutputOperator.TaskOutputOperatorFactory;
import io.trino.spi.HostAddress;
import io.trino.spi.Page;
import io.trino.spi.QueryId;
import io.trino.spi.block.TestingBlockEncodingSerde;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spiller.SpillSpaceTracker;
import io.trino.sql.planner.LocalExecutionPlanner.LocalExecutionPlan;
import io.trino.sql.planner.plan.PlanNodeId;
import org.testng.annotations.Test;

import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.tracing.Tracing.noopTracer;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.block.BlockAssertions.createStringSequenceBlock;
import static io.trino.execution.TaskState.FINISHED;
import static io.trino.execution.TaskState.FLUSHING;
import static io.trino.execution.TaskState.RUNNING;
import static io.trino.execution.TaskTestUtils.TABLE_SCAN_NODE_ID;
import static io.trino.execution.TaskTestUtils.createTestSplitMonitor;
import static io.trino.execution.buffer.PagesSerdeUtil.getSerializedPagePositionCount;
import static io.trino.execution.buffer.PipelinedOutputBuffers.BufferType.PARTITIONED;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

@Test(singleThreaded = true)
public class TestSqlTaskExecution
{
    private static final OutputBufferId OUTPUT_BUFFER_ID = new OutputBufferId(0);
    private static final Duration ASSERT_WAIT_TIMEOUT = new Duration(1, HOURS);
    public static final TaskId TASK_ID = new TaskId(new StageId("query", 0), 0, 0);

    @Test
    public void testSimple()
            throws Exception
    {
        ScheduledExecutorService taskNotificationExecutor = newScheduledThreadPool(10, threadsNamed("task-notification-%s"));
        ScheduledExecutorService driverYieldExecutor = newScheduledThreadPool(2, threadsNamed("driver-yield-%s"));
        TaskExecutor taskExecutor = new TimeSharingTaskExecutor(5, 10, 3, 4, Ticker.systemTicker());

        taskExecutor.start();
        try {
            TaskStateMachine taskStateMachine = new TaskStateMachine(TASK_ID, taskNotificationExecutor);
            PartitionedOutputBuffer outputBuffer = newTestingOutputBuffer(taskNotificationExecutor);
            OutputBufferConsumer outputBufferConsumer = new OutputBufferConsumer(outputBuffer, OUTPUT_BUFFER_ID);

            //
            // test initialization: simple task with 1 pipeline
            //
            // pipeline 0  ... pipeline id
            // partitioned ... partitioned/unpartitioned pipeline
            //
            // TaskOutput
            //      |
            //    Scan
            //
            TestingScanOperatorFactory testingScanOperatorFactory = new TestingScanOperatorFactory(0, TABLE_SCAN_NODE_ID);
            TaskOutputOperatorFactory taskOutputOperatorFactory = new TaskOutputOperatorFactory(
                    1,
                    TABLE_SCAN_NODE_ID,
                    outputBuffer,
                    Function.identity(),
                    new PagesSerdeFactory(new TestingBlockEncodingSerde(), false));
            LocalExecutionPlan localExecutionPlan = new LocalExecutionPlan(
                    ImmutableList.of(new DriverFactory(
                            0,
                            true,
                            true,
                            ImmutableList.of(testingScanOperatorFactory, taskOutputOperatorFactory),
                            OptionalInt.empty())),
                    ImmutableList.of(TABLE_SCAN_NODE_ID));
            TaskContext taskContext = newTestingTaskContext(taskNotificationExecutor, driverYieldExecutor, taskStateMachine);
            SqlTaskExecution sqlTaskExecution = new SqlTaskExecution(
                    taskStateMachine,
                    taskContext,
                    Span.getInvalid(),
                    outputBuffer,
                    localExecutionPlan,
                    taskExecutor,
                    createTestSplitMonitor(),
                    noopTracer(),
                    taskNotificationExecutor);
            sqlTaskExecution.start();

            //
            // test body
            assertEquals(taskStateMachine.getState(), RUNNING);

            // add assignment for pipeline
            try {
                // add a splitAssignments with a larger split sequence ID and a different plan node ID
                PlanNodeId tableScanNodeId = new PlanNodeId("tableScan1");
                sqlTaskExecution.addSplitAssignments(ImmutableList.of(new SplitAssignment(
                        tableScanNodeId,
                        ImmutableSet.of(newScheduledSplit(3, tableScanNodeId, 400000, 400)),
                        false)));
            }
            catch (NullPointerException e) {
                // this is expected since there is no pipeline for this
                // the purpose of this splitAssignment is setting maxAcknowledgedSplitByPlanNode in SqlTaskExecution with the larger split sequence ID
            }
            // the split below shouldn't be skipped even though its sequence ID is smaller than the sequence ID of the previous split because they have different plan node IDs
            sqlTaskExecution.addSplitAssignments(ImmutableList.of(new SplitAssignment(
                    TABLE_SCAN_NODE_ID,
                    ImmutableSet.of(
                            newScheduledSplit(0, TABLE_SCAN_NODE_ID, 100000, 123)),
                    false)));
            // assert that partial task result is produced
            outputBufferConsumer.consume(123, ASSERT_WAIT_TIMEOUT);

            // pause operator execution to make sure that
            // * operatorFactory will be closed even though operator can't execute
            // * completedDriverGroups will NOT include the newly scheduled driver group while pause is in place
            testingScanOperatorFactory.getPauser().pause();
            // add assignment for pipeline, mark as no more splits
            sqlTaskExecution.addSplitAssignments(ImmutableList.of(new SplitAssignment(
                    TABLE_SCAN_NODE_ID,
                    ImmutableSet.of(
                            newScheduledSplit(1, TABLE_SCAN_NODE_ID, 200000, 300),
                            newScheduledSplit(2, TABLE_SCAN_NODE_ID, 300000, 200)),
                    true)));
            // assert that pipeline will have no more drivers
            waitUntilEquals(testingScanOperatorFactory::isOverallNoMoreOperators, true, ASSERT_WAIT_TIMEOUT);
            // resume operator execution
            testingScanOperatorFactory.getPauser().resume();
            // assert that task result is produced
            outputBufferConsumer.consume(300 + 200, ASSERT_WAIT_TIMEOUT);
            outputBufferConsumer.assertBufferComplete(ASSERT_WAIT_TIMEOUT);

            assertEquals(taskStateMachine.getStateChange(RUNNING).get(10, SECONDS), FLUSHING);
            outputBufferConsumer.abort(); // complete the task by calling abort on it
            assertEquals(taskStateMachine.getStateChange(FLUSHING).get(10, SECONDS), FINISHED);
        }
        finally {
            taskExecutor.stop();
            taskNotificationExecutor.shutdownNow();
            driverYieldExecutor.shutdown();
        }
    }

    private TaskContext newTestingTaskContext(ScheduledExecutorService taskNotificationExecutor, ScheduledExecutorService driverYieldExecutor, TaskStateMachine taskStateMachine)
    {
        QueryContext queryContext = new QueryContext(
                new QueryId("queryid"),
                DataSize.of(1, MEGABYTE),
                new MemoryPool(DataSize.of(1, GIGABYTE)),
                new TestingGcMonitor(),
                taskNotificationExecutor,
                driverYieldExecutor,
                DataSize.of(1, MEGABYTE),
                new SpillSpaceTracker(DataSize.of(1, GIGABYTE)));
        return queryContext.addTaskContext(taskStateMachine, TEST_SESSION, () -> {}, false, false);
    }

    private PartitionedOutputBuffer newTestingOutputBuffer(ScheduledExecutorService taskNotificationExecutor)
    {
        return new PartitionedOutputBuffer(
                TASK_ID.toString(),
                new OutputBufferStateMachine(TASK_ID, taskNotificationExecutor),
                PipelinedOutputBuffers.createInitial(PARTITIONED)
                        .withBuffer(OUTPUT_BUFFER_ID, 0)
                        .withNoMoreBufferIds(),
                DataSize.of(1, MEGABYTE),
                () -> new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                taskNotificationExecutor);
    }

    private <T> void waitUntilEquals(Supplier<T> actualSupplier, T expected, Duration timeout)
    {
        long nanoUntil = System.nanoTime() + timeout.toMillis() * 1_000_000;
        while (System.nanoTime() - nanoUntil < 0) {
            if (expected.equals(actualSupplier.get())) {
                return;
            }
            try {
                Thread.sleep(10);
            }
            catch (InterruptedException e) {
                // do nothing
            }
        }
        assertEquals(actualSupplier.get(), expected);
    }

    private static class OutputBufferConsumer
    {
        private final OutputBuffer outputBuffer;
        private final OutputBufferId outputBufferId;
        private int sequenceId;
        private int surplusPositions;
        private boolean bufferComplete;

        public OutputBufferConsumer(OutputBuffer outputBuffer, OutputBufferId outputBufferId)
        {
            this.outputBuffer = outputBuffer;
            this.outputBufferId = outputBufferId;
        }

        public void consume(int positions, Duration timeout)
                throws ExecutionException, InterruptedException, TimeoutException
        {
            long nanoUntil = System.nanoTime() + timeout.toMillis() * 1_000_000;
            surplusPositions -= positions;
            while (surplusPositions < 0) {
                assertFalse(bufferComplete, "bufferComplete is set before enough positions are consumed");
                BufferResult results = outputBuffer.get(outputBufferId, sequenceId, DataSize.of(1, MEGABYTE)).get(nanoUntil - System.nanoTime(), TimeUnit.NANOSECONDS);
                bufferComplete = results.isBufferComplete();
                for (Slice serializedPage : results.getSerializedPages()) {
                    surplusPositions += getSerializedPagePositionCount(serializedPage);
                }
                sequenceId += results.getSerializedPages().size();
            }
        }

        public void assertBufferComplete(Duration timeout)
                throws InterruptedException, ExecutionException, TimeoutException
        {
            assertEquals(surplusPositions, 0);
            long nanoUntil = System.nanoTime() + timeout.toMillis() * 1_000_000;
            while (!bufferComplete) {
                BufferResult results = outputBuffer.get(outputBufferId, sequenceId, DataSize.of(1, MEGABYTE)).get(nanoUntil - System.nanoTime(), TimeUnit.NANOSECONDS);
                bufferComplete = results.isBufferComplete();
                for (Slice serializedPage : results.getSerializedPages()) {
                    assertEquals(getSerializedPagePositionCount(serializedPage), 0);
                }
                sequenceId += results.getSerializedPages().size();
            }
        }

        public void abort()
        {
            outputBuffer.destroy(outputBufferId);
            assertEquals(outputBuffer.getInfo().getState(), BufferState.FINISHED);
        }
    }

    private ScheduledSplit newScheduledSplit(int sequenceId, PlanNodeId planNodeId, int begin, int count)
    {
        return new ScheduledSplit(sequenceId, planNodeId, new Split(TEST_CATALOG_HANDLE, new TestingSplit(begin, begin + count)));
    }

    public static class Pauser
    {
        private volatile SettableFuture<Void> future = SettableFuture.create();

        public Pauser()
        {
            future.set(null);
        }

        public void pause()
        {
            if (!future.isDone()) {
                return;
            }
            future = SettableFuture.create();
        }

        public void resume()
        {
            if (future.isDone()) {
                return;
            }
            future.set(null);
        }

        public void await()
        {
            try {
                future.get();
            }
            catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class TestingScanOperatorFactory
            implements SourceOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId sourceId;
        private final Pauser pauser = new Pauser();

        private boolean overallNoMoreOperators;

        public TestingScanOperatorFactory(
                int operatorId,
                PlanNodeId sourceId)
        {
            this.operatorId = operatorId;
            this.sourceId = requireNonNull(sourceId, "sourceId is null");
        }

        @Override
        public PlanNodeId getSourceId()
        {
            return sourceId;
        }

        @Override
        public SourceOperator createOperator(DriverContext driverContext)
        {
            checkState(!overallNoMoreOperators, "noMoreOperators() has been called");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, sourceId, TestingScanOperator.class.getSimpleName());
            return new TestingScanOperator(operatorContext, sourceId);
        }

        @Override
        public void noMoreOperators()
        {
            overallNoMoreOperators = true;
        }

        public boolean isOverallNoMoreOperators()
        {
            return overallNoMoreOperators;
        }

        public Pauser getPauser()
        {
            return pauser;
        }

        public class TestingScanOperator
                implements SourceOperator
        {
            private final OperatorContext operatorContext;
            private final PlanNodeId planNodeId;

            private final SettableFuture<Void> blocked = SettableFuture.create();

            private TestingSplit split;

            private boolean finished;

            public TestingScanOperator(
                    OperatorContext operatorContext,
                    PlanNodeId planNodeId)
            {
                this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
                this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            }

            @Override
            public OperatorContext getOperatorContext()
            {
                return operatorContext;
            }

            @Override
            public PlanNodeId getSourceId()
            {
                return planNodeId;
            }

            @Override
            public void addSplit(Split split)
            {
                requireNonNull(split, "split is null");
                checkState(this.split == null, "Table scan split already set");

                if (finished) {
                    return;
                }

                this.split = (TestingSplit) split.getConnectorSplit();
                blocked.set(null);
            }

            @Override
            public void noMoreSplits()
            {
                if (split == null) {
                    finish();
                }
                blocked.set(null);
            }

            @Override
            public void close()
            {
                finish();
            }

            @Override
            public void finish()
            {
                finished = true;
            }

            @Override
            public boolean isFinished()
            {
                return finished;
            }

            @Override
            public ListenableFuture<Void> isBlocked()
            {
                return blocked;
            }

            @Override
            public boolean needsInput()
            {
                return false;
            }

            @Override
            public void addInput(Page page)
            {
                throw new UnsupportedOperationException(getClass().getName() + " cannot take input");
            }

            @Override
            public Page getOutput()
            {
                if (split == null) {
                    return null;
                }

                pauser.await();
                Page result = new Page(createStringSequenceBlock(split.getBegin(), split.getEnd()));
                finish();
                return result;
            }
        }
    }

    public static class TestingSplit
            implements ConnectorSplit
    {
        private static final int INSTANCE_SIZE = instanceSize(TestingSplit.class);

        private final int begin;
        private final int end;

        @JsonCreator
        public TestingSplit(@JsonProperty("begin") int begin, @JsonProperty("end") int end)
        {
            this.begin = begin;
            this.end = end;
        }

        @Override
        public boolean isRemotelyAccessible()
        {
            return true;
        }

        @Override
        public List<HostAddress> getAddresses()
        {
            return ImmutableList.of();
        }

        @Override
        public Object getInfo()
        {
            return this;
        }

        @Override
        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE;
        }

        public int getBegin()
        {
            return begin;
        }

        public int getEnd()
        {
            return end;
        }
    }
}

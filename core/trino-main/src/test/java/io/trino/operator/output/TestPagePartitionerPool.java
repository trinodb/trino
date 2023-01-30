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
package io.trino.operator.output;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.execution.StateMachine;
import io.trino.execution.buffer.BufferResult;
import io.trino.execution.buffer.BufferState;
import io.trino.execution.buffer.OutputBuffer;
import io.trino.execution.buffer.OutputBufferInfo;
import io.trino.execution.buffer.OutputBufferStatus;
import io.trino.execution.buffer.OutputBuffers;
import io.trino.execution.buffer.PipelinedOutputBuffers.OutputBufferId;
import io.trino.execution.buffer.TestingPagesSerdeFactory;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.operator.BucketPartitionFunction;
import io.trino.operator.DriverContext;
import io.trino.operator.Operator;
import io.trino.operator.exchange.PageChannelSelector;
import io.trino.operator.output.PartitionedOutputOperator.PartitionedOutputOperatorFactory;
import io.trino.spi.Page;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.TestingTaskContext;
import io.trino.type.BlockTypeOperators;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestPagePartitionerPool
{
    private ScheduledExecutorService driverYieldExecutor;

    @BeforeClass
    public void setUp()
    {
        driverYieldExecutor = newScheduledThreadPool(0, threadsNamed("TestPagePartitionerPool-driver-yield-%s"));
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        driverYieldExecutor.shutdown();
    }

    @Test
    public void testBuffersReusedAcrossSplits()
    {
        Page split = new Page(createLongsBlock(1));
        // one split fit in the buffer but 2 do not
        DataSize maxPagePartitioningBufferSize = DataSize.ofBytes(split.getSizeInBytes() + 1);

        OutputBufferMock outputBuffer = new OutputBufferMock();
        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        PartitionedOutputOperatorFactory factory = createFactory(maxPagePartitioningBufferSize, outputBuffer, memoryContext);

        assertEquals(memoryContext.getBytes(), 0);
        // first split, too small for a flush
        long initialRetainedBytesOneOperator = processSplitsConcurrently(factory, memoryContext, split);
        assertEquals(outputBuffer.totalEnqueuedPageCount(), 0);
        assertThat(memoryContext.getBytes()).isGreaterThanOrEqualTo(initialRetainedBytesOneOperator + split.getSizeInBytes());

        // second split makes the split partitioner buffer full so the split is flushed
        processSplitsConcurrently(factory, memoryContext, split);
        assertEquals(outputBuffer.totalEnqueuedPageCount(), 1);
        assertEquals(memoryContext.getBytes(), initialRetainedBytesOneOperator);

        // two splits are processed at once so the use different buffers and do not flush for single split per buffer
        long initialRetainedBytesTwoOperators = processSplitsConcurrently(factory, memoryContext, split, split);
        assertEquals(outputBuffer.totalEnqueuedPageCount(), 1);
        assertThat(memoryContext.getBytes()).isGreaterThanOrEqualTo(initialRetainedBytesTwoOperators + 2 * split.getSizeInBytes());

        // another pair of splits should flush both buffers
        processSplitsConcurrently(factory, memoryContext, split, split);
        assertEquals(outputBuffer.totalEnqueuedPageCount(), 3);
        assertEquals(memoryContext.getBytes(), initialRetainedBytesTwoOperators);

        // max free buffers is set to 2 so 2 buffers are going to be retained but 2 will be flushed and released
        processSplitsConcurrently(factory, memoryContext, split, split, split, split);
        assertEquals(outputBuffer.totalEnqueuedPageCount(), 5);
        assertThat(memoryContext.getBytes()).isGreaterThanOrEqualTo(initialRetainedBytesTwoOperators + 2 * split.getSizeInBytes());

        // another pair of splits should flush remaining buffers
        processSplitsConcurrently(factory, memoryContext, split, split);
        assertEquals(outputBuffer.totalEnqueuedPageCount(), 7);
        assertEquals(memoryContext.getBytes(), initialRetainedBytesTwoOperators);

        // noMoreOperators forces buffers to be flushed even though they are not full
        processSplitsConcurrently(factory, memoryContext, split);
        assertThat(memoryContext.getBytes()).isGreaterThanOrEqualTo(initialRetainedBytesTwoOperators + split.getSizeInBytes());
        Operator operator = factory.createOperator(driverContext());
        factory.noMoreOperators();
        assertEquals(outputBuffer.totalEnqueuedPageCount(), 8);
        assertEquals(memoryContext.getBytes(), initialRetainedBytesOneOperator);

        // noMoreOperators was called already so new split are flushed even though they are not full
        operator.addInput(split);
        operator.finish();
        assertEquals(outputBuffer.totalEnqueuedPageCount(), 9);
        // pool is closed, all operators are finished/flushed, the retained memory should be 0
        assertEquals(memoryContext.getBytes(), 0);
    }

    @Test
    public void testMemoryReleasedOnFailure()
    {
        Page split = new Page(createLongsBlock(1));
        // one split fit in the buffer but 2 do not
        DataSize maxPagePartitioningBufferSize = DataSize.ofBytes(split.getSizeInBytes() + 1);
        RuntimeException exception = new RuntimeException();
        OutputBufferMock outputBuffer = new OutputBufferMock() {
            @Override
            public void enqueue(int partition, List<Slice> pages)
            {
                throw exception;
            }
        };
        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        PartitionedOutputOperatorFactory factory = createFactory(maxPagePartitioningBufferSize, outputBuffer, memoryContext);

        long initialRetainedBytesOneOperator = processSplitsConcurrently(factory, memoryContext, split);
        assertThat(memoryContext.getBytes()).isGreaterThanOrEqualTo(initialRetainedBytesOneOperator + split.getSizeInBytes());

        assertThatThrownBy(factory::noMoreOperators).isEqualTo(exception);
        assertEquals(memoryContext.getBytes(), 0);
    }

    private static PartitionedOutputOperatorFactory createFactory(DataSize maxPagePartitioningBufferSize, OutputBufferMock outputBuffer, AggregatedMemoryContext memoryContext)
    {
        return new PartitionedOutputOperatorFactory(
                0,
                new PlanNodeId("0"),
                ImmutableList.of(BIGINT),
                PageChannelSelector.identitySelection(),
                new BucketPartitionFunction((page, position) -> 0, new int[1]),
                ImmutableList.of(0),
                ImmutableList.of(),
                false,
                OptionalInt.empty(),
                outputBuffer,
                new TestingPagesSerdeFactory(),
                maxPagePartitioningBufferSize,
                new PositionsAppenderFactory(new BlockTypeOperators()),
                Optional.empty(),
                memoryContext,
                2);
    }

    private long processSplitsConcurrently(PartitionedOutputOperatorFactory factory, AggregatedMemoryContext memoryContext, Page... splits)
    {
        List<Operator> operators = Stream.of(splits)
                .map(split -> factory.createOperator(driverContext()))
                .collect(toImmutableList());

        long initialRetainedBytes = memoryContext.getBytes();
        for (int i = 0; i < operators.size(); i++) {
            operators.get(i).addInput(splits[i]);
        }
        operators.forEach(Operator::finish);
        return initialRetainedBytes;
    }

    private DriverContext driverContext()
    {
        return TestingTaskContext.builder(directExecutor(), driverYieldExecutor, TEST_SESSION)
                .build()
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }

    private static class OutputBufferMock
            implements OutputBuffer
    {
        Map<Integer, Integer> partitionBufferPages = new HashMap<>();

        public int totalEnqueuedPageCount()
        {
            return partitionBufferPages.values().stream().mapToInt(Integer::intValue).sum();
        }

        @Override
        public void enqueue(int partition, List<Slice> pages)
        {
            partitionBufferPages.compute(partition, (key, value) -> value == null ? pages.size() : value + pages.size());
        }

        @Override
        public OutputBufferInfo getInfo()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public BufferState getState()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public double getUtilization()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public OutputBufferStatus getStatus()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addStateChangeListener(StateMachine.StateChangeListener<BufferState> stateChangeListener)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setOutputBuffers(OutputBuffers newOutputBuffers)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ListenableFuture<BufferResult> get(OutputBufferId bufferId, long token, DataSize maxSize)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void acknowledge(OutputBufferId bufferId, long token)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void destroy(OutputBufferId bufferId)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ListenableFuture<Void> isFull()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void enqueue(List<Slice> pages)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setNoMorePages()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void destroy()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void abort()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getPeakMemoryUsage()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<Throwable> getFailureCause()
        {
            throw new UnsupportedOperationException();
        }
    }
}

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
package io.trino.execution.buffer;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.DataSize;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.execution.buffer.PipelinedOutputBuffers.OutputBufferId;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.MemoryReservationHandler;
import io.trino.memory.context.SimpleLocalMemoryContext;
import io.trino.spi.Page;
import io.trino.spi.QueryId;
import io.trino.spi.type.BigintType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.execution.buffer.BufferResult.emptyResults;
import static io.trino.execution.buffer.BufferState.ABORTED;
import static io.trino.execution.buffer.BufferState.FINISHED;
import static io.trino.execution.buffer.BufferState.FLUSHING;
import static io.trino.execution.buffer.BufferState.NO_MORE_BUFFERS;
import static io.trino.execution.buffer.BufferState.OPEN;
import static io.trino.execution.buffer.BufferTestUtils.MAX_WAIT;
import static io.trino.execution.buffer.BufferTestUtils.NO_WAIT;
import static io.trino.execution.buffer.BufferTestUtils.acknowledgeBufferResult;
import static io.trino.execution.buffer.BufferTestUtils.addPage;
import static io.trino.execution.buffer.BufferTestUtils.assertBufferResultEquals;
import static io.trino.execution.buffer.BufferTestUtils.assertFinished;
import static io.trino.execution.buffer.BufferTestUtils.assertFutureIsDone;
import static io.trino.execution.buffer.BufferTestUtils.assertQueueClosed;
import static io.trino.execution.buffer.BufferTestUtils.assertQueueState;
import static io.trino.execution.buffer.BufferTestUtils.createBufferResult;
import static io.trino.execution.buffer.BufferTestUtils.createPage;
import static io.trino.execution.buffer.BufferTestUtils.enqueuePage;
import static io.trino.execution.buffer.BufferTestUtils.getBufferResult;
import static io.trino.execution.buffer.BufferTestUtils.getFuture;
import static io.trino.execution.buffer.BufferTestUtils.serializePage;
import static io.trino.execution.buffer.BufferTestUtils.sizeOfPages;
import static io.trino.execution.buffer.PipelinedOutputBuffers.BROADCAST_PARTITION_ID;
import static io.trino.execution.buffer.PipelinedOutputBuffers.BufferType.BROADCAST;
import static io.trino.memory.context.AggregatedMemoryContext.newRootAggregatedMemoryContext;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestBroadcastOutputBuffer
{
    private static final String TASK_INSTANCE_ID = "task-instance-id";

    private static final List<BigintType> TYPES = ImmutableList.of(BIGINT);
    private static final OutputBufferId FIRST = new OutputBufferId(0);
    private static final OutputBufferId SECOND = new OutputBufferId(1);
    private static final OutputBufferId THIRD = new OutputBufferId(2);

    private ScheduledExecutorService stateNotificationExecutor;

    @BeforeAll
    public void setUp()
    {
        stateNotificationExecutor = newScheduledThreadPool(5, daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
    }

    @AfterAll
    public void tearDown()
    {
        if (stateNotificationExecutor != null) {
            stateNotificationExecutor.shutdownNow();
            stateNotificationExecutor = null;
        }
    }

    @Test
    public void testInvalidConstructorArg()
    {
        assertThatThrownBy(() -> createBroadcastBuffer(PipelinedOutputBuffers.createInitial(BROADCAST).withBuffer(FIRST, BROADCAST_PARTITION_ID).withNoMoreBufferIds(), DataSize.ofBytes(0)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("maxBufferedBytes must be > 0");
        assertThatThrownBy(() -> createBroadcastBuffer(PipelinedOutputBuffers.createInitial(BROADCAST), DataSize.ofBytes(0)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("maxBufferedBytes must be > 0");
    }

    @Test
    public void testSimple()
    {
        PipelinedOutputBuffers outputBuffers = PipelinedOutputBuffers.createInitial(BROADCAST);
        BroadcastOutputBuffer buffer = createBroadcastBuffer(outputBuffers, sizeOfPages(10));

        // add three items
        for (int i = 0; i < 3; i++) {
            addPage(buffer, createPage(i));
        }

        outputBuffers = PipelinedOutputBuffers.createInitial(BROADCAST).withBuffer(FIRST, BROADCAST_PARTITION_ID);

        // add a queue
        buffer.setOutputBuffers(outputBuffers);
        assertQueueState(buffer, FIRST, 3, 0);

        // get the three elements
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(10), NO_WAIT), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        // pages not acknowledged yet so state is the same
        assertQueueState(buffer, FIRST, 3, 0);

        // acknowledge first three pages
        buffer.get(FIRST, 3, sizeOfPages(10)).cancel(true);
        // pages now acknowledged
        assertQueueState(buffer, FIRST, 0, 3);

        // fill the buffer (we already added 3 pages)
        for (int i = 3; i < 10; i++) {
            addPage(buffer, createPage(i));
        }
        assertQueueState(buffer, FIRST, 7, 3);

        // try to add one more page, which should block
        ListenableFuture<Void> future = enqueuePage(buffer, createPage(10));
        assertThat(future.isDone()).isFalse();
        assertQueueState(buffer, FIRST, 8, 3);

        // remove a page
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 3, sizeOfPages(1), NO_WAIT), bufferResult(3, createPage(3)));
        // page not acknowledged yet so sent count is the same
        assertQueueState(buffer, FIRST, 8, 3);

        // we should still be blocked
        assertThat(future.isDone()).isFalse();

        //
        // add another buffer and verify it sees all pages
        outputBuffers = outputBuffers.withBuffer(SECOND, BROADCAST_PARTITION_ID);
        buffer.setOutputBuffers(outputBuffers);
        assertQueueState(buffer, SECOND, 11, 0);
        assertBufferResultEquals(TYPES, getBufferResult(buffer, SECOND, 0, sizeOfPages(10), NO_WAIT), bufferResult(0, createPage(0),
                createPage(1),
                createPage(2),
                createPage(3),
                createPage(4),
                createPage(5),
                createPage(6),
                createPage(7),
                createPage(8),
                createPage(9)));
        // page not acknowledged yet so sent count is still zero
        assertQueueState(buffer, SECOND, 11, 0);
        // acknowledge the 10 pages
        buffer.get(SECOND, 10, sizeOfPages(10)).cancel(true);
        assertQueueState(buffer, SECOND, 1, 10);

        //
        // tell shared buffer there will be no more queues
        outputBuffers = outputBuffers.withNoMoreBufferIds();
        buffer.setOutputBuffers(outputBuffers);

        // queues consumed the first three pages, so they should be dropped now and the blocked page future from above should be done
        assertQueueState(buffer, FIRST, 8, 3);
        assertQueueState(buffer, SECOND, 1, 10);
        assertFutureIsDone(future);

        // we should be able to add 3 more pages (the third will be queued)
        // although the first queue fetched the 4th page, the page has not been acknowledged yet
        addPage(buffer, createPage(11));
        addPage(buffer, createPage(12));
        future = enqueuePage(buffer, createPage(13));
        assertThat(future.isDone()).isFalse();
        assertQueueState(buffer, FIRST, 11, 3);
        assertQueueState(buffer, SECOND, 4, 10);

        // acknowledge the receipt of the 3rd page and try to remove the 4th page from the first queue
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 4, sizeOfPages(1), NO_WAIT), bufferResult(4, createPage(4)));

        // the blocked page future above should be done
        assertFutureIsDone(future);
        assertQueueState(buffer, FIRST, 10, 4);
        assertQueueState(buffer, SECOND, 4, 10);

        //
        // finish the buffer
        assertThat(buffer.getState()).isEqualTo(NO_MORE_BUFFERS);
        buffer.setNoMorePages();
        assertQueueState(buffer, FIRST, 10, 4);
        assertQueueState(buffer, SECOND, 4, 10);

        // not fully finished until all pages are consumed
        assertThat(buffer.getState()).isEqualTo(FLUSHING);

        // remove a page, not finished
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 5, sizeOfPages(1), NO_WAIT), bufferResult(5, createPage(5)));
        assertQueueState(buffer, FIRST, 9, 5);
        assertQueueState(buffer, SECOND, 4, 10);
        assertThat(buffer.getState()).isEqualTo(FLUSHING);

        // remove all remaining pages from first queue, should not be finished
        BufferResult x = getBufferResult(buffer, FIRST, 6, sizeOfPages(10), NO_WAIT);
        assertBufferResultEquals(TYPES, x, bufferResult(6, createPage(6),
                createPage(7),
                createPage(8),
                createPage(9),
                createPage(10),
                createPage(11),
                createPage(12),
                createPage(13)));
        assertQueueState(buffer, FIRST, 8, 6);
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 14, sizeOfPages(10), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 14, true));

        // finish first queue
        buffer.destroy(FIRST);
        assertQueueClosed(buffer, FIRST, 14);
        assertQueueState(buffer, SECOND, 4, 10);
        assertThat(buffer.getState()).isEqualTo(FLUSHING);

        // remove all remaining pages from second queue, should be finished
        assertBufferResultEquals(TYPES, getBufferResult(buffer, SECOND, 10, sizeOfPages(10), NO_WAIT), bufferResult(10, createPage(10),
                createPage(11),
                createPage(12),
                createPage(13)));
        assertQueueState(buffer, SECOND, 4, 10);
        assertBufferResultEquals(TYPES, getBufferResult(buffer, SECOND, 14, sizeOfPages(10), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 14, true));
        buffer.destroy(SECOND);
        assertQueueClosed(buffer, FIRST, 14);
        assertQueueClosed(buffer, SECOND, 14);
        assertFinished(buffer);

        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 14, sizeOfPages(10), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 14, true));
        assertBufferResultEquals(TYPES, getBufferResult(buffer, SECOND, 14, sizeOfPages(10), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 14, true));
    }

    // TODO: remove this after PR is landed: https://github.com/prestodb/presto/pull/7987
    @Test
    public void testAcknowledge()
    {
        OutputBuffers outputBuffers = PipelinedOutputBuffers.createInitial(BROADCAST);
        BroadcastOutputBuffer buffer = createBroadcastBuffer(outputBuffers, sizeOfPages(10));

        // add three items
        for (int i = 0; i < 3; i++) {
            addPage(buffer, createPage(i));
        }

        outputBuffers = PipelinedOutputBuffers.createInitial(BROADCAST).withBuffer(FIRST, BROADCAST_PARTITION_ID);

        // add a queue
        buffer.setOutputBuffers(outputBuffers);
        assertQueueState(buffer, FIRST, 3, 0);

        // get the three elements
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(10), NO_WAIT), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        // acknowledge pages 0 and 1
        acknowledgeBufferResult(buffer, FIRST, 2);
        // only page 2 is not removed
        assertQueueState(buffer, FIRST, 1, 2);
        // acknowledge page 2
        acknowledgeBufferResult(buffer, FIRST, 3);
        // nothing left
        assertQueueState(buffer, FIRST, 0, 3);
        // acknowledge more pages will fail
        try {
            acknowledgeBufferResult(buffer, FIRST, 4);
        }
        catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).isEqualTo("Invalid sequence id");
        }

        // fill the buffer
        for (int i = 3; i < 6; i++) {
            addPage(buffer, createPage(i));
        }
        assertQueueState(buffer, FIRST, 3, 3);

        // getting new pages will again acknowledge the previously acknowledged pages but this is ok
        buffer.get(FIRST, 3, sizeOfPages(1)).cancel(true);
        assertQueueState(buffer, FIRST, 3, 3);
    }

    @Test
    public void testSharedBufferFull()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(PipelinedOutputBuffers.createInitial(BROADCAST), sizeOfPages(2));

        // Add two pages, buffer is full
        addPage(buffer, createPage(1));
        addPage(buffer, createPage(2));

        // third page is blocked
        enqueuePage(buffer, createPage(3));
    }

    @Test
    public void testNotifyStatusOnBufferFull()
    {
        AtomicInteger notifyCount = new AtomicInteger();
        BroadcastOutputBuffer buffer = createBroadcastBuffer(
                PipelinedOutputBuffers.createInitial(BROADCAST).withBuffer(FIRST, BROADCAST_PARTITION_ID),
                sizeOfPages(1),
                notifyCount::incrementAndGet);

        // Add a page to the buffer
        addPage(buffer, createPage(1));
        assertThat(buffer.isFull().isDone()).isTrue();
        assertThat(notifyCount.get()).isEqualTo(0);

        // Add another page to block
        ListenableFuture<Void> future = enqueuePage(buffer, createPage(2));
        assertThat(future.isDone()).isFalse();
        assertThat(notifyCount.get()).isEqualTo(1);

        // Set no more buffers
        buffer.setOutputBuffers(PipelinedOutputBuffers.createInitial(BROADCAST).withBuffer(FIRST, BROADCAST_PARTITION_ID).withNoMoreBufferIds());

        // Acknowledge both pages in the buffer to remove them
        buffer.acknowledge(FIRST, 2);
        assertFutureIsDone(future);
        assertThat(notifyCount.get()).isEqualTo(1);

        // Add two more pages, buffer will be blocked second time
        addPage(buffer, createPage(3));
        future = enqueuePage(buffer, createPage(4));
        assertThat(future.isDone()).isFalse();
        assertThat(notifyCount.get()).isEqualTo(1);
    }

    @Test
    public void testNotifyStatusOnBufferFullWithNoBufferIds()
    {
        AtomicInteger notifyCount = new AtomicInteger();
        BroadcastOutputBuffer buffer = createBroadcastBuffer(
                PipelinedOutputBuffers.createInitial(BROADCAST)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(1),
                notifyCount::incrementAndGet);

        // Add a page to the buffer
        addPage(buffer, createPage(1));
        assertThat(buffer.isFull().isDone()).isTrue();
        assertThat(notifyCount.get()).isEqualTo(0);

        // Add another page to block
        ListenableFuture<Void> future = enqueuePage(buffer, createPage(2));
        assertThat(future.isDone()).isFalse();
        assertThat(notifyCount.get()).isEqualTo(0); // stays 0 because no new buffers will be added

        // Acknowledge both pages in the buffer to remove them
        buffer.acknowledge(FIRST, 2);
        assertFutureIsDone(future);
        assertThat(notifyCount.get()).isEqualTo(0);

        // Add two more pages, buffer will be blocked second time
        addPage(buffer, createPage(3));
        future = enqueuePage(buffer, createPage(4));
        assertThat(future.isDone()).isFalse();
        assertThat(notifyCount.get()).isEqualTo(0);
    }

    @Test
    public void testDuplicateRequests()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(
                PipelinedOutputBuffers.createInitial(BROADCAST)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(10));

        // add three items
        for (int i = 0; i < 3; i++) {
            addPage(buffer, createPage(i));
        }

        // add a queue
        assertQueueState(buffer, FIRST, 3, 0);

        // get the three elements
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(10), NO_WAIT), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        // pages not acknowledged yet so state is the same
        assertQueueState(buffer, FIRST, 3, 0);

        // get the three elements again
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(10), NO_WAIT), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        // pages not acknowledged yet so state is the same
        assertQueueState(buffer, FIRST, 3, 0);

        // acknowledge the pages
        buffer.get(FIRST, 3, sizeOfPages(10)).cancel(true);

        // attempt to get the three elements again
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(10), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 0, false));
        // pages not acknowledged yet so state is the same
        assertQueueState(buffer, FIRST, 0, 3);
    }

    @Test
    public void testAddQueueAfterCreation()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(
                PipelinedOutputBuffers.createInitial(BROADCAST)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(10));

        assertThat(buffer.getState()).isEqualTo(NO_MORE_BUFFERS);

        assertThatThrownBy(() -> buffer.setOutputBuffers(PipelinedOutputBuffers.createInitial(BROADCAST)
                .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                .withBuffer(SECOND, BROADCAST_PARTITION_ID)
                .withNoMoreBufferIds()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Expected buffer to not change after no more buffers is set");
    }

    @Test
    public void testAddAfterFinish()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(
                PipelinedOutputBuffers.createInitial(BROADCAST)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(10));
        buffer.setNoMorePages();
        addPage(buffer, createPage(0));
        addPage(buffer, createPage(0));
        assertThat(buffer.getInfo().getTotalPagesSent()).isEqualTo(0);
    }

    @Test
    public void testAddQueueAfterNoMoreQueues()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(PipelinedOutputBuffers.createInitial(BROADCAST), sizeOfPages(10));
        assertThat(buffer.getState()).isEqualTo(OPEN);

        // tell buffer no more queues will be added
        buffer.setOutputBuffers(PipelinedOutputBuffers.createInitial(BROADCAST).withNoMoreBufferIds());
        assertThat(buffer.getState()).isEqualTo(FINISHED);

        // set no more queues a second time to assure that we don't get an exception or such
        buffer.setOutputBuffers(PipelinedOutputBuffers.createInitial(BROADCAST).withNoMoreBufferIds());
        assertThat(buffer.getState()).isEqualTo(FINISHED);

        // set no more queues a third time to assure that we don't get an exception or such
        buffer.setOutputBuffers(PipelinedOutputBuffers.createInitial(BROADCAST).withNoMoreBufferIds());
        assertThat(buffer.getState()).isEqualTo(FINISHED);
    }

    @Test
    public void testAddAfterDestroy()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(
                PipelinedOutputBuffers.createInitial(BROADCAST)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(10));
        buffer.destroy();
        addPage(buffer, createPage(0));
        addPage(buffer, createPage(0));
        assertThat(buffer.getInfo().getTotalPagesSent()).isEqualTo(0);
    }

    @Test
    public void testGetBeforeCreate()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(PipelinedOutputBuffers.createInitial(BROADCAST), sizeOfPages(10));
        assertThat(buffer.getState()).isEqualTo(OPEN);

        // get a page from a buffer that doesn't exist yet
        ListenableFuture<BufferResult> future = buffer.get(FIRST, 0L, sizeOfPages(1));
        assertThat(future.isDone()).isFalse();

        // add a page and verify the future is complete
        addPage(buffer, createPage(33));
        assertThat(future.isDone()).isTrue();
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), bufferResult(0, createPage(33)));
    }

    @Test
    public void testSetFinalBuffersWihtoutDeclaringUsedBuffer()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(PipelinedOutputBuffers.createInitial(BROADCAST), sizeOfPages(10));
        assertThat(buffer.getState()).isEqualTo(OPEN);

        // get a page from a buffer that doesn't exist yet
        ListenableFuture<BufferResult> future = buffer.get(FIRST, 0L, sizeOfPages(1));
        assertThat(future.isDone()).isFalse();

        // add a page and set no more pages
        addPage(buffer, createPage(33));
        buffer.setNoMorePages();

        // read the page
        assertThat(future.isDone()).isTrue();
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), bufferResult(0, createPage(33)));

        // acknowledge the page and verify we are finished
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 1, sizeOfPages(10), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 1, true));
        buffer.destroy(FIRST);

        // set final buffers to a set that does not contain the buffer, which will fail
        assertThatThrownBy(() -> buffer.setOutputBuffers(PipelinedOutputBuffers.createInitial(BROADCAST).withNoMoreBufferIds()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageMatching(".*does not contain.*\\[0]");
    }

    @Test
    public void testUseUndeclaredBufferAfterFinalBuffersSet()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(
                PipelinedOutputBuffers.createInitial(BROADCAST)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(10));
        assertThat(buffer.getState()).isEqualTo(NO_MORE_BUFFERS);

        // get a page from a buffer that was not declared, which will fail
        assertThatThrownBy(() -> buffer.get(SECOND, 0L, sizeOfPages(1)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("No more buffers already set");
    }

    @Test
    public void testAbortBeforeCreate()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(PipelinedOutputBuffers.createInitial(BROADCAST), sizeOfPages(2));
        assertThat(buffer.getState()).isEqualTo(OPEN);

        // get a page from a buffer that doesn't exist yet
        ListenableFuture<BufferResult> future = buffer.get(FIRST, 0, sizeOfPages(1));
        assertThat(future.isDone()).isFalse();

        // destroy that buffer, and verify the future is complete and buffer is finished
        buffer.destroy(FIRST);
        assertThat(future.isDone()).isTrue();
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(10), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 0, true));
    }

    @Test
    public void testFullBufferBlocksWriter()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(
                PipelinedOutputBuffers.createInitial(BROADCAST)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withBuffer(SECOND, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(2));

        // Add two pages, buffer is full
        addPage(buffer, createPage(1));
        addPage(buffer, createPage(2));

        // third page is blocked
        enqueuePage(buffer, createPage(3));
    }

    @Test
    public void testAcknowledgementFreesWriters()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(
                PipelinedOutputBuffers.createInitial(BROADCAST)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withBuffer(SECOND, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(2));

        // Add two pages, buffer is full
        addPage(buffer, createPage(1));
        addPage(buffer, createPage(2));
        assertQueueState(buffer, FIRST, 2, 0);

        // third page is blocked
        ListenableFuture<Void> future = enqueuePage(buffer, createPage(3));

        // we should be blocked
        assertThat(future.isDone()).isFalse();
        assertQueueState(buffer, FIRST, 3, 0);
        assertQueueState(buffer, SECOND, 3, 0);

        // acknowledge pages for first buffer, no space is freed
        buffer.get(FIRST, 2, sizeOfPages(10)).cancel(true);
        assertThat(future.isDone()).isFalse();

        // acknowledge pages for second buffer, which makes space in the buffer
        buffer.get(SECOND, 2, sizeOfPages(10)).cancel(true);

        // writer should not be blocked
        assertFutureIsDone(future);
        assertQueueState(buffer, SECOND, 1, 2);
    }

    @Test
    public void testAbort()
    {
        BroadcastOutputBuffer bufferedBuffer = createBroadcastBuffer(
                PipelinedOutputBuffers.createInitial(BROADCAST)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withBuffer(SECOND, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(10));

        // fill the buffer
        for (int i = 0; i < 10; i++) {
            addPage(bufferedBuffer, createPage(i));
        }
        bufferedBuffer.setNoMorePages();

        assertBufferResultEquals(TYPES, getBufferResult(bufferedBuffer, FIRST, 0, sizeOfPages(1), NO_WAIT), bufferResult(0, createPage(0)));
        bufferedBuffer.destroy(FIRST);
        assertQueueClosed(bufferedBuffer, FIRST, 0);
        assertBufferResultEquals(TYPES, getBufferResult(bufferedBuffer, FIRST, 1, sizeOfPages(1), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 0, true));

        assertBufferResultEquals(TYPES, getBufferResult(bufferedBuffer, SECOND, 0, sizeOfPages(1), NO_WAIT), bufferResult(0, createPage(0)));
        bufferedBuffer.destroy(SECOND);
        assertQueueClosed(bufferedBuffer, SECOND, 0);
        assertFinished(bufferedBuffer);
        assertBufferResultEquals(TYPES, getBufferResult(bufferedBuffer, SECOND, 1, sizeOfPages(1), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 0, true));
    }

    @Test
    public void testFinishClosesEmptyQueues()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(
                PipelinedOutputBuffers.createInitial(BROADCAST)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withBuffer(SECOND, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(10));

        // finish while queues are empty
        buffer.setNoMorePages();

        assertQueueState(buffer, FIRST, 0, 0);
        assertQueueState(buffer, SECOND, 0, 0);

        buffer.destroy(FIRST);
        buffer.destroy(SECOND);

        assertQueueClosed(buffer, FIRST, 0);
        assertQueueClosed(buffer, SECOND, 0);
    }

    @Test
    public void testAbortFreesReader()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(
                PipelinedOutputBuffers.createInitial(BROADCAST)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withBuffer(SECOND, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(5));
        assertThat(buffer.getState()).isEqualTo(NO_MORE_BUFFERS);

        // attempt to get a page
        ListenableFuture<BufferResult> future = buffer.get(FIRST, 0, sizeOfPages(10));

        // verify we are waiting for a page
        assertThat(future.isDone()).isFalse();

        // add one item
        addPage(buffer, createPage(0));
        assertThat(future.isDone()).isTrue();

        // verify we got one page
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), bufferResult(0, createPage(0)));

        // attempt to get another page, and verify we are blocked
        future = buffer.get(FIRST, 1, sizeOfPages(10));
        assertThat(future.isDone()).isFalse();

        // destroy the buffer
        buffer.destroy(FIRST);

        // verify the future completed
        // broadcast buffer does not return a "complete" result in this case, but it doesn't mapper
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), emptyResults(TASK_INSTANCE_ID, 1, false));

        // further requests will see a completed result
        assertQueueClosed(buffer, FIRST, 1);
    }

    @Test
    public void testFinishFreesReader()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(
                PipelinedOutputBuffers.createInitial(BROADCAST)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(5));
        assertThat(buffer.getState()).isEqualTo(NO_MORE_BUFFERS);

        // attempt to get a page
        ListenableFuture<BufferResult> future = buffer.get(FIRST, 0, sizeOfPages(10));

        // verify we are waiting for a page
        assertThat(future.isDone()).isFalse();

        // add one item
        addPage(buffer, createPage(0));

        // verify we got one page
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), bufferResult(0, createPage(0)));

        // attempt to get another page, and verify we are blocked
        future = buffer.get(FIRST, 1, sizeOfPages(10));
        assertThat(future.isDone()).isFalse();

        // finish the buffer
        buffer.setNoMorePages();
        assertQueueState(buffer, FIRST, 0, 1);

        // verify the future completed
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), emptyResults(TASK_INSTANCE_ID, 1, true));
    }

    @Test
    public void testFinishFreesWriter()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(
                PipelinedOutputBuffers.createInitial(BROADCAST)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(5));
        assertThat(buffer.getState()).isEqualTo(NO_MORE_BUFFERS);

        // fill the buffer
        for (int i = 0; i < 5; i++) {
            addPage(buffer, createPage(i));
        }

        // enqueue the addition two pages more pages
        ListenableFuture<Void> firstEnqueuePage = enqueuePage(buffer, createPage(5));
        ListenableFuture<Void> secondEnqueuePage = enqueuePage(buffer, createPage(6));

        // get and acknowledge one page
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(1), MAX_WAIT), bufferResult(0, createPage(0)));
        buffer.get(FIRST, 1, sizeOfPages(1)).cancel(true);

        // verify we are still blocked because the buffer is full
        assertThat(firstEnqueuePage.isDone()).isFalse();
        assertThat(secondEnqueuePage.isDone()).isFalse();

        // finish the query
        buffer.setNoMorePages();
        assertThat(buffer.getState()).isEqualTo(FLUSHING);

        // verify futures are complete
        assertFutureIsDone(firstEnqueuePage);
        assertFutureIsDone(secondEnqueuePage);

        // get and acknowledge the last 6 pages
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 1, sizeOfPages(100), NO_WAIT),
                bufferResult(1, createPage(1), createPage(2), createPage(3), createPage(4), createPage(5), createPage(6)));
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 7, sizeOfPages(100), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 7, true));

        buffer.destroy(FIRST);

        // verify finished
        assertFinished(buffer);
    }

    @Test
    public void testDestroyFreesReader()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(
                PipelinedOutputBuffers.createInitial(BROADCAST)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(5));
        assertThat(buffer.getState()).isEqualTo(NO_MORE_BUFFERS);

        // attempt to get a page
        ListenableFuture<BufferResult> future = buffer.get(FIRST, 0, sizeOfPages(10));

        // verify we are waiting for a page
        assertThat(future.isDone()).isFalse();

        // add one page
        addPage(buffer, createPage(0));

        // verify we got one page
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), bufferResult(0, createPage(0)));

        // attempt to get another page, and verify we are blocked
        future = buffer.get(FIRST, 1, sizeOfPages(10));
        assertThat(future.isDone()).isFalse();

        // destroy the buffer
        buffer.destroy();
        assertQueueClosed(buffer, FIRST, 1);

        // verify the future completed
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), emptyResults(TASK_INSTANCE_ID, 1, false));
    }

    @Test
    public void testDestroyFreesWriter()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(
                PipelinedOutputBuffers.createInitial(BROADCAST)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(5));
        assertThat(buffer.getState()).isEqualTo(NO_MORE_BUFFERS);

        // fill the buffer
        for (int i = 0; i < 5; i++) {
            addPage(buffer, createPage(i));
        }

        // add two pages to the buffer queue
        ListenableFuture<Void> firstEnqueuePage = enqueuePage(buffer, createPage(5));
        ListenableFuture<Void> secondEnqueuePage = enqueuePage(buffer, createPage(6));

        // get and acknowledge one page
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(1), MAX_WAIT), bufferResult(0, createPage(0)));
        buffer.get(FIRST, 1, sizeOfPages(1)).cancel(true);

        // verify we are still blocked because the buffer is full
        assertThat(firstEnqueuePage.isDone()).isFalse();
        assertThat(secondEnqueuePage.isDone()).isFalse();

        // destroy the buffer (i.e., cancel the query)
        buffer.destroy();
        assertFinished(buffer);

        // verify the futures are completed
        assertFutureIsDone(firstEnqueuePage);
        assertFutureIsDone(secondEnqueuePage);
    }

    @Test
    public void testFailDoesNotFreeReader()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(
                PipelinedOutputBuffers.createInitial(BROADCAST)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(5));
        assertThat(buffer.getState()).isEqualTo(NO_MORE_BUFFERS);

        // attempt to get a page
        ListenableFuture<BufferResult> future = buffer.get(FIRST, 0, sizeOfPages(10));

        // verify we are waiting for a page
        assertThat(future.isDone()).isFalse();

        // add one page
        addPage(buffer, createPage(0));

        // verify we got one page
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), bufferResult(0, createPage(0)));

        // attempt to get another page, and verify we are blocked
        future = buffer.get(FIRST, 1, sizeOfPages(10));
        assertThat(future.isDone()).isFalse();

        // abort the buffer
        buffer.abort();

        // future should have not finished
        assertThat(future.isDone()).isFalse();

        // attempt to get another page, and verify we are blocked
        future = buffer.get(FIRST, 1, sizeOfPages(10));
        assertThat(future.isDone()).isFalse();
    }

    @Test
    public void testFailFreesWriter()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(
                PipelinedOutputBuffers.createInitial(BROADCAST)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(5));
        assertThat(buffer.getState()).isEqualTo(NO_MORE_BUFFERS);

        // fill the buffer
        for (int i = 0; i < 5; i++) {
            addPage(buffer, createPage(i));
        }

        // add two pages to the buffer queue
        ListenableFuture<Void> firstEnqueuePage = enqueuePage(buffer, createPage(5));
        ListenableFuture<Void> secondEnqueuePage = enqueuePage(buffer, createPage(6));

        // get and acknowledge one page
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(1), MAX_WAIT), bufferResult(0, createPage(0)));
        buffer.get(FIRST, 1, sizeOfPages(1)).cancel(true);

        // verify we are still blocked because the buffer is full
        assertThat(firstEnqueuePage.isDone()).isFalse();
        assertThat(secondEnqueuePage.isDone()).isFalse();

        // abort the buffer (i.e., fail the query)
        buffer.abort();
        assertThat(buffer.getState()).isEqualTo(ABORTED);

        // verify the futures are completed
        assertFutureIsDone(firstEnqueuePage);
        assertFutureIsDone(secondEnqueuePage);
    }

    @Test
    public void testAddBufferAfterFail()
    {
        PipelinedOutputBuffers outputBuffers = PipelinedOutputBuffers.createInitial(BROADCAST)
                .withBuffer(FIRST, BROADCAST_PARTITION_ID);
        BroadcastOutputBuffer buffer = createBroadcastBuffer(outputBuffers, sizeOfPages(5));
        assertThat(buffer.getState()).isEqualTo(OPEN);

        // attempt to get a page
        ListenableFuture<BufferResult> future = buffer.get(FIRST, 0, sizeOfPages(10));

        // verify we are waiting for a page
        assertThat(future.isDone()).isFalse();

        // add one page
        addPage(buffer, createPage(0));

        // verify we got one page
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), bufferResult(0, createPage(0)));

        // abort the buffer
        buffer.abort();

        // add a buffer
        outputBuffers = outputBuffers.withBuffer(SECOND, BROADCAST_PARTITION_ID);
        buffer.setOutputBuffers(outputBuffers);

        // attempt to get page, and verify we are blocked
        future = buffer.get(FIRST, 1, sizeOfPages(10));
        assertThat(future.isDone()).isFalse();
        future = buffer.get(SECOND, 0, sizeOfPages(10));
        assertThat(future.isDone()).isFalse();

        // set no more buffers
        outputBuffers = outputBuffers.withNoMoreBufferIds();
        buffer.setOutputBuffers(outputBuffers);

        // attempt to get page, and verify we are blocked
        future = buffer.get(FIRST, 1, sizeOfPages(10));
        assertThat(future.isDone()).isFalse();
        future = buffer.get(SECOND, 0, sizeOfPages(10));
        assertThat(future.isDone()).isFalse();
    }

    @Test
    public void testBufferCompletion()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(
                PipelinedOutputBuffers.createInitial(BROADCAST)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(5));

        assertThat(buffer.getState()).isEqualTo(NO_MORE_BUFFERS);

        // fill the buffer
        List<Page> pages = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Page page = createPage(i);
            addPage(buffer, page);
            pages.add(page);
        }

        buffer.setNoMorePages();

        // get and acknowledge 5 pages
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(5), MAX_WAIT), createBufferResult(TASK_INSTANCE_ID, 0, pages));

        // there are no more pages and no more buffers, but buffer is not finished because it didn't receive an acknowledgement yet
        assertThat(buffer.getState()).isEqualTo(FLUSHING);

        // ask the buffer to finish
        buffer.destroy(FIRST);

        // verify that the buffer is finished
        assertThat(buffer.getState()).isEqualTo(FINISHED);
    }

    @Test
    public void testSharedBufferBlocking()
    {
        SettableFuture<Void> blockedFuture = SettableFuture.create();
        MockMemoryReservationHandler reservationHandler = new MockMemoryReservationHandler(blockedFuture);
        AggregatedMemoryContext memoryContext = newRootAggregatedMemoryContext(reservationHandler, 0L);

        Page page = createPage(1);
        long pageSize = serializePage(page).getRetainedSize();

        // create a buffer that can only hold two pages
        BroadcastOutputBuffer buffer = createBroadcastBuffer(PipelinedOutputBuffers.createInitial(BROADCAST), DataSize.ofBytes(pageSize * 2), memoryContext, directExecutor());
        OutputBufferMemoryManager memoryManager = buffer.getMemoryManager();

        // adding the first page will block as no memory is available (MockMemoryReservationHandler will return a future that is not done)
        enqueuePage(buffer, page);

        // more memory is available
        blockedFuture.set(null);
        memoryManager.onMemoryAvailable();
        assertThat(memoryManager.getBufferBlockedFuture().isDone())
                .describedAs("buffer shouldn't be blocked")
                .isTrue();

        // we should be able to add one more page after more memory is available
        addPage(buffer, page);

        // the buffer is full now
        enqueuePage(buffer, page);
    }

    @Test
    public void testSharedBufferBlocking2()
    {
        // start with a complete future
        SettableFuture<Void> blockedFuture = SettableFuture.create();
        blockedFuture.set(null);
        MockMemoryReservationHandler reservationHandler = new MockMemoryReservationHandler(blockedFuture);
        AggregatedMemoryContext memoryContext = newRootAggregatedMemoryContext(reservationHandler, 0L);

        Page page = createPage(1);
        long pageSize = serializePage(page).getRetainedSize();

        // create a buffer that can only hold two pages
        BroadcastOutputBuffer buffer = createBroadcastBuffer(PipelinedOutputBuffers.createInitial(BROADCAST), DataSize.ofBytes(pageSize * 2), memoryContext, directExecutor());
        OutputBufferMemoryManager memoryManager = buffer.getMemoryManager();

        // add two pages to fill up the buffer (memory is available)
        addPage(buffer, page);
        addPage(buffer, page);

        // fill up the memory pool
        blockedFuture = SettableFuture.create();
        reservationHandler.updateBlockedFuture(blockedFuture);

        // allocate one more byte to make the buffer full
        memoryManager.updateMemoryUsage(1L);

        // more memory is available
        blockedFuture.set(null);
        memoryManager.onMemoryAvailable();

        // memoryManager should still return a blocked future as the buffer is still full
        assertThat(memoryManager.getBufferBlockedFuture().isDone())
                .describedAs("buffer should be blocked")
                .isFalse();

        // remove all pages from the memory manager and the 1 byte that we added above
        memoryManager.updateMemoryUsage(-pageSize * 2 - 1);

        // now we have both buffer space and memory available, so memoryManager shouldn't be blocked
        assertThat(memoryManager.getBufferBlockedFuture().isDone())
                .describedAs("buffer shouldn't be blocked")
                .isTrue();

        // we should be able to add two pages after more memory is available
        addPage(buffer, page);
        addPage(buffer, page);

        // the buffer is full now
        enqueuePage(buffer, page);
    }

    @Test
    public void testSharedBufferBlockingNoBlockOnFull()
    {
        SettableFuture<Void> blockedFuture = SettableFuture.create();
        MockMemoryReservationHandler reservationHandler = new MockMemoryReservationHandler(blockedFuture);
        AggregatedMemoryContext memoryContext = newRootAggregatedMemoryContext(reservationHandler, 0L);

        Page page = createPage(1);
        long pageSize = serializePage(page).getRetainedSize();

        // create a buffer that can only hold two pages
        BroadcastOutputBuffer buffer = createBroadcastBuffer(PipelinedOutputBuffers.createInitial(BROADCAST), DataSize.ofBytes(pageSize * 2), memoryContext, directExecutor());
        OutputBufferMemoryManager memoryManager = buffer.getMemoryManager();

        memoryManager.setNoBlockOnFull();

        // even if setNoBlockOnFull() is called the buffer should block on memory when we add the first page
        // as no memory is available (MockMemoryReservationHandler will return a future that is not done)
        enqueuePage(buffer, page);

        // more memory is available
        blockedFuture.set(null);
        memoryManager.onMemoryAvailable();
        assertThat(memoryManager.getBufferBlockedFuture().isDone())
                .describedAs("buffer shouldn't be blocked")
                .isTrue();

        // we should be able to add one more page after more memory is available
        addPage(buffer, page);

        // the buffer is full now, but setNoBlockOnFull() is called so the buffer shouldn't block
        addPage(buffer, page);
    }

    private static class MockMemoryReservationHandler
            implements MemoryReservationHandler
    {
        private ListenableFuture<Void> blockedFuture;

        public MockMemoryReservationHandler(ListenableFuture<Void> blockedFuture)
        {
            this.blockedFuture = requireNonNull(blockedFuture, "blockedFuture is null");
        }

        @Override
        public ListenableFuture<Void> reserveMemory(String allocationTag, long delta)
        {
            return blockedFuture;
        }

        @Override
        public boolean tryReserveMemory(String allocationTag, long delta)
        {
            return true;
        }

        public void updateBlockedFuture(ListenableFuture<Void> blockedFuture)
        {
            this.blockedFuture = requireNonNull(blockedFuture);
        }
    }

    private BroadcastOutputBuffer createBroadcastBuffer(OutputBuffers outputBuffers, DataSize dataSize, AggregatedMemoryContext memoryContext, Executor notificationExecutor)
    {
        BroadcastOutputBuffer buffer = new BroadcastOutputBuffer(
                TASK_INSTANCE_ID,
                new OutputBufferStateMachine(new TaskId(new StageId(new QueryId("query"), 0), 0, 0), stateNotificationExecutor),
                dataSize,
                () -> memoryContext.newLocalMemoryContext("test"),
                notificationExecutor,
                () -> {});
        buffer.setOutputBuffers(outputBuffers);
        return buffer;
    }

    @Test
    public void testBufferFinishesWhenClientBuffersDestroyed()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(
                PipelinedOutputBuffers.createInitial(BROADCAST)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withBuffer(SECOND, BROADCAST_PARTITION_ID)
                        .withBuffer(THIRD, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(5));

        // add pages before closing the buffers to make sure
        // that the buffers close even if there are pending pages
        for (int i = 0; i < 5; i++) {
            addPage(buffer, createPage(i));
        }

        // the buffer is in the NO_MORE_BUFFERS state now
        // and if we destroy all the buffers it should destroy itself
        // and move to the FINISHED state
        buffer.destroy(FIRST);
        assertThat(buffer.getState()).isEqualTo(NO_MORE_BUFFERS);
        buffer.destroy(SECOND);
        assertThat(buffer.getState()).isEqualTo(NO_MORE_BUFFERS);
        buffer.destroy(THIRD);
        assertThat(buffer.getState()).isEqualTo(FINISHED);
    }

    @Test
    public void testForceFreeMemory()
    {
        BroadcastOutputBuffer buffer = createBroadcastBuffer(
                PipelinedOutputBuffers.createInitial(BROADCAST)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(5));
        for (int i = 0; i < 3; i++) {
            addPage(buffer, createPage(1), 0);
        }
        OutputBufferMemoryManager memoryManager = buffer.getMemoryManager();
        assertThat(memoryManager.getBufferedBytes() > 0).isTrue();
        buffer.forceFreeMemory();
        assertThat(memoryManager.getBufferedBytes()).isEqualTo(0);
        // adding a page after forceFreeMemory() should be NOOP
        addPage(buffer, createPage(1));
        assertThat(memoryManager.getBufferedBytes()).isEqualTo(0);
    }

    private BroadcastOutputBuffer createBroadcastBuffer(OutputBuffers outputBuffers, DataSize dataSize)
    {
        return createBroadcastBuffer(outputBuffers, dataSize, () -> {});
    }

    private BroadcastOutputBuffer createBroadcastBuffer(OutputBuffers outputBuffers, DataSize dataSize, Runnable notifyStatusChanged)
    {
        BroadcastOutputBuffer buffer = new BroadcastOutputBuffer(
                TASK_INSTANCE_ID,
                new OutputBufferStateMachine(new TaskId(new StageId(new QueryId("query"), 0), 0, 0), stateNotificationExecutor),
                dataSize,
                () -> new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                stateNotificationExecutor,
                notifyStatusChanged);
        buffer.setOutputBuffers(outputBuffers);
        return buffer;
    }

    private static BufferResult bufferResult(long token, Page firstPage, Page... otherPages)
    {
        List<Page> pages = ImmutableList.<Page>builder().add(firstPage).add(otherPages).build();
        return createBufferResult(TASK_INSTANCE_ID, token, pages);
    }
}

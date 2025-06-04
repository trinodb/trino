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
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.execution.buffer.PipelinedOutputBuffers.OutputBufferId;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.execution.buffer.BufferResult.emptyResults;
import static io.trino.execution.buffer.BufferState.ABORTED;
import static io.trino.execution.buffer.BufferState.FINISHED;
import static io.trino.execution.buffer.BufferState.FLUSHING;
import static io.trino.execution.buffer.BufferState.NO_MORE_BUFFERS;
import static io.trino.execution.buffer.BufferState.NO_MORE_PAGES;
import static io.trino.execution.buffer.BufferState.OPEN;
import static io.trino.execution.buffer.BufferTestUtils.MAX_WAIT;
import static io.trino.execution.buffer.BufferTestUtils.NO_WAIT;
import static io.trino.execution.buffer.BufferTestUtils.acknowledgeBufferResult;
import static io.trino.execution.buffer.BufferTestUtils.assertBufferResultEquals;
import static io.trino.execution.buffer.BufferTestUtils.assertFinished;
import static io.trino.execution.buffer.BufferTestUtils.assertFutureIsDone;
import static io.trino.execution.buffer.BufferTestUtils.createBufferResult;
import static io.trino.execution.buffer.BufferTestUtils.createPage;
import static io.trino.execution.buffer.BufferTestUtils.getFuture;
import static io.trino.execution.buffer.BufferTestUtils.serializePage;
import static io.trino.execution.buffer.BufferTestUtils.sizeOfPages;
import static io.trino.execution.buffer.PipelinedOutputBuffers.BROADCAST_PARTITION_ID;
import static io.trino.execution.buffer.PipelinedOutputBuffers.BufferType.ARBITRARY;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestArbitraryOutputBuffer
{
    private static final String TASK_INSTANCE_ID = "task-instance-id";

    private static final List<BigintType> TYPES = ImmutableList.of(BIGINT);
    private static final OutputBufferId FIRST = new OutputBufferId(0);
    private static final OutputBufferId SECOND = new OutputBufferId(1);

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
        assertThatThrownBy(() -> createArbitraryBuffer(PipelinedOutputBuffers.createInitial(ARBITRARY).withBuffer(FIRST, BROADCAST_PARTITION_ID).withNoMoreBufferIds(), DataSize.ofBytes(0)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("maxBufferSize must be at least 1");

        assertThatThrownBy(() -> createArbitraryBuffer(PipelinedOutputBuffers.createInitial(ARBITRARY), DataSize.ofBytes(0)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("maxBufferSize must be at least 1");
    }

    @Test
    public void testSimple()
    {
        PipelinedOutputBuffers outputBuffers = PipelinedOutputBuffers.createInitial(ARBITRARY);
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(outputBuffers, sizeOfPages(10));

        // add three items
        for (int i = 0; i < 3; i++) {
            addPage(buffer, createPage(i));
        }

        outputBuffers = PipelinedOutputBuffers.createInitial(ARBITRARY).withBuffer(FIRST, BROADCAST_PARTITION_ID);

        // add a queue
        buffer.setOutputBuffers(outputBuffers);
        assertQueueState(buffer, 3, FIRST, 0, 0);

        // get the three elements
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(10), NO_WAIT), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        // pages not acknowledged yet so state is the same
        assertQueueState(buffer, 0, FIRST, 3, 0);

        // acknowledge first three pages
        buffer.get(FIRST, 3, sizeOfPages(1)).cancel(true);
        // pages now acknowledged
        assertQueueState(buffer, 0, FIRST, 0, 3);

        // fill the buffer, so that it has 10 buffered pages
        for (int i = 3; i < 13; i++) {
            addPage(buffer, createPage(i));
        }
        // there is a pending read from above so one page will be assigned to the first buffer
        assertQueueState(buffer, 9, FIRST, 1, 3);

        // try to add one more page, which should block
        ListenableFuture<Void> future = enqueuePage(buffer, createPage(13));
        assertThat(future.isDone()).isFalse();
        assertQueueState(buffer, 10, FIRST, 1, 3);

        // remove a page
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 3, sizeOfPages(1), NO_WAIT), bufferResult(3, createPage(3)));
        // page not acknowledged yet so sent count is the same
        assertQueueState(buffer, 10, FIRST, 1, 3);

        // we should still be blocked
        assertThat(future.isDone()).isFalse();

        //
        // add another buffer and verify it sees buffered pages
        outputBuffers = outputBuffers.withBuffer(SECOND, BROADCAST_PARTITION_ID);
        buffer.setOutputBuffers(outputBuffers);
        assertQueueState(buffer, 10, SECOND, 0, 0);
        assertBufferResultEquals(TYPES, getBufferResult(buffer, SECOND, 0, sizeOfPages(10), NO_WAIT), bufferResult(0,
                createPage(4),
                createPage(5),
                createPage(6),
                createPage(7),
                createPage(8),
                createPage(9),
                createPage(10),
                createPage(11),
                createPage(12),
                createPage(13)));
        // page not acknowledged yet so sent count is still zero
        assertQueueState(buffer, 0, SECOND, 10, 0);
        // acknowledge the 10 pages
        buffer.get(SECOND, 10, sizeOfPages(10)).cancel(true);
        assertQueueState(buffer, 0, SECOND, 0, 10);

        //
        // tell shared buffer there will be no more queues
        outputBuffers = outputBuffers.withNoMoreBufferIds();
        buffer.setOutputBuffers(outputBuffers);

        // buffers should see the same stats and the blocked page future from above should be done
        assertQueueState(buffer, 0, FIRST, 1, 3);
        assertQueueState(buffer, 0, SECOND, 0, 10);
        assertFutureIsDone(future);

        // add 3 more pages, buffers always show the same stats
        addPage(buffer, createPage(14));
        addPage(buffer, createPage(15));
        addPage(buffer, createPage(16));
        assertQueueState(buffer, 2, FIRST, 1, 3);
        assertQueueState(buffer, 2, SECOND, 1, 10);

        // pull one page from the second buffer
        assertBufferResultEquals(TYPES, getBufferResult(buffer, SECOND, 10, sizeOfPages(1), NO_WAIT), bufferResult(10, createPage(14)));
        assertQueueState(buffer, 2, FIRST, 1, 3);
        assertQueueState(buffer, 2, SECOND, 1, 10);

        // acknowledge the page in the first buffer and pull remaining ones
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 4, sizeOfPages(10), NO_WAIT), bufferResult(4, createPage(15), createPage(16)));
        assertQueueState(buffer, 0, FIRST, 2, 4);
        assertQueueState(buffer, 0, SECOND, 1, 10);

        //
        // finish the buffer
        assertThat(buffer.getState()).isEqualTo(NO_MORE_BUFFERS);
        buffer.setNoMorePages();
        assertQueueState(buffer, 0, FIRST, 2, 4);
        assertQueueState(buffer, 0, SECOND, 1, 10);

        // not fully finished until all pages are consumed
        assertThat(buffer.getState()).isEqualTo(FLUSHING);

        // acknowledge the pages from the first buffer; buffer should not close automatically
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 6, sizeOfPages(10), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 6, true));
        assertQueueState(buffer, 0, FIRST, 0, 6);
        assertQueueState(buffer, 0, SECOND, 1, 10);
        assertThat(buffer.getState()).isEqualTo(FLUSHING);

        // finish first queue
        buffer.destroy(FIRST);
        assertQueueClosed(buffer, 0, FIRST, 6);
        assertQueueState(buffer, 0, SECOND, 1, 10);
        assertThat(buffer.getState()).isEqualTo(FLUSHING);

        // acknowledge a page from the second queue; queue should not close automatically
        assertBufferResultEquals(TYPES, getBufferResult(buffer, SECOND, 11, sizeOfPages(1), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 11, true));
        assertQueueState(buffer, 0, SECOND, 0, 11);
        assertThat(buffer.getState()).isEqualTo(FLUSHING);

        // finish second queue
        buffer.destroy(SECOND);
        assertQueueClosed(buffer, 0, FIRST, 6);
        assertQueueClosed(buffer, 0, SECOND, 11);
        assertFinished(buffer);

        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 6, sizeOfPages(10), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 6, true));
        assertBufferResultEquals(TYPES, getBufferResult(buffer, SECOND, 11, sizeOfPages(10), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 11, true));
    }

    // TODO: remove this after PR is landed: https://github.com/prestodb/presto/pull/7987
    @Test
    public void testAcknowledge()
    {
        OutputBuffers outputBuffers = PipelinedOutputBuffers.createInitial(ARBITRARY);
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(outputBuffers, sizeOfPages(10));

        // add three items
        for (int i = 0; i < 3; i++) {
            addPage(buffer, createPage(i));
        }

        outputBuffers = PipelinedOutputBuffers.createInitial(ARBITRARY).withBuffer(FIRST, BROADCAST_PARTITION_ID);

        // add a queue
        buffer.setOutputBuffers(outputBuffers);
        assertQueueState(buffer, 3, FIRST, 0, 0);

        // get the three elements
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(10), NO_WAIT), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        // acknowledge pages 0 and 1
        acknowledgeBufferResult(buffer, FIRST, 2);
        // only page 2 is not removed
        assertQueueState(buffer, 0, FIRST, 1, 2);
        // acknowledge page 2
        acknowledgeBufferResult(buffer, FIRST, 3);
        // nothing left
        assertQueueState(buffer, 0, FIRST, 0, 3);
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
        assertQueueState(buffer, 3, FIRST, 0, 3);

        // getting new pages will again acknowledge the previously acknowledged pages but this is ok
        buffer.get(FIRST, 3, sizeOfPages(1)).cancel(true);
        assertQueueState(buffer, 2, FIRST, 1, 3);
    }

    @Test
    public void testBufferFull()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(PipelinedOutputBuffers.createInitial(ARBITRARY), sizeOfPages(2));

        // Add two pages, buffer is full
        addPage(buffer, createPage(1));
        addPage(buffer, createPage(2));

        // third page is blocked
        enqueuePage(buffer, createPage(3));
    }

    @Test
    public void testDuplicateRequests()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(
                PipelinedOutputBuffers.createInitial(ARBITRARY)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(10));

        // add three items
        for (int i = 0; i < 3; i++) {
            addPage(buffer, createPage(i));
        }

        // add a queue
        assertQueueState(buffer, 3, FIRST, 0, 0);

        // get the three elements
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(10), NO_WAIT), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        // pages not acknowledged yet so state is the same
        assertQueueState(buffer, 0, FIRST, 3, 0);

        // get the three elements again
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(10), NO_WAIT), bufferResult(0, createPage(0), createPage(1), createPage(2)));
        // pages not acknowledged yet so state is the same
        assertQueueState(buffer, 0, FIRST, 3, 0);

        // acknowledge the pages
        buffer.get(FIRST, 3, sizeOfPages(10)).cancel(true);

        // attempt to get the three elements again
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(10), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 0, false));
        // pages are acknowledged
        assertQueueState(buffer, 0, FIRST, 0, 3);
    }

    @Test
    public void testAddQueueAfterCreation()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(
                PipelinedOutputBuffers.createInitial(ARBITRARY)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(10));

        assertThat(buffer.getState()).isEqualTo(NO_MORE_BUFFERS);

        assertThatThrownBy(() -> buffer.setOutputBuffers(PipelinedOutputBuffers.createInitial(ARBITRARY)
                .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                .withBuffer(SECOND, BROADCAST_PARTITION_ID)
                .withNoMoreBufferIds()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Expected buffer to not change after no more buffers is set");
    }

    @Test
    public void testAddAfterFinish()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(
                PipelinedOutputBuffers.createInitial(ARBITRARY)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(10));
        buffer.setNoMorePages();
        addPage(buffer, createPage(0));
        addPage(buffer, createPage(1));
        assertThat(buffer.getInfo().getTotalPagesSent()).isEqualTo(0);
    }

    @Test
    public void testAddQueueAfterNoMoreQueues()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(PipelinedOutputBuffers.createInitial(ARBITRARY), sizeOfPages(10));
        assertThat(buffer.getState()).isEqualTo(OPEN);

        // tell buffer no more queues will be added
        buffer.setOutputBuffers(PipelinedOutputBuffers.createInitial(ARBITRARY).withNoMoreBufferIds());
        assertThat(buffer.getState()).isEqualTo(NO_MORE_BUFFERS);

        // set no more queues a second time to assure that we don't get an exception or such
        buffer.setOutputBuffers(PipelinedOutputBuffers.createInitial(ARBITRARY).withNoMoreBufferIds());
        assertThat(buffer.getState()).isEqualTo(NO_MORE_BUFFERS);

        // set no more queues a third time to assure that we don't get an exception or such
        buffer.setOutputBuffers(PipelinedOutputBuffers.createInitial(ARBITRARY).withNoMoreBufferIds());
        assertThat(buffer.getState()).isEqualTo(NO_MORE_BUFFERS);

        OutputBuffers outputBuffers = PipelinedOutputBuffers.createInitial(ARBITRARY)
                .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                .withNoMoreBufferIds();
        assertThatThrownBy(() -> buffer.setOutputBuffers(outputBuffers))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Expected buffer to not change after no more buffers is set");
    }

    @Test
    public void testAddAfterDestroy()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(
                PipelinedOutputBuffers.createInitial(ARBITRARY)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(10));
        buffer.destroy();
        addPage(buffer, createPage(0));
        addPage(buffer, createPage(1));
        assertThat(buffer.getInfo().getTotalPagesSent()).isEqualTo(0);
    }

    @Test
    public void testGetBeforeCreate()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(PipelinedOutputBuffers.createInitial(ARBITRARY), sizeOfPages(10));
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
    public void testResumeFromPreviousPosition()
    {
        PipelinedOutputBuffers outputBuffers = PipelinedOutputBuffers.createInitial(ARBITRARY);
        OutputBufferId[] ids = new OutputBufferId[5];
        for (int i = 0; i < ids.length; i++) {
            ids[i] = new OutputBufferId(i);
            outputBuffers = outputBuffers.withBuffer(ids[i], i);
        }

        ArbitraryOutputBuffer buffer = createArbitraryBuffer(outputBuffers, sizeOfPages(5));
        assertThat(buffer.getState()).isEqualTo(OPEN);

        Map<OutputBufferId, ListenableFuture<BufferResult>> firstReads = new HashMap<>();
        for (OutputBufferId id : ids) {
            firstReads.put(id, buffer.get(id, 0L, sizeOfPages(1)));
        }
        // All must be blocked initially
        assertThat(firstReads.values()).noneMatch(Future::isDone);

        List<ListenableFuture<BufferResult>> secondReads = new ArrayList<>();

        for (int i = 0; i < ids.length; i++) {
            // add one page
            addPage(buffer, createPage(33));
            assertThat(secondReads).allMatch(future -> !future.isDone(), "No secondary reads should complete until after all first reads");
            List<OutputBufferId> completedIds = firstReads.entrySet().stream()
                    .filter(entry -> entry.getValue().isDone())
                    .map(Map.Entry::getKey)
                    .collect(toList());
            assertThat(completedIds.size())
                    .describedAs("One completed buffer read per page addition")
                    .isEqualTo(1);
            OutputBufferId completed = completedIds.get(0);

            BufferResult result = getFuture(firstReads.remove(completed), NO_WAIT);
            // Store completion order of first for follow up sequence
            secondReads.add(buffer.get(completed, result.getNextToken(), sizeOfPages(1)));
        }
        // Test sanity
        assertThat(secondReads).hasSize(ids.length);

        // Completion order should be identical to the first iteration at this point
        for (int i = 0; i < ids.length; i++) {
            // add one page
            addPage(buffer, createPage(33));
            assertThat(secondReads.get(i).isDone())
                    .describedAs("Invalid second read completion order at index: " + i)
                    .isTrue();
        }
    }

    @Test
    public void testUseUndeclaredBufferAfterFinalBuffersSet()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(
                PipelinedOutputBuffers.createInitial(ARBITRARY)
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
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(PipelinedOutputBuffers.createInitial(ARBITRARY), sizeOfPages(10));
        assertThat(buffer.getState()).isEqualTo(OPEN);

        // get a page from a buffer that doesn't exist yet
        ListenableFuture<BufferResult> future = buffer.get(FIRST, 0L, sizeOfPages(1));
        assertThat(future.isDone()).isFalse();

        // destroy that buffer, and verify the future is finished
        buffer.destroy(FIRST);
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), emptyResults(TASK_INSTANCE_ID, 0, false));
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(10), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 0, true));

        // add a page and verify the future is not complete
        addPage(buffer, createPage(33));

        // add the buffer and verify we did not get the page
        buffer.setOutputBuffers(PipelinedOutputBuffers.createInitial(ARBITRARY).withBuffer(FIRST, 0));
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(10), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 0, true));
    }

    @Test
    public void testFullBufferBlocksWriter()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(
                PipelinedOutputBuffers.createInitial(ARBITRARY)
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
    public void testAbort()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(PipelinedOutputBuffers.createInitial(ARBITRARY), sizeOfPages(10));

        // fill the buffer
        for (int i = 0; i < 10; i++) {
            addPage(buffer, createPage(i));
        }
        buffer.setNoMorePages();

        // add one output buffer
        PipelinedOutputBuffers outputBuffers = PipelinedOutputBuffers.createInitial(ARBITRARY).withBuffer(FIRST, 0);
        buffer.setOutputBuffers(outputBuffers);

        // read a page from the first buffer
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(1), NO_WAIT), bufferResult(0, createPage(0)));

        // destroy buffer, and verify page cannot be acknowledged
        buffer.destroy(FIRST);
        assertQueueClosed(buffer, 9, FIRST, 0);
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 1, sizeOfPages(1), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 0, true));

        outputBuffers = outputBuffers.withBuffer(SECOND, 0).withNoMoreBufferIds();
        buffer.setOutputBuffers(outputBuffers);

        // first page is lost because the first buffer was destroyed
        assertBufferResultEquals(TYPES, getBufferResult(buffer, SECOND, 0, sizeOfPages(1), NO_WAIT), bufferResult(0, createPage(1)));
        buffer.destroy(SECOND);
        assertQueueClosed(buffer, 0, SECOND, 0);
        assertFinished(buffer);
        assertBufferResultEquals(TYPES, getBufferResult(buffer, SECOND, 1, sizeOfPages(1), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 0, true));
    }

    @Test
    public void testFinishClosesEmptyQueues()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(
                PipelinedOutputBuffers.createInitial(ARBITRARY)
                        .withBuffer(FIRST, BROADCAST_PARTITION_ID)
                        .withBuffer(SECOND, BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds(),
                sizeOfPages(10));

        // finish while queues are empty
        buffer.setNoMorePages();

        assertQueueState(buffer, 0, FIRST, 0, 0);
        assertQueueState(buffer, 0, SECOND, 0, 0);

        buffer.destroy(FIRST);
        buffer.destroy(SECOND);

        assertQueueClosed(buffer, 0, FIRST, 0);
        assertQueueClosed(buffer, 0, SECOND, 0);
    }

    @Test
    public void testAbortFreesReader()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(PipelinedOutputBuffers.createInitial(ARBITRARY), sizeOfPages(10));
        buffer.setOutputBuffers(PipelinedOutputBuffers.createInitial(ARBITRARY).withBuffer(FIRST, 0));
        assertThat(buffer.getState()).isEqualTo(OPEN);

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

        // destroy the buffer
        buffer.destroy(FIRST);
        assertQueueClosed(buffer, 0, FIRST, 1);

        // verify the future completed
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), emptyResults(TASK_INSTANCE_ID, 1, false));
    }

    @Test
    public void testFinishFreesReader()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(PipelinedOutputBuffers.createInitial(ARBITRARY), sizeOfPages(10));
        buffer.setOutputBuffers(PipelinedOutputBuffers.createInitial(ARBITRARY).withBuffer(FIRST, 0));
        assertThat(buffer.getState()).isEqualTo(OPEN);

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
        assertQueueState(buffer, 0, FIRST, 0, 1);
        buffer.destroy(FIRST);
        assertQueueClosed(buffer, 0, FIRST, 1);

        // verify the future completed
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), emptyResults(TASK_INSTANCE_ID, 1, false));
    }

    @Test
    public void testFinishFreesWriter()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(PipelinedOutputBuffers.createInitial(ARBITRARY), sizeOfPages(5));
        buffer.setOutputBuffers(PipelinedOutputBuffers.createInitial(ARBITRARY)
                .withBuffer(FIRST, 0)
                .withNoMoreBufferIds());
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
        buffer.get(FIRST, 1, sizeOfPages(100)).cancel(true);

        // verify we are still blocked because the buffer is full
        assertThat(firstEnqueuePage.isDone()).isFalse();
        assertThat(secondEnqueuePage.isDone()).isFalse();

        // finish the query
        buffer.setNoMorePages();
        assertThat(buffer.getState()).isEqualTo(FLUSHING);

        // verify futures are complete
        assertFutureIsDone(firstEnqueuePage);
        assertFutureIsDone(secondEnqueuePage);

        // get and acknowledge the last 5 pages
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 1, sizeOfPages(100), NO_WAIT),
                bufferResult(1, createPage(1), createPage(2), createPage(3), createPage(4), createPage(5), createPage(6)));
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 7, sizeOfPages(100), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 7, true));

        // verify not finished
        assertThat(buffer.getState()).isEqualTo(FLUSHING);

        // finish the queue
        buffer.destroy(FIRST);

        // verify finished
        assertFinished(buffer);
    }

    @Test
    public void testDestroyFreesReader()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(PipelinedOutputBuffers.createInitial(ARBITRARY), sizeOfPages(5));
        buffer.setOutputBuffers(PipelinedOutputBuffers.createInitial(ARBITRARY)
                .withBuffer(FIRST, 0)
                .withNoMoreBufferIds());
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
        assertQueueClosed(buffer, 0, FIRST, 1);

        // verify the future completed
        assertBufferResultEquals(TYPES, getFuture(future, NO_WAIT), emptyResults(TASK_INSTANCE_ID, 1, false));
    }

    @Test
    public void testDestroyFreesWriter()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(PipelinedOutputBuffers.createInitial(ARBITRARY), sizeOfPages(5));
        buffer.setOutputBuffers(PipelinedOutputBuffers.createInitial(ARBITRARY)
                .withBuffer(FIRST, 0)
                .withNoMoreBufferIds());
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
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(
                PipelinedOutputBuffers.createInitial(ARBITRARY)
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
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(
                PipelinedOutputBuffers.createInitial(ARBITRARY)
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
        PipelinedOutputBuffers outputBuffers = PipelinedOutputBuffers.createInitial(ARBITRARY)
                .withBuffer(FIRST, BROADCAST_PARTITION_ID);
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(outputBuffers, sizeOfPages(5));
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
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(PipelinedOutputBuffers.createInitial(ARBITRARY), sizeOfPages(5));
        buffer.setOutputBuffers(PipelinedOutputBuffers.createInitial(ARBITRARY)
                .withBuffer(FIRST, 0)
                .withNoMoreBufferIds());

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
    public void testNoMorePagesFreesReader()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(PipelinedOutputBuffers.createInitial(ARBITRARY), sizeOfPages(10));
        buffer.setOutputBuffers(PipelinedOutputBuffers.createInitial(ARBITRARY).withBuffer(FIRST, 0));
        assertThat(buffer.getState()).isEqualTo(OPEN);

        ListenableFuture<BufferResult> future = buffer.get(FIRST, 0, sizeOfPages(10));
        assertThat(future.isDone()).isFalse();

        buffer.setNoMorePages();

        assertThat(future.isDone()).isTrue();
        assertThat(buffer.get(FIRST, 0, sizeOfPages(10)).isDone()).isTrue();
    }

    @Test
    public void testFinishBeforeNoMoreBuffers()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(PipelinedOutputBuffers.createInitial(ARBITRARY), sizeOfPages(10));

        // fill the buffer
        for (int i = 0; i < 3; i++) {
            addPage(buffer, createPage(i));
        }
        buffer.setNoMorePages();
        assertThat(buffer.getState()).isEqualTo(NO_MORE_PAGES);

        // add one output buffer
        PipelinedOutputBuffers outputBuffers = PipelinedOutputBuffers.createInitial(ARBITRARY).withBuffer(FIRST, 0);
        buffer.setOutputBuffers(outputBuffers);
        assertThat(buffer.getState()).isEqualTo(NO_MORE_PAGES);

        // read a page from the first buffer
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 0, sizeOfPages(1), NO_WAIT), bufferResult(0, createPage(0)));
        assertThat(buffer.getState()).isEqualTo(NO_MORE_PAGES);

        // read remaining pages from the first buffer and acknowledge
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 1, sizeOfPages(10), NO_WAIT), bufferResult(1, createPage(1), createPage(2)));
        assertBufferResultEquals(TYPES, getBufferResult(buffer, FIRST, 3, sizeOfPages(1), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 3, true));
        assertThat(buffer.getState()).isEqualTo(NO_MORE_PAGES);

        // finish first queue
        buffer.destroy(FIRST);
        assertQueueClosed(buffer, 0, FIRST, 3);
        assertFinished(buffer);

        // add another buffer after finish
        outputBuffers = outputBuffers.withBuffer(SECOND, 0);
        buffer.setOutputBuffers(outputBuffers);

        // verify second buffer has no results
        assertBufferResultEquals(TYPES, getBufferResult(buffer, SECOND, 0, sizeOfPages(1), NO_WAIT), emptyResults(TASK_INSTANCE_ID, 0, true));
    }

    @Test
    public void testForceFreeMemory()
    {
        ArbitraryOutputBuffer buffer = createArbitraryBuffer(PipelinedOutputBuffers.createInitial(ARBITRARY), sizeOfPages(10));
        for (int i = 0; i < 3; i++) {
            addPage(buffer, createPage(i));
        }
        OutputBufferMemoryManager memoryManager = buffer.getMemoryManager();
        assertThat(memoryManager.getBufferedBytes() > 0).isTrue();
        buffer.forceFreeMemory();
        assertThat(memoryManager.getBufferedBytes()).isEqualTo(0);
        // adding a page after forceFreeMemory() should be NOOP
        addPage(buffer, createPage(1));
        assertThat(memoryManager.getBufferedBytes()).isEqualTo(0);
    }

    private static BufferResult getBufferResult(OutputBuffer buffer, OutputBufferId bufferId, long sequenceId, DataSize maxSize, Duration maxWait)
    {
        ListenableFuture<BufferResult> future = buffer.get(bufferId, sequenceId, maxSize);
        return getFuture(future, maxWait);
    }

    private static ListenableFuture<Void> enqueuePage(OutputBuffer buffer, Page page)
    {
        buffer.enqueue(ImmutableList.of(serializePage(page)));
        ListenableFuture<Void> future = buffer.isFull();
        assertThat(future.isDone()).isFalse();
        return future;
    }

    private static void addPage(OutputBuffer buffer, Page page)
    {
        buffer.enqueue(ImmutableList.of(serializePage(page)));
        assertThat(buffer.isFull().isDone())
                .describedAs("Expected add page to not block")
                .isTrue();
    }

    private static void assertQueueState(
            OutputBuffer buffer,
            int unassignedPages,
            OutputBufferId bufferId,
            int bufferedPages,
            int pagesSent)
    {
        OutputBufferInfo outputBufferInfo = buffer.getInfo();

        long assignedPages = outputBufferInfo.getPipelinedBufferStates().orElse(ImmutableList.of()).stream().mapToInt(PipelinedBufferInfo::getBufferedPages).sum();

        assertThat(outputBufferInfo.getTotalBufferedPages() - assignedPages)
                .describedAs("unassignedPages")
                .isEqualTo(unassignedPages);

        PipelinedBufferInfo bufferInfo = outputBufferInfo.getPipelinedBufferStates().orElse(ImmutableList.of()).stream()
                .filter(info -> info.getBufferId().equals(bufferId))
                .findAny()
                .orElse(null);

        assertThat(bufferInfo).isEqualTo(new PipelinedBufferInfo(
                bufferId,
                // every page has one row
                bufferedPages + pagesSent,
                bufferedPages + pagesSent,
                bufferedPages,
                sizeOfPages(bufferedPages).toBytes(),
                pagesSent,
                false));
    }

    @SuppressWarnings("ConstantConditions")
    private static void assertQueueClosed(OutputBuffer buffer, int unassignedPages, OutputBufferId bufferId, int pagesSent)
    {
        OutputBufferInfo outputBufferInfo = buffer.getInfo();

        long assignedPages = outputBufferInfo.getPipelinedBufferStates().orElse(ImmutableList.of()).stream().mapToInt(PipelinedBufferInfo::getBufferedPages).sum();
        assertThat(outputBufferInfo.getTotalBufferedPages() - assignedPages)
                .describedAs("unassignedPages")
                .isEqualTo(unassignedPages);

        PipelinedBufferInfo bufferInfo = outputBufferInfo.getPipelinedBufferStates().orElse(ImmutableList.of()).stream()
                .filter(info -> info.getBufferId().equals(bufferId))
                .findAny()
                .orElse(null);

        assertThat(bufferInfo.getBufferedPages()).isEqualTo(0);
        assertThat(bufferInfo.getPagesSent()).isEqualTo(pagesSent);
        assertThat(bufferInfo.isFinished()).isTrue();
    }

    private ArbitraryOutputBuffer createArbitraryBuffer(OutputBuffers buffers, DataSize dataSize)
    {
        ArbitraryOutputBuffer buffer = new ArbitraryOutputBuffer(
                TASK_INSTANCE_ID,
                new OutputBufferStateMachine(new TaskId(new StageId(new QueryId("query"), 0), 0, 0), stateNotificationExecutor),
                dataSize,
                () -> new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                stateNotificationExecutor);
        buffer.setOutputBuffers(buffers);
        return buffer;
    }

    private static BufferResult bufferResult(long token, Page firstPage, Page... otherPages)
    {
        List<Page> pages = ImmutableList.<Page>builder().add(firstPage).add(otherPages).build();
        return createBufferResult(TASK_INSTANCE_ID, token, pages);
    }
}

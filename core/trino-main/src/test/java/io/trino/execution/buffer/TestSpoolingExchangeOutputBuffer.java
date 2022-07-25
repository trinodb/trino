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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.spi.QueryId;
import io.trino.spi.exchange.ExchangeSink;
import io.trino.spi.exchange.ExchangeSinkInstanceHandle;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.execution.buffer.BufferState.ABORTED;
import static io.trino.execution.buffer.BufferState.FAILED;
import static io.trino.execution.buffer.BufferState.FINISHED;
import static io.trino.execution.buffer.BufferState.FLUSHING;
import static io.trino.execution.buffer.BufferState.NO_MORE_BUFFERS;
import static io.trino.execution.buffer.OutputBuffers.createSpoolingExchangeOutputBuffers;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestSpoolingExchangeOutputBuffer
{
    @Test
    public void testIsFull()
    {
        TestingExchangeSink exchangeSink = new TestingExchangeSink();
        OutputBuffer outputBuffer = createSpoolingExchangeOutputBuffer(exchangeSink);
        assertEquals(outputBuffer.getState(), NO_MORE_BUFFERS);
        assertNotBlocked(outputBuffer.isFull());

        CompletableFuture<Void> blocked = new CompletableFuture<>();
        exchangeSink.setBlocked(blocked);

        ListenableFuture<Void> full = outputBuffer.isFull();
        assertBlocked(full);

        blocked.complete(null);
        assertNotBlocked(full);
    }

    @Test
    public void testFinishSuccess()
    {
        TestingExchangeSink exchangeSink = new TestingExchangeSink();
        CompletableFuture<Void> finish = new CompletableFuture<>();
        exchangeSink.setFinish(finish);

        OutputBuffer outputBuffer = createSpoolingExchangeOutputBuffer(exchangeSink);
        assertEquals(outputBuffer.getState(), NO_MORE_BUFFERS);

        outputBuffer.setNoMorePages();
        // call it for the second time to verify that the buffer handles it correctly
        outputBuffer.setNoMorePages();
        assertEquals(outputBuffer.getState(), FLUSHING);

        finish.complete(null);
        assertEquals(outputBuffer.getState(), FINISHED);
    }

    @Test
    public void testFinishFailure()
    {
        TestingExchangeSink exchangeSink = new TestingExchangeSink();
        CompletableFuture<Void> finish = new CompletableFuture<>();
        exchangeSink.setFinish(finish);

        OutputBuffer outputBuffer = createSpoolingExchangeOutputBuffer(exchangeSink);
        assertEquals(outputBuffer.getState(), NO_MORE_BUFFERS);

        outputBuffer.setNoMorePages();
        // call it for the second time to verify that the buffer handles it correctly
        outputBuffer.setNoMorePages();
        assertEquals(outputBuffer.getState(), FLUSHING);

        RuntimeException failure = new RuntimeException("failure");
        finish.completeExceptionally(failure);
        assertEquals(outputBuffer.getState(), FAILED);
        assertEquals(outputBuffer.getFailureCause(), Optional.of(failure));
    }

    @Test
    public void testDestroyAfterFinishCompletion()
    {
        TestingExchangeSink exchangeSink = new TestingExchangeSink();
        CompletableFuture<Void> finish = new CompletableFuture<>();
        exchangeSink.setFinish(finish);

        OutputBuffer outputBuffer = createSpoolingExchangeOutputBuffer(exchangeSink);
        assertEquals(outputBuffer.getState(), NO_MORE_BUFFERS);

        outputBuffer.setNoMorePages();
        // call it for the second time to verify that the buffer handles it correctly
        outputBuffer.setNoMorePages();
        assertEquals(outputBuffer.getState(), FLUSHING);

        finish.complete(null);
        assertEquals(outputBuffer.getState(), FINISHED);

        outputBuffer.destroy();
        assertEquals(outputBuffer.getState(), FINISHED);
    }

    @Test
    public void testDestroyBeforeFinishCompletion()
    {
        TestingExchangeSink exchangeSink = new TestingExchangeSink();
        CompletableFuture<Void> finish = new CompletableFuture<>();
        exchangeSink.setFinish(finish);

        OutputBuffer outputBuffer = createSpoolingExchangeOutputBuffer(exchangeSink);
        assertEquals(outputBuffer.getState(), NO_MORE_BUFFERS);

        outputBuffer.setNoMorePages();
        assertEquals(outputBuffer.getState(), FLUSHING);

        outputBuffer.destroy();
        assertEquals(outputBuffer.getState(), ABORTED);

        finish.complete(null);
        assertEquals(outputBuffer.getState(), ABORTED);
    }

    @Test
    public void testAbortBeforeNoMorePages()
    {
        TestingExchangeSink exchangeSink = new TestingExchangeSink();

        OutputBuffer outputBuffer = createSpoolingExchangeOutputBuffer(exchangeSink);
        assertEquals(outputBuffer.getState(), NO_MORE_BUFFERS);

        outputBuffer.abort();
        assertEquals(outputBuffer.getState(), ABORTED);
        outputBuffer.setNoMorePages();
        assertEquals(outputBuffer.getState(), ABORTED);
    }

    @Test
    public void testAbortBeforeFinishCompletion()
    {
        TestingExchangeSink exchangeSink = new TestingExchangeSink();
        CompletableFuture<Void> finish = new CompletableFuture<>();
        exchangeSink.setFinish(finish);
        CompletableFuture<Void> abort = new CompletableFuture<>();
        exchangeSink.setAbort(abort);

        OutputBuffer outputBuffer = createSpoolingExchangeOutputBuffer(exchangeSink);
        assertEquals(outputBuffer.getState(), NO_MORE_BUFFERS);

        outputBuffer.setNoMorePages();
        // call it for the second time to verify that the buffer handles it correctly
        outputBuffer.setNoMorePages();
        assertEquals(outputBuffer.getState(), FLUSHING);

        // if abort is called before finish completes it should abort the buffer
        outputBuffer.abort();
        assertEquals(outputBuffer.getState(), ABORTED);

        // abort failure shouldn't impact the buffer state
        abort.completeExceptionally(new RuntimeException("failure"));
        assertEquals(outputBuffer.getState(), ABORTED);
    }

    @Test
    public void testAbortAfterFinishCompletion()
    {
        TestingExchangeSink exchangeSink = new TestingExchangeSink();
        CompletableFuture<Void> finish = new CompletableFuture<>();
        exchangeSink.setFinish(finish);
        CompletableFuture<Void> abort = new CompletableFuture<>();
        exchangeSink.setAbort(abort);

        OutputBuffer outputBuffer = createSpoolingExchangeOutputBuffer(exchangeSink);
        assertEquals(outputBuffer.getState(), NO_MORE_BUFFERS);

        outputBuffer.setNoMorePages();
        // call it for the second time to verify that the buffer handles it correctly
        outputBuffer.setNoMorePages();
        assertEquals(outputBuffer.getState(), FLUSHING);

        finish.complete(null);
        assertEquals(outputBuffer.getState(), FINISHED);

        // abort is no op
        outputBuffer.abort();
        assertEquals(outputBuffer.getState(), FINISHED);

        // abort success doesn't change the buffer state
        abort.complete(null);
        assertEquals(outputBuffer.getState(), FINISHED);
    }

    @Test
    public void testEnqueueAfterFinish()
    {
        TestingExchangeSink exchangeSink = new TestingExchangeSink();
        CompletableFuture<Void> finish = new CompletableFuture<>();
        exchangeSink.setFinish(finish);

        OutputBuffer outputBuffer = createSpoolingExchangeOutputBuffer(exchangeSink);
        assertEquals(outputBuffer.getState(), NO_MORE_BUFFERS);

        outputBuffer.enqueue(0, ImmutableList.of(utf8Slice("page1")));
        outputBuffer.enqueue(1, ImmutableList.of(utf8Slice("page2"), utf8Slice("page3")));

        ImmutableListMultimap<Integer, Slice> expectedDataBufferState = ImmutableListMultimap.<Integer, Slice>builder()
                .put(0, utf8Slice("page1"))
                .put(1, utf8Slice("page2"))
                .put(1, utf8Slice("page3"))
                .build();

        assertEquals(exchangeSink.getDataBuffer(), expectedDataBufferState);

        outputBuffer.setNoMorePages();
        assertEquals(outputBuffer.getState(), FLUSHING);
        // the buffer is flushing, this page is expected to be rejected
        outputBuffer.enqueue(0, ImmutableList.of(utf8Slice("page4")));
        assertEquals(exchangeSink.getDataBuffer(), expectedDataBufferState);

        finish.complete(null);
        assertEquals(outputBuffer.getState(), FINISHED);
        outputBuffer.enqueue(0, ImmutableList.of(utf8Slice("page5")));
        assertEquals(exchangeSink.getDataBuffer(), expectedDataBufferState);
    }

    @Test
    public void testEnqueueAfterAbort()
    {
        TestingExchangeSink exchangeSink = new TestingExchangeSink();
        CompletableFuture<Void> abort = new CompletableFuture<>();
        exchangeSink.setAbort(abort);

        OutputBuffer outputBuffer = createSpoolingExchangeOutputBuffer(exchangeSink);
        assertEquals(outputBuffer.getState(), NO_MORE_BUFFERS);

        outputBuffer.enqueue(0, ImmutableList.of(utf8Slice("page1")));
        outputBuffer.enqueue(1, ImmutableList.of(utf8Slice("page2"), utf8Slice("page3")));

        ImmutableListMultimap<Integer, Slice> expectedDataBufferState = ImmutableListMultimap.<Integer, Slice>builder()
                .put(0, utf8Slice("page1"))
                .put(1, utf8Slice("page2"))
                .put(1, utf8Slice("page3"))
                .build();

        assertEquals(exchangeSink.getDataBuffer(), expectedDataBufferState);

        outputBuffer.abort();
        assertEquals(outputBuffer.getState(), ABORTED);
        // the buffer is flushing, this page is expected to be rejected
        outputBuffer.enqueue(0, ImmutableList.of(utf8Slice("page4")));
        assertEquals(exchangeSink.getDataBuffer(), expectedDataBufferState);

        abort.complete(null);
        assertEquals(outputBuffer.getState(), ABORTED);
        outputBuffer.enqueue(0, ImmutableList.of(utf8Slice("page5")));
        assertEquals(exchangeSink.getDataBuffer(), expectedDataBufferState);
    }

    private static SpoolingExchangeOutputBuffer createSpoolingExchangeOutputBuffer(ExchangeSink exchangeSink)
    {
        return new SpoolingExchangeOutputBuffer(
                new OutputBufferStateMachine(new TaskId(new StageId(new QueryId("query"), 0), 0, 0), directExecutor()),
                createSpoolingExchangeOutputBuffers(TestingExchangeSinkInstanceHandle.INSTANCE),
                exchangeSink,
                TestingLocalMemoryContext::new);
    }

    private static void assertNotBlocked(ListenableFuture<Void> blocked)
    {
        assertTrue(blocked.isDone());
    }

    private static void assertBlocked(ListenableFuture<Void> blocked)
    {
        assertFalse(blocked.isDone());
    }

    private static class TestingExchangeSink
            implements ExchangeSink
    {
        private final ListMultimap<Integer, Slice> dataBuffer = ArrayListMultimap.create();
        private CompletableFuture<Void> blocked = CompletableFuture.completedFuture(null);
        private CompletableFuture<Void> finish = CompletableFuture.completedFuture(null);
        private CompletableFuture<Void> abort = CompletableFuture.completedFuture(null);

        private boolean finishCalled;
        private boolean abortCalled;

        @Override
        public CompletableFuture<Void> isBlocked()
        {
            return blocked;
        }

        public void setBlocked(CompletableFuture<Void> blocked)
        {
            this.blocked = requireNonNull(blocked, "blocked is null");
        }

        @Override
        public void add(int partitionId, Slice data)
        {
            this.dataBuffer.put(partitionId, data);
        }

        public ListMultimap<Integer, Slice> getDataBuffer()
        {
            return dataBuffer;
        }

        @Override

        public long getMemoryUsage()
        {
            return 0;
        }

        @Override
        public CompletableFuture<Void> finish()
        {
            assertFalse(abortCalled);
            assertFalse(finishCalled);
            finishCalled = true;
            return finish;
        }

        public void setFinish(CompletableFuture<Void> finish)
        {
            this.finish = requireNonNull(finish, "finish is null");
        }

        @Override
        public CompletableFuture<Void> abort()
        {
            assertFalse(abortCalled);
            abortCalled = true;
            return abort;
        }

        public void setAbort(CompletableFuture<Void> abort)
        {
            this.abort = requireNonNull(abort, "abort is null");
        }
    }

    private enum TestingExchangeSinkInstanceHandle
            implements ExchangeSinkInstanceHandle
    {
        INSTANCE
    }

    private static class TestingLocalMemoryContext
            implements LocalMemoryContext
    {
        @Override
        public long getBytes()
        {
            return 0;
        }

        @Override
        public ListenableFuture<Void> setBytes(long bytes)
        {
            return immediateVoidFuture();
        }

        @Override
        public boolean trySetBytes(long bytes)
        {
            return true;
        }

        @Override
        public void close()
        {
        }
    }
}

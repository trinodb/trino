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
package io.trino.split;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.BoundedExecutor;
import io.trino.split.SplitSource.SplitBatch;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.split.MockSplitSource.Action.FAIL;
import static io.trino.split.MockSplitSource.Action.FINISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.CONCURRENT)
public class TestBufferingSplitSource
{
    private final ExecutorService executorService = newCachedThreadPool(daemonThreadsNamed(TestBufferingSplitSource.class.getSimpleName() + "-%s"));
    private final Executor executor = new BoundedExecutor(executorService, 10);

    @AfterAll
    public void cleanup()
    {
        executorService.shutdown();
    }

    @Test
    public void testSlowSource()
    {
        MockSplitSource mockSource = new MockSplitSource()
                .setBatchSize(1)
                .increaseAvailableSplits(25)
                .atSplitCompletion(FINISH);
        try (SplitSource source = new BufferingSplitSource(mockSource, executor, 10)) {
            getFutureValue(getNextBatch(source, 20))
                    .assertSize(10)
                    .assertNoMoreSplits(false);
            getFutureValue(getNextBatch(source, 6))
                    .assertSize(6)
                    .assertNoMoreSplits(false);
            getFutureValue(getNextBatch(source, 20))
                    .assertSize(9)
                    .assertNoMoreSplits(true);
            assertThat(source.isFinished()).isTrue();
            assertThat(mockSource.getNextBatchInvocationCount()).isEqualTo(25);
        }
    }

    @Test
    public void testFastSource()
    {
        MockSplitSource mockSource = new MockSplitSource()
                .setBatchSize(11)
                .increaseAvailableSplits(22)
                .atSplitCompletion(FINISH);
        try (SplitSource source = new BufferingSplitSource(mockSource, executor, 10)) {
            getFutureValue(getNextBatch(source, 200))
                    .assertSize(11)
                    .assertNoMoreSplits(false);
            getFutureValue(getNextBatch(source, 200))
                    .assertSize(11)
                    .assertNoMoreSplits(true);
            assertThat(source.isFinished()).isTrue();
            assertThat(mockSource.getNextBatchInvocationCount()).isEqualTo(2);
        }
    }

    @Test
    public void testNoStackOverFlow()
    {
        MockSplitSource mockSource = new MockSplitSource()
                .setBatchSize(1)
                .increaseAvailableSplits(10000)
                .atSplitCompletion(FINISH);
        try (SplitSource source = new BufferingSplitSource(mockSource, executor, Integer.MAX_VALUE)) {
            while (!source.isFinished()) {
                getFutureValue(getNextBatch(source, 1000))
                        .assertSize(1000);
            }
        }
    }

    @Test
    public void testEmptySource()
    {
        MockSplitSource mockSource = new MockSplitSource()
                .setBatchSize(1)
                .atSplitCompletion(FINISH);
        try (SplitSource source = new BufferingSplitSource(mockSource, executor, 100)) {
            getFutureValue(getNextBatch(source, 200))
                    .assertSize(0)
                    .assertNoMoreSplits(true);
            assertThat(source.isFinished()).isTrue();
            assertThat(mockSource.getNextBatchInvocationCount()).isEqualTo(1);
        }
    }

    @Test
    public void testBlocked()
    {
        MockSplitSource mockSource = new MockSplitSource()
                .setBatchSize(1);
        try (SplitSource source = new BufferingSplitSource(mockSource, executor, 10)) {
            // Source has 0 out of 10 needed.
            ListenableFuture<NextBatchResult> nextBatchFuture = getNextBatch(source, 10);
            assertThat(nextBatchFuture.isDone()).isFalse();
            mockSource.increaseAvailableSplits(9);
            assertThat(nextBatchFuture.isDone()).isFalse();
            mockSource.increaseAvailableSplits(1);
            getFutureValue(nextBatchFuture)
                    .assertSize(10)
                    .assertNoMoreSplits(false);

            // Source is completed after getNextBatch invocation.
            nextBatchFuture = getNextBatch(source, 10);
            assertThat(nextBatchFuture.isDone()).isFalse();
            mockSource.atSplitCompletion(FINISH);
            getFutureValue(nextBatchFuture)
                    .assertSize(0)
                    .assertNoMoreSplits(true);
            assertThat(source.isFinished()).isTrue();
        }

        mockSource = new MockSplitSource()
                .setBatchSize(1);
        try (SplitSource source = new BufferingSplitSource(mockSource, executor, 10)) {
            // Source has 1 out of 10 needed.
            mockSource.increaseAvailableSplits(1);
            ListenableFuture<NextBatchResult> nextBatchFuture = getNextBatch(source, 10);
            assertThat(nextBatchFuture.isDone()).isFalse();
            mockSource.increaseAvailableSplits(9);
            getFutureValue(nextBatchFuture)
                    .assertSize(10)
                    .assertNoMoreSplits(false);

            // Source is completed with 5 last splits after getNextBatch invocation.
            nextBatchFuture = getNextBatch(source, 10);
            mockSource.increaseAvailableSplits(5);
            assertThat(nextBatchFuture.isDone()).isFalse();
            mockSource.atSplitCompletion(FINISH);
            getFutureValue(nextBatchFuture)
                    .assertSize(5)
                    .assertNoMoreSplits(true);
            assertThat(source.isFinished()).isTrue();
        }

        mockSource = new MockSplitSource()
                .setBatchSize(1);
        try (SplitSource source = new BufferingSplitSource(mockSource, executor, 10)) {
            // Source has 9 out of 10 needed.
            mockSource.increaseAvailableSplits(9);
            ListenableFuture<NextBatchResult> nextBatchFuture = getNextBatch(source, 10);
            assertThat(nextBatchFuture.isDone()).isFalse();
            mockSource.increaseAvailableSplits(1);
            getFutureValue(nextBatchFuture)
                    .assertSize(10)
                    .assertNoMoreSplits(false);

            // Source failed after getNextBatch invocation.
            nextBatchFuture = getNextBatch(source, 10);
            mockSource.increaseAvailableSplits(5);
            assertThat(nextBatchFuture.isDone()).isFalse();
            mockSource.atSplitCompletion(FAIL);
            assertFutureFailsWithMockFailure(nextBatchFuture);
            assertThat(source.isFinished()).isFalse();
        }

        // Fast source: source produce 8 before, and 8 after invocation. BufferedSource should return all 16 at once.
        mockSource = new MockSplitSource()
                .setBatchSize(8);
        try (SplitSource source = new BufferingSplitSource(mockSource, executor, 10)) {
            mockSource.increaseAvailableSplits(8);
            ListenableFuture<NextBatchResult> nextBatchFuture = getNextBatch(source, 20);
            assertThat(nextBatchFuture.isDone()).isFalse();
            mockSource.increaseAvailableSplits(8);
            getFutureValue(nextBatchFuture)
                    .assertSize(16)
                    .assertNoMoreSplits(false);
        }
    }

    @Test
    public void testFinishedSetWithoutIndicationFromSplitBatch()
    {
        MockSplitSource mockSource = new MockSplitSource()
                .setBatchSize(1)
                .increaseAvailableSplits(1);
        try (SplitSource source = new BufferingSplitSource(mockSource, executor, 100)) {
            getFutureValue(getNextBatch(source, 1))
                    .assertSize(1)
                    .assertNoMoreSplits(false);
            assertThat(source.isFinished()).isFalse();
            // Most of the time, mockSource.isFinished() returns the same value as
            // the SplitBatch.noMoreSplits field of the preceding mockSource.getNextBatch() call.
            // However, this is NOT always the case.
            // In this case, the preceding getNextBatch() indicates the noMoreSplits is false,
            // but the next isFinished call will return true.
            mockSource.atSplitCompletion(FINISH);
            getFutureValue(getNextBatch(source, 1))
                    .assertSize(0)
                    .assertNoMoreSplits(true);
            assertThat(source.isFinished()).isTrue();
            assertThat(mockSource.getNextBatchInvocationCount()).isEqualTo(2);
        }
    }

    @Test
    public void testFailImmediate()
    {
        MockSplitSource mockSource = new MockSplitSource()
                .setBatchSize(1)
                .atSplitCompletion(FAIL);
        try (SplitSource source = new BufferingSplitSource(mockSource, executor, 100)) {
            assertFutureFailsWithMockFailure(getNextBatch(source, 200));
            assertThat(mockSource.getNextBatchInvocationCount()).isEqualTo(1);
        }
    }

    @Test
    public void testFail()
    {
        MockSplitSource mockSource = new MockSplitSource()
                .setBatchSize(1)
                .increaseAvailableSplits(1)
                .atSplitCompletion(FAIL);
        try (SplitSource source = new BufferingSplitSource(mockSource, executor, 100)) {
            assertFutureFailsWithMockFailure(getNextBatch(source, 2));
            assertThat(mockSource.getNextBatchInvocationCount()).isEqualTo(2);
        }
    }

    private static void assertFutureFailsWithMockFailure(ListenableFuture<?> future)
    {
        assertThatThrownBy(future::get)
                .hasMessageContaining("Mock failure");
    }

    private static ListenableFuture<NextBatchResult> getNextBatch(SplitSource splitSource, int maxSize)
    {
        ListenableFuture<SplitBatch> future = splitSource.getNextBatch(maxSize);
        return Futures.transform(future, NextBatchResult::new, directExecutor());
    }

    private static class NextBatchResult
    {
        private final SplitBatch splitBatch;

        public NextBatchResult(SplitBatch splitBatch)
        {
            this.splitBatch = requireNonNull(splitBatch, "splitBatch is null");
        }

        public NextBatchResult assertSize(int expectedSize)
        {
            assertThat(expectedSize).isEqualTo(splitBatch.getSplits().size());
            return this;
        }

        @SuppressWarnings("UnusedReturnValue")
        public NextBatchResult assertNoMoreSplits(boolean expectedNoMoreSplits)
        {
            assertThat(expectedNoMoreSplits).isEqualTo(splitBatch.isLastBatch());
            return this;
        }
    }
}

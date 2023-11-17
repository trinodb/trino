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

package io.trino.plugin.hive.util;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestThrottledAsyncQueue
{
    private ExecutorService executor;

    @BeforeAll
    public void setUpClass()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("TestThrottledAsyncQueue-%s"));
    }

    @AfterAll
    public void tearDownClass()
    {
        executor.shutdownNow();
        executor = null;
    }

    @Test
    @Timeout(10)
    public void testThrottle()
    {
        // Make sure that the dequeuing is throttled even if we have enough elements in the queue

        ThrottledAsyncQueue<Integer> queue = new ThrottledAsyncQueue<>(3, 10, executor);
        assertThat(queue.offer(1).isDone()).isTrue();
        assertThat(queue.offer(2).isDone()).isTrue();
        assertThat(queue.offer(3).isDone()).isTrue();
        assertThat(queue.offer(4).isDone()).isTrue();
        assertThat(queue.offer(5).isDone()).isTrue();
        assertThat(queue.offer(6).isDone()).isTrue();
        queue.finish();

        // no throttling, enough elements in the queue
        ListenableFuture<List<Integer>> future1 = queue.getBatchAsync(2);
        assertThat(future1.isDone()).isTrue();
        assertThat(getFutureValue(future1)).isEqualTo(ImmutableList.of(1, 2));
        assertThat(queue.isFinished()).isFalse();

        // we can only dequeue one more element before being throttled
        ListenableFuture<List<Integer>> future2 = queue.getBatchAsync(2);
        assertThat(future2.isDone()).isFalse();
        assertThat(getFutureValue(future2)).isEqualTo(ImmutableList.of(3, 4));
        assertThat(queue.isFinished()).isFalse();

        // we are now throttled, this future will not be immediate
        ListenableFuture<List<Integer>> future3 = queue.getBatchAsync(2);
        assertThat(future3.isDone()).isFalse();
        assertThat(getFutureValue(future3)).isEqualTo(ImmutableList.of(5, 6));
        assertThat(queue.isFinished()).isTrue();
    }

    @Test
    @Timeout(10)
    public void testThrottleEmptyQueue()
            throws Exception
    {
        // Make sure that dequeuing is throttled if we dequeued enough elements before, even if it is empty.
        // The future should only complete once the queue becomes non-empty again.

        ThrottledAsyncQueue<Integer> queue = new ThrottledAsyncQueue<>(2, 10, executor);
        assertThat(queue.offer(1).isDone()).isTrue();
        assertThat(queue.offer(2).isDone()).isTrue();

        // no throttling, enough elements in the queue
        ListenableFuture<List<Integer>> future1 = queue.getBatchAsync(2);
        assertThat(future1.isDone()).isTrue();
        assertThat(getFutureValue(future1)).isEqualTo(ImmutableList.of(1, 2));
        assertThat(queue.isFinished()).isFalse();

        // we are now throttled and the queue is empty
        ListenableFuture<List<Integer>> future2 = queue.getBatchAsync(2);
        assertThat(future2.isDone()).isFalse();

        Thread.sleep(1000L); // wait one second, after which we should not be throttled any more

        // no batch is ready at that point as the queue is still empty
        assertThat(future2.isDone()).isFalse();

        assertThat(queue.offer(3).isDone()).isTrue();
        queue.finish();

        assertThat(getFutureValue(future2)).isEqualTo(ImmutableList.of(3));

        assertThat(queue.isFinished()).isTrue();
    }

    @Test
    @Timeout(10)
    public void testBorrowThrows()
            throws Exception
    {
        // It doesn't matter the exact behavior when the caller-supplied function to borrow fails.
        // However, it must not block pending futures.

        AsyncQueue<Integer> queue = new ThrottledAsyncQueue<>(100, 4, executor);
        queue.offer(1);
        queue.offer(2);
        queue.offer(3);
        queue.offer(4);
        queue.offer(5);

        ListenableFuture<Void> future1 = queue.offer(6);
        assertThat(future1.isDone()).isFalse();

        Runnable runnable = () -> {
            getFutureValue(queue.borrowBatchAsync(1, elements -> {
                throw new RuntimeException("test fail");
            }));
        };

        assertThatThrownBy(() -> executor.submit(runnable).get())
                .isInstanceOf(ExecutionException.class)
                .hasMessageContaining("test fail");

        ListenableFuture<Void> future2 = queue.offer(7);
        assertThat(future1.isDone()).isFalse();
        assertThat(future2.isDone()).isFalse();
        queue.finish();
        future1.get();
        future2.get();
        assertThat(queue.offer(8).isDone()).isTrue();

        assertThatThrownBy(() -> executor.submit(runnable).get())
                .isInstanceOf(ExecutionException.class)
                .hasMessageContaining("test fail");

        assertThat(queue.offer(9).isDone()).isTrue();

        assertThat(queue.isFinished()).isFalse();
        // 1 and 2 were removed by borrow call; 8 and 9 were never inserted because insertion happened after finish.
        assertThat(queue.getBatchAsync(100).get()).isEqualTo(ImmutableList.of(3, 4, 5, 6, 7));
        assertThat(queue.isFinished()).isTrue();
    }

    @Test
    @Timeout(10)
    public void testGetPartial()
            throws Exception
    {
        AsyncQueue<String> queue = new ThrottledAsyncQueue<>(100, 4, executor);

        queue.offer("1");
        queue.offer("2");
        queue.offer("3");
        assertThat(queue.getBatchAsync(100).get()).isEqualTo(ImmutableList.of("1", "2", "3"));

        queue.finish();
        assertThat(queue.isFinished()).isTrue();
    }

    @Test
    @Timeout(10)
    public void testFullQueue()
            throws Exception
    {
        AsyncQueue<String> queue = new ThrottledAsyncQueue<>(100, 4, executor);

        assertThat(queue.offer("1").isDone()).isTrue();
        assertThat(queue.offer("2").isDone()).isTrue();
        assertThat(queue.offer("3").isDone()).isTrue();

        assertThat(queue.offer("4").isDone()).isFalse();
        assertThat(queue.offer("5").isDone()).isFalse();
        ListenableFuture<Void> offerFuture = queue.offer("6");
        assertThat(offerFuture.isDone()).isFalse();

        assertThat(queue.getBatchAsync(2).get()).isEqualTo(ImmutableList.of("1", "2"));
        assertThat(offerFuture.isDone()).isFalse();

        assertThat(queue.getBatchAsync(1).get()).isEqualTo(ImmutableList.of("3"));
        offerFuture.get();

        offerFuture = queue.offer("7");
        assertThat(offerFuture.isDone()).isFalse();

        queue.finish();
        offerFuture.get();
        assertThat(queue.isFinished()).isFalse();
        assertThat(queue.getBatchAsync(4).get()).isEqualTo(ImmutableList.of("4", "5", "6", "7"));
        assertThat(queue.isFinished()).isTrue();
    }

    @Test
    @Timeout(10)
    public void testEmptyQueue()
            throws Exception
    {
        AsyncQueue<String> queue = new ThrottledAsyncQueue<>(100, 4, executor);

        assertThat(queue.offer("1").isDone()).isTrue();
        assertThat(queue.offer("2").isDone()).isTrue();
        assertThat(queue.offer("3").isDone()).isTrue();
        assertThat(queue.getBatchAsync(2).get()).isEqualTo(ImmutableList.of("1", "2"));
        assertThat(queue.getBatchAsync(2).get()).isEqualTo(ImmutableList.of("3"));
        ListenableFuture<List<String>> batchFuture = queue.getBatchAsync(2);
        assertThat(batchFuture.isDone()).isFalse();

        assertThat(queue.offer("4").isDone()).isTrue();
        assertThat(batchFuture.get()).isEqualTo(ImmutableList.of("4"));

        batchFuture = queue.getBatchAsync(2);
        assertThat(batchFuture.isDone()).isFalse();
        queue.finish();
        batchFuture.get();
        assertThat(queue.isFinished()).isTrue();
    }

    @Test
    @Timeout(10)
    public void testOfferAfterFinish()
            throws Exception
    {
        AsyncQueue<String> queue = new ThrottledAsyncQueue<>(100, 4, executor);

        assertThat(queue.offer("1").isDone()).isTrue();
        assertThat(queue.offer("2").isDone()).isTrue();
        assertThat(queue.offer("3").isDone()).isTrue();
        assertThat(queue.offer("4").isDone()).isFalse();

        queue.finish();
        assertThat(queue.offer("5").isDone()).isTrue();
        assertThat(queue.offer("6").isDone()).isTrue();
        assertThat(queue.offer("7").isDone()).isTrue();
        assertThat(queue.isFinished()).isFalse();

        assertThat(queue.getBatchAsync(100).get()).isEqualTo(ImmutableList.of("1", "2", "3", "4"));
        assertThat(queue.isFinished()).isTrue();
    }
}

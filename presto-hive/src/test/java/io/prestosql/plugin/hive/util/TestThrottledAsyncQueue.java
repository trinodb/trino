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

package io.prestosql.plugin.hive.util;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestThrottledAsyncQueue
{
    private ExecutorService executor;

    @BeforeClass
    public void setUpClass()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-async-queue-%s"));
    }

    @AfterClass(alwaysRun = true)
    public void tearDownClass()
    {
        executor.shutdownNow();
    }

    @Test(timeOut = 10_000)
    public void testThrottle()
    {
        // Make sure that the dequeuing is throttled even if we have enough elements in the queue

        ThrottledAsyncQueue<Integer> queue = new ThrottledAsyncQueue<>(3, 10, executor);
        assertTrue(queue.offer(1).isDone());
        assertTrue(queue.offer(2).isDone());
        assertTrue(queue.offer(3).isDone());
        assertTrue(queue.offer(4).isDone());
        assertTrue(queue.offer(5).isDone());
        assertTrue(queue.offer(6).isDone());
        queue.finish();

        // no throttling, enough elements in the queue
        ListenableFuture<List<Integer>> future1 = queue.getBatchAsync(2);
        assertTrue(future1.isDone());
        assertEquals(getFutureValue(future1), ImmutableList.of(1, 2));
        assertFalse(queue.isFinished());

        // we can only dequeue one more element before being throttled
        ListenableFuture<List<Integer>> future2 = queue.getBatchAsync(2);
        assertFalse(future2.isDone());
        assertEquals(getFutureValue(future2), ImmutableList.of(3, 4));
        assertFalse(queue.isFinished());

        // we are now throttled, this future will not be immediate
        ListenableFuture<List<Integer>> future3 = queue.getBatchAsync(2);
        assertFalse(future3.isDone());
        assertEquals(getFutureValue(future3), ImmutableList.of(5, 6));
        assertTrue(queue.isFinished());
    }

    @Test(timeOut = 10_000)
    public void testThrottleEmptyQueue()
            throws Exception
    {
        // Make sure that dequeuing is throttled if we dequeued enough elements before, even if it is empty.
        // The future should only complete once the queue becomes non-empty again.

        ThrottledAsyncQueue<Integer> queue = new ThrottledAsyncQueue<>(2, 10, executor);
        assertTrue(queue.offer(1).isDone());
        assertTrue(queue.offer(2).isDone());

        // no throttling, enough elements in the queue
        ListenableFuture<List<Integer>> future1 = queue.getBatchAsync(2);
        assertTrue(future1.isDone());
        assertEquals(getFutureValue(future1), ImmutableList.of(1, 2));
        assertFalse(queue.isFinished());

        // we are now throttled and the queue is empty
        ListenableFuture<List<Integer>> future2 = queue.getBatchAsync(2);
        assertFalse(future2.isDone());

        Thread.sleep(1000L); // wait one second, after which we should not be throttled any more

        // no batch is ready at that point as the queue is still empty
        assertFalse(future2.isDone());

        assertTrue(queue.offer(3).isDone());
        assertTrue(queue.offer(4).isDone());
        queue.finish();

        assertEquals(getFutureValue(future2), ImmutableList.of(3, 4));

        assertTrue(queue.isFinished());
    }

    @Test(timeOut = 10_000)
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

        ListenableFuture<?> future1 = queue.offer(6);
        assertFalse(future1.isDone());

        Runnable runnable = () -> {
            getFutureValue(queue.borrowBatchAsync(1, elements -> {
                throw new RuntimeException("test fail");
            }));
        };

        assertThatThrownBy(() -> executor.submit(runnable).get())
                .isInstanceOf(ExecutionException.class)
                .hasMessageContaining("test fail");

        ListenableFuture<?> future2 = queue.offer(7);
        assertFalse(future1.isDone());
        assertFalse(future2.isDone());
        queue.finish();
        future1.get();
        future2.get();
        assertTrue(queue.offer(8).isDone());

        assertThatThrownBy(() -> executor.submit(runnable).get())
                .isInstanceOf(ExecutionException.class)
                .hasMessageContaining("test fail");

        assertTrue(queue.offer(9).isDone());

        assertFalse(queue.isFinished());
        // 1 and 2 were removed by borrow call; 8 and 9 were never inserted because insertion happened after finish.
        assertEquals(queue.getBatchAsync(100).get(), ImmutableList.of(3, 4, 5, 6, 7));
        assertTrue(queue.isFinished());
    }

    @Test(timeOut = 10_000)
    public void testGetPartial()
            throws Exception
    {
        AsyncQueue<String> queue = new ThrottledAsyncQueue<>(100, 4, executor);

        queue.offer("1");
        queue.offer("2");
        queue.offer("3");
        assertEquals(queue.getBatchAsync(100).get(), ImmutableList.of("1", "2", "3"));

        queue.finish();
        assertTrue(queue.isFinished());
    }

    @Test(timeOut = 10_000)
    public void testFullQueue()
            throws Exception
    {
        AsyncQueue<String> queue = new ThrottledAsyncQueue<>(100, 4, executor);

        assertTrue(queue.offer("1").isDone());
        assertTrue(queue.offer("2").isDone());
        assertTrue(queue.offer("3").isDone());

        assertFalse(queue.offer("4").isDone());
        assertFalse(queue.offer("5").isDone());
        ListenableFuture<?> offerFuture = queue.offer("6");
        assertFalse(offerFuture.isDone());

        assertEquals(queue.getBatchAsync(2).get(), ImmutableList.of("1", "2"));
        assertFalse(offerFuture.isDone());

        assertEquals(queue.getBatchAsync(1).get(), ImmutableList.of("3"));
        offerFuture.get();

        offerFuture = queue.offer("7");
        assertFalse(offerFuture.isDone());

        queue.finish();
        offerFuture.get();
        assertFalse(queue.isFinished());
        assertEquals(queue.getBatchAsync(4).get(), ImmutableList.of("4", "5", "6", "7"));
        assertTrue(queue.isFinished());
    }

    @Test(timeOut = 10_000)
    public void testEmptyQueue()
            throws Exception
    {
        AsyncQueue<String> queue = new ThrottledAsyncQueue<>(100, 4, executor);

        assertTrue(queue.offer("1").isDone());
        assertTrue(queue.offer("2").isDone());
        assertTrue(queue.offer("3").isDone());
        assertEquals(queue.getBatchAsync(2).get(), ImmutableList.of("1", "2"));
        assertEquals(queue.getBatchAsync(2).get(), ImmutableList.of("3"));
        ListenableFuture<?> batchFuture = queue.getBatchAsync(2);
        assertFalse(batchFuture.isDone());

        assertTrue(queue.offer("4").isDone());
        assertEquals(batchFuture.get(), ImmutableList.of("4"));

        batchFuture = queue.getBatchAsync(2);
        assertFalse(batchFuture.isDone());
        queue.finish();
        batchFuture.get();
        assertTrue(queue.isFinished());
    }

    @Test(timeOut = 10_000)
    public void testOfferAfterFinish()
            throws Exception
    {
        AsyncQueue<String> queue = new ThrottledAsyncQueue<>(100, 4, executor);

        assertTrue(queue.offer("1").isDone());
        assertTrue(queue.offer("2").isDone());
        assertTrue(queue.offer("3").isDone());
        assertFalse(queue.offer("4").isDone());

        queue.finish();
        assertTrue(queue.offer("5").isDone());
        assertTrue(queue.offer("6").isDone());
        assertTrue(queue.offer("7").isDone());
        assertFalse(queue.isFinished());

        assertEquals(queue.getBatchAsync(100).get(), ImmutableList.of("1", "2", "3", "4"));
        assertTrue(queue.isFinished());
    }
}

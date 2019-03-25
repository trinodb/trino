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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.getFutureValue;

/**
 * An asynchronous queue that limits the rate at which batches will be made available as well as the number
 * of elements they will contain.
 *
 * @param <T>           The type of elements accepted by the queue.
 */
@ThreadSafe
public class ThrottledAsyncQueue<T>
        extends AsyncQueue<T>
{
    private final int maxBatchSizePerSec;
    private final ExecutorService throttleService;

    private ListenableFuture<Bucket> throttleSignal;

    public ThrottledAsyncQueue(int maxBatchSizePerSec, int targetQueueSize, Executor executor)
    {
        super(targetQueueSize, executor);
        this.throttleService = Executors.newSingleThreadExecutor();
        this.maxBatchSizePerSec = maxBatchSizePerSec;
        this.throttleSignal = immediateFuture(new Bucket(System.currentTimeMillis(), maxBatchSizePerSec));
    }

    @Override
    public synchronized <O> ListenableFuture<O> borrowBatchAsync(int maxSize, Function<List<T>, BorrowResult<T, O>> function)
    {
        /*
        We throttle the dequeue operation to make sure we don't make return more than `maxBatchSizePerSec` elements per
        second.
        We throttle in both size and time, for instance with maxBatchSizePerSec = 10:
        - size: if we de-queued 8 elements in less than a second, then we can still immediately get a batch but only of
                2 elements (as 8 + 2 = 10).
        - time: if we have de-queued 10 elements in the past second, we need to wait for the next second to start before
                we are allowed to get dequeue again.

        The throttling mechanism is the following:
                                   t                           t+1s                     t+2s
                                   |                            |                        |
        batch future:              | batch   batch  batch       |   batch       batch    |
                                   X-------->X----->X--->X      X----------->X---------->X
                                   |                    throttle|                        |
        throttle future:           X         X      X    X----->X           X            X
                                   |                            |                        |

        batch size:                |   4        4      2        |     5           4      |
        remain. before throttle:   | 10-4=6   6-4=2   0:throttle|     5           1      |
         */

        /*
        A bucket represents a throttling period (second). During a period, we can only poll at most `maxBatchSizePerSec`
        elements, after which we need to create a new bucket for the next period (second) and wait for it to begin.
        This future completes immediately if we polled less than `maxBatchSizePerSec` elements in the last second
        - OR -
        If during the last second we polled `maxBatchSizePerSec`, after a full second has elapsed, at which time
        we can reset the counter (create a new bucket) for the new second that starts.
         */
        throttleSignal = thenAsync(
                throttleSignal,
                currentBucket -> {
                    final long nextBucketMs = currentBucket.bucketMs + 1000L;
                    final long now = System.currentTimeMillis();

                    if (currentBucket.getRemainingSize() > 0) {
                        // there are still elements to dequeue in this bucket, do not throttle
                        return immediateFuture(currentBucket);
                    }
                    else if (nextBucketMs - now <= 0) {
                        // more than 1 second has elapsed, we create a new bucket with reset counters.
                        return immediateFuture(new Bucket(now, maxBatchSizePerSec));
                    }
                    else {
                        // wait until a second has elapsed since the creation of the last bucket and reset the counters.
                        return Futures.transform(
                                throttleSignal,
                                prevBucket -> {
                                    try {
                                        Thread.sleep(Math.max(0L, nextBucketMs - now));
                                    }
                                    catch (InterruptedException e) {
                                        throw new RuntimeException(e);
                                    }
                                    return new Bucket(nextBucketMs, maxBatchSizePerSec);
                                },
                                throttleService);
                    }
                });

        // after throttling we might still need for the queue to be not empty, otherwise it is too early to try to
        // compute the maximum size of the batch we can request (it would return 0).
        throttleSignal = thenAsync(
                throttleSignal,
                bucket -> {
                    if (maxSize > 0 && size() == 0) {
                        // in case the queue is not empty after we are done throttling, we need to wait for the queue to
                        // not be empty any more so that we can then query a batch.
                        return thenAsync(notEmptySignal, any -> immediateFuture(bucket));
                    }
                    return immediateFuture(bucket);
                });

        /*
        Once the future above completes we can then query for a new batch. Its size is the minimum between:
        1.  The number of elements that can be still polled during this second before throttling.
        2.  The size of the queue before polling.
        3.  The `maxSize` parameter passed by the caller.
         */
        final ListenableFuture<O> batchFuture = thenAsync(
                throttleSignal,
                bucket -> super.borrowBatchAsync(bucket.borrowAtMost(maxSize), function));

        // make sure the executor service is closed after the last batch
        onComplete(batchFuture,
                () -> {
                    if (isFinished()) {
                        throttleService.shutdownNow();
                    }
                });

        return batchFuture;
    }

    // chain an async operation after a future, run synchronously if the future is succeeded.
    private static <T, V> ListenableFuture<V> thenAsync(ListenableFuture<T> future,
                                                        Function<T, ListenableFuture<V>> function)
    {
        if (future.isDone()) {
            return function.apply(getFutureValue(future));
        }
        return Futures.transformAsync(future, function::apply, directExecutor());
    }

    // execute code after a future completes, or immediately if the future is completed
    private static <T> void onComplete(ListenableFuture<T> future, Runnable function)
    {
        if (future.isDone()) {
            function.run();
        }
        Futures.whenAllComplete(future).call(
                () -> {
                    function.run();
                    return null;
                },
                directExecutor());
    }

    // a Bucket is used to count the number of elements that can still be de-queued in the ongoing second before
    // the polling of the queue needs to be throttled.
    private class Bucket
    {
        private final long bucketMs;
        private int remainingSize;

        private Bucket(long bucketMs, int remainingSize)
        {
            this.bucketMs = bucketMs;
            this.remainingSize = remainingSize;
        }

        private synchronized int borrowAtMost(int maxSize)
        {
            // update the remaining bucket size atomically
            int max = Math.min(size(), maxSize);
            int batchSize = Math.min(max, remainingSize);
            remainingSize -= batchSize;
            return batchSize;
        }

        private synchronized int getRemainingSize()
        {
            return remainingSize;
        }
    }
}

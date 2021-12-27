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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.RateLimiter;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;

/**
 * An asynchronous queue that limits the rate at which batches will be
 * made available as well as the number of elements they will contain.
 *
 * @param <T> The type of elements accepted by the queue.
 */
@ThreadSafe
public class ThrottledAsyncQueue<T>
        extends AsyncQueue<T>
{
    private final int maxBatchSizePerSec;
    private final Executor executor;
    private final RateLimiter rateLimiter;

    public ThrottledAsyncQueue(int maxBatchSizePerSec, int targetQueueSize, Executor executor)
    {
        super(targetQueueSize, executor);
        this.executor = executor;
        this.maxBatchSizePerSec = maxBatchSizePerSec;
        this.rateLimiter = RateLimiter.create(maxBatchSizePerSec);
    }

    @Override
    public synchronized <O> ListenableFuture<O> borrowBatchAsync(int maxSize, Function<List<T>, BorrowResult<T, O>> function)
    {
        checkArgument(maxSize >= 0, "maxSize must be at least 0");

        ListenableFuture<Void> throttleFuture = immediateVoidFuture();
        if (size() > 0) {
            // the queue is not empty, try to return a batch immediately if we are not throttled
            int size = maxBatchSize(maxSize);
            if (rateLimiter.tryAcquire(size)) {
                return super.borrowBatchAsync(size, function);
            }
        }
        else if (!isFinished()) {
            // the queue is empty but not finished, wait before we can query a batch
            throttleFuture = getNotEmptySignal();
        }

        return Futures.transformAsync(
                throttleFuture,
                any -> {
                    int size = maxBatchSize(maxSize);
                    if (size > 0) {
                        rateLimiter.acquire(size);
                    }
                    return super.borrowBatchAsync(size, function);
                },
                executor);
    }

    private int maxBatchSize(int maxSize)
    {
        return Math.min(maxSize, Math.min(size(), maxBatchSizePerSec));
    }
}

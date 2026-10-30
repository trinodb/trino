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
package io.trino.plugin.iceberg.delete;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.AsyncSemaphore;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.newSequentialExecutor;
import static io.airlift.concurrent.MoreFutures.addExceptionCallback;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

final class BoundedLoader
{
    static final long ONE_MEGABYTE = 1024 * 1024;
    // Lower bound of the estimated memory of a load, so that loads that estimate close to zero do not allow an unbounded number of loads in progress
    static final long MIN_LOAD_BYTES = ONE_MEGABYTE / 2;
    // Upper bound of the estimated memory of all loads in progress, unless that would allow fewer than MIN_CONCURRENT_LOADS loads in progress
    static final long MAX_TOTAL_LOAD_BYTES = 24 * ONE_MEGABYTE;
    static final int MIN_CONCURRENT_LOADS = 8;
    static final int MAX_CONCURRENT_LOADS = toIntExact(MAX_TOTAL_LOAD_BYTES / MIN_LOAD_BYTES);

    private BoundedLoader() {}

    /**
     * Starts the loads in order such that the number of loads in progress is bounded.
     * A load is started as soon as a previous load in progress finishes.
     * The bound is derived so that the memory held by the loads in progress adds up to about
     * {@link #MAX_TOTAL_LOAD_BYTES}, based on the largest {@link PendingLoad#estimatedBytes}.
     * The returned future completes when all loads finished. If a load fails or the returned future is canceled,
     * the futures of the loads in progress are canceled and the loads that have not yet started never start.
     */
    static ListenableFuture<Void> loadAll(List<PendingLoad> loads)
    {
        if (loads.isEmpty()) {
            return immediateVoidFuture();
        }
        long largestEstimatedBytes = loads.stream()
                .mapToLong(PendingLoad::estimatedBytes)
                .max()
                .orElseThrow();
        int maxConcurrentLoads = toIntExact(min(loads.size(), max(MIN_CONCURRENT_LOADS, MAX_TOTAL_LOAD_BYTES / max(largestEstimatedBytes, MIN_LOAD_BYTES))));
        AtomicReference<Throwable> failure = new AtomicReference<>();
        ListenableFuture<List<Void>> result = AsyncSemaphore.processAll(
                loads,
                load -> {
                    Throwable recordedFailure = failure.get();
                    if (recordedFailure != null) {
                        return immediateFailedFuture(recordedFailure);
                    }
                    ListenableFuture<Void> started = Futures.transform(load.start().get(), _ -> null, directExecutor());
                    addExceptionCallback(started, throwable -> failure.compareAndSet(null, throwable));
                    return started;
                },
                maxConcurrentLoads,
                // Avoid directExecutor usage with AsyncSemaphore, risks stack overflow due to listener chaining.
                newSequentialExecutor(directExecutor()));
        return Futures.transform(result, _ -> null, directExecutor());
    }

    /**
     * A load that has not been started yet.
     *
     * @param estimatedBytes estimates the memory held by the load while it is in progress
     * @param start starts the load, and returns a future that completes when the load is finished
     */
    record PendingLoad(long estimatedBytes, Supplier<ListenableFuture<?>> start)
    {
        PendingLoad
        {
            requireNonNull(start, "start is null");
        }
    }
}
